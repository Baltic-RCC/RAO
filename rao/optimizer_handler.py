import uuid
import json
from io import BytesIO
from pathlib import Path
import pypowsybl
from loguru import logger
import config
from common.object_storage import ObjectStorage
from common.config_parser import parse_app_properties
from common.decorators import performance_counter
from rao.optimizer import Optimizer
from rao.parameters.manager import RaoSettingsManager, LoadflowSettingsManager
from rao.parameters.profiles import RAO_PROFILES
from rao.results import publish_results

parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("config.properties")),
                     section="OPTIMIZER_HANDLER",
                     eval_types=True)


class HandlerOptimizer:
    """Run the RAO for a single contingency task published by HandlerVirtualOperator."""

    def __init__(self, debug: bool = DEBUG):
        self.debug = debug
        self.object_storage = ObjectStorage()

    def _download(self, bucket: str, reference: str) -> BytesIO:
        content = self.object_storage.s3_service.download_object(bucket_name=bucket, object_name=reference)
        if content is None:
            raise FileNotFoundError(f"Object not found in bucket {bucket}: {reference}")
        buffer = BytesIO(content)
        buffer.name = reference
        return buffer

    def optimize(self, network, crac_object: BytesIO, loadflow_parameters):
        """Run RAO with each profile in order and return the first successful attempt.

        If all profiles fail, the last attempt is returned so its failure status still reaches Elastic.
        """
        optimizer, profile = None, None
        for profile in RAO_PROFILES:
            settings = RaoSettingsManager()
            settings.set(profile["overrides"])
            logger.info(f"Running optimization with RAO profile: {profile['name']}")

            optimizer = Optimizer(network=network,
                                  crac_source=crac_object,
                                  parameters_source=settings.to_bytesio(),
                                  loadflow_parameters=loadflow_parameters,
                                  debug=self.debug)
            optimizer.run()

            if optimizer.results is not None and optimizer.results.to_json()['computationStatus'] != 'failure':
                logger.info(f"Optimization succeeded with RAO profile: {profile['name']}")
                return optimizer, profile, settings
            logger.warning(f"Optimization failed with RAO profile: {profile['name']}")

        return optimizer, profile, settings

    @performance_counter(units='seconds')
    def handle(self, message: bytes, properties: object, **kwargs):
        headers = properties.headers
        task = json.loads(message)
        contingency_id = task['contingency_id']

        message_id = headers.get('message-id', str(uuid.uuid4()))
        if getattr(config.initialize_logging, 'elastic_handler', None):
            config.initialize_logging.elastic_handler.extra.update({
                'message-id': message_id,
                'batch-id': headers.get('batch-id'),
                'contingency-id': contingency_id,
            })
        logger.info(f"Optimizing contingency: {contingency_id} (batch {headers.get('batch-id')})")

        # Load prepared network and CRAC built by the preparer
        network = pypowsybl.network.load_from_binary_buffer(
            buffer=self._download(task['bucket'], task['network_reference']))
        crac_object = self._download(task['bucket'], task['crac_reference'])
        crac = json.loads(crac_object.getvalue())

        lf_settings_manager = LoadflowSettingsManager(
            elastic_server=self.object_storage.elastic_service.server,
            elastic_api_key=self.object_storage.elastic_service.api_key,
            settings_keyword=task['loadflow_settings'],
        )

        optimizer, profile, settings = self.optimize(
            network=network,
            crac_object=crac_object,
            loadflow_parameters=lf_settings_manager.build_pypowsybl_parameters())

        if optimizer.results is None:
            logger.error(f"Optimizer has no results for contingency: {contingency_id}")
            return message, properties

        rao_results = optimizer.results.to_json()
        if not rao_results['networkActionResults'] and not rao_results['rangeActionResults']:
            logger.warning(f"No possible actions proposed by optimizer")
        for optimized_action in rao_results['networkActionResults']:
            logger.info(f"Optimized network action: {optimized_action}")
        for optimized_action in rao_results['rangeActionResults']:
            logger.info(f"Optimized range action: {optimized_action}")

        publish_results(rao_results=rao_results,
                        crac=crac,
                        network=network,
                        voltage_monitoring_results=optimizer.voltage_monitoring_results,
                        headers=headers,
                        violation_terminals=task['violation_terminals'],
                        settings={"objective-function": settings.get("objective-function.type"),
                                  "profile": profile['name']},
                        elastic_service=self.object_storage.elastic_service,
                        results_index=ELASTIC_RESULTS_INDEX,
                        voltage_results_index=ELASTIC_VOLTAGE_RESULTS_INDEX)

        logger.success(f"Optimization handling completed for contingency: {contingency_id}")

        return message, properties
