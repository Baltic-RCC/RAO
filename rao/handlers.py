import uuid
from datetime import datetime, timezone
from textwrap import indent

from pika import BasicProperties
from io import BytesIO
import pandas as pd
import json
import pypowsybl
from typing_extensions import override

import config
import re
from pathlib import Path
from common.object_storage import ObjectStorage
from common.config_parser import parse_app_properties
from common.decorators import performance_counter
from rao.crac.builder import CracBuilder
from rao.optimizer import Optimizer
from rao.loadflow_tool_settings import CGMES_IMPORT_PARAMETERS
from loguru import logger
from rao.params_utils import ParameterOverride
from copy import deepcopy
from typing import Dict


parse_app_properties(caller_globals=globals(), path=config.paths.object_storage.object_storage)
parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("config.properties")),
                     section="HANDLER",
                     eval_types=True)


class HandlerVirtualOperator:

    def __init__(self, current_violations_only: bool = OPTIMIZE_ONLY_CURRENT_VIOLATIONS, debug: bool = DEBUG):

        self.current_violations_only = current_violations_only
        self.debug = debug
        self.network = None

        # Services initialization
        try:
            self.object_storage = ObjectStorage()
        except Exception as e:
            logger.error(f"Failed to initialize ObjectStorage service: {e}")

        # Metadata
        self.scenario_timestamp = None
        self.network_model_meta = None
        self.crac = None

    def get_input_profiles(self):
        content = self.object_storage.get_latest_input_data(type_keyword=["CO", "AE", "RA"],
                                                            scenario_timestamp=self.scenario_timestamp)

        return content

    def get_network_model(self, content_reference: str):
        # Query merge reports
        metadata = {'content_reference': content_reference}
        self.network_model_meta = self.object_storage.query(metadata_query=metadata, index=ELASTIC_MODELS_INDEX)[0]
        content = self.object_storage.get_content(metadata=self.network_model_meta, bucket_name=S3_BUCKET_IN_MODELS)

        return content

    @performance_counter(units='seconds')
    def post_process_results(self, results: pd.DataFrame):

        # Separate actions from CNEC results
        _cols_to_pop = ["networkActionResults", "rangeActionResults"]
        actions = results[_cols_to_pop]
        results = results.drop(columns=_cols_to_pop)

        # Transform dataframe from wide format to long by results type using melt
        _cols_to_melt = ["flowCnecResults", "angleCnecResults", "voltageCnecResults"]
        results = results.melt(id_vars=[col for col in results.columns if col not in _cols_to_melt],
                               value_vars=_cols_to_melt,
                               var_name='cnecResultsType',
                               value_name='cnecResults')

        # Drop CNEC result types where it is empty
        results = results.explode(column=["cnecResults"]).dropna(subset=["cnecResults"])
        results = pd.json_normalize(results.to_dict("records"))

        # Map CNEC data
        cnec_df = pd.DataFrame(self.crac['flowCnecs'])
        cnec_df.columns = [f"cnec.{col}" for col in cnec_df.columns]
        results = results.merge(cnec_df, how='left', left_on='cnecResults.flowCnecId', right_on='cnec.id').drop(columns='cnec.id')

        # Map contingency data
        contingency_df = pd.DataFrame(self.crac['contingencies'])
        if not contingency_df.empty:
            contingency_df.columns = [f"contingency.{col}" for col in contingency_df.columns]
            results = results.merge(contingency_df,
                                    how='left',
                                    left_on='cnec.contingencyId',
                                    right_on='contingency.id').drop(columns='contingency.id')

        # Normalize thresholds
        results = pd.json_normalize(results.explode("cnec.thresholds").to_dict('records'))

        # Explode and flatten network actions
        ## Check if there are any actions received from optimizer
        _optimized_actions_flag = bool(actions.apply(lambda col: col.map(lambda x: x != [])).values.any())
        if _optimized_actions_flag:
            actions = pd.json_normalize(actions['networkActionResults'].explode())
            actions = pd.json_normalize(actions.explode("activatedStates").to_dict("records"))

            # Combine dataframes
            results = results.merge(actions,
                                    how='left',
                                    left_on=["cnec.instant", "cnec.contingencyId"],
                                    right_on=["activatedStates.instant", "activatedStates.contingency"])

            # Map network action data
            action_df = pd.DataFrame(self.crac['networkActions'])
            if not action_df.empty:
                action_df.columns = [f"action.{col}" for col in action_df.columns]
                results = results.merge(action_df,
                                        how='left',
                                        left_on='networkActionId',
                                        right_on='action.id').drop(columns='action.id')

        # TODO - explode by optimized network actions
        # results = results.explode("action.terminalsConnectionActions")

        return results

    @performance_counter(units='seconds')
    def handle(self, message: bytes, properties: object, **kwargs):
        """
        Process received SAR profile
        """
        # Get unique x-message-id from headers, if not there - create
        message_id = properties.headers.get('x-message-id', str(uuid.uuid4()))
        if getattr(config.initialize_logging, 'elastic_handler', None):
            config.initialize_logging.elastic_handler.extra.update({
                'x-message-id': message_id,
                'x-source-module': properties.headers.get('x-source-module', 'unknown')
            })
        logger.info(f"Handling message with id: {message_id}")

        # Get metadata from properties
        self.scenario_timestamp = getattr(properties, 'headers').get('scenario_time', datetime.now(timezone.utc))
        if isinstance(self.scenario_timestamp, str):
            self.scenario_timestamp = datetime.fromisoformat(self.scenario_timestamp)

        # Store SAR to BytesIO object and load to triplets to scan violations
        sar = BytesIO(message)
        sar.name = f"{getattr(properties, 'headers').get('project_name', 'undefined')}.xml"
        logger.info(f"Loading received SAR profile")
        sar_data = pd.read_RDF([sar])
        for key, value in sar_data.types_dict().items():
            logger.debug(f"Loaded objects: {value} {key}")

        # Get all violations from SAR profile
        violations = sar_data.key_tableview("PowerFlowResult.isViolation")
        violations = violations[violations['PowerFlowResult.isViolation'] == 'true']

        # Filter to current violations only if defined by configuration
        if self.current_violations_only:
            if 'PowerFlowResult.valueA' in violations.columns:
                violations = violations[violations['PowerFlowResult.valueA'].notna()]
            else:
                violations = pd.DataFrame()

        # Exit if there is no relevant violations
        if violations.empty:
            logger.warning("No violations found in SAR profile, exiting VirtualOperator process")
            return message, properties

        # Get other input data from object storage
        input_file_objects = self.get_input_profiles()

        # Load input files and SAR to triplets
        logger.info(f"Loading additional input data")
        input_files_data = pd.read_RDF(input_file_objects)
        for key, value in input_files_data.types_dict().items():
            logger.debug(f"Loaded objects: {value} {key}")

        # Get network model from object storage
        content_reference = properties.headers.get('content_reference', None)
        if not content_reference:
            logger.error(f"RMQ message does not have content reference in headers")
            return message, properties
        network_object = self.get_network_model(content_reference=content_reference)
        logger.info(f"Loading network model to pypowsybl")
        self.network = pypowsybl.network.load_from_binary_buffer(buffer=network_object,
                                                                 parameters=CGMES_IMPORT_PARAMETERS)

        # Get original parameter file path
        parameters_path = Path(__file__).parent / "parameters_v30.json"
        override_path = None

        # If time horizon is ID, create modified temporary parameter file
        time_horizon = self.network_model_meta.get("@time_horizon")

        if time_horizon == "ID":
            keys_path = [
                "extensions",
                "open-rao-search-tree-parameters",
                "topological-actions-optimization",
                "max-curative-search-tree-depth"
            ]
            override_context = ParameterOverride(parameters_path, keys_path, new_value=1)
            override_path = override_context.__enter__()

        # Create CRAC service
        crac_service = CracBuilder(data=input_files_data, network=pd.read_RDF(network_object))
        crac_service.get_limits()  # get limits from model and store in CRAC service object

        # Group by contingency id
        # TODO assess performance and consider to avoid groupby and only iterator over unique contingencies
        for mrid, data in violations.groupby("ContingencyPowerFlowResult.Contingency"):

            logger.info(f"Processing contingency: {mrid} with {len(data)} violations")

            # Build CRAC for each contingency
            self.crac = crac_service.build_crac(contingency_ids=[mrid])

            # For debugging
            with open("test-crac.json", "w") as f:
                json.dump(self.crac, f, ensure_ascii=False, indent=4)

            # Store built CRAC files in S3 storage
            crac_object = BytesIO(json.dumps(self.crac).encode('utf-8'))
            crac_object.name = f"RAO/CRAC_{properties.headers['time_horizon']}_{self.scenario_timestamp:%Y%m%dT%H%M}_CO_{mrid}.json"
            self.object_storage.s3_service.upload_object(file_path_or_file_object=crac_object,
                                                         bucket_name=S3_BUCKET_RESULTS,
                                                         metadata=properties.headers)

            # Start the optimization
            print(f"Using param path: {override_path or parameters_path}")
            optimizer = Optimizer(network=self.network, crac=crac_object, debug=self.debug, parameters_path=str(override_path or parameters_path))
            optimizer.run()

            logger.info(f"Optimization finished for contingency: {mrid}")

            # Check optimizer results
            if optimizer.results is None:
                logger.warning("Optimizer has no results to be processed")
                continue

            # Serialize results to json
            results = optimizer.results.to_json()
            if not results['networkActionResults'] and not results['rangeActionResults']:
                logger.warning(f"No possible actions proposed by optimizer")

            # Post-process optimizer results
            logger.info(f"Post-processing results")
            results = self.post_process_results(results=pd.json_normalize(results))

            # Logging status of successful optimization process for contingency
            logger.success(f"Optimization successful for contingency {mrid}")

            # Include message properties as meta
            results['rmq'] = [properties.headers] * len(results)

            # Delete the temporary parameters file if one was created
            if override_context:
                override_context.__exit__(None, None, None)

            # Send results to Elastic
            data_to_send = results.astype(object).where(pd.notna(results), None).to_dict("records")
            logger.info(f"Sending optimization results to Elastic index: {ELASTIC_RESULTS_INDEX}")
            self.object_storage.elastic_service.send_to_elastic_bulk(
                index=ELASTIC_RESULTS_INDEX,
                json_message_list=data_to_send,
            )

        logger.info(f"Message handling completed successfully")

        return message, properties


if __name__ == '__main__':
    # Define RMQ test message
    headers = {
        "baCorrelationID": f"{uuid.uuid4()}",
        "baMessageID": f"{uuid.uuid4()}",
        "businessType": "CSA-INPUT",
        "messageID": f"{uuid.uuid4()}",
        "sendTimestamp": datetime.utcnow().isoformat(),
        "sender": "TSOX",
        "senderApplication": "APPX",
        "service": "INPUT-DATA",
        "scenario_time": datetime(2025, 7, 10, 10, 30),
        "time_horizon": "ID",
        "content_reference": "EMFOS/RMM/RMM_20_001_20250709T1730Z_BA_9ac94769-6d91-4eee-9e87-9ba4144e657c.zip",
    }
    properties = BasicProperties(
        content_type='application/octet-stream',
        delivery_mode=2,
        priority=4,
        message_id=f"{uuid.uuid4()}",
        timestamp=1747208205,
        headers=headers,
    )
    with open(r"C:\Users\lukas.navickas\Documents\test_data_rao\SAR_20250709T1830_ID_1.xml", "rb") as file:
        file_bytes = file.read()

    # Create instance
    service = HandlerVirtualOperator()
    result = service.handle(message=file_bytes, properties=properties)

    # Test input data
    # contingencies = r"../test-data/TC1_contingencies.xml"
    # assessed_elements = r"../test-data/TC1_assessed_elements.xml"
    # remedial_actions = r"../test-data/TC1_remedial_actions.xml"
