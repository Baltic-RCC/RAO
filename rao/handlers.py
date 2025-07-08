import uuid
from datetime import datetime
from pika import BasicProperties
from io import BytesIO
import pandas as pd
import json
import pypowsybl
import config
from pathlib import Path
from common.object_storage import ObjectStorage
from common.config_parser import parse_app_properties
from common.decorators import performance_counter
from rao.crac.builder import CracBuilder
from rao.optimizer import Optimizer
from rao.loadflow_tool_settings import CGMES_IMPORT_PARAMETERS
from loguru import logger


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
    def handle(self, message: bytes, properties: dict, **kwargs):
        """
        Process received SAR profile
        """

        # Get metadata from properties
        self.scenario_timestamp = getattr(properties, 'headers').get('scenario_time', None)

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

        # Create CRAC service
        crac_service = CracBuilder(data=input_files_data, network=pd.read_RDF(network_object))
        crac_service.get_limits()  # get limits from model and store in CRAC service object

        # Group by contingency id
        # TODO assess performance and consider to avoid groupby and only iterator over unique contingencies
        for mrid, data in violations.groupby("ContingencyPowerFlowResult.Contingency"):

            logger.info(f"Processing contingency: {mrid} with {len(data)} violations")

            # Build CRAC for each contingency
            crac_file = crac_service.build_crac(contingency_ids=[mrid])

            # TODO for debugging - also we can store in minio
            with open("test_crac.json", "w") as f:
                json.dump(crac_file, f, ensure_ascii=False, indent=4)

            # Start the optimization
            crac_object = BytesIO(json.dumps(crac_file).encode('utf-8'))
            optimizer = Optimizer(network=self.network, crac=crac_object, debug=self.debug)
            optimizer.run()

            logger.info(f"Optimization finished for contingency: {mrid}")

            # Aggregate results
            logger.info(f"Post-processing results")
            if optimizer.results is None:
                logger.warning("Optimizer has no results to be processed")
                continue
            cnec_data = optimizer.cnec_results
            cost_data = optimizer.cost_results
            results = pd.concat([cnec_data, cost_data], ignore_index=True, sort=False)
            if results.empty:
                logger.warning("Cost and CNEC results are empty")
                continue

            # Send results to Elastic
            results = results.fillna("").to_dict(orient="records")
            logger.info(f"Sending optimization results to Elastic index: {ELASTIC_RESULTS_INDEX}")
            self.object_storage.elastic_service.send_to_elastic_bulk(
                index=ELASTIC_RESULTS_INDEX,
                json_message_list=results,
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
        "@scenario_timestamp": datetime(2025, 6, 2, 10, 30)
    }
    properties = BasicProperties(
        content_type='application/octet-stream',
        delivery_mode=2,
        priority=4,
        message_id=f"{uuid.uuid4()}",
        timestamp=1747208205,
        headers=headers,
    )
    with open(r"../test-data/SAR_20250609T1230_1D_1.xml", "rb") as file:
        file_bytes = file.read()

    # Create instance
    service = HandlerVirtualOperator()
    result = service.handle(message=file_bytes, properties=properties)

    # Test input data
    # contingencies = r"../test-data/TC1_contingencies.xml"
    # assessed_elements = r"../test-data/TC1_assessed_elements.xml"
    # remedial_actions = r"../test-data/TC1_remedial_actions.xml"
