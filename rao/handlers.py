import uuid
from datetime import datetime
from pika import BasicProperties
import config
from io import BytesIO
import pandas as pd
import json
import pypowsybl
from pathlib import Path
from common.object_storage import ObjectStorage
from integrations.elastic import Elastic
from common.config_parser import parse_app_properties
from rao.crac_builder import CracBuilder
from rao.crac.update_crac_limits_from_model import update_limits
from rao.optimizer import Optimizer
from rao.loadflow_tool_settings import CGMES_IMPORT_PARAMETERS
from loguru import logger


parse_app_properties(caller_globals=globals(), path=config.paths.object_storage.object_storage)


class HandlerVirtualOperator:

    def __init__(self):
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

    def handle(self, message: bytes, properties: dict, **kwargs):
        """
        Process received SAR profile
        """

        # Get metadata from properties
        self.scenario_timestamp = getattr(properties, 'headers').get('scenario_time', None)

        # Store SAR to BytesIO object
        sar = BytesIO(message)
        sar.name = f"{getattr(properties, 'headers').get('project_name', 'undefined')}.xml"

        # Get other input data from object storage
        input_file_objects = self.get_input_profiles()

        # Get network model from object storage
        content_reference = properties.headers.get('content_reference', None)
        if not content_reference:
            logger.error(f"RMQ message does not have content reference in headers")
            return
        network_model = self.get_network_model(content_reference=content_reference)
        network = pypowsybl.network.load_from_binary_buffer(
            buffer=network_model,
            parameters=CGMES_IMPORT_PARAMETERS
        )

        # Load input files to triplets
        data = pd.read_RDF([sar] + input_file_objects)
        for key, value in data.types_dict().items():
            logger.debug(f"Loaded objects: {value} {key}")

        # Get all violations from SAR profile
        violations = data.key_tableview("PowerFlowResult.isViolation")
        violations = violations[violations['PowerFlowResult.isViolation'] == 'true']
        if violations.empty:
            logger.warning("No violations found in SAR profile, exiting CRAC building process")
            return None

        # Group by contingency id
        # TODO assess performance and consider to avoid groupby and only iterator over unique contingencies
        crac_service = CracBuilder(data=data)
        for mrid, data in violations.groupby("ContingencyPowerFlowResult.Contingency"):
            logger.info(f"Processing contingency: {mrid} with {len(data)} violations")
            # Build CRAC for each contingency
            crac_file = crac_service.build_crac(contingency_ids=[mrid])

            # Update CRAC file with limits from network model
            modified_crac = update_limits(models=network_model, crac_to_update=crac_file)

            # Start the optimization
            crac_object = BytesIO(json.dumps(modified_crac).encode('utf-8'))
            optimizer = Optimizer(network=network, crac=crac_object)
            optimizer.run()

        logger.info(f"Finished")


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
