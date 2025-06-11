import uuid
from datetime import datetime
from pika import BasicProperties
import config
from io import BytesIO
from loguru import logger
import pandas as pd
from common.object_storage import ObjectStorage
from integrations.elastic import Elastic
from common.config_parser import parse_app_properties
from rao.crac_builder import CracBuilder

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

    def get_input_profiles(self):
        content = self.object_storage.get_latest_data(type_keyword=["CO", "AE", "RA"],
                                                      scenario_timestamp=self.scenario_timestamp)

        return content

    def network_model(self):
        pass

    def handle(self, message: bytes, properties: dict, **kwargs):
        """
        Process received SAR profile
        """

        # Get metadata from properties
        self.scenario_timestamp = properties.headers.get("@scenario_timestamp", None)

        # Store SAR to BytesIO object
        sar = BytesIO(message)
        sar.name = f"{properties.headers['messageID']}.xml"

        # Get other input data from object storage
        input_file_objects = self.get_input_profiles()

        # Get network model from object storage
        pass

        # Load input files to triplets
        file_to_load = [sar, input_file_objects]
        data = pd.read_RDF(file_to_load)

        for key, value in data.types_dict().items():
            logger.info(f"Loaded objects: {value} {key}")  # TODO might be changes to debug

        # Get all violations from SAR profile
        violations = self.data.key_tableview("PowerFlowResult.isViolation")
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
            crac_file = crac_service.build_crac()

            # TODO update CRAC with limits from model

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
