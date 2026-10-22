from pika import BasicProperties
import uuid
import json
from datetime import datetime
import pandas as pd
from loguru import logger
from integrations.elastic import Elastic
from common.rdf_converter import convert_cim_rdf_to_json, normalize_cim_payload
from remedial_action_schedules.settings import SchedulesHandlerSettings

handler_settings = SchedulesHandlerSettings()


class HandlerRemedialActionScheduleToElastic:

    def __init__(self):
        self.elastic_service = Elastic()

    def handle(self, message: bytes, properties: BasicProperties,  **kwargs):

        # Convert message from NC to JSON
        data = convert_cim_rdf_to_json(rdfxml=message,
                                       root_class=["RemedialActionSchedule"],
                                       key_mode=handler_settings.converter_key_mode)

        # JSON normalize and transform to DataFrame
        df = normalize_cim_payload(payload=data, root_only=False)

        # TODO need to get CO and RA from object storage and merge

        # Convert to dictionary
        data_to_send = df.astype(object).where(pd.notna(df), None).to_dict("records")

        response = self.elastic_service.send_to_elastic_bulk(
            index=handler_settings.elastic_schedules_index,
            json_message_list=data_to_send,
        )

        logger.info(f"Message sending to Elastic successful: {response}")

        return message, properties


if __name__ == "__main__":
    # Define RMQ test message
    headers = {
        "baCorrelationID": f"{uuid.uuid4()}",
        "baseMessageID": f"{uuid.uuid4()}",
        "businessType": "CSA-INPUT",
        "messageID": f"{uuid.uuid4()}",
        "sendTimestamp": datetime.utcnow().isoformat(),
        "sender": "TSOX",
        "senderApplication": "APPX",
        "service": "INPUT-DATA",
    }

    properties = BasicProperties(
        content_type="application/octet-stream",
        delivery_mode=2,
        priority=4,
        message_id=f"{uuid.uuid4()}",
        timestamp=147728025,
        headers=headers,
    )

    with open(r"C:\Users\martynas.karobcikas\Downloads\ras-example.xml", "rb") as file:
        file_bytes = file.read()

    # Create instance
    service = HandlerRemedialActionScheduleToElastic()
    result = service.handle(message=file_bytes, properties=properties)
