import logging
import triplets
from pika import BasicProperties
import uuid
from datetime import datetime
from io import BytesIO
import config
from rao.integrations.s3_storage import S3Minio
from rao.integrations.elastic import Elastic
from rao.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.object_storage.object_storage)


class HandlerMetadataToObjectStorage:

    def __init__(self):
        self.s3_service = S3Minio()
        self.elastic_service = Elastic()

    def handle(self, message: bytes, properties: BasicProperties,  **kwargs):

        # Store body content in BytesIO
        content = BytesIO(message)
        content.name = f"{headers['baMessageID']}.xml"

        # Load content to triplestore
        data = triplets.rdf_parser.load_RDF_to_dataframe(content)

        # Extract header metadata
        metadata_header = data.type_tableview('FullModel').to_dict('records')[0]

        # Combine message delivery properties and content metadata to single object
        metadata_object = {
            "rmq": {"headers": properties.headers}
        }
        metadata_object.update(metadata_header)

        # Upload payload to S3 storage
        ## Update content object name
        _keyword = metadata_object.get("keyword", "UNDEFINED")
        _publisher = metadata_object.get("publisher", "UNDEFINED")
        if _publisher != "UNDEFINED":
            _publisher = _publisher.split("/")[-1]
        _start_date = metadata_object.get("startDate", "UNDEFINED")
        _end_date = metadata_object.get("endDate", "UNDEFINED")
        content.name = f"{S3_BUCKET_OUT_PREFIX}/{_keyword}_{_publisher}_{_start_date}_{_end_date}.xml"
        self.s3_service.upload_object(
            file_path_or_file_object=content,
            bucket_name=S3_BUCKET_OUT,
            metadata=properties.headers,
        )

        # Send metadata object to Elastic
        metadata_object["content-bucket"] = S3_BUCKET_OUT
        metadata_object["content-reference"] = content.name
        self.elastic_service.send_to_elastic(
            index=ELASTIC_INDEX,
            json_message=metadata_object,
            id=metadata_object.get('identifier', None)
        )

        # logger.info(f"Message sending to Elastic successful: {response}")

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
    }
    properties = BasicProperties(
        content_type='application/octet-stream',
        delivery_mode=2,
        priority=4,
        message_id=f"{uuid.uuid4()}",
        timestamp=1747208205,
        headers=headers,
    )
    with open(r"C:\Users\martynas.karobcikas\Downloads\rcc-test-upload.xml", "rb") as file:
        file_bytes = file.read()

    # Create instance
    service = HandlerMetadataToObjectStorage()
    result = service.handle(message=file_bytes, properties=properties)
