import triplets
from pika import BasicProperties
import uuid
from datetime import datetime
import pandas as pd
from io import BytesIO
import config
from pathlib import Path
from loguru import logger
from integrations.s3_storage import S3Minio
from integrations.elastic import Elastic
from common.rdf_converter import convert_cim_rdf_to_json, normalize_cim_payload
from common.config_parser import parse_app_properties

parse_app_properties(caller_globals=globals(),
                     path=config.paths.object_storage.object_storage)
parse_app_properties(caller_globals=globals(),
                     path=str(Path(__file__).parent.joinpath("config.properties")),
                     section="HANDLER",
                     eval_types=True)


class HandlerMetadataToObjectStorage:

    def __init__(self):
        self.s3_service = S3Minio()
        self.elastic_service = Elastic()

    def handle(self, message: bytes, properties: BasicProperties,  **kwargs):

        # Store body content in BytesIO
        content = BytesIO(message)
        content.name = f"{properties.headers['messageID']}.xml"

        # Load content to triplestore
        data = triplets.rdf_parser.load_RDF_to_dataframe(content)

        # Extract header metadata
        metadata_header = data.type_tableview('FullModel').to_dict('records')[0]

        # Combine message delivery properties and content metadata to single object
        metadata_object = {
            "rmq": {"headers": properties.headers}
        }
        metadata_object.update(metadata_header)

        # Store profile keyword in message properties for further handlers
        properties.headers["keyword"] = metadata_header.get("keyword", "UNDEFINED")

        # Upload payload to S3 storage
        ## Update content object name
        _keyword = metadata_object.get("keyword", "UNDEFINED")
        _version = metadata_object.get("Model.version", "UNDEFINED")
        _publisher = metadata_object.get("publisher", "UNDEFINED")
        if _publisher != "UNDEFINED":
            _publisher = _publisher.split("/")[-1]
        _start_date = metadata_object.get("startDate", "UNDEFINED")
        _end_date = metadata_object.get("endDate", "UNDEFINED")
        ## Check for empty headers from RMQ message headers and remove them
        headers_to_check = metadata_object["rmq"]["headers"]
        for key in list(headers_to_check.keys()):
            if headers_to_check[key] == "":
                del headers_to_check[key]

        content.name = f"{S3_BUCKET_OUT_PREFIX}/{_keyword}_{_version}_{_publisher}_{_start_date}_{_end_date}.xml"
        self.s3_service.upload_object(
            file_path_or_file_object=content,
            bucket_name=S3_BUCKET_OUT,
            metadata=properties.headers,
        )

        # Send metadata object to Elastic
        metadata_object["content_bucket"] = S3_BUCKET_OUT
        metadata_object["content_reference"] = content.name
        self.elastic_service.send_to_elastic(
            index=ELASTIC_METADATA_INDEX,
            json_message=metadata_object,
            id=metadata_object.get('identifier', None)
        )

        # logger.info(f"Message sending to Elastic successful: {response}")

        return message, properties


class HandlerInputDataToElastic:

    KEYWORD_MAP = {
        "CO": {"root_class": ["OrdinaryContingency", "ExceptionalContingency", "OutOfRangeContingency"], "index": ELASTIC_CONTINGENCIES_INDEX},
        "AE": {"root_class": ["AssessedElement"], "index": ELASTIC_ASSESSED_ELEMENTS_INDEX},
        "RA": {"root_class": ["GridStateAlterationRemedialAction"], "index": ELASTIC_REMEDIAL_ACTIONS_INDEX},
    }

    def __init__(self):
        self.elastic_service = Elastic()

    def handle(self, message: bytes, properties: BasicProperties,  **kwargs):

        # Get profile keyword
        keyword = properties.headers.get("keyword", None)
        if not keyword or keyword == 'UNDEFINED':
            logger.error(f"RMQ message does not have profile 'keyword' in headers")
            return message, properties

        # Convert message from NC to JSON
        data = convert_cim_rdf_to_json(rdfxml=message,
                                       root_class=self.KEYWORD_MAP[keyword]["root_class"],
                                       key_mode=CONVERTER_KEY_MODE)

        # JSON normalize and transform to DataFrame
        df = normalize_cim_payload(payload=data, root_only=True)

        # Convert to dictionary
        data_to_send = df.astype(object).where(pd.notna(df), None).to_dict("records")

        # Send to Elastic
        _index = self.KEYWORD_MAP[keyword]["index"]
        logger.info(f"Sending data to index: {_index}")
        response = self.elastic_service.send_to_elastic_bulk(
            index=_index,
            json_message_list=data_to_send,
        )

        logger.info(f"Message sending to Elastic successful: {response}")

        return message, properties


if __name__ == '__main__':
    # Define RMQ test message
    headers = {
        "baCorrelationID": f"{uuid.uuid4()}",
        "baMessageID": f"{uuid.uuid4()}",
        "businessType": "CSA-INPUT",
        "messageID": f"{uuid.uuid4()}",
        "sendTimestamp": datetime.utcnow().isoformat(),
        "sender": "",
        "senderApplication": "",
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
    with open(r"C:\Users\lukas.navickas\Downloads\1222_23_RA_TEST_FOR_MINIO_IGNORE.xml", "rb") as file:
        file_bytes = file.read()

    # Create instance
    service = HandlerMetadataToObjectStorage()
    result = service.handle(message=file_bytes, properties=properties)
