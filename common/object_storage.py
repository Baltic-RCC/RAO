import datetime
import pandas as pd
import sys
import config
from typing import List, Dict
from io import BytesIO
from loguru import logger
from common.config_parser import parse_app_properties
from integrations.s3_storage import S3Minio
from integrations.elastic import Elastic


parse_app_properties(caller_globals=globals(), path=config.paths.object_storage.object_storage)


class ObjectStorage:

    def __init__(self):
        self.s3_service = S3Minio()
        self.elastic_service = Elastic()

    @staticmethod
    def flatten_dict(d):
        result = {}
        for k, v in d.items():
            if isinstance(v, dict):
                result.update(v)
            else:
                result[k] = v
        return result

    def query(self,
              metadata_query: Dict,
              range_query: List[Dict] | None = None,
              index: str = ELASTIC_METADATA_INDEX,
              return_payload: bool = False,
              size: str = '10000',
              sort: Dict | None = None,
              scroll: str = '1m',
              ):

        # Create elastic query syntax
        # {
        #     "bool": {
        #         "must": [
        #             {"match": {"key": "CO"}},
        #             {"terms": {"key": ["01", "02"]}}
        #         ]
        #     }
        # }

        # Validate index definition to be able to search all index by pattern
        if "*" not in index:
            index = f"{index}*"

        # Defined types of query keys of metadata query
        bool_must_list = []
        for key, value in metadata_query.items():
            if isinstance(value, list):
                bool_must_list.append({"terms": {f"{key}.keyword": value}})
            else:
                bool_must_list.append({"term": {f"{key}.keyword": value}})

        # Include range query if defined
        if range_query:
            bool_must_list.extend(range_query)

        # Build a query
        query = {"bool": {"must": bool_must_list}}

        # Return query results
        response = self.elastic_service.client.search(index=index, query=query, size=size, sort=sort, scroll=scroll)
        scroll_id = response['_scroll_id']
        hits = response["hits"]["hits"]
        content_list = [self.flatten_dict(content) for content in hits]
        while len(hits) > 0:
            response = self.elastic_service.client.scroll(scroll_id=scroll_id, scroll=scroll)
            hits = response["hits"]["hits"]
            if hits:
                content_list.extend([self.flatten_dict(content) for content in hits])

        if return_payload:
            for num, item in enumerate(content_list):
                content_list[num] = self.get_content(item)

        # Delete scroll after retrieving data
        self.elastic_service.client.clear_scroll(scroll_id=scroll_id)

        return content_list

    def get_content(self, metadata: dict, bucket_name: str | None = None):
        """
        Retrieves content data from MinIO based on metadata information.

        Args:
            metadata (dict): A dictionary containing metadata information.

        Returns:
            bytes: Content object in BytesIO format
        """
        logger.info(f"Getting content of metadata object from MinIO: {metadata.get('_id', 'unknown')}")
        if not bucket_name:
            bucket_name = metadata.get("content_bucket", "pdn-data")
        logger.debug(f"S3 storage bucket used: {bucket_name}")
        # Get content reference
        content_reference = metadata.get("content_reference", None)
        if not content_reference:
            logger.error(f"Metadata object does not have field: 'content_reference'")
            return
        logger.info(f"Downloading object: {content_reference}")
        content = BytesIO(self.s3_service.download_object(bucket_name, content_reference))
        content.name = content_reference

        return content

    def get_latest_input_data(self,
                              type_keyword: List,
                              scenario_timestamp: str | datetime.datetime,  # in case of str type it should be ISO compatible
                              entity: List | None = None,
                              index: str = ELASTIC_METADATA_INDEX,
                              ):

        logger.info(f"Retrieving input data files of keyword: {type_keyword}")

        # Build Elastic query from given scenario_timestamp and metadata
        metadata_query = {"keyword": type_keyword}
        if entity:
            metadata_query['entity'] = entity

        range_query = [
            {"range": {"startDate": {"lte": scenario_timestamp}}},
            {"range": {"endDate": {"gte": scenario_timestamp}}},
        ]

        files_metadata = self.query(metadata_query=metadata_query,
                                    range_query=range_query,
                                    return_payload=False,
                                    index=index,
                                    )

        files_downloaded = []
        if files_metadata:
            # Sort by latest version
            df = pd.json_normalize(files_metadata)
            # TODO need to handle if there are multiple files with same version - then use created atrribute
            df = df.sort_values(by="Model.version", ascending=True).groupby(["entity", "keyword"]).first()

            for file_object in df.to_dict("records"):
                try:
                    files_downloaded.append(self.get_content(metadata=file_object))
                except Exception as e:
                    logger.error(f"Could not download file for: {file_object}")
                    logger.error(sys.exc_info())
        else:
            logger.warning(f"Requested files not available on Object Storage")

        return files_downloaded


if __name__ == "__main__":
    # Test script to run ObjectStorage methods
    test_query = {"keyword": "CO",
                  "publisher": ["https://energy.referencedata.eu/EIC/38X-BALTIC-RSC-H"],
                  }
    service = ObjectStorage()
    # Test 1
    # response = service.query(metadata_query=test_query, return_payload=True)
    # Test 2
    latest_files = service.get_latest_input_data(
        entity=["LITGRID"],
        type_keyword=["CO"],
        scenario_timestamp=datetime.datetime(2025, 5, 13, 10, 30)
    )
    logger.info("Test script finished")
