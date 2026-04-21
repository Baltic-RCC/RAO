import datetime
import os
import requests
import ndjson
import pandas as pd
import json
import uuid
from typing import List, Dict
from elasticsearch import Elasticsearch
import config
from common.config_parser import parse_app_properties
from loguru import logger

parse_app_properties(caller_globals=globals(), path=config.paths.integrations.elastic)


class Elastic:

    def __init__(self,
                 server: str = ELK_SERVER,
                 api_key: str = ELK_TOKEN,
                 ssl_verify: bool = ELK_SSL_VERIFY,
                 debug: bool = False
                 ):

        self.server = server
        self.api_key = api_key
        self.ssl_verify = ssl_verify
        self.debug = debug

        # Override SSL CERT FILE from ENV variables if defined
        ssl_cert_file = None
        if self.ssl_verify:
            ssl_cert_file = os.getenv("SSL_CERT_FILE", None)
            if not ssl_cert_file:
                raise Exception("SSL verification is enabled but SSL_CERT_FILE environment variable is not set.")
            else:
                logger.debug(f"Using SSL certificate file: {ssl_cert_file}")

        # Create client
        self.client = Elasticsearch(self.server, api_key=self.api_key, verify_certs=self.ssl_verify, ca_certs=ssl_cert_file)

    @staticmethod
    def send_to_elastic(index: str,
                        json_message: dict,
                        id: str = None,
                        server: str = ELK_SERVER,
                        api_key: str = ELK_TOKEN,
                        ssl_verify: bool = ELK_SSL_VERIFY,
                        iso_timestamp: str = None,
                        index_rollover: bool = True,
                        debug: bool = False):
        """
        Method to send single message to ELK
        :param index: index pattern in ELK
        :param json_message: message in json format
        :param id:
        :param server: url of ELK server
        :param iso_timestamp: message timestamp
        :param index_rollover: if true, adds monthly indication to index name
        :param debug: flag for debug mode
        :return:
        """

        # Creating timestamp value if it is not provided in function call
        if not iso_timestamp:
            iso_timestamp = datetime.datetime.utcnow().isoformat(sep="T")

        # Adding timestamp value to message
        json_message["@timestamp"] = iso_timestamp

        # Create server url with relevant index pattern
        if index_rollover:
            index = f"{index}-{datetime.datetime.today():%Y%m}"
        url = f"{server}/{index}/_doc"

        if id:
            url = url + f"/{id}"

        # Executing POST to push message into ELK
        if debug:
            logger.debug(f"Sending data to {url}")
        if json_message.get('args', None):  # TODO revise if this is proper solution
            json_message.pop('args')
        json_data = json.dumps(json_message, default=str, ensure_ascii=True, skipkeys=True)
        headers = {"Authorization": f"ApiKey {api_key}", "Content-Type": "application/json"}
        response = requests.post(url=url, data=json_data.encode(), headers=headers, verify=ssl_verify)
        if json.loads(response.content).get('error'):
            logger.error(f"Send to Elasticsearch responded with error: {response.text}")
        if debug:
            logger.debug(f"ELK response: {response.content}")

        return response

    @staticmethod
    def send_to_elastic_bulk(index: str,
                             json_message_list: List[dict],
                             id_from_metadata: bool = False,
                             id_metadata_list: List[str] | None = None,
                             hashing: bool = False,
                             server: str = ELK_SERVER,
                             api_key: str = ELK_TOKEN,
                             ssl_verify: bool = ELK_SSL_VERIFY,
                             batch_size: int = int(BATCH_SIZE),
                             iso_timestamp: str | None = None,
                             index_rollover: bool = True,
                             debug: bool = False):
        """
        Method to send bulk message to ELK
        :param index: index pattern in ELK
        :param json_message_list: list of messages in json format
        :param id_from_metadata:
        :param id_metadata_list:
        :param hashing: generate UUID5 hash from namespace and given id_metadata_list
        :param server: url of ELK server
        :param batch_size: maximum size of batch
        :param iso_timestamp: timestamp to be included in documents
        :param index_rollover: if true, adds monthly indication to index name
        :param debug: flag for debug mode
        :return:
        """
        def __generate_id(element):
            doc_id = id_separator.join([str(element.get(key, '')) for key in id_metadata_list])
            if hashing:
                doc_id = str(uuid.uuid5(namespace=uuid.NAMESPACE_OID, name=doc_id))
            return doc_id

        # Validate if_metadata_list parameter if id_from_metadata is True
        if id_from_metadata and id_metadata_list is None:
            raise Exception(f"Argument id_metadata_list not provided")

        # Creating timestamp value if it is not provided in function call
        if not iso_timestamp:
            iso_timestamp = datetime.datetime.utcnow().isoformat(sep="T")

        # Adding timestamp value to messages
        json_message_list = [{**element, '@timestamp': iso_timestamp} for element in json_message_list]

        # Define server url with relevant index pattern (monthly indication is added)
        if index_rollover:
            index = f"{index}-{datetime.datetime.today():%Y%m}"
        url = f"{server}/{index}/_bulk"

        if id_from_metadata:
            id_separator = "_"
            json_message_list = [value for element in json_message_list for value in ({"index": {"_index": index, "_id": __generate_id(element)}}, element)]
        else:
            json_message_list = [value for element in json_message_list for value in ({"index": {"_index": index}}, element)]

        response_list = []
        for batch in range(0, len(json_message_list), batch_size):
            # Executing POST to push messages into ELK
            if debug:
                logger.debug(f"Sending batch ({batch}-{batch + batch_size})/{len(json_message_list)} to {url}")
            headers = {"Authorization": f"ApiKey {api_key}", "Content-Type": "application/x-ndjson"}
            response = requests.post(url=url,
                                     data=(ndjson.dumps(json_message_list[batch:batch + batch_size])+"\n").encode(),
                                     timeout=None,
                                     headers=headers,
                                     verify=ssl_verify)
            if json.loads(response.content).get('errors'):
                logger.error(f"Send to Elasticsearch responded with errors: {response.text}")
            if debug:
                logger.debug(f"ELK response: {response.content}")
            response_list.append(response.ok)

        return all(response_list)

    def get_doc_by_id(self, index: str, id: str):
        response = self.client.get(index=index, id=id)

        return response

    def update_document(self, index: str, id: str, body: dict):
        return self.client.update(index=index, id=id, body={'doc': body})

    def get_docs_by_query(self, index: str, query: dict, size: int | None = None, return_df: bool = True):

        response = self.client.search(index=index, query=query, size=size)
        if self.debug:
            logger.info(f"Returned total {response['hits']['total']['value']} document")
        response = response['hits']['hits']
        if return_df:
            response = pd.json_normalize(response)
            response.columns = response.columns.astype(str).map(lambda x: x.replace("_source.", ""))

        return response


class HandlerSendToElastic:

    def __init__(self,
                 index: str,
                 server: str = ELK_SERVER,
                 api_key: str = ELK_TOKEN,
                 id_from_metadata: bool = False,
                 id_metadata_list: List[str] | None = None,
                 hashing: bool = False,
                 headers: Dict | None = None,
                 verify: bool = False,
                 index_rollover: bool = True,
                 debug: bool = False):

        self.index = index
        self.server = server
        self.api_key = api_key
        self.id_from_metadata = id_from_metadata
        self.id_metadata_list = id_metadata_list
        self.hashing = hashing
        self.index_rollover = index_rollover
        self.debug = debug

        if not headers:
            headers = {"Authorization": f"ApiKey {api_key}", "Content-Type": "text/json"}

        self.session = requests.Session()
        self.session.verify = verify
        self.session.headers.update(headers)

    def handle(self, message: bytes, properties: dict,  **kwargs):

        # Send to Elastic
        response = Elastic.send_to_elastic_bulk(index=self.index,
                                                json_message_list=json.loads(message),
                                                id_from_metadata=self.id_from_metadata,
                                                id_metadata_list=self.id_metadata_list,
                                                hashing=self.hashing,
                                                server=self.server,
                                                api_key=self.api_key,
                                                index_rollover=self.index_rollover,
                                                debug=self.debug)

        logger.info(f"Message sending to Elastic successful: {response}")

        return message, properties


if __name__ == '__main__':

    # Create client
    server = "access_url"
    api_key = "acces_token"
    service = Elastic(server=server, api_key=api_key, debug=True)

    # Example get documents by query
    # query = {"match": {"scenario_date": "2023-03-21"}}
    # df = service.get_docs_by_query(index='csa-debug', size=200, query=query)

    # Example send document
    # json_message = {'user': 'testuser', 'message': None}
    #
    # try:
    #     Elk.send_to_elastic(index="test", json_message=json_message, server=server, api_key=api_key, debug=True)
    # except Exception as error:
    #     print(f"Message sending failed with error {error}")
    #     print(json_message)
    #     raise error
