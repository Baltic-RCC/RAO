import requests
from lxml import etree
import minio
from minio.commonconfig import Tags
import urllib3
import sys
import mimetypes
import re
import config
import functools
from io import BytesIO
from datetime import datetime, timedelta, timezone
from aniso8601 import parse_datetime
from common.config_parser import parse_app_properties
from loguru import logger

urllib3.disable_warnings()

parse_app_properties(globals(), config.paths.integrations.minio)


def renew_authentication_token(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if datetime.now(timezone.utc) >= self.token_expiration - timedelta(seconds=int(TOKEN_RENEW_MARGIN)):  # 120s margin before token expiration
            logger.warning("Authentication token going to expire soon, renewing token")
            self._create_client()
        return func(self, *args, **kwargs)

    return wrapper


class S3Minio:

    def __init__(self, server: str = MINIO_SERVER, username: str = MINIO_USERNAME, password: str = MINIO_PASSWORD):
        self.server = server
        self.username = username
        self.password = password
        self.token_expiration = datetime.now(timezone.utc)
        self.http_client = urllib3.PoolManager(
                maxsize=int(MAXSIZE),
                cert_reqs='CERT_NONE',
            )

        # Initialize client
        self._create_client()

    def _create_client(self):
        """Connect to Minio"""
        credentials = self._get_credentials()
        self.token_expiration = parse_datetime(credentials['Expiration']).astimezone(timezone.utc)
        self.client = minio.Minio(endpoint=self.server,
                                  access_key=credentials['AccessKeyId'],
                                  secret_key=credentials['SecretAccessKey'],
                                  session_token=credentials['SessionToken'],
                                  secure=True,
                                  http_client=self.http_client,
                                  )

    def _get_credentials(self, action: str = "AssumeRoleWithLDAPIdentity", version: str = "2011-06-15"):
        """
        Method to get temporary credentials for LDAP user
        :param action: string of action
        :param version: version
        :return: dictionary with authentication details
        """
        # Define LDAP service user parameters
        params = {
            "Action": action,
            "LDAPUsername": self.username,
            "LDAPPassword": self.password,
            "Version": version,
            "DurationSeconds": TOKEN_EXPIRATION,
        }

        # Sending request for temporary credentials and parsing it out from returned xml
        response = requests.post(f"https://{self.server}", params=params, verify=False).content
        credentials = {}
        root = etree.fromstring(response)
        et = root.find("{*}AssumeRoleWithLDAPIdentityResult/{*}Credentials")
        for element in et:
            _, _, tag = element.tag.rpartition("}")
            credentials[tag] = element.text

        return credentials

    @staticmethod
    def dict_to_tags(tags: dict):
        converted = Tags.new_object_tags()
        for k, v in tags.items():
            converted[k] = v

        return converted

    @renew_authentication_token
    def upload_object(self,
                      file_path_or_file_object: str | BytesIO,
                      bucket_name: str,
                      metadata: dict | None = None,
                      tags: dict | None = None,
                      ):
        """
        Method to upload file to Minio storage
        :param file_path_or_file_object: file path or BytesIO object
        :param bucket_name: bucket name
        :param metadata: object metadata
        :param tags: object tags
        :return: response from Minio
        """
        file_object = file_path_or_file_object

        if type(file_path_or_file_object) == str:
            file_object = open(file_path_or_file_object, "rb")
            length = sys.getsizeof(file_object)
        else:
            length = file_object.getbuffer().nbytes

        # Handle metadata - remove empty values as it caueses S3 error at upload
        metadata = {k: v for k, v in metadata.items() if v}
                          
        # Handle tags if provided
        if tags:
            tags = self.dict_to_tags(tags)

        # Just to be sure that pointer is at the beginning of the content
        file_object.seek(0)

        # TODO - check that bucket exists and it has access to it, maybe also try to create one
        logger.info(f"Uploading object to bucket {bucket_name}: {file_object.name}")
        response = self.client.put_object(
            bucket_name=bucket_name,
            object_name=file_object.name,
            data=file_object,
            length=length,
            content_type=mimetypes.guess_type(file_object.name)[0],
            metadata=metadata,
            tags=tags,
        )

        return response

    @renew_authentication_token
    def download_object(self, bucket_name: str, object_name: str):
        response = None
        try:
            object_name = object_name.replace("//", "/")
            file_data = self.client.get_object(bucket_name, object_name)
            response = file_data.read()
        except minio.error.S3Error as err:
            logger.error(f"Error downloading object {object_name} from bucket {bucket_name}: {err}", exc_info=True)

        return response

    @renew_authentication_token
    def object_exists(self, object_name: str, bucket_name: str) -> bool:
        """Check whether object exists in specified bucket by its object name"""
        exists = False
        try:
            self.client.stat_object(bucket_name, object_name)
            exists = True
        except minio.error.S3Error as e:
            pass

        return exists

    @renew_authentication_token
    def list_objects(self,
                     bucket_name: str,
                     prefix: str | None = None,
                     recursive: bool = False,
                     start_after: str | None = None,
                     include_user_meta: bool = True,
                     include_version:  bool = False):
        """Return all object of specified bucket"""
        objects = []
        try:
            response = self.client.list_objects(bucket_name, prefix, recursive, start_after, include_user_meta, include_version)
            objects.extend(response)
        except minio.error.S3Error as err:
            logger.error(f"Error listing objects in bucket {bucket_name} with prefix {prefix}: {err}", exc_info=True)

        return objects

    @renew_authentication_token
    def query_objects(self, bucket_name: str, metadata: dict = None, prefix: str = None, use_regex: bool = False):
        """Example: service.query_objects(prefix="IGM", metadata={'bamessageid': '20230215T1630Z-1D-LITGRID-001'})"""

        objects = self.client.list_objects(bucket_name, prefix, recursive=True, include_user_meta=True)

        if not metadata:
            return objects

        result_list = []
        regex_hit = False
        for object in objects:
            object_metadata = self.client.stat_object(bucket_name, object.object_name).metadata

            meta_match = True
            for query_key, query_value in metadata.items():
                meta_value = object_metadata.get(f"x-amz-meta-{query_key}", None)
                # meta_match true if it was true and meta_value equals query_value or regex was used and found
                if meta_value:
                    regex_hit = bool(re.search(pattern=query_value, string=meta_value)) if use_regex else False
                meta_match = (meta_match and ((meta_value == query_value) or regex_hit))

            if meta_match:
                result_list.append(object)

        return result_list

    def get_all_objects_name(self, bucket_name: str, prefix: str = None):
        objects = self.client.list_objects(bucket_name=bucket_name, prefix=prefix,recursive=True)
        list_elements = []
        for obj in objects:
            try:
                object_name = obj.object_name.split("/")[-1]
                list_elements.append(object_name)
            except Exception as e:
                logger.warning(f"Object name not present: {e}")

        return list_elements


if __name__ == '__main__':
    # Test Minio API
    service = S3Minio()
    buckets = service.client.list_buckets()
    objects = service.list_objects(bucket_name='iop')
    print(buckets)

