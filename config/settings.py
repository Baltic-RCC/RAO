from pathlib import Path
from typing import Any
from loguru import logger
from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Optional local file for debugging, see config/.example.env
ENV_FILE = Path(__file__).parent / ".env"


class AppSettings(BaseSettings):
    """Base for all application settings.

    Field names map to environment variables of the same name in upper case (e.g. ``rmq_server`` -> ``RMQ_SERVER``).
    Values are taken from the environment, then from the optional ``config/.env`` file, then from the field default.
    Empty environment variables are ignored, so the default is used instead.
    """

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_ignore_empty=True, extra="ignore")

    def model_post_init(self, context: Any) -> None:
        for name, value in self:
            defined_in = "ENVIRONMENT" if name in self.model_fields_set else "DEFAULT"
            logger.info(f"[{defined_in}] {name.upper()} = {value}",
                        extra={"parameter_defined_in": defined_in,
                               "parameter_name": name.upper(),
                               "parameter_value": str(value)})


class LoggingSettings(AppSettings):
    log_level: int | str = 20
    forward_std_logs: bool = True
    enable_powsybl_logs: bool = False
    exclude_loggers: str = "pika"  # comma separated logger names
    enqueue: bool = False
    catch: bool = False
    elastic_logs_handler: bool = False
    elastic_logs_index: str = "rao-logs"

    @field_validator("log_level", mode="before")
    @classmethod
    def numeric_log_level(cls, value: Any) -> Any:
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return value


class ElasticSettings(AppSettings):
    elk_server: str = "access_url"
    elk_token: SecretStr = SecretStr("access_token")
    elk_ssl_verify: bool = True
    batch_size: int = 20000


class MinioSettings(AppSettings):
    minio_server: str = "access_url"
    minio_username: str
    minio_password: SecretStr
    token_expiration: int = 86400
    token_renew_margin: int = 120
    maxsize: int = 50


class RmqSettings(AppSettings):
    rmq_server: str = "url"
    rmq_port: int = 80
    rmq_vhost: str = "/"
    rmq_username: str
    rmq_password: SecretStr
    rmq_heartbeat: int = 15


class ObjectStorageSettings(AppSettings):
    s3_bucket_in_models: str = "opde-confidential-models"
    elastic_metadata_index: str = "csa-input-metadata"
    elastic_models_index: str = "emfos-merge-reports"


class ConsumerSettings(AppSettings):
    """Base for worker consumer settings, services override the default queue"""
    rmq_queue_in: str
