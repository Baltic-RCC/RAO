import pytest
from pydantic import ValidationError
from config.settings import LoggingSettings, ElasticSettings, RmqSettings
from rao.settings import OperatorConsumerSettings, OptimizerConsumerSettings, RaoHandlerSettings


@pytest.fixture
def rmq_credentials(monkeypatch):
    monkeypatch.setenv("RMQ_USERNAME", "user")
    monkeypatch.setenv("RMQ_PASSWORD", "secret")


def test_defaults_used_without_environment(monkeypatch, rmq_credentials):
    monkeypatch.delenv("RMQ_PORT", raising=False)
    assert RmqSettings(_env_file=None).rmq_port == 80


@pytest.mark.parametrize("missing", ["RMQ_USERNAME", "RMQ_PASSWORD"])
def test_credentials_are_required(monkeypatch, rmq_credentials, missing):
    monkeypatch.delenv(missing)
    with pytest.raises(ValidationError):
        RmqSettings(_env_file=None)


def test_environment_overrides_and_casts(monkeypatch, rmq_credentials):
    monkeypatch.setenv("RMQ_PORT", "5672")
    monkeypatch.setenv("ELK_SSL_VERIFY", "false")
    rmq = RmqSettings(_env_file=None)
    assert rmq.rmq_port == 5672
    assert rmq.rmq_password.get_secret_value() == "secret"
    assert "secret" not in repr(rmq)
    assert ElasticSettings(_env_file=None).elk_ssl_verify is False


def test_empty_environment_value_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("BATCH_SIZE", "")
    assert ElasticSettings(_env_file=None).batch_size == 20000


@pytest.mark.parametrize("value, expected", [("10", 10), ("DEBUG", "DEBUG")])
def test_log_level_accepts_number_or_name(monkeypatch, value, expected):
    monkeypatch.setenv("LOG_LEVEL", value)
    assert LoggingSettings(_env_file=None).log_level == expected


def test_consumer_queue_defaults_per_worker(monkeypatch):
    monkeypatch.delenv("RMQ_QUEUE_IN", raising=False)
    assert OperatorConsumerSettings(_env_file=None).rmq_queue_in == "rao.violations"
    assert OptimizerConsumerSettings(_env_file=None).rmq_queue_in == "rao.optimization"


def test_invalid_consumer_type_rejected(monkeypatch):
    monkeypatch.setenv("CONSUMER_TYPE", "UNKNOWN")
    with pytest.raises(ValidationError):
        OperatorConsumerSettings(_env_file=None)


def test_invalid_value_rejected(monkeypatch):
    monkeypatch.setenv("CONTINGENCIES_COUNT_THRESHOLD", "ten")
    with pytest.raises(ValidationError):
        RaoHandlerSettings(_env_file=None)
