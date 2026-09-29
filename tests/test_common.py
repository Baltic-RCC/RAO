"""Shared infrastructure used by every worker: properties parsing, timing decorator, Elastic bulk export."""
import json
import uuid
from unittest.mock import MagicMock

import pytest

from common.config_parser import parse_app_properties
from common.decorators import performance_counter
from integrations.elastic import Elastic


def properties_file(tmp_path, body: str, section: str = "MAIN"):
    path = tmp_path / "app.properties"
    path.write_text(f"[{section}]\n{body}\n")
    return str(path)


class TestParseAppProperties:
    def test_values_and_environment_override(self, tmp_path, monkeypatch):
        path = properties_file(tmp_path, "ELASTIC_INDEX = rao-results\nlower_case = value\nDB_PASSWORD = secret")
        monkeypatch.setenv("ELASTIC_INDEX", "from-env")
        parsed = {}
        parse_app_properties(parsed, path)
        assert parsed == {"ELASTIC_INDEX": "from-env", "LOWER_CASE": "value", "DB_PASSWORD": "secret"}

    def test_empty_environment_value_ignored(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SETTING_X", "")
        parsed = {}
        parse_app_properties(parsed, properties_file(tmp_path, "SETTING_X = file"))
        assert parsed["SETTING_X"] == "file"

    def test_secrets_masked_in_logs(self, tmp_path, loguru_messages):
        parse_app_properties({}, properties_file(tmp_path, "API_TOKEN = abc123"))
        assert "[PROPERTIES] API_TOKEN = ****" in [r["message"] for r in loguru_messages]

    def test_eval_types(self, tmp_path):
        parsed = {}
        parse_app_properties(parsed, properties_file(tmp_path, "FLAG = True\nCOUNT = 10\nNAME = rao-results\nNONE = None"),
                             eval_types=True)
        assert parsed == {"FLAG": True, "COUNT": 10, "NAME": "rao-results", "NONE": None}

    def test_missing_section(self, tmp_path):
        import configparser
        with pytest.raises(configparser.NoSectionError):
            parse_app_properties({}, properties_file(tmp_path, "A = 1"), section="HANDLER")

    @pytest.mark.xfail(strict=True, raises=SyntaxError,
                       reason="BUG: eval_types only catches ValueError; values such as 'a b', URLs or '/' make "
                              "ast.literal_eval raise SyntaxError at import time")
    @pytest.mark.parametrize("value", ["opde confidential", "http://elastic:9200"])
    def test_eval_types_keeps_unparsable_strings(self, tmp_path, value):
        parsed = {}
        parse_app_properties(parsed, properties_file(tmp_path, f"VALUE = {value}"), eval_types=True)
        assert parsed["VALUE"] == value


def test_performance_counter(loguru_messages):
    @performance_counter(units="seconds")
    def add(a, b):
        return a + b

    assert add(2, 3) == 5 and add.__name__ == "add"
    record = next(r for r in loguru_messages if "finished with duration" in r["message"])
    assert record["extra"]["measured_function"].endswith(".add")


@pytest.fixture
def bulk_post(monkeypatch):
    post = MagicMock(name="requests.post")
    post.return_value.content = b'{"errors": false}'
    post.return_value.ok = True
    monkeypatch.setattr("integrations.elastic.requests.post", post)
    return post


def bulk_lines(post) -> list[dict]:
    lines = []
    for call in post.call_args_list:
        lines += [json.loads(line) for line in call.kwargs["data"].decode().splitlines() if line]
    return lines


class TestElasticBulk:
    def test_deterministic_ids_from_metadata(self, bulk_post):
        documents = [{"@time_horizon": "1D", "@scenario_timestamp": "ts", "cnec_id": "vc-1", "limit_type": "HIGH_VOLTAGE"}]
        Elastic.send_to_elastic_bulk(index="rao-voltage-results", json_message_list=documents, server="http://es",
                                     ssl_verify=False, id_from_metadata=True, hashing=True,
                                     id_metadata_list=["@time_horizon", "@scenario_timestamp", "cnec_id", "limit_type"])
        action, document = bulk_lines(bulk_post)
        assert action["index"]["_id"] == str(uuid.uuid5(uuid.NAMESPACE_OID, "1D_ts_vc-1_HIGH_VOLTAGE"))
        assert action["index"]["_index"].startswith("rao-voltage-results-")  # monthly rollover suffix
        assert "@timestamp" in document
        assert bulk_post.call_args.kwargs["url"] == f"http://es/{action['index']['_index']}/_bulk"

    def test_id_metadata_list_required(self, bulk_post):
        with pytest.raises(Exception):
            Elastic.send_to_elastic_bulk(index="i", json_message_list=[{}], server="http://es", ssl_verify=False,
                                         id_from_metadata=True)

    def test_empty_list_sends_nothing(self, bulk_post):
        assert Elastic.send_to_elastic_bulk(index="i", json_message_list=[], server="http://es", ssl_verify=False) is True
        bulk_post.assert_not_called()

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason="BUG: batch_size counts NDJSON lines, not documents; an odd BATCH_SIZE splits an "
                              "action line from its document across two _bulk requests")
    def test_batches_keep_action_document_pairs(self, bulk_post):
        Elastic.send_to_elastic_bulk(index="i", json_message_list=[{"a": 1}, {"a": 2}], server="http://es",
                                     ssl_verify=False, batch_size=3)
        for call in bulk_post.call_args_list:
            assert len([line for line in call.kwargs["data"].decode().splitlines() if line]) % 2 == 0
