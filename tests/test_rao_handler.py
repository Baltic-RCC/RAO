"""HandlerVirtualOperator (rao/handlers.py): SAR screening, input retrieval and the per-contingency RAO loop.

Orchestration tests use a real SAR parse and mock every heavy collaborator; the
integration test runs the full chain on TC1 with only ObjectStorage mocked.
"""
import itertools
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

import rao.find_low_impedance_gap as low_impedance
import rao.handlers as handlers
from helpers import (FIXTURES_DIR, TC1_AE, TC1_CGMES, TC1_CO, TC1_CONTINGENCY_BE_LINE_1, TC1_RA, flow_cnec_result,
                     logged, make_properties, rao_result, read_rdf_adapting_tc1, tc1_buffer)
from rao.handlers import HandlerVirtualOperator

BE_LINE_3 = "78736387-5f60-4832-b3fe-d50daf81b0a6"
TR3 = "tr3"


def sar_xml(results) -> bytes:
    """NC SAR with ``(kind, contingency, equipment, isViolation, value, valueA)`` results."""
    elements = []
    for n, (kind, contingency, equipment, violation, value, value_a) in enumerate(results):
        fields = [f"<nc:PowerFlowResult.isViolation>{str(violation).lower()}</nc:PowerFlowResult.isViolation>",
                  f"<nc:PowerFlowResult.value>{value}</nc:PowerFlowResult.value>",
                  f'<nc:PowerFlowResult.ACDCTerminal rdf:resource="#_{equipment}"/>',
                  f"<brcc:PowerFlowResult.EquipmentName>{equipment}</brcc:PowerFlowResult.EquipmentName>"]
        if value_a is not None:
            fields.append(f"<nc:PowerFlowResult.valueA>{value_a}</nc:PowerFlowResult.valueA>")
        if contingency is not None:
            fields.append(f'<nc:ContingencyPowerFlowResult.Contingency rdf:resource="#_{contingency}"/>')
        elements.append(f'<nc:{kind} rdf:ID="_r-{n}">{"".join(fields)}</nc:{kind}>')
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:nc="https://cim4.eu/ns/nc#" '
            'xmlns:brcc="https://baltic-rcc.eu/ns/brcc-nc#">' + "".join(elements) + "</rdf:RDF>").encode()


def contingency_violation(contingency="co-1", equipment=BE_LINE_3, value=110.0, value_a=550.0, violation=True):
    return ("ContingencyPowerFlowResult", contingency, equipment, violation, value, value_a)


HEADERS = {"time-horizon": "1D", "scenario-time": "2024-06-13T09:30:00+00:00", "content-reference": "models/cgm.zip",
           "project-name": "TC1", "message-id": "msg-1"}

CRAC = {
    "contingencies": [{"id": "co-1", "name": "CO 1", "networkElementsIds": ["_line-2"]}],
    "flowCnecs": [
        {"id": "ae-1-curative", "name": "BE-Line_3", "networkElementId": f"_{BE_LINE_3}", "operator": "BE",
         "thresholds": [{"unit": "ampere", "min": -500.0, "max": 500.0, "side": 1}], "instant": "curative",
         "contingencyId": "co-1", "optimized": True, "monitored": False},
        {"id": "ae-2-leg1-curative", "name": "TR3", "networkElementId": f"_{TR3}-Leg1", "operator": "BE",
         "thresholds": [{"unit": "ampere", "min": -900.0, "max": 900.0, "side": 1}], "instant": "curative",
         "contingencyId": "co-1", "optimized": True, "monitored": False},
        {"id": "ae-3-curative", "name": "Other", "networkElementId": "_other", "operator": "BE",
         "thresholds": [{"unit": "ampere", "min": -100.0, "max": 100.0, "side": 1}], "instant": "curative",
         "contingencyId": "co-1", "optimized": True, "monitored": False},
    ],
    "voltageCnecs": [{"id": "vc-1", "name": "VL", "networkElementId": "_vl-1", "operator": "BE", "instant": "curative",
                      "contingencyId": "co-1", "thresholds": [{"unit": "kilovolt", "min": 380.0, "max": 420.0}]}],
    "networkActions": [{"id": "na-1", "name": "RA 1", "operator": "BE", "onInstantUsageRules": [{"instant": "curative"}],
                        "terminalsConnectionActions": [{"networkElementId": "_sw-1", "actionType": "open"}]}],
}
RESULT = rao_result(
    [flow_cnec_result(c["id"], {"curative": {"ampere": 250.0}}) for c in CRAC["flowCnecs"]],
    network_actions=[{"networkActionId": "na-1", "activatedStates": [{"instant": "curative", "contingency": "co-1"}]}])


@pytest.fixture
def storage(object_storage_mock):
    object_storage_mock.query.return_value = [{"content_reference": "models/cgm.zip", "included": ["BE", "NL"]}]
    object_storage_mock.get_content.return_value = MagicMock(name="network-zip")
    object_storage_mock.get_input_data_for_timestamp.return_value = [
        {"keyword": k, "content": f"{k}-content"} for k in ("CO", "AE", "RA")]
    return object_storage_mock


@pytest.fixture
def handler(monkeypatch, storage):
    monkeypatch.setattr(handlers, "ObjectStorage", lambda: storage)
    return HandlerVirtualOperator(current_violations_only=True)


def optimizer_double(result=RESULT, voltage_results=None):
    optimizer = MagicMock(name="Optimizer()")
    optimizer.results.to_json.return_value = json.loads(json.dumps(result))
    if voltage_results is None:
        optimizer.voltage_monitoring_results = None
    else:
        optimizer.voltage_monitoring_results.get_voltage_cnec_results.return_value = voltage_results
    return optimizer


@pytest.fixture
def pipeline(monkeypatch, tmp_path):
    """Mocks for everything downstream of SAR parsing; returns them for assertions."""
    monkeypatch.chdir(tmp_path)
    original_read_rdf = pd.read_RDF

    def read_rdf(objects, *args, **kwargs):
        if isinstance(objects, list) and objects and hasattr(objects[0], "getvalue"):
            return original_read_rdf(objects, *args, **kwargs)  # the SAR message
        return MagicMock(name="triplets")

    monkeypatch.setattr(pd, "read_RDF", read_rdf)

    network = MagicMock(name="network")
    network.get_3_windings_transformers.return_value = pd.DataFrame({"rated_u1": [400.0, 220.0]}, index=[TR3, "tr-220"])
    network.get_2_windings_transformers.return_value = pd.DataFrame(
        {"rated_u1": [400.0, 110.0, 10.0, 110.0]}, index=[f"{TR3}-Leg1", f"{TR3}-Leg2", f"{TR3}-Leg3", "tr2w"])
    fake_pypowsybl = MagicMock(name="pypowsybl")
    fake_pypowsybl.network.load_from_binary_buffer.return_value = network
    fake_pypowsybl.loadflow.run_ac.return_value = [SimpleNamespace(status=SimpleNamespace(value=0))]
    monkeypatch.setattr(handlers, "pypowsybl", fake_pypowsybl)

    crac_builder = MagicMock(name="CracBuilder")
    crac_builder.return_value.build_crac.side_effect = lambda contingency_ids: json.loads(json.dumps(CRAC))
    monkeypatch.setattr(handlers, "CracBuilder", crac_builder)

    optimizer = MagicMock(name="Optimizer", return_value=optimizer_double())
    monkeypatch.setattr(handlers, "Optimizer", optimizer)

    empty_gap = pd.DataFrame(columns=["r", "x", "kind", "z_pu"])
    monkeypatch.setattr(low_impedance, "find_threshold_gap_branches", MagicMock(return_value=(empty_gap, empty_gap, empty_gap)))
    monkeypatch.setattr(low_impedance, "zero_out", MagicMock())
    return SimpleNamespace(pypowsybl=fake_pypowsybl, network=network, crac_builder=crac_builder, optimizer=optimizer,
                           cwd=tmp_path)


def bulk_calls(storage, index):
    return [c.kwargs for c in storage.elastic_service.send_to_elastic_bulk.call_args_list if c.kwargs["index"] == index]


# --- construction / input retrieval ------------------------------------------------------

def test_object_storage_failure_is_swallowed(monkeypatch, loguru_messages):
    def broken():
        raise ConnectionError("minio down")
    monkeypatch.setattr(handlers, "ObjectStorage", broken)
    handler = HandlerVirtualOperator()
    assert not hasattr(handler, "object_storage")  # later calls fail with AttributeError
    assert "Failed to initialize ObjectStorage service: minio down" in logged(loguru_messages, "ERROR")


def test_defaults_from_config_properties(handler):
    assert handler.current_violations_only is True and handler.debug is False
    assert (handlers.VIOLATION_THRESHOLD_PERCENT, handlers.CONTINGENCIES_COUNT_THRESHOLD) == (100, 10)


class TestGetInputProfiles:
    @pytest.fixture
    def prepared(self, handler):
        handler.scenario_timestamp = datetime(2024, 6, 13, 9, 30, tzinfo=timezone.utc)
        handler.network_model_meta = {"included": ["BE", "NL"]}
        return handler

    def test_complete_per_entity_inputs(self, prepared, storage):
        storage.get_input_data_for_timestamp.return_value = [
            {"keyword": k, "entity": e, "content": f"{k}-{e}"} for k, e in itertools.product(["CO", "AE", "RA"], ["BE", "NL"])]
        assert len(prepared.get_input_profiles()) == 6
        storage.get_input_data_for_timestamp.assert_called_once_with(type_keyword=["CO", "AE", "RA"],
                                                                     scenario_timestamp=prepared.scenario_timestamp)
        storage.get_latest_available_input_data.assert_not_called()

    def test_missing_entity_profiles_fetched_individually(self, prepared, storage):
        storage.get_input_data_for_timestamp.return_value = [
            {"keyword": k, "entity": "BE", "content": f"{k}-BE"} for k in ("CO", "AE", "RA")]
        storage.get_latest_available_input_data.side_effect = lambda type_keyword, scenario_timestamp, entity: [
            {"keyword": type_keyword[0], "entity": entity[0], "content": f"{type_keyword[0]}-{entity[0]}-latest"}]
        contents = prepared.get_input_profiles()
        requested = {(c.kwargs["type_keyword"][0], c.kwargs["entity"][0])
                     for c in storage.get_latest_available_input_data.call_args_list}
        assert requested == {("CO", "NL"), ("AE", "NL"), ("RA", "NL")}
        assert sorted(contents) == sorted(["CO-BE", "AE-BE", "RA-BE", "CO-NL-latest", "AE-NL-latest", "RA-NL-latest"])

    def test_profiles_without_entity_skip_completeness_check(self, prepared, storage, loguru_messages):
        assert prepared.get_input_profiles() == ["CO-content", "AE-content", "RA-content"]
        assert any("skipping per-TSO completeness check" in m for m in logged(loguru_messages, "WARNING"))

    def test_falls_back_to_latest_available(self, prepared, storage):
        storage.get_input_data_for_timestamp.return_value = []
        storage.get_latest_available_input_data.return_value = [{"keyword": "CO", "content": "CO-old"}]
        assert prepared.get_input_profiles() == ["CO-old"]

    @pytest.mark.xfail(strict=True, raises=KeyError,
                       reason="BUG: when neither query returns a profile the empty DataFrame has no 'keyword' column")
    def test_no_input_profiles_at_all(self, prepared, storage):
        storage.get_input_data_for_timestamp.return_value = []
        assert prepared.get_input_profiles() == []


class TestGetNetworkModel:
    def test_metadata_and_content(self, handler, storage):
        assert handler.get_network_model("models/cgm.zip") is storage.get_content.return_value
        storage.query.assert_called_once_with(metadata_query={"content_reference": "models/cgm.zip"},
                                              index=handlers.ELASTIC_MODELS_INDEX)
        storage.get_content.assert_called_once_with(metadata=storage.query.return_value[0],
                                                    bucket_name=handlers.S3_BUCKET_IN_MODELS)
        assert handler.network_model_meta["included"] == ["BE", "NL"]

    def test_unknown_model_fails_fast(self, handler, storage):
        storage.query.return_value = []
        with pytest.raises(IndexError):
            handler.get_network_model("missing.zip")


# --- SAR screening (early exits) ----------------------------------------------------------

class TestViolationScreening:
    @pytest.mark.parametrize("results", [
        pytest.param([contingency_violation(violation=False)], id="no-violation"),
        pytest.param([contingency_violation(value=99.9)], id="below-threshold"),
        pytest.param([contingency_violation(value_a=None)], id="no-current-value"),
    ])
    def test_nothing_to_optimise(self, handler, pipeline, storage, results, loguru_messages):
        message = sar_xml(results)
        properties = make_properties(**HEADERS)
        assert handler.handle(message, properties) == (message, properties)
        storage.query.assert_not_called()
        assert "No violations found in SAR profile, exiting VirtualOperator process" in logged(loguru_messages, "INFO")

    def test_power_violations_kept_when_current_only_disabled(self, handler, pipeline, storage):
        handler.current_violations_only = False
        handler.handle(sar_xml([contingency_violation(value_a=None)]), make_properties(**HEADERS))
        storage.query.assert_called_once()

    def test_violation_at_threshold_is_relevant(self, handler, pipeline, storage):
        handler.handle(sar_xml([contingency_violation(value=100.0)]), make_properties(**HEADERS))
        pipeline.optimizer.assert_called_once()

    def test_too_many_contingencies(self, handler, pipeline, storage, loguru_messages):
        results = [contingency_violation(contingency=f"co-{n}") for n in range(handlers.CONTINGENCIES_COUNT_THRESHOLD + 1)]
        handler.handle(sar_xml(results), make_properties(**HEADERS))
        storage.query.assert_not_called()
        assert "Number of unique contingencies is above threshold and message can not be processed" in logged(
            loguru_messages, "ERROR")

    def test_missing_content_reference(self, handler, pipeline, storage, loguru_messages):
        headers = {k: v for k, v in HEADERS.items() if k != "content-reference"}
        handler.handle(sar_xml([contingency_violation()]), make_properties(**headers))
        storage.query.assert_not_called()
        assert "RMQ message does not have content reference in headers" in logged(loguru_messages, "ERROR")

    def test_initial_loadflow_not_converged(self, handler, pipeline, storage):
        pipeline.pypowsybl.loadflow.run_ac.return_value = [SimpleNamespace(status=SimpleNamespace(value=2))]
        handler.handle(sar_xml([contingency_violation()]), make_properties(**HEADERS))
        pipeline.crac_builder.assert_not_called()
        storage.get_input_data_for_timestamp.assert_not_called()

    @pytest.mark.xfail(strict=True, raises=KeyError,
                       reason="BUG: the contingency-count check guards a missing ContingencyPowerFlowResult.Contingency "
                              "column, but the per-contingency groupby then raises KeyError for base-case-only SARs")
    def test_base_case_only_violations(self, handler, pipeline):
        handler.handle(sar_xml([("BaseCasePowerFlowResult", None, BE_LINE_3, True, 110.0, 550.0)]), make_properties(**HEADERS))
        pipeline.crac_builder.return_value.build_crac.assert_not_called()

    def test_base_case_rows_ignored_when_contingency_rows_exist(self, handler, pipeline):
        handler.handle(sar_xml([contingency_violation(), ("BaseCasePowerFlowResult", None, BE_LINE_3, True, 110.0, 550.0)]),
                       make_properties(**HEADERS))
        assert [c.kwargs for c in pipeline.crac_builder.return_value.build_crac.call_args_list] == [
            {"contingency_ids": ["co-1"]}]


class TestScenarioTime:
    @pytest.mark.parametrize("header, expected", [
        ("2024-06-13T09:30:00+00:00", datetime(2024, 6, 13, 9, 30, tzinfo=timezone.utc)),
        ("2024-06-13T09:30:00Z", datetime(2024, 6, 13, 9, 30, tzinfo=timezone.utc)),
        (datetime(2024, 6, 13, 9, 30), datetime(2024, 6, 13, 9, 30)),
    ])
    def test_parsed(self, handler, pipeline, header, expected):
        handler.handle(sar_xml([contingency_violation(violation=False)]), make_properties(**{**HEADERS, "scenario-time": header}))
        assert handler.scenario_timestamp == expected

    def test_defaults_to_now(self, handler, pipeline):
        headers = {k: v for k, v in HEADERS.items() if k != "scenario-time"}
        handler.handle(sar_xml([contingency_violation(violation=False)]), make_properties(**headers))
        assert (datetime.now(timezone.utc) - handler.scenario_timestamp).total_seconds() < 60


# --- per-contingency optimisation loop ----------------------------------------------------

class TestOptimisationLoop:
    def run(self, handler, results=None, **headers):
        message = sar_xml(results or [contingency_violation(), contingency_violation(equipment=TR3)])
        properties = make_properties(**{**HEADERS, **headers})
        return handler.handle(message, properties), message, properties

    def test_only_transmission_3w_transformers_replaced(self, handler, pipeline):
        self.run(handler)
        replace = pipeline.pypowsybl.network.replace_3_windings_transformers_with_3_2_windings_transformers
        replace.assert_called_once_with(pipeline.network, [TR3])
        workaround = pipeline.crac_builder.call_args.kwargs["workaround"]
        assert workaround.replaced_3w_trafos.index.tolist() == [f"{TR3}-Leg1", f"{TR3}-Leg2", f"{TR3}-Leg3"]
        pipeline.crac_builder.return_value.get_limits.assert_called_once()

    def test_failed_3w_replacement_is_logged(self, handler, pipeline, loguru_messages):
        pipeline.pypowsybl.network.replace_3_windings_transformers_with_3_2_windings_transformers.side_effect = RuntimeError("x")
        self.run(handler)
        assert f"Failed to replace {TR3}: x" in logged(loguru_messages, "ERROR")
        pipeline.optimizer.assert_called_once()

    def test_crac_upload_and_debug_copy(self, handler, pipeline, storage):
        self.run(handler)
        upload = storage.s3_service.upload_object.call_args.kwargs
        assert upload["file_path_or_file_object"].name == "RAO/CRAC_1D_20240613T0930_CO_co-1.json"
        assert upload["bucket_name"] == handlers.S3_BUCKET_RESULTS
        assert json.loads(upload["file_path_or_file_object"].getvalue()) == CRAC
        assert json.loads((pipeline.cwd / "test-crac-3w-testing.json").read_text()) == CRAC

    def test_missing_time_horizon_header(self, handler, pipeline):
        headers = {k: v for k, v in HEADERS.items() if k != "time-horizon"}
        with pytest.raises(KeyError, match="time-horizon"):
            handler.handle(sar_xml([contingency_violation()]), make_properties(**headers))

    def test_optimizer_receives_crac_parameters_and_loadflow_settings(self, handler, pipeline):
        self.run(handler)
        kwargs = pipeline.optimizer.call_args.kwargs
        assert kwargs["network"] is pipeline.network
        assert json.loads(kwargs["crac_source"].getvalue()) == CRAC
        assert json.load(kwargs["parameters_source"])["objective-function"]["type"] == "MAX_MIN_MARGIN"
        assert kwargs["loadflow_parameters"] is not None
        low_impedance.zero_out.assert_called_once()

    def test_one_optimisation_per_violated_contingency(self, handler, pipeline):
        self.run(handler, results=[contingency_violation("co-1"), contingency_violation("co-2"), contingency_violation("co-2", TR3)])
        assert [c.kwargs["contingency_ids"] for c in pipeline.crac_builder.return_value.build_crac.call_args_list] == [
            ["co-1"], ["co-2"]]
        assert pipeline.optimizer.call_count == 2

    def test_results_sent_to_elastic(self, handler, pipeline, storage):
        (message, properties), sent_message, sent_properties = self.run(handler)
        assert (message, properties) == (sent_message, sent_properties)
        [call] = bulk_calls(storage, handlers.ELASTIC_RESULTS_INDEX)
        documents = {d["cnecResults.flowCnecId"]: d for d in call["json_message_list"]}
        # SAR equipment ids get the CRAC '_' prefix; 3W legs are matched on their base transformer
        assert {k: d["cnec.sourceViolation"] for k, d in documents.items()} == {
            "ae-1-curative": True, "ae-2-leg1-curative": True, "ae-3-curative": False}
        document = documents["ae-1-curative"]
        assert document["rmq"] == HEADERS
        assert document["settings"] == {"objective-function": "MAX_MIN_MARGIN"}
        assert document["networkActionId"] == "na-1"
        assert document["cnecResults.curative.ampere.side1.loading"] == pytest.approx(0.5)
        assert not any(isinstance(v, float) and v != v for d in documents.values() for v in d.values())  # NaN -> None

    def test_sar_terminal_id_is_not_matched_to_equipment(self, handler, pipeline, storage):
        # Edge: PowerFlowResult.ACDCTerminal is compared with CNEC equipment ids. A SAR carrying a
        # spec-compliant Terminal mRID never flags any CNEC as the source violation.
        self.run(handler, results=[contingency_violation(equipment="terminal-of-be-line-3")])
        [call] = bulk_calls(storage, handlers.ELASTIC_RESULTS_INDEX)
        assert not any(d["cnec.sourceViolation"] for d in call["json_message_list"])

    def test_voltage_documents_sent_with_deterministic_ids(self, handler, pipeline, storage):
        voltage_results = pd.DataFrame([{"cnec_id": "vc-1", "min_voltage": 400.0, "max_voltage": 425.0, "margin": -5.0}])
        pipeline.optimizer.return_value = optimizer_double(voltage_results=voltage_results)
        self.run(handler)
        [call] = bulk_calls(storage, handlers.ELASTIC_VOLTAGE_RESULTS_INDEX)
        assert call["id_from_metadata"] is True and call["hashing"] is True
        assert call["id_metadata_list"] == ["@time_horizon", "@scenario_timestamp", "cnec_id", "limit_type"]
        assert [(d["limit_type"], d["is_violation"]) for d in call["json_message_list"]] == [
            ("HIGH_VOLTAGE", True), ("LOW_VOLTAGE", False)]
        assert call["json_message_list"][0]["raoAppliedActionCount"] == 1

    def test_empty_voltage_results_not_sent(self, handler, pipeline, storage):
        pipeline.optimizer.return_value = optimizer_double(voltage_results=pd.DataFrame())
        self.run(handler)
        assert bulk_calls(storage, handlers.ELASTIC_VOLTAGE_RESULTS_INDEX) == []

    def test_missing_results_skip_contingency(self, handler, pipeline, storage, loguru_messages):
        pipeline.optimizer.return_value.results = None
        self.run(handler)
        storage.elastic_service.send_to_elastic_bulk.assert_not_called()
        assert "Optimizer has no results to be processed" in logged(loguru_messages, "WARNING")

    def test_failure_retried_with_relaxed_parameters(self, handler, pipeline, storage):
        failed = optimizer_double(result={**RESULT, "computationStatus": "failure"})
        pipeline.optimizer.side_effect = [failed, optimizer_double()]
        self.run(handler)
        first, second = pipeline.optimizer.call_args_list
        relaxed = json.load(second.kwargs["parameters_source"])
        node = relaxed["extensions"]["open-rao-search-tree-parameters"]["load-flow-and-sensitivity-computation"]
        assert node["sensitivity-parameters"]["load-flow-parameters"]["useReactiveLimits"] is False
        assert "loadflow_parameters" not in second.kwargs  # edge: retry monitors voltages with default LF settings
        assert second.kwargs["crac_source"] is first.kwargs["crac_source"]
        assert len(bulk_calls(storage, handlers.ELASTIC_RESULTS_INDEX)) == 1

    def test_failure_of_relaxed_retry_is_still_published(self, handler, pipeline, storage):
        # Edge: the relaxed result status is not re-checked
        failed = optimizer_double(result={**RESULT, "computationStatus": "failure"})
        pipeline.optimizer.side_effect = [failed, failed]
        self.run(handler)
        [call] = bulk_calls(storage, handlers.ELASTIC_RESULTS_INDEX)
        assert {d["computationStatus"] for d in call["json_message_list"]} == {"failure"}

    @pytest.mark.xfail(strict=True, raises=TypeError,
                       reason="BUG: when the relaxed retry yields no results, `results` is None and "
                              "results['networkActionResults'] raises instead of skipping the contingency")
    def test_relaxed_retry_without_results(self, handler, pipeline, storage):
        failed = optimizer_double(result={**RESULT, "computationStatus": "failure"})
        empty = optimizer_double()
        empty.results = None
        pipeline.optimizer.side_effect = [failed, empty]
        self.run(handler)
        storage.elastic_service.send_to_elastic_bulk.assert_not_called()

    def test_optimised_action_missing_from_crac(self, handler, pipeline):
        result = {**RESULT, "networkActionResults": [{"networkActionId": "ghost", "activatedStates": [
            {"instant": "curative", "contingency": "co-1"}]}]}
        pipeline.optimizer.return_value = optimizer_double(result=result)
        with pytest.raises(IndexError):  # details lookup `_details[0]` for logging
            self.run(handler)


# --- end-to-end on TC1 ------------------------------------------------------------------

@pytest.mark.integration
def test_handle_tc1_end_to_end(monkeypatch, tmp_path, object_storage_mock, tc1_dir, loguru_messages):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pd, "read_RDF", read_rdf_adapting_tc1(pd.read_RDF))
    object_storage_mock.query.return_value = [{"content_reference": TC1_CGMES, "included": ["BE", "NL"]}]
    object_storage_mock.get_content.side_effect = lambda **kwargs: tc1_buffer(TC1_CGMES)
    object_storage_mock.get_input_data_for_timestamp.return_value = [
        {"keyword": keyword, "content": tc1_buffer(name)} for keyword, name in (("CO", TC1_CO), ("AE", TC1_AE), ("RA", TC1_RA))]
    monkeypatch.setattr(handlers, "ObjectStorage", lambda: object_storage_mock)

    message = (FIXTURES_DIR / "TC1_SAR_violation.xml").read_bytes()
    properties = make_properties(**HEADERS)
    assert HandlerVirtualOperator().handle(message, properties) == (message, properties)

    upload = object_storage_mock.s3_service.upload_object.call_args.kwargs["file_path_or_file_object"]
    assert upload.name == f"RAO/CRAC_1D_20240613T0930_CO_{TC1_CONTINGENCY_BE_LINE_1}.json"
    crac = json.loads(upload.getvalue())
    assert [c["name"] for c in crac["contingencies"]] == ["OCO_BE-Line_1"]

    [call] = bulk_calls(object_storage_mock, handlers.ELASTIC_RESULTS_INDEX)
    documents = call["json_message_list"]
    assert len(documents) == len(crac["flowCnecs"]) == 16
    assert {d["cnec.name"] for d in documents if d["cnec.sourceViolation"]} == {"BE-Line_3"}
    curative = [d for d in documents if d["cnec.instant"] == "curative"]
    assert {d["action.name"] for d in curative} == {"RA_NL-Line_3"}
    assert all(0 <= d["cnecResults.curative.ampere.side1.loading"] < 1 for d in curative)
    assert {d["computationStatus"] for d in documents} == {"default"}
    assert bulk_calls(object_storage_mock, handlers.ELASTIC_VOLTAGE_RESULTS_INDEX) == []
    assert "[TEMPORARY] Replaced 3w transformers with 3 x 2w transformers in the network" in logged(loguru_messages, "INFO")
    assert f"Optimization successful for contingency: {TC1_CONTINGENCY_BE_LINE_1}" in logged(loguru_messages, "SUCCESS")
