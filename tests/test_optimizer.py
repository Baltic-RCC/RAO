"""Optimizer (rao/optimizer.py): parameter/CRAC loading, OpenRAO run, voltage monitoring."""
import copy
import json
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pandas as pd
import pypowsybl
import pytest

import rao.optimizer as optimizer_module
from helpers import TC1_CONTINGENCY_BE_LINE_1, logged, named_buffer
from rao.optimizer import Optimizer
from rao.parameters.manager import RaoSettingsManager

FAILURE = pypowsybl._pypowsybl.RaoComputationStatus.FAILURE
DEFAULT = pypowsybl._pypowsybl.RaoComputationStatus.DEFAULT


@pytest.fixture
def mocked_pypowsybl(monkeypatch):
    fake = MagicMock(name="pypowsybl")
    monkeypatch.setattr(optimizer_module, "pypowsybl", fake)
    return fake


def exhausted_buffer(content: bytes = b"{}") -> BytesIO:
    buffer = BytesIO(content)
    buffer.read()
    return buffer


class TestLoadParameters:
    def test_buffer_is_rewound(self, mocked_pypowsybl):
        positions = []
        mocked_pypowsybl.rao.Parameters.from_buffer_source.side_effect = lambda buffer: positions.append(buffer.tell())
        Optimizer(network=MagicMock(), crac_source=BytesIO(), parameters_source=exhausted_buffer()).load_parameters()
        assert positions == [0]

    @pytest.mark.parametrize("source", ["params.json", Path("params.json")])
    def test_path_sources(self, mocked_pypowsybl, source):
        optimizer = Optimizer(network=MagicMock(), crac_source="crac.json", parameters_source=source)
        optimizer.load_parameters()
        mocked_pypowsybl.rao.Parameters.from_file_source.assert_called_once_with(parameters_file="params.json")
        assert optimizer.parameters is mocked_pypowsybl.rao.Parameters.from_file_source.return_value

    @pytest.mark.parametrize("source", [None, ""])
    def test_missing_source_uses_repository_settings(self, mocked_pypowsybl, source):
        optimizer = Optimizer(network=MagicMock(), crac_source=BytesIO(), parameters_source=source)
        optimizer.load_parameters()
        passed = mocked_pypowsybl.rao.Parameters.from_buffer_source.call_args.args[0]
        assert json.load(passed) == json.load(RaoSettingsManager().to_bytesio())

    def test_unsupported_source_type(self, mocked_pypowsybl):
        with pytest.raises(TypeError, match="Unsupported parameter source"):
            Optimizer(network=MagicMock(), crac_source=BytesIO(), parameters_source=b"{}").load_parameters()


class TestLoadCrac:
    def test_buffer_is_rewound(self, mocked_pypowsybl):
        network = MagicMock()
        positions = []
        mocked_pypowsybl.rao.Crac.from_buffer_source.side_effect = lambda network, crac_source: positions.append(crac_source.tell())
        Optimizer(network=network, crac_source=exhausted_buffer()).load_crac()
        assert positions == [0]

    @pytest.mark.parametrize("source", ["crac.json", Path("crac.json")])
    def test_path_sources(self, mocked_pypowsybl, source):
        network = MagicMock()
        Optimizer(network=network, crac_source=source).load_crac()
        mocked_pypowsybl.rao.Crac.from_file_source.assert_called_once_with(network=network, crac_file=source)

    @pytest.mark.xfail(strict=True, raises=pytest.fail.Exception,
                       reason="BUG: an unsupported crac_source (e.g. raw bytes) is silently ignored, leaving crac=None "
                              "for runner.run(); load_parameters raises TypeError for the same case")
    def test_unsupported_source_type_raises(self, mocked_pypowsybl):
        with pytest.raises(TypeError):
            Optimizer(network=MagicMock(), crac_source=b"{}").load_crac()


def voltage_cnec_table(*rows):
    return pd.DataFrame([{"id": cnec_id, "name": name, "network_element_id": element, "operator": operator}
                         for cnec_id, name, element, operator in rows]).set_index("id")


@pytest.fixture
def monitoring_optimizer():
    """Real Optimizer with its native runner replaced, so the status enum is the real one."""
    optimizer = Optimizer(network=MagicMock(), crac_source=BytesIO(), loadflow_parameters="lf-params")
    optimizer.runner = MagicMock(name="runner")
    optimizer.crac = MagicMock(name="crac")
    optimizer.results = SimpleNamespace(status=DEFAULT)
    return optimizer


class TestRunVoltageMonitoring:
    def test_skipped_when_status_attribute_is_failure(self, monitoring_optimizer):
        monitoring_optimizer.results = SimpleNamespace(status=FAILURE)
        monitoring_optimizer.run_voltage_monitoring()
        monitoring_optimizer.runner.run_voltage_monitoring.assert_not_called()

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason="BUG: pypowsybl RaoResult.status is a method; getattr() returns the bound method, "
                              "which never equals FAILURE, so the failed-RAO guard never triggers")
    def test_skipped_for_failed_rao_result(self, monitoring_optimizer):
        class FailedRaoResult:
            def status(self):
                return FAILURE

        monitoring_optimizer.results = FailedRaoResult()
        monitoring_optimizer.crac.get_voltage_cnecs.return_value = voltage_cnec_table(("vc", "VC", "vl", "BE"))
        monitoring_optimizer.run_voltage_monitoring()
        monitoring_optimizer.runner.run_voltage_monitoring.assert_not_called()

    def test_skipped_without_voltage_cnecs(self, monitoring_optimizer, loguru_messages):
        monitoring_optimizer.crac.get_voltage_cnecs.return_value = pd.DataFrame()
        monitoring_optimizer.run_voltage_monitoring()
        monitoring_optimizer.runner.run_voltage_monitoring.assert_not_called()
        assert "No voltage CNECs in CRAC; skipping voltage monitoring" in logged(loguru_messages, "INFO")

    def test_runs_open_load_flow_monitoring(self, monitoring_optimizer, loguru_messages):
        monitoring_optimizer.crac.get_voltage_cnecs.return_value = voltage_cnec_table(("vc-1", "VL 400", "vl-400", "Elia"))
        result = monitoring_optimizer.runner.run_voltage_monitoring.return_value
        result.get_voltage_cnec_results.return_value = pd.DataFrame([{
            "cnec_id": "vc-1", "optimized_instant": "curative", "contingency": "co-1",
            "min_voltage": 401.0, "max_voltage": 402.0, "margin": 18.0}])
        monitoring_optimizer.run_voltage_monitoring()

        monitoring_optimizer.runner.run_voltage_monitoring.assert_called_once_with(
            crac=monitoring_optimizer.crac, network=monitoring_optimizer.network,
            rao_result=monitoring_optimizer.results, provider_str="OpenLoadFlow", load_flow_parameters="lf-params")
        assert monitoring_optimizer.voltage_monitoring_results is result
        assert any("CNEC vc-1 at VoltageLevel VL 400 (vl-400) [TSO=Elia; curative, contingency=co-1]" in m
                   for m in logged(loguru_messages, "DEBUG"))

    def test_empty_monitoring_result_is_logged(self, monitoring_optimizer, loguru_messages):
        monitoring_optimizer.crac.get_voltage_cnecs.return_value = voltage_cnec_table(("vc-1", "VL", "vl", ""))
        monitoring_optimizer.runner.run_voltage_monitoring.return_value.get_voltage_cnec_results.return_value = pd.DataFrame()
        monitoring_optimizer.run_voltage_monitoring()
        assert "Voltage monitoring completed without voltage CNEC results" in logged(loguru_messages, "WARNING")


class TestRun:
    def test_pipeline_order_and_monitoring_errors_are_swallowed(self, mocked_pypowsybl, loguru_messages):
        network = MagicMock()
        network.get_variant_ids.return_value = ["InitialState", "rao-variant"]
        optimizer = Optimizer(network=network, crac_source=BytesIO(b"{}"), parameters_source=BytesIO(b"{}"))
        mocked_pypowsybl.rao.Crac.from_buffer_source.return_value.get_voltage_cnecs.side_effect = RuntimeError(
            "monitoring blew up")
        optimizer.run()

        optimizer.runner.run.assert_called_once_with(crac=mocked_pypowsybl.rao.Crac.from_buffer_source.return_value,
                                                     network=network,
                                                     parameters=mocked_pypowsybl.rao.Parameters.from_buffer_source.return_value)
        assert optimizer.results is optimizer.runner.run.return_value
        assert "Voltage monitoring skipped: monitoring blew up" in logged(loguru_messages, "WARNING")
        assert network.method_calls[-3:] == [call.set_working_variant("InitialState"), call.get_variant_ids(),
                                             call.remove_variant("rao-variant")]

    def test_rao_errors_propagate(self, mocked_pypowsybl):
        optimizer = Optimizer(network=MagicMock(), crac_source=BytesIO(b"{}"), parameters_source=BytesIO(b"{}"))
        optimizer.runner.run.side_effect = RuntimeError("rao failed")
        with pytest.raises(RuntimeError, match="rao failed"):
            optimizer.run()

    def test_result_frames(self, mocked_pypowsybl):
        optimizer = Optimizer(network=MagicMock(), crac_source=BytesIO())
        optimizer.results = MagicMock()
        optimizer.results.to_json.return_value = {
            "flowCnecResults": [{"flowCnecId": "c", "initial": {"ampere": {"margin": 1.0}}}],
            "costResults": {"initial": {"functionalCost": -5.0}}}
        assert optimizer.cnec_results.columns.tolist() == ["flowCnecId", "initial.ampere.margin"]
        assert optimizer.cost_results["initial.functionalCost"].tolist() == [-5.0]


def test_clean_network_variants_keeps_only_initial_state():
    network = pypowsybl.network.create_ieee14()
    network.clone_variant("InitialState", "a")
    network.clone_variant("InitialState", "b")
    network.set_working_variant("b")
    Optimizer(network=network, crac_source=BytesIO()).clean_network_variants()
    assert network.get_variant_ids() == ["InitialState"]
    assert network.get_working_variant_id() == "InitialState"


def test_solve_loadflow_with_repository_settings():
    network = pypowsybl.network.create_ieee14()
    result = Optimizer(network=network, crac_source=BytesIO()).solve_loadflow()
    assert result[0].status == pypowsybl.loadflow.ComponentStatus.CONVERGED


# --- OpenRAO on TC1 ---------------------------------------------------------------------

def run_rao(network, crac_source, lf_settings, parameters_source=None):
    optimizer = Optimizer(network=network, crac_source=crac_source,
                          parameters_source=parameters_source or RaoSettingsManager().to_bytesio(),
                          loadflow_parameters=lf_settings.build_pypowsybl_parameters())
    optimizer.run()
    return optimizer


@pytest.mark.integration
class TestOpenRaoOnTc1:
    def test_rao_result(self, tc1_network, tc1_crac_buffer, lf_settings):
        optimizer = run_rao(tc1_network, tc1_crac_buffer, lf_settings)
        result = optimizer.results.to_json()
        assert result["computationStatus"] == "default"
        assert {"flowCnecResults", "networkActionResults", "rangeActionResults", "costResults"} <= set(result)
        assert {r["flowCnecId"] for r in result["flowCnecResults"]} == {c["id"] for c in json.load(tc1_crac_buffer)["flowCnecs"]}
        # RA_NL-Line_3 (curative) is selected for OCO_BE-Line_1
        assert result["networkActionResults"] == [{
            "networkActionId": "eadf1901-f858-4a05-81e8-0665641d6237",
            "activatedStates": [{"instant": "curative", "contingency": TC1_CONTINGENCY_BE_LINE_1}]}]
        assert "flowCnecId" in optimizer.cnec_results.columns
        assert "curative.functionalCost" in optimizer.cost_results.columns
        assert optimizer.voltage_monitoring_results is None  # TC1 has no voltage limits
        assert tc1_network.get_variant_ids() == ["InitialState"]

    def test_crac_and_parameters_from_files(self, tc1_network, tc1_crac, lf_settings, tmp_path):
        crac_file = tmp_path / "crac.json"
        crac_file.write_text(json.dumps(tc1_crac))
        params_file = tmp_path / "params.json"
        params_file.write_bytes(RaoSettingsManager().to_bytesio().getvalue())
        optimizer = run_rao(tc1_network, str(crac_file), lf_settings, parameters_source=str(params_file))
        assert optimizer.results.to_json()["computationStatus"] == "default"

    def test_voltage_monitoring_after_rao(self, tc1_network, tc1_crac, lf_settings):
        voltage_level = "_469df5f7-058f-4451-a998-57a48e8a56fe"  # 380 kV, PP_Brussels
        crac = copy.deepcopy(tc1_crac)
        crac["voltageCnecs"] = [{
            "id": "vc-brussels", "name": "Brussels 380", "networkElementId": voltage_level, "operator": "Elia",
            "thresholds": [{"unit": "kilovolt", "min": 395.0, "max": 400.0}], "instant": "curative",
            "optimized": False, "monitored": True, "contingencyId": TC1_CONTINGENCY_BE_LINE_1}]
        optimizer = run_rao(tc1_network, named_buffer(json.dumps(crac).encode(), "crac.json"), lf_settings)
        results = optimizer.voltage_monitoring_results.get_voltage_cnec_results()
        row = results.iloc[0]
        assert (row["cnec_id"], row["optimized_instant"], row["contingency"]) == ("vc-brussels", "curative",
                                                                                 TC1_CONTINGENCY_BE_LINE_1)
        assert row["max_voltage"] > 400.0 and row["margin"] == pytest.approx(400.0 - row["max_voltage"])

    @pytest.mark.xfail(strict=True, raises=pypowsybl.PyPowsyblError,
                       reason="BUG: serialize_flow_cnecs only drops 'apparent' thresholds for PL operators; any other "
                              "apparent-power CNEC reaches OpenRAO, which rejects the unit")
    def test_non_polish_apparent_power_cnec_is_loadable(self, tc1_network, tc1_crac):
        crac = copy.deepcopy(tc1_crac)
        crac["flowCnecs"][0]["thresholds"][0]["unit"] = "apparent"
        pypowsybl.rao.Crac.from_buffer_source(network=tc1_network, crac_source=BytesIO(json.dumps(crac).encode()))

    @pytest.mark.xfail(strict=True, raises=pypowsybl.PyPowsyblError,
                       reason="BUG: build_crac() over several contingencies repeats curative CNEC ids and OpenRAO "
                              "refuses the CRAC; only the handler's one-contingency-per-CRAC path works")
    def test_multi_contingency_crac_is_loadable(self, tc1_network, tc1_profiles_adapted, tc1_network_triplets_adapted):
        from rao.crac.builder import CracBuilder
        crac = CracBuilder(data=tc1_profiles_adapted, network=tc1_network_triplets_adapted).build_crac()
        pypowsybl.rao.Crac.from_buffer_source(network=tc1_network, crac_source=BytesIO(json.dumps(crac).encode()))
