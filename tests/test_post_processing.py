"""RAO result post-processing (rao/handlers.py): flow-CNEC result flattening, applied actions, voltage documents."""
import math
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from helpers import TC1_CONTINGENCY_BE_LINE_1, flow_cnec_result, rao_result
from rao.handlers import (HandlerVirtualOperator, _collapse, _elementary_actions, _finite, _lookup_element,
                          _voltage_level_meta, build_applied_actions, build_voltage_documents)

CRAC = {
    "contingencies": [{"id": "co-1", "name": "CO 1", "networkElementsIds": ["_line-2"]}],
    "flowCnecs": [
        {"id": "c1-preventive", "name": "Line 1", "networkElementId": "_line-1", "operator": "BE",
         "thresholds": [{"unit": "ampere", "min": -500.0, "max": 400.0, "side": 1}],
         "instant": "preventive", "optimized": True, "monitored": False},
        {"id": "c1-curative", "name": "Line 1", "networkElementId": "_line-1", "operator": "BE",
         "thresholds": [{"unit": "megawatt", "min": -300.0, "max": 300.0, "side": 1}],
         "instant": "curative", "optimized": True, "monitored": False, "contingencyId": "co-1"},
    ],
    "networkActions": [
        {"id": "na-1", "name": "RA 1", "operator": "BE", "onInstantUsageRules": [{"instant": "curative"}],
         "terminalsConnectionActions": [{"networkElementId": "_sw-1", "actionType": "open"}]},
        {"id": "na-2", "name": "RA 2", "operator": "BE", "onInstantUsageRules": [{"instant": "preventive"}],
         "terminalsConnectionActions": [{"networkElementId": "_sw-2", "actionType": "close"}]},
    ],
}
FLOWS = [
    flow_cnec_result("c1-preventive", {"initial": {"ampere": -250.0, "megawatt": -100.0}, "preventive": {"ampere": 200.0}}),
    flow_cnec_result("c1-curative", {"initial": {"megawatt": 150.0}, "curative": {"megawatt": -60.0}}),
]
CURATIVE_ACTION = {"networkActionId": "na-1", "activatedStates": [{"instant": "curative", "contingency": "co-1"}]}
PREVENTIVE_ACTION = {"networkActionId": "na-2", "activatedStates": [{"instant": "preventive"}]}


def post_process(result: dict, crac: dict = CRAC) -> pd.DataFrame:
    handler = HandlerVirtualOperator.__new__(HandlerVirtualOperator)
    handler.crac = crac
    return handler.post_process_results(results=pd.json_normalize(result))


def by_cnec(frame: pd.DataFrame) -> dict:
    return {row["cnecResults.flowCnecId"]: row for _, row in frame.iterrows()}


class TestPostProcessResults:
    def test_one_row_per_flow_cnec_with_crac_and_contingency_data(self):
        rows = by_cnec(post_process(rao_result(FLOWS)))
        assert set(rows) == {"c1-preventive", "c1-curative"}
        curative = rows["c1-curative"]
        assert curative["cnecResultsType"] == "flowCnecResults"
        assert (curative["cnec.name"], curative["cnec.instant"], curative["cnec.contingencyId"]) == ("Line 1", "curative", "co-1")
        assert curative["contingency.name"] == "CO 1"
        assert (curative["cnec.thresholds.unit"], curative["cnec.thresholds.max"]) == ("megawatt", 300.0)
        assert pd.isna(rows["c1-preventive"]["contingency.name"])

    def test_loading_uses_signed_limit_and_matching_unit(self):
        rows = by_cnec(post_process(rao_result(FLOWS)))
        preventive = rows["c1-preventive"]
        assert preventive["cnecResults.initial.ampere.side1.loading"] == pytest.approx(-250.0 / -500.0)
        assert preventive["cnecResults.preventive.ampere.side1.loading"] == pytest.approx(200.0 / 400.0)
        assert pd.isna(preventive["cnecResults.initial.megawatt.side1.loading"])  # unit mismatch
        curative = rows["c1-curative"]
        assert curative["cnecResults.curative.megawatt.side1.loading"] == pytest.approx(-60.0 / -300.0)
        assert pd.isna(curative["cnecResults.initial.ampere.side1.loading"])     # flow absent for CNEC

    def test_zero_limit_gives_no_loading(self):
        zero = {**CRAC["flowCnecs"][0], "thresholds": [{"unit": "ampere", "min": 0.0, "max": 0.0}]}
        rows = by_cnec(post_process(rao_result(FLOWS), {**CRAC, "flowCnecs": [zero, CRAC["flowCnecs"][1]]}))
        assert pd.isna(rows["c1-preventive"]["cnecResults.preventive.ampere.side1.loading"])

    def test_multiple_thresholds_explode_rows(self):
        cnec = {**CRAC["flowCnecs"][0], "thresholds": [{"unit": "ampere", "min": -500.0, "max": 400.0},
                                                      {"unit": "megawatt", "min": -200.0, "max": 200.0}]}
        frame = post_process(rao_result(FLOWS), {**CRAC, "flowCnecs": [cnec, CRAC["flowCnecs"][1]]})
        preventive = frame[frame["cnecResults.flowCnecId"] == "c1-preventive"]
        assert preventive["cnec.thresholds.unit"].tolist() == ["ampere", "megawatt"]
        assert preventive["cnecResults.initial.megawatt.side1.loading"].iloc[1] == pytest.approx(-100.0 / -200.0)

    @pytest.mark.xfail(strict=True, raises=KeyError,
                       reason="BUG: a CRAC with contingencies but only preventive CNECs has no "
                              "'cnec.contingencyId' column and the contingency merge crashes")
    def test_preventive_only_cnecs_with_contingencies(self):
        assert len(post_process(rao_result(FLOWS[:1]), {**CRAC, "flowCnecs": CRAC["flowCnecs"][:1]})) == 1

    def test_no_actions_no_action_columns(self):
        frame = post_process(rao_result(FLOWS))
        assert not [c for c in frame.columns if c.startswith(("action.", "networkActionId", "activatedStates"))]

    def test_curative_action_joined_on_instant_and_contingency(self):
        rows = by_cnec(post_process(rao_result(FLOWS, network_actions=[CURATIVE_ACTION])))
        assert (rows["c1-curative"]["networkActionId"], rows["c1-curative"]["action.name"]) == ("na-1", "RA 1")
        assert pd.isna(rows["c1-preventive"]["networkActionId"])

    def test_preventive_and_curative_actions(self):
        frame = post_process(rao_result(FLOWS, network_actions=[PREVENTIVE_ACTION, CURATIVE_ACTION]))
        assert dict(zip(frame["cnec.instant"], frame["networkActionId"])) == {"preventive": "na-2", "curative": "na-1"}

    def test_crac_without_contingencies(self):
        frame = post_process(rao_result(FLOWS[:1]), {**CRAC, "contingencies": []})
        assert not [c for c in frame.columns if c.startswith("contingency.")]

    @pytest.mark.xfail(strict=True, raises=KeyError,
                       reason="BUG: with range actions but no network actions the action flag is set, "
                              "networkActionResults explodes to NaN and 'activatedStates' is missing")
    def test_range_action_only_result(self):
        range_action = {"rangeActionId": "pst-1", "initialSetpoint": 0.0,
                        "activatedStates": [{"instant": "preventive", "setpoint": 2.0}]}
        assert len(post_process(rao_result(FLOWS, range_actions=[range_action]))) == 2

    @pytest.mark.xfail(strict=True, raises=KeyError,
                       reason="BUG: preventive activated states have no 'contingency' key, so the merge column "
                              "'activatedStates.contingency' does not exist when only preventive actions apply")
    def test_preventive_only_network_action(self):
        rows = by_cnec(post_process(rao_result(FLOWS, network_actions=[PREVENTIVE_ACTION])))
        assert rows["c1-preventive"]["networkActionId"] == "na-2"

    @pytest.mark.xfail(strict=True, raises=KeyError,
                       reason="BUG: a result without flow CNEC results (e.g. every CNEC dropped for missing limits) "
                              "crashes on the CNEC merge instead of producing no documents")
    def test_empty_flow_cnec_results(self):
        assert post_process(rao_result([])).empty


class TestAppliedActions:
    def test_elementary_actions_flattened(self):
        action = {"terminalsConnectionActions": [{"networkElementId": "_sw", "actionType": "open"}],
                  "shuntCompensatorPositionActions": [{"networkElementId": "_sh", "sectionCount": 0}]}
        assert list(_elementary_actions(action)) == [
            {"elementaryType": "terminalsConnection", "networkElementId": "_sw", "actionType": "open", "sectionCount": None},
            {"elementaryType": "shuntCompensatorPosition", "networkElementId": "_sh", "actionType": None, "sectionCount": 0}]
        assert list(_elementary_actions({"terminalsConnectionActions": None})) == []

    @pytest.mark.parametrize("values, expected", [
        ([None, None], None), (["open", "open", None], "open"), (["open", "close"], ["close", "open"]), ([0], 0)])
    def test_collapse(self, values, expected):
        assert _collapse(values) == expected

    def test_one_entry_per_activated_state(self):
        rao = {"networkActionResults": [{"networkActionId": "na-1", "activatedStates": [
            {"instant": "curative", "contingency": "co-1"}, {"instant": "curative", "contingency": "co-2"}]}]}
        entries = build_applied_actions(rao, CRAC)
        assert [(e["contingencyId"], e["label"]) for e in entries] == [("co-1", "RA 1 | curative | open"),
                                                                      ("co-2", "RA 1 | curative | open")]
        assert entries[0]["networkElementIds"] == ["_sw-1"] and entries[0]["operator"] == "BE"

    def test_unknown_action_and_missing_states(self):
        entries = build_applied_actions({"networkActionResults": [{"networkActionId": "ghost"}]}, CRAC)
        assert entries == [{"id": "ghost", "name": None, "operator": None, "instant": None, "contingencyId": None,
                            "elementaryType": None, "actionType": None, "sectionCount": None,
                            "networkElementIds": [], "label": "ghost"}]

    def test_mixed_elementary_actions_and_zero_section_count(self):
        crac = {"networkActions": [{"id": "na", "name": "Mixed", "shuntCompensatorPositionActions": [
            {"networkElementId": "_sh", "sectionCount": 0}], "terminalsConnectionActions": [
            {"networkElementId": "_sw", "actionType": "close"}]}]}
        entry = build_applied_actions({"networkActionResults": [{"networkActionId": "na", "activatedStates": [
            {"instant": "preventive"}]}]}, crac)[0]
        assert entry["elementaryType"] == ["shuntCompensatorPosition", "terminalsConnection"]
        assert entry["label"] == "Mixed | preventive | close"

    def test_range_actions_are_not_reported(self):
        assert build_applied_actions({"rangeActionResults": [{"rangeActionId": "pst"}]}, CRAC) == []


class TestVoltageHelpers:
    @pytest.mark.parametrize("value, expected", [
        (None, None), (math.nan, None), (math.inf, None), (np.float64(1.5), 1.5), (2, 2.0), ("x", "x")])
    def test_finite(self, value, expected):
        assert _finite(value) == expected

    @pytest.mark.parametrize("element_id", ["vl", "_vl", "__vl"])
    def test_lookup_tolerates_underscore_prefix(self, element_id):
        assert _lookup_element({"vl": {"country": "BE"}}, element_id) == {"country": "BE"}

    def test_lookup_prefixed_meta(self):
        assert _lookup_element({"_vl": {"country": "NL"}}, "vl") == {"country": "NL"}
        assert _lookup_element({}, None) == {}

    def test_voltage_level_meta(self):
        network = SimpleNamespace(
            get_voltage_levels=lambda: pd.DataFrame({"substation_id": ["s1", "missing"], "nominal_v": [380.0, np.nan]},
                                                    index=["vl-1", "vl-2"]),
            get_substations=lambda: pd.DataFrame({"name": ["Brussels"], "country": ["BE"]}, index=["s1"]))
        assert _voltage_level_meta(network) == {
            "vl-1": {"nominal_v": 380.0, "substation_name": "Brussels", "country": "BE"},
            "vl-2": {"nominal_v": None, "substation_name": None, "country": None}}

    def test_voltage_level_meta_tolerates_network_errors(self, loguru_messages):
        def broken():
            raise RuntimeError("no network")
        assert _voltage_level_meta(SimpleNamespace(get_voltage_levels=broken, get_substations=broken)) == {}


VOLTAGE_CRAC = {
    "contingencies": [{"id": "co-1", "name": "CO 1"}],
    "voltageCnecs": [
        {"id": "vc-both", "name": "VL 400 voltage", "networkElementId": "_vl-1", "operator": "TSO",
         "instant": "curative", "contingencyId": "co-1", "thresholds": [{"unit": "kilovolt", "min": 380.0, "max": 420.0}]},
        {"id": "vc-high", "name": "VL high", "networkElementId": "_vl-1", "operator": "TSO",
         "instant": "curative", "contingencyId": "co-1", "thresholds": [{"unit": "kilovolt", "max": 410.0}]},
    ],
}
HEADERS = {"time-horizon": "1D", "scenario-time": "2024-06-13T09:30:00+00:00", "run-id": "run-1", "username": "op"}


def voltage_network():
    return SimpleNamespace(
        get_voltage_levels=lambda: pd.DataFrame({"substation_id": ["s1"], "nominal_v": [400.0]}, index=["_vl-1"]),
        get_substations=lambda: pd.DataFrame({"name": ["Sub"], "country": ["BE"]}, index=["s1"]))


class TestVoltageDocuments:
    def documents(self, rows, applied=()):
        return build_voltage_documents(pd.DataFrame(rows), VOLTAGE_CRAC, voltage_network(), HEADERS, list(applied))

    def test_one_document_per_limit_direction(self):
        docs = self.documents([{"cnec_id": "vc-both", "min_voltage": 385.0, "max_voltage": 425.0, "margin": -5.0}],
                              applied=[{"id": "na-1"}])
        high, low = docs
        assert (high["limit_type"], high["v_mag"], high["is_violation"], high["margin"]) == ("HIGH_VOLTAGE", 425.0, True, -5.0)
        assert high["v_mag_percent"] == pytest.approx(425.0 / 420.0 * 100)
        assert (low["limit_type"], low["v_mag"], low["is_violation"], low["margin"]) == ("LOW_VOLTAGE", 385.0, False, 5.0)
        assert high["margin_reported"] == -5.0
        assert (high["subject_id"], high["substation_name"], high["country"], high["nominal_v"]) == ("_vl-1", "Sub", "BE", 400.0)
        assert (high["contingency_name"], high["@time_horizon"], high["@scenario_timestamp"]) == (
            "CO 1", "1D", "2024-06-13T09:30:00+00:00")
        assert high["metadata"]["run_id"] == "run-1" and high["metadata"]["username"] == "op"
        assert high["raoAppliedActionCount"] == 1 and high["state"] == "POST_RAO"

    def test_one_sided_threshold_yields_one_document(self):
        docs = self.documents([{"cnec_id": "vc-high", "min_voltage": 405.0, "max_voltage": 410.0, "margin": 0.0}])
        assert [(d["limit_type"], d["is_violation"]) for d in docs] == [("HIGH_VOLTAGE", False)]  # strict inequality

    def test_non_finite_voltage_and_unknown_cnec_skipped(self):
        docs = self.documents([{"cnec_id": "vc-both", "min_voltage": np.nan, "max_voltage": 400.0, "margin": 20.0},
                               {"cnec_id": "unknown", "min_voltage": 1.0, "max_voltage": 1.0, "margin": 0.0}])
        assert [d["limit_type"] for d in docs] == ["HIGH_VOLTAGE"]


@pytest.mark.integration
def test_post_process_real_openrao_result(tc1_network, tc1_crac, tc1_crac_buffer, lf_settings):
    from rao.optimizer import Optimizer
    from rao.parameters.manager import RaoSettingsManager
    optimizer = Optimizer(network=tc1_network, crac_source=tc1_crac_buffer,
                          parameters_source=RaoSettingsManager().to_bytesio(),
                          loadflow_parameters=lf_settings.build_pypowsybl_parameters())
    optimizer.run()
    result = optimizer.results.to_json()
    frame = post_process(result, tc1_crac)
    assert len(frame) == len(tc1_crac["flowCnecs"])
    curative = frame[frame["cnec.instant"] == "curative"]
    assert set(curative["networkActionId"]) == {"eadf1901-f858-4a05-81e8-0665641d6237"}
    assert set(curative["contingency.name"]) == {"OCO_BE-Line_1"}
    flow, limit_max, limit_min = (curative["cnecResults.curative.ampere.side1.flow"], curative["cnec.thresholds.max"],
                                  curative["cnec.thresholds.min"])
    expected = np.where(flow >= 0, flow / limit_max, flow / limit_min)
    assert np.allclose(curative["cnecResults.curative.ampere.side1.loading"], expected)
    assert (curative["cnecResults.curative.ampere.side1.loading"] >= 0).all()
    applied = build_applied_actions(result, tc1_crac)
    assert [(a["name"], a["contingencyId"], a["actionType"]) for a in applied] == [
        ("RA_NL-Line_3", TC1_CONTINGENCY_BE_LINE_1, "open")]
