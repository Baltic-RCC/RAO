"""OpenRAO JSON CRAC pydantic models (rao/crac/models.py) and the 3W workaround context."""
import math

import pandas as pd
import pytest
from pydantic import ValidationError

from helpers import TSO_BE, TSO_PL
from rao.crac import models
from rao.crac.context import CracWorkaroundContext


def flow_cnec(**overrides):
    fields = dict(id="cnec-1-preventive", name="Line 1", description="", networkElementId="line-1",
                  operator=TSO_BE, thresholds=[models.Threshold(unit="ampere", min=-1000, max=1000)])
    fields.update(overrides)
    return models.FlowCnec(**fields)


def voltage_cnec(**overrides):
    fields = dict(id="vl-1-voltage-curative-co-1", name="VL voltage", networkElementId="vl-1", operator=TSO_BE,
                  thresholds=[models.MonitoringThreshold(min=380.0, max=420.0)], instant="curative",
                  contingencyId="co-1")
    fields.update(overrides)
    return models.VoltageCnec(**fields)


class TestNetworkElementPrefix:
    """IDs come from triplets without the leading '_' and are re-prefixed for pypowsybl (rdfID import)."""

    def test_flow_cnec_element_is_prefixed(self):
        assert flow_cnec().model_dump()["networkElementId"] == "_line-1"

    def test_contingency_elements_are_prefixed(self):
        contingency = models.Contingency(id="co-1", name="CO", networkElementsIds=["a", "b"])
        assert contingency.model_dump()["networkElementsIds"] == ["_a", "_b"]

    def test_contingency_already_prefixed_gets_double_underscore(self):
        # Edge: the serializer does not normalise; builders must strip first (see _normalize_grid_element_id).
        contingency = models.Contingency(id="co-1", name="CO", networkElementsIds=["_a"])
        assert contingency.model_dump()["networkElementsIds"] == ["__a"]

    def test_voltage_and_angle_cnec_elements_are_prefixed(self):
        assert voltage_cnec().model_dump()["networkElementId"] == "_vl-1"
        angle = models.AngleCnec(id="a", name="a", exportingNetworkElementId="bb-1", importingNetworkElementId="bb-2",
                                 operator="", thresholds=[models.MonitoringThreshold(unit="degree", max=30.0)])
        dumped = angle.model_dump()
        assert (dumped["exportingNetworkElementId"], dumped["importingNetworkElementId"]) == ("_bb-1", "_bb-2")

    def test_actions_are_prefixed(self):
        assert models.TerminalsAction(networkElementId="sw-1").model_dump()["networkElementId"] == "_sw-1"
        assert models.ShuntCompensatorPositionAction(networkElementId="sh-1").model_dump()["networkElementId"] == "_sh-1"


class TestThreshold:
    @pytest.mark.parametrize("minimum, maximum, valid", [
        (-100.0, 100.0, True),
        (0, 100.0, False),            # one-sided flow limits are treated as missing
        (-100.0, 0, False),
        (math.nan, 100.0, False),
        (-100.0, math.nan, False),
    ])
    def test_is_valid(self, minimum, maximum, valid):
        assert models.Threshold(min=minimum, max=maximum).is_valid() is valid

    def test_defaults_are_invalid_ampere_side_one(self):
        threshold = models.Threshold()
        assert (threshold.unit, threshold.side, threshold.is_valid()) == ("ampere", 1, False)

    def test_unknown_unit_rejected(self):
        with pytest.raises(ValidationError):
            models.Threshold(unit="degree")


class TestMonitoringThreshold:
    @pytest.mark.parametrize("minimum, maximum, valid", [
        (380.0, 420.0, True),
        (None, 420.0, True),
        (380.0, None, True),
        (0.0, None, True),            # unlike flow thresholds, zero is a valid voltage/angle bound
        (None, None, False),
        (math.nan, 420.0, False),
    ])
    def test_is_valid(self, minimum, maximum, valid):
        assert models.MonitoringThreshold(min=minimum, max=maximum).is_valid() is valid


class TestTerminalsAction:
    @pytest.mark.parametrize("normal_value, expected", [
        ("0.0", "open"), ("0", "open"), (0, "open"),
        ("1.0", "close"), ("1", "close"), (1.0, "close"),
        ("open", "open"), ("close", "close"),
        (math.nan, "close"),          # edge: NaN is truthy -> close
    ])
    def test_normal_value_maps_to_action_type(self, normal_value, expected):
        assert models.TerminalsAction(networkElementId="sw", normalValue=normal_value).actionType == expected

    @pytest.mark.parametrize("normal_value", ["true", "Open"])
    def test_non_numeric_non_literal_rejected(self, normal_value):
        with pytest.raises(ValidationError):
            models.TerminalsAction(networkElementId="sw", normalValue=normal_value)

    def test_dump_uses_action_type_key(self):
        assert models.TerminalsAction(networkElementId="sw", normalValue="1").model_dump() == {
            "networkElementId": "_sw", "actionType": "close"}


class TestShuntCompensatorPositionAction:
    @pytest.mark.parametrize("normal_value", ["2", "2.0", 2.0, 2])
    def test_section_count_from_triplets_string(self, normal_value):
        assert models.ShuntCompensatorPositionAction(networkElementId="sh", normalValue=normal_value).sectionCount == 2

    def test_fractional_section_count_rejected(self):
        with pytest.raises(ValidationError):
            models.ShuntCompensatorPositionAction(networkElementId="sh", normalValue="2.5")


class TestNetworkAction:
    def test_empty_action_lists_become_none_and_are_dropped(self):
        action = models.NetworkAction(id="ra", name="RA", operator=TSO_BE, onInstantUsageRules=[{"instant": "curative"}],
                                      terminalsConnectionActions=[], shuntCompensatorPositionActions=[])
        assert action.terminalsConnectionActions is None
        assert "terminalsConnectionActions" not in action.model_dump(exclude_none=True)


class TestFlowCnec:
    def test_description_is_required_but_never_serialised(self):
        with pytest.raises(ValidationError):
            models.FlowCnec(id="c", name="c", networkElementId="e", operator="o", thresholds=[])
        assert "description" not in flow_cnec(description="text").model_dump()

    def test_auto_instant_not_supported(self):
        with pytest.raises(ValidationError):
            flow_cnec(instant="auto")


class TestCracSerialisation:
    def test_header_and_aliases(self):
        dumped = models.Crac().model_dump(exclude_none=True, by_alias=True)
        assert dumped["type"] == "CRAC" and dumped["version"] == "2.11"
        assert [i["kind"] for i in dumped["instants"]] == ["PREVENTIVE", "OUTAGE", "CURATIVE"]
        assert "ra-usage-limits-per-instant" in dumped
        assert "angleCnecs" not in dumped  # opt-in angle monitoring stays absent

    def test_flow_cnec_without_valid_threshold_is_dropped(self, loguru_messages):
        crac = models.Crac(flowCnecs=[flow_cnec(), flow_cnec(id="x", thresholds=[models.Threshold()])])
        assert [c["id"] for c in crac.model_dump(by_alias=True)["flowCnecs"]] == ["cnec-1-preventive"]
        assert any("CNEC excluded due to no valid thresholds" in r["message"] for r in loguru_messages)

    def test_invalid_thresholds_filtered_valid_kept(self):
        cnec = flow_cnec(thresholds=[models.Threshold(), models.Threshold(min=-5, max=5, unit="megawatt")])
        dumped = models.Crac(flowCnecs=[cnec]).model_dump(by_alias=True)["flowCnecs"][0]
        assert dumped["thresholds"] == [{"unit": "megawatt", "min": -5.0, "max": 5.0, "side": 1}]

    def test_polish_apparent_power_cnec_dropped(self):
        cnec = flow_cnec(operator=TSO_PL, thresholds=[models.Threshold(unit="apparent", min=-5, max=5)])
        assert models.Crac(flowCnecs=[cnec]).model_dump(by_alias=True)["flowCnecs"] == []

    def test_voltage_cnec_without_valid_threshold_dropped(self):
        crac = models.Crac(voltageCnecs=[voltage_cnec(), voltage_cnec(id="bad", thresholds=[models.MonitoringThreshold()])])
        dumped = crac.model_dump(exclude_none=True, by_alias=True)["voltageCnecs"]
        assert [c["id"] for c in dumped] == ["vl-1-voltage-curative-co-1"]
        assert dumped[0]["optimized"] is False and dumped[0]["monitored"] is True

    def test_one_sided_voltage_threshold_keeps_only_that_bound(self):
        crac = models.Crac(voltageCnecs=[voltage_cnec(thresholds=[models.MonitoringThreshold(max=420.0)])])
        assert crac.model_dump(exclude_none=True, by_alias=True)["voltageCnecs"][0]["thresholds"] == [
            {"unit": "kilovolt", "max": 420.0}]


class TestCracWorkaroundContext:
    legs = pd.DataFrame({"rated_u1": [400.0]}, index=["tr-Leg1"])

    @pytest.mark.parametrize("enabled, frame, expected", [
        (True, legs, True),
        (False, legs, False),
        (True, None, False),
        (True, legs.iloc[0:0], False),
        (True, pd.DataFrame(index=["tr-Leg1"]), False),  # edge: a column-less frame counts as empty
    ])
    def test_has_3w_replacement(self, enabled, frame, expected):
        assert CracWorkaroundContext(enabled, frame).has_3w_replacement() is expected

    def test_default_context_is_inactive(self):
        assert CracWorkaroundContext().has_3w_replacement() is False
