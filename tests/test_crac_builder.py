"""CracBuilder (rao/crac/builder.py) on synthetic triplets.

Each test builds only the NC-profile / EQ objects the step under test reads, so the
edge cases (missing attributes, missing profiles, unit fallbacks, 3W replacement) are
explicit. Confirmed defects are strict xfails that assert the intended behaviour.
"""
import pandas as pd
import pytest

from helpers import (DIRECTION_NONE, DIRECTION_UP_AND_DOWN, KIND_CURATIVE, KIND_PREVENTIVE, LIMIT_HIGH_VOLTAGE,
                     LIMIT_LOW_VOLTAGE, LIMIT_PATL, LIMIT_TATL, TSO_BE, concat_triplets, logged, make_triplets)
from rao.crac import models
from rao.crac.builder import CracBuilder
from rao.crac.context import CracWorkaroundContext

AUTHORITY_BE = "http://www.elia.be/OperationalPlanning"


# --- synthetic profile builders -------------------------------------------------------

def assessed_element(mrid, equipment, **overrides):
    attributes = {
        "IdentifiedObject.mRID": mrid,
        "IdentifiedObject.name": f"AE {mrid}",
        "IdentifiedObject.description": f"description {mrid}",
        "AssessedElement.ConductingEquipment": equipment,
        "AssessedElement.AssessedSystemOperator": TSO_BE,
        "AssessedElement.normalEnabled": "true",
        "AssessedElement.inBaseCase": "true",
        "AssessedElement.SecuredForRegion": "https://energy.referencedata.eu/EIC/10Y1001C--00059P",
    }
    attributes.update(overrides)
    return {mrid: ("AssessedElement", attributes)}


def contingency(mrid, equipment_ids, must_study="true", contingency_type="OrdinaryContingency"):
    objects = {mrid: (contingency_type, {"IdentifiedObject.mRID": mrid, "IdentifiedObject.name": f"CO {mrid}",
                                         "Contingency.normalMustStudy": must_study})}
    for n, equipment in enumerate(equipment_ids):
        objects[f"{mrid}-ce{n}"] = ("ContingencyEquipment", {
            "IdentifiedObject.mRID": f"{mrid}-ce{n}",
            "IdentifiedObject.name": f"CE {equipment}",
            "ContingencyElement.Contingency": mrid,
            "ContingencyEquipment.Equipment": equipment,
        })
    return objects


TOPOLOGY = ("TopologyAction", "TopologyAction.Equipment")
SHUNT = ("ShuntCompensatorModification", "ShuntCompensatorModification.ShuntCompensator")
TAP = ("TapPositionAction", "TapPositionAction.TapChanger")


def remedial_action(mrid, kind, alterations):
    """``alterations``: ``{alteration_id: (TYPE, equipment_id, (direction, normalValue) | None)}``."""
    objects = {mrid: ("GridStateAlterationRemedialAction", {
        "IdentifiedObject.mRID": mrid,
        "IdentifiedObject.name": f"RA {mrid}",
        "RemedialAction.kind": kind,
        "RemedialAction.RemedialActionSystemOperator": TSO_BE,
    })}
    for alteration_id, ((alteration_type, equipment_key), equipment, property_range) in alterations.items():
        objects[alteration_id] = (alteration_type, {
            "IdentifiedObject.mRID": alteration_id,
            "IdentifiedObject.name": f"ALT {alteration_id}",
            "GridStateAlteration.GridStateAlterationRemedialAction": mrid,
            equipment_key: equipment,
        })
        if property_range is not None:
            direction, normal_value = property_range
            objects[f"{alteration_id}-range"] = ("StaticPropertyRange", {
                "IdentifiedObject.mRID": f"{alteration_id}-range",
                "RangeConstraint.GridStateAlteration": alteration_id,
                "RangeConstraint.direction": direction,
                "RangeConstraint.normalValue": normal_value,
            })
    return objects


def limit_set(equipment, terminal, node, limits, voltage_kv=400.0):
    """Terminal-based OperationalLimitSet with limits ``[(LimitClass, type_id, value)]`` and an SvVoltage."""
    objects = {
        terminal: ("Terminal", {"Terminal.ConductingEquipment": equipment, "Terminal.TopologicalNode": node}),
        f"ols-{equipment}": ("OperationalLimitSet", {"OperationalLimitSet.Terminal": terminal}),
    }
    if voltage_kv is not None:
        objects[f"sv-{node}"] = ("SvVoltage", {"SvVoltage.TopologicalNode": node, "SvVoltage.v": str(voltage_kv)})
    for n, (limit_class, limit_type, value) in enumerate(limits):
        objects[f"lim-{equipment}-{n}"] = (limit_class, {
            "OperationalLimit.OperationalLimitSet": f"ols-{equipment}",
            "OperationalLimit.OperationalLimitType": limit_type,
            f"{limit_class}.value": value,
        })
    return objects


LIMIT_TYPES = {
    "olt-patl": ("OperationalLimitType", {"OperationalLimitType.limitType": LIMIT_PATL}),
    "olt-tatl": ("OperationalLimitType", {"OperationalLimitType.limitType": LIMIT_TATL}),
}

GRID = {
    "line-1": ("ACLineSegment", {"IdentifiedObject.name": "Line 1"}),
    "line-2": ("ACLineSegment", {"IdentifiedObject.name": "Line 2"}),
    "sw-1": ("Breaker", {"IdentifiedObject.name": "Switch 1"}),
    "sh-1": ("LinearShuntCompensator", {"IdentifiedObject.name": "Shunt 1"}),
    "tr3": ("PowerTransformer", {"IdentifiedObject.name": "3W transformer"}),
}


@pytest.fixture
def grid():
    return make_triplets(GRID, instance="eq", label="BE_EQ.xml")


def builder_for(data, network, workaround=None) -> CracBuilder:
    builder = CracBuilder(data=data, network=network, workaround=workaround)
    builder._crac = models.Crac()
    return builder


def valid_flow_cnec(cnec_id, element, instant="preventive", contingency_id=None):
    return models.FlowCnec(id=cnec_id, name=cnec_id, description="", networkElementId=element, operator=TSO_BE,
                           thresholds=[models.Threshold(min=-100, max=100)], instant=instant,
                           contingencyId=contingency_id)


# --- construction / boundary ---------------------------------------------------------

class TestConstruction:
    def test_boundary_set_excluded_after_base_voltages_captured(self):
        boundary = make_triplets({"bv-400": ("BaseVoltage", {"BaseVoltage.nominalVoltage": "400"})},
                                 instance="bd", label="20171002T0930Z_ENTSOE_EQ_BD_2.xml")
        builder = CracBuilder(data=make_triplets({}), network=concat_triplets(boundary, make_triplets(GRID, "eq")))
        assert builder.base_voltages == {"bv-400": 400}
        assert set(builder.network["INSTANCE_ID"]) == {"eq"}

    def test_boundary_label_with_hyphenated_entso_e_is_not_excluded(self):
        # Edge: only labels containing "ENTSOE" are excluded; CGMES 3 BDS files such as
        # TC1's "..._ENTSO-E_EQ_BD_2.xml" stay in the network triplets.
        boundary = make_triplets({"bv-220": ("BaseVoltage", {"BaseVoltage.nominalVoltage": "220"})},
                                 instance="bd", label="20171002T0930Z_ENTSO-E_EQ_BD_2.xml")
        builder = CracBuilder(data=make_triplets({}), network=boundary)
        assert set(builder.network["INSTANCE_ID"]) == {"bd"}

    def test_missing_base_voltages_logged(self, grid, loguru_messages):
        assert CracBuilder(data=make_triplets({}), network=grid).base_voltages == {}
        assert "BaseVoltage nominal voltages not available in network model" in logged(loguru_messages, "WARNING")

    @pytest.mark.xfail(strict=True, raises=AttributeError,
                       reason="BUG: network is typed `pd.DataFrame | None` and get_limits() guards None, "
                              "but __init__ dereferences it for the boundary filter")
    def test_network_none_is_accepted(self):
        builder = CracBuilder(data=make_triplets({}), network=None)
        assert builder.get_limits() is None

    def test_crac_property_before_build_is_none(self, grid, loguru_messages):
        assert CracBuilder(data=make_triplets({}), network=grid).crac is None
        assert any("CRAC model is not built yet" in m for m in logged(loguru_messages, "ERROR"))


class TestNormalizeGridElementId:
    @pytest.mark.parametrize("raw, expected", [("__a", "a"), ("_a", "a"), ("a", "a"), (None, None), (123, "123")])
    def test_strips_all_leading_underscores(self, raw, expected):
        # Docstring says "collapse to a single underscore"; the code strips all of them and
        # relies on the model serializer to add exactly one back.
        assert CracBuilder._normalize_grid_element_id(raw) == expected

    def test_serialised_contingency_element_has_single_underscore(self):
        normalized = CracBuilder._normalize_grid_element_id("__a")
        dumped = models.Contingency(id="c", name="c", networkElementsIds=[normalized]).model_dump()
        assert dumped["networkElementsIds"] == ["_a"]


# --- contingencies --------------------------------------------------------------------

class TestProcessContingencies:
    def test_groups_equipment_per_contingency(self, grid):
        data = make_triplets(contingency("co-1", ["line-1", "line-2"]) | contingency("co-2", ["line-2"]))
        builder = builder_for(data, grid)
        builder.process_contingencies()
        assert {c.id: c.networkElementsIds for c in builder._crac.contingencies} == {
            "co-1": ["line-1", "line-2"], "co-2": ["line-2"]}
        assert builder._crac.contingencies[0].name == "CO co-1"

    def test_filter_by_specific_ids(self, grid):
        data = make_triplets(contingency("co-1", ["line-1"]) | contingency("co-2", ["line-2"]))
        builder = builder_for(data, grid)
        builder.process_contingencies(specific_contingencies=["co-2"])
        assert [c.id for c in builder._crac.contingencies] == ["co-2"]

    def test_unknown_id_yields_no_contingency(self, grid, loguru_messages):
        builder = builder_for(make_triplets(contingency("co-1", ["line-1"])), grid)
        builder.process_contingencies(specific_contingencies=["unknown"])
        assert builder._crac.contingencies == []
        assert any("No contingencies found for specified IDs" in m for m in logged(loguru_messages, "WARNING"))

    def test_equipment_missing_from_network_is_kept_with_warning(self, grid, loguru_messages):
        builder = builder_for(make_triplets(contingency("co-1", ["not-in-model"])), grid)
        builder.process_contingencies()
        assert builder._crac.contingencies[0].networkElementsIds == ["not-in-model"]
        assert any("does not exist in network model" in m for m in logged(loguru_messages, "WARNING"))

    def test_normal_must_study_false_is_not_filtered(self, grid):
        builder = builder_for(make_triplets(contingency("co-1", ["line-1"], must_study="false")), grid)
        builder.process_contingencies()
        assert [c.id for c in builder._crac.contingencies] == ["co-1"]

    def test_contingency_without_normal_must_study_is_dropped(self, grid):
        data = make_triplets(contingency("co-1", ["line-1"]) | contingency("co-2", ["line-2"], must_study=None))
        builder = builder_for(data, grid)
        builder.process_contingencies()
        assert [c.id for c in builder._crac.contingencies] == ["co-1"]

    def test_missing_contingency_profile_fails_fast(self, grid):
        builder = builder_for(make_triplets(assessed_element("ae-1", "line-1")), grid)
        with pytest.raises(AttributeError):
            builder.process_contingencies()


# --- flow CNECs -------------------------------------------------------------------------

class TestProcessCnecs:
    def build(self, grid, *elements, contingencies=("co-1",)):
        data = {}
        for element in elements:
            data |= element
        for contingency_id in contingencies:
            data |= contingency(contingency_id, ["line-2"])
        builder = builder_for(make_triplets(data), grid)
        builder.process_contingencies()
        builder.process_cnecs()
        return builder

    def test_preventive_and_curative_copies(self, grid):
        cnecs = self.build(grid, assessed_element("ae-1", "line-1"))._crac.flowCnecs
        assert [(c.id, c.instant, c.contingencyId) for c in cnecs] == [
            ("ae-1-preventive", "preventive", None), ("ae-1-curative", "curative", "co-1")]
        assert cnecs[0].networkElementId == "line-1" and cnecs[0].description == "description ae-1"

    def test_not_in_base_case_gives_curative_only(self, grid):
        cnecs = self.build(grid, assessed_element("ae-1", "line-1", **{"AssessedElement.inBaseCase": "false"}))._crac.flowCnecs
        assert [c.instant for c in cnecs] == ["curative"]

    def test_normal_enabled_false_excluded(self, grid, loguru_messages):
        builder = self.build(grid, assessed_element("ae-1", "line-1", **{"AssessedElement.normalEnabled": "false"}))
        assert builder._crac.flowCnecs == []
        assert any("'normalEnabled' is false or missing" in m for m in logged(loguru_messages, "WARNING"))

    def test_normal_enabled_absent_everywhere_excludes_all(self, grid):
        builder = self.build(grid, assessed_element("ae-1", "line-1", **{"AssessedElement.normalEnabled": None}))
        assert builder._crac.flowCnecs == []

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason="BUG: when other AEs carry normalEnabled, a missing value arrives as None and "
                              "`None == 'false'` is False, so the AE is included despite the 'false or missing' rule")
    def test_normal_enabled_missing_in_mixed_profile_excluded(self, grid):
        builder = self.build(grid, assessed_element("ae-1", "line-1"),
                             assessed_element("ae-2", "line-2", **{"AssessedElement.normalEnabled": None}))
        assert {c.networkElementId for c in builder._crac.flowCnecs} == {"line-1"}

    @pytest.mark.xfail(strict=True, raises=AttributeError,
                       reason="BUG: a missing inBaseCase in a mixed profile is None and `.lower()` crashes")
    def test_in_base_case_missing_in_mixed_profile_means_curative_only(self, grid):
        builder = self.build(grid, assessed_element("ae-1", "line-1"),
                             assessed_element("ae-2", "line-2", **{"AssessedElement.inBaseCase": None}))
        assert [c.instant for c in builder._crac.flowCnecs if c.networkElementId == "line-2"] == ["curative"]

    def test_secured_and_scanned_regions_map_to_optimized_and_monitored(self, grid):
        builder = self.build(
            grid,
            assessed_element("ae-1", "line-1"),
            assessed_element("ae-2", "line-2", **{"AssessedElement.SecuredForRegion": None,
                                                 "AssessedElement.ScannedForRegion": "region"}),
        )
        flags = {c.networkElementId: (c.optimized, c.monitored) for c in builder._crac.flowCnecs}
        assert flags == {"line-1": (True, False), "line-2": (False, True)}

    def test_element_not_in_network_dropped(self, grid, loguru_messages):
        builder = self.build(grid, assessed_element("ae-1", "missing-line"))
        assert builder._crac.flowCnecs == []
        assert "Assessed element does not exist in network model: AE ae-1" in logged(loguru_messages, "WARNING")

    def test_limit_based_assessed_element_ignored(self, grid):
        builder = self.build(grid, assessed_element("ae-1", "line-1"),
                             assessed_element("ae-v", None, **{"AssessedElement.OperationalLimit": "vlim-1"}))
        assert {c.id.rsplit("-", 1)[0] for c in builder._crac.flowCnecs} == {"ae-1"}

    def test_missing_description_in_mixed_profile_defaults_to_empty(self, grid):
        builder = self.build(grid, assessed_element("ae-1", "line-1", **{"IdentifiedObject.description": None}),
                             assessed_element("ae-2", "line-2"))
        assert {c.id: c.description for c in builder._crac.flowCnecs}["ae-1-preventive"] == ""

    @pytest.mark.xfail(strict=True, raises=KeyError,
                       reason="BUG: IdentifiedObject.description is indexed directly; a profile where no AE has a "
                              "description (e.g. TC1) raises KeyError")
    def test_profile_without_any_description(self, grid):
        builder = self.build(grid, assessed_element("ae-1", "line-1", **{"IdentifiedObject.description": None}))
        assert builder._crac.flowCnecs[0].description == ""

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason="BUG: curative CNEC id is '{mRID}-curative' for every contingency, so a CRAC with "
                              "several contingencies has duplicate CNEC ids (OpenRAO rejects it)")
    def test_curative_cnec_ids_unique_across_contingencies(self, grid):
        builder = self.build(grid, assessed_element("ae-1", "line-1"), contingencies=("co-1", "co-2"))
        ids = [c.id for c in builder._crac.flowCnecs]
        assert len(ids) == len(set(ids))

    def test_missing_assessed_element_profile_fails_fast(self, grid):
        builder = builder_for(make_triplets(contingency("co-1", ["line-1"])), grid)
        with pytest.raises(AttributeError):
            builder.process_cnecs()


# --- voltage CNECs from EQ --------------------------------------------------------------

@pytest.fixture
def voltage_network():
    boundary = make_triplets({
        "bv-400": ("BaseVoltage", {"BaseVoltage.nominalVoltage": "400"}),
        "bv-110": ("BaseVoltage", {"BaseVoltage.nominalVoltage": "110"}),
    }, instance="bd", label="BD_ENTSOE_EQ.xml")
    eq = make_triplets({
        "vl-400": ("VoltageLevel", {"IdentifiedObject.name": "VL 400", "VoltageLevel.BaseVoltage": "bv-400"}),
        "vl-110": ("VoltageLevel", {"IdentifiedObject.name": "VL 110", "VoltageLevel.BaseVoltage": "bv-110"}),
        "vl-nobv": ("VoltageLevel", {"IdentifiedObject.name": "VL unknown"}),
        "bay-1": ("Bay", {"Bay.VoltageLevel": "vl-400"}),
        "bb-1": ("BusbarSection", {"Equipment.EquipmentContainer": "vl-400"}),
        "bb-2": ("BusbarSection", {"Equipment.EquipmentContainer": "bay-1"}),
        "bb-3": ("BusbarSection", {"Equipment.EquipmentContainer": "vl-110"}),
        "bb-4": ("BusbarSection", {"Equipment.EquipmentContainer": "vl-nobv"}),
        "bb-orphan": ("BusbarSection", {}),
        "olt-high": ("OperationalLimitType", {"OperationalLimitType.limitType": LIMIT_HIGH_VOLTAGE,
                                              "OperationalLimitType.isInfiniteDuration": "true"}),
        "olt-low": ("OperationalLimitType", {"OperationalLimitType.limitType": LIMIT_LOW_VOLTAGE}),
        "olt-high-temp": ("OperationalLimitType", {"OperationalLimitType.limitType": LIMIT_HIGH_VOLTAGE,
                                                   "OperationalLimitType.isInfiniteDuration": "false"}),
        **{f"ols-{bb}": ("OperationalLimitSet", {"OperationalLimitSet.Equipment": bb})
           for bb in ("bb-1", "bb-2", "bb-3", "bb-4", "bb-orphan")},
        "vlim-1": ("VoltageLimit", {"OperationalLimit.OperationalLimitSet": "ols-bb-1",
                                    "OperationalLimit.OperationalLimitType": "olt-high", "VoltageLimit.value": "420"}),
        "vlim-2": ("VoltageLimit", {"OperationalLimit.OperationalLimitSet": "ols-bb-2",
                                    "OperationalLimit.OperationalLimitType": "olt-high", "VoltageLimit.value": "415"}),
        "vlim-3": ("VoltageLimit", {"OperationalLimit.OperationalLimitSet": "ols-bb-1",
                                    "OperationalLimit.OperationalLimitType": "olt-low", "VoltageLimit.value": "360"}),
        "vlim-4": ("VoltageLimit", {"OperationalLimit.OperationalLimitSet": "ols-bb-2",
                                    "OperationalLimit.OperationalLimitType": "olt-low", "VoltageLimit.value": "370"}),
        "vlim-5": ("VoltageLimit", {"OperationalLimit.OperationalLimitSet": "ols-bb-1",
                                    "OperationalLimit.OperationalLimitType": "olt-high-temp", "VoltageLimit.value": "400"}),
        "vlim-6": ("VoltageLimit", {"OperationalLimit.OperationalLimitSet": "ols-bb-1",
                                    "OperationalLimit.OperationalLimitType": "olt-high", "VoltageLimit.value": "0"}),
        "vlim-7": ("VoltageLimit", {"OperationalLimit.OperationalLimitSet": "ols-bb-3",
                                    "OperationalLimit.OperationalLimitType": "olt-high", "VoltageLimit.value": "123"}),
        "vlim-8": ("VoltageLimit", {"OperationalLimit.OperationalLimitSet": "ols-bb-4",
                                    "OperationalLimit.OperationalLimitType": "olt-high", "VoltageLimit.value": "250"}),
        "vlim-9": ("VoltageLimit", {"OperationalLimit.OperationalLimitSet": "ols-bb-orphan",
                                    "OperationalLimit.OperationalLimitType": "olt-high", "VoltageLimit.value": "999"}),
    }, instance="eq", label="BE_EQ.xml", header={"Model.modelingAuthoritySet": AUTHORITY_BE})
    return concat_triplets(boundary, eq)


class TestVoltageCnecsFromNetwork:
    def build(self, network, contingencies=("co-1", "co-2")):
        data = {}
        for contingency_id in contingencies:
            data |= contingency(contingency_id, ["bb-1"])
        builder = builder_for(make_triplets(data), network)
        if contingencies:
            builder.process_contingencies()
        builder.process_voltage_cnecs_from_network()
        return builder

    def test_curative_voltage_cnec_per_voltage_level_and_contingency(self, voltage_network):
        cnecs = self.build(voltage_network)._crac.voltageCnecs
        assert sorted(c.id for c in cnecs) == [
            "vl-400-voltage-curative-co-1", "vl-400-voltage-curative-co-2",
            "vl-nobv-voltage-curative-co-1", "vl-nobv-voltage-curative-co-2"]
        assert {(c.instant, c.optimized, c.monitored) for c in cnecs} == {("curative", False, True)}

    def test_strictest_permanent_limits_per_voltage_level(self, voltage_network):
        cnec = next(c for c in self.build(voltage_network)._crac.voltageCnecs if c.networkElementId == "vl-400")
        # highs 420 (bb-1) and 415 (bb-2 via Bay) -> min; lows 360/370 -> max; temporary 400 and 0 ignored
        assert (cnec.thresholds[0].max, cnec.thresholds[0].min, cnec.thresholds[0].unit) == (415.0, 370.0, "kilovolt")
        assert cnec.name == "VL 400 voltage" and cnec.operator == AUTHORITY_BE

    def test_below_330kv_skipped_unknown_nominal_kept_one_sided(self, voltage_network):
        crac = self.build(voltage_network).crac
        by_element = {c["networkElementId"]: c for c in crac["voltageCnecs"]}
        assert "_vl-110" not in by_element
        assert by_element["_vl-nobv"]["thresholds"] == [{"unit": "kilovolt", "max": 250.0}]

    def test_no_contingencies_no_voltage_cnecs(self, voltage_network, loguru_messages):
        assert self.build(voltage_network, contingencies=())._crac.voltageCnecs == []
        assert any("No contingencies defined" in m for m in logged(loguru_messages, "WARNING"))

    def test_model_without_voltage_limits(self, grid):
        data = make_triplets(contingency("co-1", ["line-1"]))
        builder = builder_for(data, grid)
        builder.process_contingencies()
        builder.process_voltage_cnecs_from_network()
        assert builder._crac.voltageCnecs == []

    def test_minimum_voltage_threshold_is_configurable(self, voltage_network, monkeypatch):
        monkeypatch.setattr(CracBuilder, "MIN_MONITORED_NOMINAL_VOLTAGE_KV", 100.0)
        assert "vl-110" in {c.networkElementId for c in self.build(voltage_network)._crac.voltageCnecs}


# --- angle CNECs (dormant) -----------------------------------------------------------

def angle_profile(direction, value="30.0", flow_to_reference="true"):
    return make_triplets({
        "olt-angle": ("OperationalLimitType", {"OperationalLimitType.direction": f"http://iec.ch/TC57/CIM100#OperationalLimitDirectionKind.{direction}"}),
        "ols-angle": ("OperationalLimitSet", {"OperationalLimitSet.Terminal": "term-2"}),
        "val-1": ("VoltageAngleLimit", {
            "IdentifiedObject.mRID": "val-1", "IdentifiedObject.name": "Angle 1",
            "OperationalLimit.OperationalLimitSet": "ols-angle", "OperationalLimit.OperationalLimitType": "olt-angle",
            "VoltageAngleLimit.normalValue": value, "VoltageAngleLimit.isFlowToRefTerminal": flow_to_reference,
            "VoltageAngleLimit.AngleReferenceTerminal": "term-1",
        }),
    }, instance="er")


@pytest.fixture
def angle_network():
    return make_triplets({
        "bb-1": ("BusbarSection", {}), "bb-2": ("BusbarSection", {}),
        "term-1": ("Terminal", {"Terminal.ConductingEquipment": "bb-1"}),
        "term-2": ("Terminal", {"Terminal.ConductingEquipment": "bb-2"}),
    })


class TestAngleCnecs:
    def build(self, profile, network):
        builder = builder_for(concat_triplets(profile, make_triplets(contingency("co-1", ["bb-1"]), instance="co")),
                              network)
        builder.process_contingencies()
        builder.process_angle_cnecs_from_data()
        return builder

    @pytest.mark.parametrize("direction, expected", [
        ("absoluteValue", {"unit": "degree", "min": -30.0, "max": 30.0}),
        ("high", {"unit": "degree", "max": 30.0}),
        ("low", {"unit": "degree", "min": -30.0}),
    ])
    def test_threshold_by_direction(self, angle_network, direction, expected):
        crac = self.build(angle_profile(direction), angle_network).crac
        assert [c["id"] for c in crac["angleCnecs"]] == ["val-1-preventive", "val-1-curative"]
        assert crac["angleCnecs"][0]["thresholds"] == [expected]

    @pytest.mark.parametrize("flow_to_reference, exporting, importing", [
        ("true", "_bb-1", "_bb-2"), ("false", "_bb-2", "_bb-1")])
    def test_flow_direction_orders_elements(self, angle_network, flow_to_reference, exporting, importing):
        cnec = self.build(angle_profile("high", flow_to_reference=flow_to_reference), angle_network).crac["angleCnecs"][0]
        assert (cnec["exportingNetworkElementId"], cnec["importingNetworkElementId"]) == (exporting, importing)

    @pytest.mark.parametrize("profile_kwargs, message", [
        ({"direction": "none"}, "unsupported OperationalLimitType direction"),
        ({"direction": "absoluteValue", "value": "-5"}, "normalValue is negative"),
        ({"direction": "absoluteValue", "value": "abc"}, "missing or invalid VoltageAngleLimit normalValue"),
        ({"direction": "high", "flow_to_reference": None}, "isFlowToRefTerminal is missing"),
    ])
    def test_rejected_limits(self, angle_network, loguru_messages, profile_kwargs, message):
        builder = self.build(angle_profile(**profile_kwargs), angle_network)
        assert not builder._crac.angleCnecs
        assert any(message in m for m in logged(loguru_messages, "WARNING"))

    def test_unresolved_terminal_rejected(self, loguru_messages):
        builder = self.build(angle_profile("absoluteValue"), make_triplets({"bb-1": ("BusbarSection", {})}))
        assert not builder._crac.angleCnecs
        assert any("terminal equipment could not be resolved" in m for m in logged(loguru_messages, "WARNING"))

    def test_build_crac_does_not_create_angle_cnecs(self, angle_network):
        data = concat_triplets(angle_profile("absoluteValue"), make_triplets(
            contingency("co-1", ["bb-1"]) | assessed_element("ae-1", "bb-1")
            | remedial_action("ra-1", KIND_CURATIVE, {"alt-1": (TOPOLOGY, "bb-1", (DIRECTION_NONE, "0"))}),
            instance="nc"))
        network = concat_triplets(angle_network, make_triplets(
            LIMIT_TYPES | limit_set("bb-1", "term-1", "tn-1", [("CurrentLimit", "olt-patl", "100")]), instance="lt"))
        assert "angleCnecs" not in CracBuilder(data=data, network=network).build_crac()


# --- remedial actions ----------------------------------------------------------------

class TestProcessRemedialActions:
    def build(self, grid, *actions):
        data = {}
        for action in actions:
            data |= action
        builder = builder_for(make_triplets(data), grid)
        builder.process_remedial_actions()
        return builder

    def test_topology_action_with_direction_none(self, grid):
        builder = self.build(grid, remedial_action("ra-1", KIND_PREVENTIVE, {"alt-1": (TOPOLOGY, "sw-1", (DIRECTION_NONE, "0.0"))}))
        assert builder.crac["networkActions"] == [{
            "id": "ra-1", "name": "RA ra-1", "operator": TSO_BE,
            "onInstantUsageRules": [{"instant": "preventive"}],
            "terminalsConnectionActions": [{"networkElementId": "_sw-1", "actionType": "open"}],
        }]

    def test_up_and_down_adds_opposite_direction_action(self, grid):
        builder = self.build(grid, remedial_action("ra-1", KIND_CURATIVE, {"alt-1": (TOPOLOGY, "sw-1", (DIRECTION_UP_AND_DOWN, "1"))}))
        actions = {a["id"]: a["terminalsConnectionActions"][0]["actionType"] for a in builder.crac["networkActions"]}
        assert actions == {"ra-1": "close", "ra-1-opposite-direction": "open"}

    def test_shunt_section_count(self, grid):
        builder = self.build(grid, remedial_action("ra-1", KIND_CURATIVE, {"alt-1": (SHUNT, "sh-1", (DIRECTION_NONE, "2.0"))}))
        action = builder.crac["networkActions"][0]
        assert action["shuntCompensatorPositionActions"] == [{"networkElementId": "_sh-1", "sectionCount": 2}]
        assert action["onInstantUsageRules"] == [{"instant": "curative"}]

    def test_multiple_alterations_in_one_action(self, grid):
        builder = self.build(grid, remedial_action("ra-1", KIND_CURATIVE, {
            "alt-1": (TOPOLOGY, "sw-1", (DIRECTION_NONE, "0")), "alt-2": (TOPOLOGY, "line-1", (DIRECTION_NONE, "1"))}))
        elements = builder.crac["networkActions"][0]["terminalsConnectionActions"]
        assert sorted((e["networkElementId"], e["actionType"]) for e in elements) == [("_line-1", "close"), ("_sw-1", "open")]

    def test_mixed_directions_skipped(self, grid, loguru_messages):
        builder = self.build(grid, remedial_action("ra-1", KIND_CURATIVE, {
            "alt-1": (TOPOLOGY, "sw-1", (DIRECTION_NONE, "0")), "alt-2": (TOPOLOGY, "line-1", (DIRECTION_UP_AND_DOWN, "0"))}))
        assert builder._crac.networkActions == []
        assert any("different property range directions" in m for m in logged(loguru_messages, "WARNING"))

    def test_unsupported_alteration_and_missing_equipment_skipped(self, grid, loguru_messages):
        builder = self.build(
            grid,
            remedial_action("ra-tap", KIND_PREVENTIVE, {"alt-1": (TAP, "tc-1", (DIRECTION_UP_AND_DOWN, "14"))}),
            remedial_action("ra-missing", KIND_PREVENTIVE, {"alt-2": (TOPOLOGY, "sw-unknown", (DIRECTION_NONE, "0"))}),
            remedial_action("ra-ok", KIND_PREVENTIVE, {"alt-3": (TOPOLOGY, "sw-1", (DIRECTION_NONE, "0"))}),
        )
        assert [a.id for a in builder._crac.networkActions] == ["ra-ok"]
        warnings = logged(loguru_messages, "WARNING")
        assert "Grid state alteration type is not supported: TapPositionAction" in warnings
        assert any("does not exist in network model" in m for m in warnings)

    @pytest.mark.xfail(strict=True, raises=AttributeError,
                       reason="BUG: an alteration without StaticPropertyRange logs 'using default 0' but "
                              "`directions.unique().item()` then fails on the empty StringArray (TC1 RA_BE_S1)")
    def test_alteration_without_property_range_uses_default(self, grid):
        builder = self.build(grid, remedial_action("ra-1", KIND_PREVENTIVE, {"alt-1": (SHUNT, "sh-1", None)}),
                             remedial_action("ra-2", KIND_PREVENTIVE, {"alt-2": (TOPOLOGY, "sw-1", (DIRECTION_NONE, "0"))}))
        assert builder.crac["networkActions"][0]["shuntCompensatorPositionActions"][0]["sectionCount"] == 0

    @pytest.mark.xfail(strict=True, raises=TypeError,
                       reason="BUG: an RA profile without any StaticPropertyRange makes type_tableview return None")
    def test_profile_without_any_property_range(self, grid):
        builder = self.build(grid, remedial_action("ra-1", KIND_PREVENTIVE, {"alt-1": (TOPOLOGY, "sw-1", None)}))
        assert len(builder._crac.networkActions) == 1

    @pytest.mark.xfail(strict=True, raises=AttributeError,
                       reason="BUG: upAndDown opposite actions are built from all actions, shunt actions have no actionType")
    def test_up_and_down_with_topology_and_shunt(self, grid):
        builder = self.build(grid, remedial_action("ra-1", KIND_PREVENTIVE, {
            "alt-1": (TOPOLOGY, "sw-1", (DIRECTION_UP_AND_DOWN, "0")), "alt-2": (SHUNT, "sh-1", (DIRECTION_UP_AND_DOWN, "1"))}))
        assert {a.id for a in builder._crac.networkActions} == {"ra-1", "ra-1-opposite-direction"}

    @pytest.mark.xfail(strict=True, raises=AttributeError,
                       reason="BUG: the handler forwards whatever profiles it got; without an RA profile "
                              "type_tableview returns None and CRAC building crashes instead of yielding no actions")
    def test_missing_remedial_action_profile_yields_no_actions(self, grid):
        builder = self.build(grid, contingency("co-1", ["line-1"]))
        assert builder._crac.networkActions == []


# --- limits from the grid model ----------------------------------------------------------

def limits_network(*limit_sets, limit_types=LIMIT_TYPES):
    objects = dict(GRID) | limit_types
    for limit_objects in limit_sets:
        objects |= limit_objects
    return make_triplets(objects)


def update_limits(network, *cnecs, workaround=None):
    builder = builder_for(make_triplets({}), network, workaround)
    builder._crac.flowCnecs = [models.FlowCnec(id=cnec_id, name=cnec_id, description="", networkElementId=element,
                                               operator=TSO_BE, thresholds=[models.Threshold()], instant=instant,
                                               contingencyId=None if instant == "preventive" else "co-1")
                               for cnec_id, element, instant in cnecs]
    builder.update_limits_from_network()
    return {c.id: c.thresholds[0] for c in builder._crac.flowCnecs}, builder


class TestLimitsFromNetwork:
    def test_patl_for_preventive_tatl_for_curative_minimum_wins(self):
        network = limits_network(limit_set("line-1", "t1", "tn1", [
            ("CurrentLimit", "olt-patl", "1000"), ("CurrentLimit", "olt-patl", "900"), ("CurrentLimit", "olt-tatl", "1200")]))
        thresholds, _ = update_limits(network, ("p", "line-1", "preventive"), ("c", "line-1", "curative"))
        assert (thresholds["p"].unit, thresholds["p"].max, thresholds["p"].min) == ("ampere", 900.0, -900.0)
        assert (thresholds["c"].unit, thresholds["c"].max) == ("ampere", 1200.0)

    def test_curative_falls_back_to_patl(self, loguru_messages):
        network = limits_network(limit_set("line-1", "t1", "tn1", [("CurrentLimit", "olt-patl", "1000")]))
        thresholds, _ = update_limits(network, ("c", "line-1", "curative"))
        assert thresholds["c"].max == 1000.0
        assert "TATL limit is missing for c, using PATL value instead" in logged(loguru_messages, "WARNING")

    def test_active_power_approximated_from_current_and_sv_voltage(self):
        network = limits_network(limit_set("line-1", "t1", "tn1", [("CurrentLimit", "olt-patl", "1000")], voltage_kv=400.0))
        _, builder = update_limits(network, ("p", "line-1", "preventive"))
        assert builder.limits["ActivePowerLimit.value"].iloc[0] == pytest.approx(round(3 ** 0.5 * 1000 * 400 / 1000, 1))

    def test_megawatt_when_only_active_power_limits(self):
        network = limits_network(
            limit_set("line-1", "t1", "tn1", [("CurrentLimit", "olt-patl", "1000")]),
            limit_set("line-2", "t2", "tn2", [("ActivePowerLimit", "olt-patl", "500.0"), ("ActivePowerLimit", "olt-tatl", "600.0")]))
        thresholds, _ = update_limits(network, ("p", "line-2", "preventive"), ("c", "line-2", "curative"))
        assert [(t.unit, t.max) for t in thresholds.values()] == [("megawatt", 500.0), ("megawatt", 600.0)]

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason="BUG: get_limits always adds an all-NA ActivePowerLimit.value column; its NaN minimum "
                              "is truthy, so apparent-power-only equipment gets a 'megawatt NaN' threshold and is dropped")
    def test_apparent_power_limit(self):
        network = limits_network(limit_set("line-1", "t1", "tn1", [("ApparentPowerLimit", "olt-patl", "700.0")], voltage_kv=None))
        thresholds, _ = update_limits(network, ("p", "line-1", "preventive"))
        assert (thresholds["p"].unit, thresholds["p"].max) == ("apparent", 700.0)

    def test_current_limits_without_sv_voltages(self):
        network = limits_network(limit_set("line-1", "t1", "tn1", [("CurrentLimit", "olt-patl", "1000")], voltage_kv=None))
        thresholds, _ = update_limits(network, ("p", "line-1", "preventive"))
        assert (thresholds["p"].unit, thresholds["p"].max) == ("ampere", 1000.0)

    def test_no_limit_leaves_invalid_threshold_and_cnec_is_dropped(self, loguru_messages):
        network = limits_network(limit_set("line-1", "t1", "tn1", [("CurrentLimit", "olt-patl", "1000")]))
        thresholds, builder = update_limits(network, ("p", "line-2", "preventive"))
        assert not thresholds["p"].is_valid()
        assert builder.crac["flowCnecs"] == []
        assert any("Limit not found for p" in m for m in logged(loguru_messages, "WARNING"))

    def test_zero_current_limit_masks_active_power_fallback(self):
        # Edge: 0 A is falsy, and its MW approximation (0 MW) becomes the strictest MW limit,
        # so the explicit 300 MW limit is never used and the CNEC ends up without a threshold.
        network = limits_network(limit_set("line-1", "t1", "tn1", [
            ("CurrentLimit", "olt-patl", "0"), ("ActivePowerLimit", "olt-patl", "300.0")]))
        thresholds, _ = update_limits(network, ("p", "line-1", "preventive"))
        assert not thresholds["p"].is_valid()

    @pytest.mark.xfail(strict=True, raises=TypeError,
                       reason="BUG: integer ActivePowerLimit values give a nullable Int64 column; writing the "
                              "1-decimal MW approximation for current limits into it raises")
    def test_integer_active_power_limits_with_current_limits(self):
        network = limits_network(
            limit_set("line-1", "t1", "tn1", [("CurrentLimit", "olt-patl", "1000")]),
            limit_set("line-2", "t2", "tn2", [("ActivePowerLimit", "olt-patl", "500")]))
        thresholds, _ = update_limits(network, ("p", "line-2", "preventive"))
        assert thresholds["p"].max == 500.0

    def test_network_without_operational_limit_sets_fails_fast(self):
        builder = builder_for(make_triplets({}), make_triplets(GRID))
        with pytest.raises(AttributeError):
            builder.get_limits()

    def test_cgmes3_limit_kind_is_not_supported(self):
        cgmes3_types = {"olt-patl": ("OperationalLimitType", {
            "OperationalLimitType.kind": "http://iec.ch/TC57/CIM100-European#LimitKind.patl"})}
        network = limits_network(limit_set("line-1", "t1", "tn1", [("CurrentLimit", "olt-patl", "1000")]),
                                 limit_types=cgmes3_types)
        with pytest.raises(KeyError, match="OperationalLimitType.limitType"):
            update_limits(network, ("p", "line-1", "preventive"))

    def test_limits_are_cached_between_builds(self):
        network = limits_network(limit_set("line-1", "t1", "tn1", [("CurrentLimit", "olt-patl", "1000")]))
        _, builder = update_limits(network, ("p", "line-1", "preventive"))
        cached = builder.limits
        builder.update_limits_from_network()
        assert builder.limits is cached


# --- 3W -> 3x2W transformer workaround -------------------------------------------------

LEGS = pd.DataFrame({"rated_u1": [400.0, 110.0, 10.0]}, index=["_tr3-Leg1", "_tr3-Leg2", "_tr3-Leg3"])


@pytest.fixture
def workaround():
    return CracWorkaroundContext(enable_3w_trafo_replacement=True, replaced_3w_trafos=LEGS)


class TestThreeWindingWorkaround:
    def test_base_to_legs_map(self, grid, workaround):
        builder = builder_for(make_triplets({}), grid, workaround)
        assert builder._get_base_to_legs_map(include_leg3=True) == {"tr3": ["_tr3-Leg1", "_tr3-Leg2", "_tr3-Leg3"]}
        assert builder._get_base_to_legs_map(include_leg3=False) == {"tr3": ["_tr3-Leg1", "_tr3-Leg2"]}

    def test_lowercase_leg_suffix_not_split(self, grid):
        legs = pd.DataFrame({"rated_u1": [400.0]}, index=["tr4-leg1"])
        builder = builder_for(make_triplets({}), grid, CracWorkaroundContext(True, legs))
        # filter is case-insensitive, the base-id split is not
        assert builder._get_base_to_legs_map(include_leg3=False) == {"tr4-leg1": ["tr4-leg1"]}

    def test_inactive_workaround_is_a_no_op(self, grid):
        builder = builder_for(make_triplets({}), grid)
        assert builder.get_limits_for_replaced_3w_trafos({"tr3": 1.0}, kind="current") == {"tr3": 1.0}
        assert builder._get_base_to_legs_map() == {}

    def test_current_limit_copied_to_legs_leg2_scaled(self, grid, workaround):
        builder = builder_for(make_triplets({}), grid, workaround)
        limits = builder.get_limits_for_replaced_3w_trafos({"tr3": 100.0, "_tr3-Leg1": 80.0}, kind="current")
        assert limits["_tr3-Leg1"] == 80.0  # existing leg values are kept
        assert limits["tr3-Leg1"] == 100.0
        assert limits["tr3-Leg2"] == pytest.approx(100.0 * 330.0 / 115.0)  # default HV/MV ratio
        assert not any("Leg3" in key for key in limits)

    def test_power_limits_are_not_scaled(self, grid, workaround):
        builder = builder_for(make_triplets({}), grid, workaround)
        limits = builder.get_limits_for_replaced_3w_trafos({"_tr3": 500.0})
        assert limits["tr3-Leg1"] == limits["tr3-Leg2"] == limits["__tr3-Leg2"] == 500.0

    @pytest.mark.parametrize("voltages, expected", [
        (None, (330.0, 115.0)),
        ([401.2, 110.4, 10.5], (401.0, 110.0)),
        ([100.0], (100.0, 115.0)),   # edge: single level -> default MV above inferred HV
    ])
    def test_infer_hv_mv(self, grid, workaround, voltages, expected):
        builder = builder_for(make_triplets({}), grid, workaround)
        if voltages is not None:
            builder.limits = pd.DataFrame({"ID_Equipment": ["tr3"] * len(voltages), "SvVoltage.v": voltages})
        assert builder._infer_hv_mv_nominal_kv("_tr3") == expected

    def test_flow_cnecs_replaced_by_leg1_and_leg2(self, grid, workaround):
        builder = builder_for(make_triplets({}), grid, workaround)
        builder._crac.flowCnecs = [valid_flow_cnec("ae-preventive", "tr3"),
                                   valid_flow_cnec("ae3-curative", "tr3-Leg3", "curative", "co-1"),
                                   valid_flow_cnec("line-preventive", "line-1")]
        builder.flowcnecs_3w_workaround()
        assert [(c["id"], c["networkElementId"]) for c in builder.crac["flowCnecs"]] == [
            ("ae-leg1-preventive", "_tr3-Leg1"), ("ae-leg2-preventive", "_tr3-Leg2"), ("line-preventive", "_line-1")]

    def test_contingency_expanded_to_all_legs_and_normalised(self, grid, workaround):
        builder = builder_for(make_triplets({}), grid, workaround)
        builder._crac.contingencies = [models.Contingency(id="co", name="co",
                                                          networkElementsIds=["tr3", "line-1", "__line-2", "line-1"])]
        builder.contingencies_3w_workaround()
        assert builder.crac["contingencies"][0]["networkElementsIds"] == [
            "_tr3-Leg1", "_tr3-Leg2", "_tr3-Leg3", "_line-1", "_line-2"]

    def test_remedial_action_expanded_to_sorted_legs(self, grid):
        legs = LEGS.iloc[[2, 0, 1]]
        builder = builder_for(make_triplets({}), grid, CracWorkaroundContext(True, legs))
        builder._crac.networkActions = [models.NetworkAction(
            id="na", name="na", operator=TSO_BE, onInstantUsageRules=[{"instant": "preventive"}],
            terminalsConnectionActions=[models.TerminalsAction(networkElementId="tr3", actionType="open"),
                                        models.TerminalsAction(networkElementId="sw-1", actionType="close")])]
        builder.remedial_actions_3w_workaround()
        assert [(a["networkElementId"], a["actionType"]) for a in builder.crac["networkActions"][0]["terminalsConnectionActions"]] == [
            ("_tr3-Leg1", "open"), ("_tr3-Leg2", "open"), ("_tr3-Leg3", "open"), ("_sw-1", "close")]

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason="BUG: each leg reference is expanded to all legs without de-duplication")
    def test_remedial_action_referencing_legs_not_duplicated(self, grid, workaround):
        builder = builder_for(make_triplets({}), grid, workaround)
        builder._crac.networkActions = [models.NetworkAction(
            id="na", name="na", operator=TSO_BE, onInstantUsageRules=[{"instant": "preventive"}],
            terminalsConnectionActions=[models.TerminalsAction(networkElementId="tr3-Leg1", actionType="open"),
                                        models.TerminalsAction(networkElementId="tr3-Leg2", actionType="open")])]
        builder.remedial_actions_3w_workaround()
        elements = [a["networkElementId"] for a in builder.crac["networkActions"][0]["terminalsConnectionActions"]]
        assert len(elements) == len(set(elements))

    @pytest.mark.xfail(strict=True, raises=AssertionError,
                       reason="BUG: `for candidate in \"TopologyAction.Equipment\"` iterates characters, so the "
                              "equipment column resolves to 'T' and no alteration is ever matched")
    def test_grid_state_alterations_of_replaced_transformer(self, grid, workaround):
        builder = builder_for(make_triplets({"alt-1": ("TopologyAction", {
            "IdentifiedObject.mRID": "alt-1", "TopologyAction.Equipment": "tr3"})}), grid, workaround)
        assert builder._get_replaced_3w_grid_state_alteration_ids() == {"tr3": ["alt-1"]}


# --- full pipeline ----------------------------------------------------------------------

class TestBuildCrac:
    def test_pipeline_with_3w_replacement(self, workaround):
        data = make_triplets(
            assessed_element("ae-tr", "tr3") | assessed_element("ae-line", "line-1")
            | contingency("co-1", ["line-2"])
            | remedial_action("ra-tr", KIND_CURATIVE, {"alt-1": (TOPOLOGY, "tr3", (DIRECTION_NONE, "0"))}),
            instance="nc")
        network = limits_network(
            limit_set("tr3", "t-tr3", "tn-tr3", [("CurrentLimit", "olt-patl", "1000"), ("CurrentLimit", "olt-tatl", "1100")]),
            limit_set("line-1", "t-l1", "tn-l1", [("CurrentLimit", "olt-patl", "500"), ("CurrentLimit", "olt-tatl", "600")]))
        crac = CracBuilder(data=data, network=network, workaround=workaround).build_crac(contingency_ids=["co-1"])

        assert crac["contingencies"] == [{"id": "co-1", "name": "CO co-1", "networkElementsIds": ["_line-2"]}]
        cnecs = {c["id"]: (c["networkElementId"], c["thresholds"][0]["max"]) for c in crac["flowCnecs"]}
        assert cnecs == {
            "ae-tr-leg1-preventive": ("_tr3-Leg1", 1000.0),
            "ae-tr-leg2-preventive": ("_tr3-Leg2", pytest.approx(1000.0 * 400 / 115.0)),
            "ae-tr-leg1-curative": ("_tr3-Leg1", 1100.0),
            "ae-tr-leg2-curative": ("_tr3-Leg2", pytest.approx(1100.0 * 400 / 115.0)),
            "ae-line-preventive": ("_line-1", 500.0),
            "ae-line-curative": ("_line-1", 600.0),
        }
        assert [a["networkElementId"] for a in crac["networkActions"][0]["terminalsConnectionActions"]] == [
            "_tr3-Leg1", "_tr3-Leg2", "_tr3-Leg3"]
        assert crac["voltageCnecs"] == []

    def test_rebuild_per_contingency_is_independent(self, grid):
        data = make_triplets(assessed_element("ae-1", "line-1") | contingency("co-1", ["line-1"])
                             | contingency("co-2", ["line-2"])
                             | remedial_action("ra-1", KIND_CURATIVE, {"alt-1": (TOPOLOGY, "sw-1", (DIRECTION_NONE, "0"))}))
        network = concat_triplets(grid, make_triplets(LIMIT_TYPES | limit_set("line-1", "t1", "tn1", [
            ("CurrentLimit", "olt-patl", "500")]), instance="lim"))
        builder = CracBuilder(data=data, network=network)
        first = builder.build_crac(contingency_ids=["co-1"])
        second = builder.build_crac(contingency_ids=["co-2"])
        assert [c["id"] for c in first["contingencies"]] == ["co-1"]
        assert [c["id"] for c in second["contingencies"]] == ["co-2"]
        assert {c.get("contingencyId") for c in second["flowCnecs"]} == {None, "co-2"}
