"""Unit tests for the redispatching CRAC builder (rao.crac.redispatch)."""
import io
import json
import random
import pytest
import pandas as pd
from conftest import (
    EXAMPLES_DIR, create_network, make_row, nc_profile, nc_remedial_action, nc_unit, read_nc_profile, unit_rows,
)
from rao.crac import models
from rao.crac.builder import CracBuilder
from rao.crac.redispatch import (
    BIG, CostConfig, CracMergeError, CsvRowSource, NcRemedialActionRowSource, RowParseError, build_crac,
    build_injection_range_actions, check_ra_usage_limits, crac_to_json, merge_into_crac, normalize_element_id,
    unit_action_id,
)
from rao.crac.redispatch.cli import main as cli_main

KHES = "_f157276b-ba01-4a30-a510-d5939c71018b"
PHES_G1 = "_a1c42d01-ba01-4def-bae4-37bea73d2960"
PHES_G3 = "_4df6a958-ba01-40ca-bcdc-c71e464681df"

ABSOLUTE = "absolute"
RELATIVE = "relativeToInitialNetwork"

SAMPLE_COSTS = CostConfig.from_dict({
    "defaults": {"activationCost": 100.0, "variationCosts": {"up": 50.0, "down": 50.0}},
    "units": {
        "RA_RD_KHES_G5": {"activationCost": 100.0, "variationCosts": {"up": 50.0, "down": 50.0}},
        "RA_RD_PHES_G1": {"activationCost": 100.0, "variationCosts": {"up": 50.0, "down": 50.0}},
    },
})


def _single(result):
    assert len(result.actions) == 1, result.summary()
    return result.to_dicts()[0]


def _ranges(action: dict) -> list[tuple]:
    return [(r["rangeType"], r["min"], r["max"]) for r in action["ranges"]]


def _generator_network(*generators):
    """Single-bus network holding the given (id, min_p, max_p, target_p) generators."""
    return create_network(lines=[], loads=[], buses=("B1",),
                          generators=[(g[0], "B1", g[1], g[2], g[3]) for g in generators])


# ---------------------------------------------------------------- input adapter


def test_csv_source_reads_typed_rows():
    rows = CsvRowSource(EXAMPLES_DIR / "rd_rows.csv").read()
    assert len(rows) == 6
    first = rows[0]
    assert first.ra_name == "RA_RD_KHES_G5_UP"
    assert first.available is True
    assert first.normal_value == 56.0
    assert first.direction == "up"
    assert rows[4].available is False


def test_csv_source_rejects_missing_columns():
    with pytest.raises(RowParseError, match="missing required columns"):
        CsvRowSource(io.StringIO("kind,ra_name\ncurative,RA_X_UP\n")).read()


def test_csv_source_rejects_invalid_boolean():
    text = (EXAMPLES_DIR / "rd_rows.csv").read_text().replace(",true,Latvia", ",maybe,Latvia", 1)
    with pytest.raises(RowParseError, match="Row 2: invalid 'available' value 'maybe'"):
        CsvRowSource(io.StringIO(text)).read()


def test_nc_source_reads_rotating_machine_actions(tmp_path):
    data = read_nc_profile(nc_unit("RA_RD_KHES_G5", KHES, p_min=0.0, p_max=56.0),
                           nc_remedial_action("RA_RD_PHES_G1_UP", PHES_G1, "up", 98.0, enabled=False),
                           tmp_path=tmp_path)
    rows = sorted(NcRemedialActionRowSource(data).read(), key=lambda row: row.ra_name)

    assert [(r.ra_name, r.direction, r.normal_value, r.available) for r in rows] == [
        ("RA_RD_KHES_G5_DOWN", "down", 0.0, True),
        ("RA_RD_KHES_G5_UP", "up", 56.0, True),
        ("RA_RD_PHES_G1_UP", "up", 98.0, False),  # GridStateAlteration.normalEnabled = false
    ]
    up = rows[1]
    assert (up.kind, up.alteration_type, up.alteration_name, up.property, up.value_kind) == (
        "curative", "RotatingMachineAction", "RD_KHES_G5_UP", "RotatingMachine.p", "absolute")
    # Triplets drop the rdf:resource '#_', the mapping restores the leading underscore
    assert up.grid_element_id == KHES.lstrip("_")
    assert up.party == "https://energy.referencedata.eu/EIC/10X1001A1001B54W"
    assert up.area == "10Y1001C--00059P"


def test_nc_source_without_rotating_machine_actions(tmp_path):
    assert NcRemedialActionRowSource(read_nc_profile(tmp_path=tmp_path)).read() == []


def test_nc_profile_maps_ranges_and_availability_from_remedial_actions(tmp_path):
    """Pmin/Pmax/availability from the RA list only, no network model involved."""
    data = read_nc_profile(
        nc_unit("RA_RD_KHES_G5", KHES, p_min=0.0, p_max=56.0),
        nc_unit("RA_RD_PHES_G1", PHES_G1, p_min=0.0, p_max=98.0, down=False),   # DOWN normalAvailable = false
        nc_unit("RA_RD_PHES_G3", "_4df6a958", p_min=0.0, p_max=97.0, up=False, down=False),
        nc_remedial_action("RA_RD_KRU_G1_DOWN", "_kru-g1", "down", -225.0),     # pumping, DOWN row only
        nc_remedial_action("RA_Q_NL_G2", "_nl-g2", "upAndDown", -60.0, property_name="RotatingMachine.q"),
        tmp_path=tmp_path,
    )
    result = build_injection_range_actions(NcRemedialActionRowSource(data).read(), costs=SAMPLE_COSTS)
    actions = {a["id"]: a for a in result.to_dicts()}

    assert sorted(actions) == ["RA_RD_KHES_G5", "RA_RD_KRU_G1", "RA_RD_PHES_G1"]
    assert actions["RA_RD_KHES_G5"]["networkElementIdsAndKeys"] == {KHES: 1.0}
    assert _ranges(actions["RA_RD_KHES_G5"]) == [(ABSOLUTE, 0.0, 56.0)]
    assert _ranges(actions["RA_RD_PHES_G1"]) == [(ABSOLUTE, 0.0, 98.0), (RELATIVE, 0.0, BIG)]
    assert _ranges(actions["RA_RD_KRU_G1"]) == [(ABSOLUTE, -225.0, BIG), (RELATIVE, -BIG, 0.0)]
    assert [(u.unit_id, u.reason) for u in result.skipped] == [
        ("RA_RD_PHES_G3", "neither UP nor DOWN direction is available")]
    assert len(result.ignored_rows) == 1  # RotatingMachine.q


def test_crac_builder_adds_redispatch_actions_without_network_model(tmp_path):
    data = read_nc_profile(nc_unit("RA_RD_KHES_G5", KHES, p_min=0.0, p_max=56.0),
                           nc_unit("RA_RD_PHES_G1", PHES_G1, p_min=0.0, p_max=98.0, up=False), tmp_path=tmp_path)
    # Empty network triplets: ranges must not depend on the model
    empty_network = pd.DataFrame(columns=["ID", "KEY", "VALUE", "INSTANCE_ID"])
    builder = CracBuilder(data=data, network=empty_network, redispatch_costs=SAMPLE_COSTS)
    builder._crac = models.Crac()

    result = builder.process_redispatch_actions()

    assert [a.id for a in result.actions] == ["RA_RD_KHES_G5", "RA_RD_PHES_G1"]
    crac = builder.crac
    assert [a["id"] for a in crac["injectionRangeActions"]] == ["RA_RD_KHES_G5", "RA_RD_PHES_G1"]
    assert crac["injectionRangeActions"][0] == {
        "id": "RA_RD_KHES_G5", "name": "RA_RD_KHES_G5", "operator": "https://energy.referencedata.eu/EIC/10X1001A1001B54W",
        "activationCost": 100.0, "variationCosts": {"up": 50.0, "down": 50.0},
        "onInstantUsageRules": [{"instant": "curative"}], "networkElementIdsAndKeys": {KHES: 1.0},
        "ranges": [{"rangeType": "absolute", "min": 0.0, "max": 56.0}],
    }
    assert _ranges(crac["injectionRangeActions"][1]) == [(ABSOLUTE, 0.0, 98.0), (RELATIVE, -BIG, 0.0)]


def test_crac_builder_without_redispatch_actions_keeps_crac_unchanged(tmp_path):
    data = read_nc_profile(nc_remedial_action("RA_Q_NL_G2", "_nl-g2", "upAndDown", -60.0,
                                              property_name="RotatingMachine.q"), tmp_path=tmp_path)
    builder = CracBuilder(data=data, network=pd.DataFrame(columns=["ID", "KEY", "VALUE", "INSTANCE_ID"]))
    builder._crac = models.Crac()
    builder.process_redispatch_actions()
    assert "injectionRangeActions" not in builder.crac


# ---------------------------------------------------------------- mapping


def test_unit_action_id_strips_direction_suffix():
    assert unit_action_id("RA_RD_KHES_G5_UP") == "RA_RD_KHES_G5"
    assert unit_action_id("RA_RD_KHES_G5_DOWN") == "RA_RD_KHES_G5"
    assert unit_action_id("RA_RD_KHES_G5") == "RA_RD_KHES_G5"


def test_up_and_down_rows_merge_into_one_action():
    rows = CsvRowSource(EXAMPLES_DIR / "rd_rows.csv").read()
    result = build_injection_range_actions(rows, costs=SAMPLE_COSTS)

    assert [a.id for a in result.actions] == ["RA_RD_KHES_G5", "RA_RD_PHES_G1"]
    khes = result.to_dicts()[0]
    expected = {
        "id": "RA_RD_KHES_G5",
        "name": "RA_RD_KHES_G5",
        "operator": "AST",
        "activationCost": 100.0,
        "variationCosts": {"up": 50.0, "down": 50.0},
        "onInstantUsageRules": [{"instant": "curative"}],
        "networkElementIdsAndKeys": {KHES: 1.0},
        "ranges": [{"rangeType": "absolute", "min": 0.0, "max": 56.0}],
    }
    assert khes == expected
    assert list(khes) == list(expected)  # stable key order
    assert _ranges(result.to_dicts()[1]) == [(ABSOLUTE, 0.0, 98.0)]

    # PHES_G3 has neither direction available
    assert [(u.unit_id, u.reason) for u in result.skipped] == [
        ("RA_RD_PHES_G3", "neither UP nor DOWN direction is available")]


def test_pmin_and_pmax_come_from_down_and_up_rows():
    action = _single(build_injection_range_actions(unit_rows("RA_U", "G1", p_min=12.5, p_max=93.0)))
    assert _ranges(action) == [(ABSOLUTE, 12.5, 93.0)]


def test_up_only_unit():
    action = _single(build_injection_range_actions(unit_rows("RA_U", "G1", p_min=0.0, p_max=93.0, down=False)))
    assert _ranges(action) == [(ABSOLUTE, 0.0, 93.0), (RELATIVE, 0.0, BIG)]


def test_down_only_unit():
    action = _single(build_injection_range_actions(unit_rows("RA_U", "G1", p_min=10.0, p_max=93.0, up=False)))
    assert _ranges(action) == [(ABSOLUTE, 10.0, 93.0), (RELATIVE, -BIG, 0.0)]


def test_unit_with_no_direction_available_is_skipped(log_messages):
    result = build_injection_range_actions(unit_rows("RA_U", "G1", 0.0, 93.0, up=False, down=False))
    assert result.actions == []
    assert [(u.unit_id, u.grid_element_id, u.reason) for u in result.skipped] == [
        ("RA_U", "_G1", "neither UP nor DOWN direction is available")]
    assert any("RA_U (_G1) skipped" in m for m in log_messages)


def test_missing_rows_open_the_bound_and_keep_the_direction_closed(log_messages):
    """No model lookup: the missing side is opened to BIG, the relative range keeps it at P0."""
    up_only = _single(build_injection_range_actions([make_row("RA_U_UP", "G1", "up", 93.0)]))
    assert _ranges(up_only) == [(ABSOLUTE, -BIG, 93.0), (RELATIVE, 0.0, BIG)]
    assert any(f"DOWN row missing, DOWN not offered, using Pmin = {-BIG}" in m for m in log_messages)

    down_only = _single(build_injection_range_actions([make_row("RA_U_DOWN", "G1", "down", 5.0)]))
    assert _ranges(down_only) == [(ABSOLUTE, 5.0, BIG), (RELATIVE, -BIG, 0.0)]
    assert any(f"UP row missing, UP not offered, using Pmax = {BIG}" in m for m in log_messages)


@pytest.mark.parametrize("row_id", ["f157276b", "_f157276b", "__f157276b"])
def test_element_id_gets_single_leading_underscore(row_id):
    action = _single(build_injection_range_actions(unit_rows("RA_U", row_id, 0.0, 56.0)))
    assert action["networkElementIdsAndKeys"] == {"_f157276b": 1.0}
    assert normalize_element_id(row_id) == "_f157276b"


def test_rows_of_same_element_with_and_without_underscore_are_one_unit():
    rows = [make_row("RA_U_UP", "_G1", "up", 56.0), make_row("RA_U_DOWN", "G1", "down", 0.0)]
    action = _single(build_injection_range_actions(rows))
    assert _ranges(action) == [(ABSOLUTE, 0.0, 56.0)]


def test_non_absolute_value_kind_is_rejected():
    rows = unit_rows("RA_U", "G1", 0.0, 56.0, value_kind="relative")
    with pytest.raises(ValueError, match="Only value_kind 'absolute' is supported.*RA_U_UP"):
        build_injection_range_actions(rows)


def test_rows_other_than_rotating_machine_p_are_ignored():
    rows = unit_rows("RA_U", "G1", 0.0, 56.0) + [
        make_row("RA_Q_UP", "G2", "up", 10.0, property="RotatingMachine.q"),
        make_row("RA_T_UP", "G3", "up", 10.0, alteration_type="TopologyAction"),
        # Fixed set-point (direction none) is not a range, out of scope here
        make_row("RA_S", "G4", "none", 10.0),
    ]
    result = build_injection_range_actions(rows)
    assert [a.id for a in result.actions] == ["RA_U"]
    assert len(result.ignored_rows) == 3


def test_duplicate_direction_rows_skip_the_unit():
    rows = unit_rows("RA_U", "G1", 0.0, 56.0) + [make_row("RA_U_UP", "G1", "up", 60.0)]
    result = build_injection_range_actions(rows)
    assert result.actions == []
    assert result.skipped[0].reason == "2 UP and 1 DOWN rows, expected at most one each"


def test_same_action_id_on_two_elements_skips_both():
    rows = unit_rows("RA_U", "G1", 0.0, 56.0) + unit_rows("RA_U", "G2", 0.0, 56.0)
    result = build_injection_range_actions(rows)
    assert result.actions == []
    assert {u.grid_element_id for u in result.skipped} == {"_G1", "_G2"}


def test_pmin_above_pmax_is_skipped():
    result = build_injection_range_actions(unit_rows("RA_U", "G1", p_min=60.0, p_max=56.0))
    assert result.skipped[0].reason == "Pmin 60.0 is greater than Pmax 56.0"


def test_contingency_state_usage_rules():
    action = _single(build_injection_range_actions(unit_rows("RA_U", "G1", 0.0, 56.0),
                                                   contingency_ids=["CO_1", "CO_2", "CO_1"]))
    assert "onInstantUsageRules" not in action
    assert action["onContingencyStateUsageRules"] == [
        {"instant": "curative", "contingencyId": "CO_1"}, {"instant": "curative", "contingencyId": "CO_2"}]


def test_output_is_deterministic():
    rows = (unit_rows("RA_B", "G2", 0.0, 50.0) + unit_rows("RA_A", "G1", 0.0, 60.0)
            + unit_rows("RA_C", "G3", 0.0, 70.0, up=False))
    expected = crac_to_json(build_crac(build_injection_range_actions(rows).actions))
    for seed in range(5):
        shuffled = rows[:]
        random.Random(seed).shuffle(shuffled)
        assert crac_to_json(build_crac(build_injection_range_actions(shuffled).actions)) == expected
    assert [a["id"] for a in json.loads(expected)["injectionRangeActions"]] == ["RA_A", "RA_B", "RA_C"]


# ---------------------------------------------------------------- costs


def test_cost_config_overrides_defaults(log_messages):
    costs = CostConfig.from_dict({
        "defaults": {"activationCost": 10.0, "variationCosts": {"up": 20.0, "down": 30.0}},
        "units": {"RA_A": {"activationCost": 1.0, "variationCosts": {"up": 2.0, "down": 3.0}},
                  "RA_B": {"variationCosts": {"down": 7.0}}},
    })
    rows = unit_rows("RA_A", "G1", 0.0, 50.0) + unit_rows("RA_B", "G2", 0.0, 50.0) + unit_rows("RA_C", "G3", 0.0, 50.0)
    result = build_injection_range_actions(rows, costs=costs)
    actions = {a["id"]: a for a in result.to_dicts()}

    assert (actions["RA_A"]["activationCost"], actions["RA_A"]["variationCosts"]) == (1.0, {"up": 2.0, "down": 3.0})
    assert (actions["RA_B"]["activationCost"], actions["RA_B"]["variationCosts"]) == (10.0, {"up": 20.0, "down": 7.0})
    assert (actions["RA_C"]["activationCost"], actions["RA_C"]["variationCosts"]) == (10.0, {"up": 20.0, "down": 30.0})

    assert "RA_A" not in result.defaulted_costs
    assert result.defaulted_costs["RA_B"] == ["activationCost", "variationCosts.up"]
    assert result.defaulted_costs["RA_C"] == ["activationCost", "variationCosts.up", "variationCosts.down"]
    assert any("RA_B: using default costs for activationCost, variationCosts.up" in m for m in log_messages)
    assert any("RA_C: using default costs" in m for m in log_messages)
    assert not any("RA_A: using default costs" in m for m in log_messages)


def test_cost_config_builtin_defaults():
    action = _single(build_injection_range_actions(unit_rows("RA_A", "G1", 0.0, 50.0)))
    assert action["activationCost"] == 0.0
    assert action["variationCosts"] == {"up": 1.0, "down": 1.0}


def test_cost_config_from_yaml_file(tmp_path):
    path = tmp_path / "costs.yaml"
    path.write_text("units:\n  RA_A:\n    activationCost: 5\n    variationCosts: {up: 6, down: 7}\n")
    cost, defaulted = CostConfig.from_file(path).resolve("RA_A")
    assert (cost.activation_cost, cost.up, cost.down, defaulted) == (5.0, 6.0, 7.0, [])


@pytest.mark.parametrize("data, message", [
    ({"unit": {}}, "Unknown sections"),
    ({"units": {"RA_A": {"activation": 1.0}}}, "Unknown keys"),
    ({"units": {"RA_A": {"activationCost": -1.0}}}, "must not be negative"),
    ({"units": {"RA_A": {"variationCosts": {"up": "cheap"}}}}, "must be a number"),
])
def test_cost_config_rejects_invalid_values(data, message):
    with pytest.raises(ValueError, match=message):
        CostConfig.from_dict(data).resolve("RA_A")


# ---------------------------------------------------------------- CRAC assembly


def _base_crac() -> dict:
    return {
        "type": "CRAC", "version": "2.10", "id": "BASE", "name": "BASE",
        "instants": [{"id": "preventive", "kind": "PREVENTIVE"}, {"id": "outage", "kind": "OUTAGE"},
                     {"id": "curative", "kind": "CURATIVE"}],
        "contingencies": [{"id": "CO_1", "name": "CO_1", "networkElementsIds": ["L1"]}],
        "flowCnecs": [{"id": "CNEC_1", "name": "CNEC_1", "networkElementId": "L1", "operator": "AST",
                       "instant": "preventive", "optimized": True, "monitored": False,
                       "thresholds": [{"unit": "megawatt", "min": -100.0, "max": 100.0, "side": 1}]}],
        "networkActions": [{"id": "NA_1", "name": "NA_1", "operator": "AST",
                            "onInstantUsageRules": [{"instant": "curative"}],
                            "terminalsConnectionActions": [{"networkElementId": "L1", "actionType": "open"}]}],
        "pstRangeActions": [{"id": "PST_1", "name": "PST_1", "operator": "AST", "networkElementId": "_PST",
                             "onInstantUsageRules": [{"instant": "preventive"}]}],
    }


def _actions(*units):
    return build_injection_range_actions([row for unit in units for row in unit_rows(*unit)]).actions


def test_build_crac_header_and_no_usage_limits_by_default():
    crac = build_crac(_actions(("RA_A", "G1", 0.0, 50.0)), crac_id="RD")
    assert list(crac) == ["type", "version", "id", "name", "instants", "injectionRangeActions"]
    assert (crac["type"], crac["version"], crac["id"], crac["name"]) == ("CRAC", "2.10", "RD", "RD")
    assert [(i["id"], i["kind"]) for i in crac["instants"]] == [
        ("preventive", "PREVENTIVE"), ("outage", "OUTAGE"), ("curative", "CURATIVE")]
    assert "ra-usage-limits-per-instant" not in crac


def test_build_crac_with_usage_limits_warns_about_max_ra(log_messages):
    limits = [{"instant": "curative", "max-ra": 1}]
    crac = build_crac(_actions(("RA_A", "G1", 0.0, 50.0)), ra_usage_limits=limits)
    assert crac["ra-usage-limits-per-instant"] == limits
    assert any("'max-ra: 1' at instant 'curative' blocks redispatch" in m for m in log_messages)


def test_usage_limits_multi_curative_topo_without_ra_per_tso_warns(log_messages):
    instants = [{"id": "preventive", "kind": "PREVENTIVE"}, {"id": "outage", "kind": "OUTAGE"},
                {"id": "curative1", "kind": "CURATIVE"}, {"id": "curative2", "kind": "CURATIVE"}]
    check_ra_usage_limits([{"instant": "curative1", "max-topo-per-tso": {"AST": 1}}], instants)
    assert any("cumulative across curative instants" in m for m in log_messages)
    assert any("NullPointerException" in m and "AST" in m for m in log_messages)

    log_messages.clear()
    check_ra_usage_limits([{"instant": "curative1", "max-topo-per-tso": {"AST": 1}, "max-ra-per-tso": {"AST": 4}}],
                          instants)
    assert not any("NullPointerException" in m for m in log_messages)


def test_merge_preserves_existing_content():
    base = _base_crac()
    merged = merge_into_crac(base, _actions(("RA_B", "G2", 0.0, 50.0), ("RA_A", "G1", 0.0, 50.0)))
    assert base == _base_crac()  # input untouched
    for key in base:
        assert merged[key] == base[key]
    assert [a["id"] for a in merged["injectionRangeActions"]] == ["RA_A", "RA_B"]


def test_merge_rejects_missing_instant():
    base = _base_crac()
    base["instants"] = [i for i in base["instants"] if i["id"] != "curative"]
    with pytest.raises(CracMergeError, match="instant 'curative' is not defined"):
        merge_into_crac(base, _actions(("RA_A", "G1", 0.0, 50.0)))


def test_merge_rejects_missing_contingency():
    actions = build_injection_range_actions(unit_rows("RA_A", "G1", 0.0, 50.0), contingency_ids=["CO_X"]).actions
    with pytest.raises(CracMergeError, match="contingency 'CO_X' is not defined"):
        merge_into_crac(_base_crac(), actions)
    merged = merge_into_crac(_base_crac(), build_injection_range_actions(
        unit_rows("RA_A", "G1", 0.0, 50.0), contingency_ids=["CO_1"]).actions)
    assert merged["injectionRangeActions"][0]["onContingencyStateUsageRules"] == [
        {"instant": "curative", "contingencyId": "CO_1"}]


@pytest.mark.parametrize("action_id", ["NA_1", "PST_1"])
def test_merge_rejects_duplicate_action_id(action_id):
    with pytest.raises(CracMergeError, match=f"id '{action_id}' already exists"):
        merge_into_crac(_base_crac(), _actions((action_id, "G1", 0.0, 50.0)))


def test_merge_rejects_duplicate_action_id_against_existing_injection_action():
    base = merge_into_crac(_base_crac(), _actions(("RA_A", "G1", 0.0, 50.0)))
    with pytest.raises(CracMergeError, match="id 'RA_A' already exists"):
        merge_into_crac(base, _actions(("RA_A", "G9", 0.0, 50.0)))


@pytest.mark.parametrize("element_id, used_by", [("G1", "RA_A"), ("_G1", "RA_A"), ("PST", "PST_1")])
def test_merge_rejects_element_already_used_by_a_range_action(element_id, used_by):
    base = merge_into_crac(_base_crac(), _actions(("RA_A", "G1", 0.0, 50.0)))
    with pytest.raises(CracMergeError, match=f"already used by range action '{used_by}'"):
        merge_into_crac(base, _actions(("RA_NEW", element_id, 0.0, 50.0)))


# ---------------------------------------------------------------- CLI


def test_cli_from_nc_profile_writes_crac_and_summary(tmp_path, capsys):
    profile = tmp_path / "RA.xml"
    profile.write_text(nc_profile(nc_unit("RA_RD_KHES_G5", KHES, p_min=0.0, p_max=56.0),
                                  nc_unit("RA_RD_PHES_G1", PHES_G1, p_min=0.0, p_max=98.0),
                                  nc_unit("RA_RD_PHES_G3", PHES_G3, p_min=0.0, p_max=97.0, up=False, down=False)))
    out = tmp_path / "crac_out.json"

    code = cli_main(["--ra-profile", str(profile), "--costs", str(EXAMPLES_DIR / "costs.yaml"),
                     "--out", str(out), "--log-level", "ERROR"])

    assert code == 0
    crac = json.loads(out.read_text())
    assert [a["id"] for a in crac["injectionRangeActions"]] == ["RA_RD_KHES_G5", "RA_RD_PHES_G1"]
    assert crac["injectionRangeActions"][0]["networkElementIdsAndKeys"] == {KHES: 1.0}
    printed = capsys.readouterr().out
    assert "Injection range actions written: 2" in printed
    assert f"RA_RD_PHES_G3 ({PHES_G3}): neither UP nor DOWN direction is available" in printed


def test_cli_from_csv_validates_by_openrao_import(tmp_path, capsys):
    # The network is only used for the import check, not for ranges
    network = _generator_network((KHES, 0.0, 60.0, 30.0), (PHES_G1, 0.0, 100.0, 50.0))
    network_path = tmp_path / "model.xiidm"
    network.save(str(network_path), format="XIIDM")
    out = tmp_path / "crac_out.json"

    code = cli_main(["--rows", str(EXAMPLES_DIR / "rd_rows.csv"), "--network", str(network_path),
                     "--out", str(out), "--log-level", "ERROR"])

    assert code == 0
    assert "Injection range actions written: 2" in capsys.readouterr().out


def test_cli_merges_into_base_crac(tmp_path):
    base_path = tmp_path / "base.json"
    base_path.write_text(json.dumps(_base_crac()))
    out = tmp_path / "crac_out.json"
    code = cli_main(["--rows", str(EXAMPLES_DIR / "rd_rows.csv"), "--base-crac", str(base_path),
                     "--contingency", "CO_1", "--out", str(out), "--log-level", "ERROR"])
    assert code == 0
    crac = json.loads(out.read_text())
    assert crac["id"] == "BASE"
    assert crac["networkActions"] == _base_crac()["networkActions"]
    assert crac["injectionRangeActions"][0]["onContingencyStateUsageRules"] == [
        {"instant": "curative", "contingencyId": "CO_1"}]


def test_cli_rejects_contingency_without_base_crac(tmp_path):
    code = cli_main(["--rows", str(EXAMPLES_DIR / "rd_rows.csv"), "--contingency", "CO_1",
                     "--out", str(tmp_path / "out.json"), "--log-level", "CRITICAL"])
    assert code == 2
