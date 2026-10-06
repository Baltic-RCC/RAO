"""Integration tests: generated CRAC imported and optimized by pypowsybl 1.16.1 / OpenRAO 7.3.0 (DC)."""
import pandas as pd
import pypowsybl
import pytest
from conftest import TC1_CGMES, nc_unit, read_nc_profile, triangle_network, unit_rows
from rao.crac import models
from rao.crac.builder import CracBuilder
from rao.crac.costly_ra import build_crac, build_injection_range_actions, merge_into_crac, validate_crac
from rao.crac.costs import CostConfig
from rao.parameters.loadflow import CGMES_IMPORT_PARAMETERS
from rao.parameters.manager import RaoSettingsManager
from rao.costly_ra import apply_redispatch, load_min_cost_parameters, redispatch_results, run_rao

COSTS = CostConfig.from_dict({
    "defaults": {"activationCost": 100.0, "variationCosts": {"up": 10.0, "down": 10.0}},
    "remedialActions": {"RA_A": {"variationCosts": {"down": 40.0}}, "RA_B": {"variationCosts": {"down": 30.0}}},
})
TOLERANCE_MW = 1.0


def _flow_cnec(cnec_id: str, line_id: str, instant: str, limit: float, contingency_id: str | None = None) -> dict:
    cnec = {"id": cnec_id, "name": cnec_id, "networkElementId": line_id, "operator": "AST", "instant": instant,
            "optimized": True, "monitored": False,
            "thresholds": [{"unit": "megawatt", "min": -limit, "max": limit, "side": 1}]}
    if contingency_id:
        cnec["contingencyId"] = contingency_id
    return cnec


def _base_crac(flow_cnecs: list[dict], contingencies: list[dict] | None = None) -> dict:
    crac = build_crac([], crac_id="BASE")
    crac["contingencies"] = contingencies or []
    crac["flowCnecs"] = flow_cnecs
    return crac


def _curative_base_crac() -> dict:
    """Parallel L13a/L13b case: after losing L13b, L13a carries 300 MW against a curative 250 MW limit."""
    return _base_crac(
        flow_cnecs=[_flow_cnec("L13a-preventive", "L13a", "preventive", 250.0),
                    _flow_cnec("L13a-outage", "L13a", "outage", 1000.0, "CO_L13b"),
                    _flow_cnec("L13a-curative", "L13a", "curative", 250.0, "CO_L13b")],
        contingencies=[{"id": "CO_L13b", "name": "CO_L13b", "networkElementsIds": ["L13b"]}],
    )


def _generator_rows(**availability):
    """Rows for GEN_A [0, 500] and GEN_B [0, 800], availability as RA_A=(up, down)."""
    up_a, down_a = availability.get("RA_A", (True, True))
    up_b, down_b = availability.get("RA_B", (True, True))
    return (unit_rows("RA_A", "GEN_A", 0.0, 500.0, up=up_a, down=down_a, kind="preventive")
            + unit_rows("RA_B", "GEN_B", 0.0, 800.0, up=up_b, down=down_b, kind="preventive"))


def _preventive_case(**availability):
    network = triangle_network()
    result = build_injection_range_actions(_generator_rows(**availability), instant="preventive")
    COSTS.apply(result.actions)
    crac = merge_into_crac(_base_crac([_flow_cnec("L13-preventive", "L13", "preventive", 300.0)]), result.actions)
    return network, crac


def test_import_round_trip_initial_set_point_equals_target_p():
    network = triangle_network()
    rows = _generator_rows(RA_B=(True, False))
    actions = build_injection_range_actions(rows).actions
    COSTS.apply(actions)
    crac = build_crac(actions)

    imported = validate_crac(network, crac)

    actions = imported.get_injection_range_actions()
    assert sorted(actions.index) == ["RA_A", "RA_B"]
    target_p = network.get_generators()["target_p"]
    assert actions.loc["RA_A", "initial_set_point"] == pytest.approx(target_p["_GEN_A"])
    assert actions.loc["RA_B", "initial_set_point"] == pytest.approx(target_p["_GEN_B"])
    assert actions.loc["RA_B", "variation_cost_down"] == pytest.approx(30.0)
    ranges = imported.get_ranges()
    assert sorted(ranges.loc[["RA_B"], "range_type"]) == ["ABSOLUTE", "RELATIVE_TO_INITIAL_NETWORK"]


@pytest.mark.skipif(not TC1_CGMES.exists(), reason="test-data submodule not checked out")
def test_import_round_trip_on_tc1_cgmes_model():
    """Rows hold mRIDs without '_', the CGMES import keeps the rdf:ID underscore."""
    network = pypowsybl.network.load(str(TC1_CGMES), parameters=CGMES_IMPORT_PARAMETERS)
    generators = network.get_generators()
    rows = []
    for generator_id, generator in generators.iterrows():
        rows += unit_rows(f"RA_RD_{generator['name']}", generator_id.lstrip("_"),
                          p_min=generator["min_p"], p_max=generator["max_p"], party="TSO")
    result = build_injection_range_actions(rows)
    assert result.skipped == []

    imported = validate_crac(network, build_crac(result.actions))

    actions = imported.get_injection_range_actions()
    assert len(actions) == len(generators)
    elements = imported.get_network_element_ids_and_keys()
    for action_id, element in elements.iterrows():
        assert element["network_element_id"].startswith("_")
        assert actions.loc[action_id, "initial_set_point"] == pytest.approx(
            generators.loc[element["network_element_id"], "target_p"])


def test_min_cost_parameters_contain_costly_block():
    parameters = load_min_cost_parameters().to_json()
    extension = parameters["extensions"]["open-rao-search-tree-parameters"]
    assert parameters["objective-function"]["type"] == "MIN_COST"
    assert extension["range-actions-optimization"]["pst-model"] == "APPROXIMATED_INTEGERS"
    assert extension["costly-min-margin-parameters"]["shifted-violation-threshold"] == 5.0
    sensitivity = extension["load-flow-and-sensitivity-computation"]["sensitivity-parameters"]
    assert sensitivity["load-flow-parameters"]["dc"] is True

    ac = load_min_cost_parameters(dc=False).to_json()
    ac_sensitivity = ac["extensions"]["open-rao-search-tree-parameters"]["load-flow-and-sensitivity-computation"]
    assert ac_sensitivity["sensitivity-parameters"]["load-flow-parameters"]["dc"] is False


def test_balanced_preventive_redispatch():
    """
    L13 carries 400 MW against a 300 MW limit. Shifting d MW from GEN_A to GEN_B relieves
    L13 by d/3; with the 5 MW shifted-violation threshold the RAO targets 295 MW:
    d = 315, GEN_A 400 -> 85 MW, GEN_B 400 -> 715 MW.
    """
    network, crac = _preventive_case()
    imported = validate_crac(network, crac)

    rao_result = run_rao(network, imported, load_min_cost_parameters(dc=True))
    results = redispatch_results(imported, rao_result, network).set_index("action_id")

    assert list(results.columns) == ["generator_id", "instant", "contingency", "initial_p", "optimized_p", "delta"]
    assert results.loc["RA_A", "generator_id"] == "_GEN_A"
    assert results.loc["RA_A", "optimized_p"] == pytest.approx(85.0, abs=TOLERANCE_MW)
    assert results.loc["RA_B", "optimized_p"] == pytest.approx(715.0, abs=TOLERANCE_MW)
    assert results.loc["RA_A", "delta"] == pytest.approx(-315.0, abs=TOLERANCE_MW)
    assert results["delta"].sum() == pytest.approx(0.0, abs=TOLERANCE_MW)
    assert set(results["instant"]) == {"preventive"}

    cnecs = rao_result.get_flow_cnec_results()
    optimized = cnecs[(cnecs["cnec_id"] == "L13-preventive") & (cnecs["optimized_instant"] == "preventive")
                      & (cnecs["side"] == "ONE")].iloc[0]
    assert optimized["flow"] == pytest.approx(295.0, abs=TOLERANCE_MW)
    assert optimized["margin"] == pytest.approx(5.0, abs=TOLERANCE_MW)

    # The RaoResult does not touch the network, apply_redispatch does
    assert network.get_generators().loc["_GEN_A", "target_p"] == pytest.approx(400.0)
    apply_redispatch(network, results.reset_index(), instant="preventive")
    assert network.get_generators().loc["_GEN_A", "target_p"] == pytest.approx(85.0, abs=TOLERANCE_MW)
    pypowsybl.loadflow.run_dc(network)
    assert network.get_lines().loc["L13", "p1"] == pytest.approx(295.0, abs=TOLERANCE_MW)


def test_nc_profile_through_crac_builder_to_rao(tmp_path):
    """
    RA list (NC RemedialAction profile) -> CracBuilder -> OpenRAO, same case as the
    balanced preventive redispatch. The CRAC builder never reads the network model.
    """
    data = read_nc_profile(nc_unit("RA_A", "GEN_A", p_min=0.0, p_max=500.0, kind="preventive", operator="AST"),
                           nc_unit("RA_B", "GEN_B", p_min=0.0, p_max=800.0, kind="preventive", operator="AST"),
                           tmp_path=tmp_path)
    builder = CracBuilder(data=data, network=pd.DataFrame(columns=["ID", "KEY", "VALUE", "INSTANCE_ID"]),
                          costs=COSTS)
    builder._crac = models.Crac()
    builder.process_redispatch_actions(instant="preventive")
    builder.apply_costs()
    crac = builder.crac
    crac["flowCnecs"] = [_flow_cnec("L13-preventive", "L13", "preventive", 300.0)]

    network = triangle_network()
    imported = validate_crac(network, crac)
    results = redispatch_results(imported, run_rao(network, imported), network).set_index("action_id")

    assert results.loc["RA_A", "optimized_p"] == pytest.approx(85.0, abs=TOLERANCE_MW)
    assert results.loc["RA_B", "optimized_p"] == pytest.approx(715.0, abs=TOLERANCE_MW)


def test_curative_redispatch_without_costs_under_worker_max_min_margin_parameters():
    """
    CRAC without costs, run with the worker's default (MAX_MIN_MARGIN, AC) parameters.
    The redispatch stays balanced, but MAX_MIN_MARGIN maximizes the margin rather than
    minimizing the volume, so the units go to their limits (GEN_A -> 0, GEN_B -> 800).
    """
    network = triangle_network(parallel_l13=True)
    rows = unit_rows("RA_A", "GEN_A", 0.0, 500.0) + unit_rows("RA_B", "GEN_B", 0.0, 800.0)
    result = build_injection_range_actions(rows)
    assert "activationCost" not in result.to_dicts()[0]
    imported = validate_crac(network, merge_into_crac(_curative_base_crac(), result.actions))
    parameters = pypowsybl.rao.Parameters.from_buffer_source(RaoSettingsManager().to_bytesio())
    assert parameters.to_json()["objective-function"]["type"] == "MAX_MIN_MARGIN"

    rao_result = run_rao(network, imported, parameters)
    results = redispatch_results(imported, rao_result, network).set_index("action_id")

    assert set(results["instant"]) == {"curative"}
    assert results["delta"].sum() == pytest.approx(0.0, abs=TOLERANCE_MW)
    assert results.loc["RA_A", "optimized_p"] == pytest.approx(0.0, abs=TOLERANCE_MW)
    assert results.loc["RA_B", "optimized_p"] == pytest.approx(800.0, abs=TOLERANCE_MW)
    cnecs = rao_result.get_flow_cnec_results()
    curative = cnecs[(cnecs["cnec_id"] == "L13a-curative") & (cnecs["optimized_instant"] == "curative")]
    assert curative["margin"].min() > 0


def test_up_only_units_cannot_redispatch():
    """With every unit up-only, any change breaks the balance: no range action is activated."""
    network, crac = _preventive_case(RA_A=(True, False), RA_B=(True, False))
    imported = validate_crac(network, crac)

    rao_result = run_rao(network, imported)

    assert rao_result.get_range_action_results().empty
    assert redispatch_results(imported, rao_result, network).empty


def test_balanced_curative_redispatch_with_default_instant():
    """
    L13 is split into L13a/L13b. After losing L13b, L13a carries 300 MW against a curative
    limit of 250 MW and shifting d MW from GEN_A to GEN_B relieves it by d/4. With the 5 MW
    threshold: d = 220, GEN_A -> 180 MW, GEN_B -> 620 MW, only in the curative state.
    """
    network = triangle_network(parallel_l13=True)
    rows = (unit_rows("RA_A", "GEN_A", 0.0, 500.0) + unit_rows("RA_B", "GEN_B", 0.0, 800.0))
    result = build_injection_range_actions(rows)  # curative by default
    COSTS.apply(result.actions)
    imported = validate_crac(network, merge_into_crac(_curative_base_crac(), result.actions))

    rao_result = run_rao(network, imported)
    results = redispatch_results(imported, rao_result, network)

    assert set(zip(results["instant"], results["contingency"])) == {("curative", "CO_L13b")}
    by_action = results.set_index("action_id")
    assert by_action.loc["RA_A", "optimized_p"] == pytest.approx(180.0, abs=TOLERANCE_MW)
    assert by_action.loc["RA_B", "optimized_p"] == pytest.approx(620.0, abs=TOLERANCE_MW)
    assert results["delta"].sum() == pytest.approx(0.0, abs=TOLERANCE_MW)

    apply_redispatch(network, results, instant="curative", contingency="CO_L13b")
    network.update_lines(id="L13b", connected1=False, connected2=False)
    pypowsybl.loadflow.run_dc(network)
    assert network.get_lines().loc["L13a", "p1"] == pytest.approx(245.0, abs=TOLERANCE_MW)


class _StubRaoResult:
    def __init__(self, rows: list[dict]):
        self.rows = rows

    def get_range_action_results(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=["remedial_action_id", "optimized_instant", "contingency",
                                                "optimized_set_point"])


def test_redispatch_results_warns_on_imbalance(log_messages):
    network, crac = _preventive_case()
    imported = validate_crac(network, crac)
    stub = _StubRaoResult([{"remedial_action_id": "RA_A", "optimized_instant": "preventive", "contingency": "",
                            "optimized_set_point": 380.0},
                           {"remedial_action_id": "RA_B", "optimized_instant": "preventive", "contingency": "",
                            "optimized_set_point": 410.0}])

    results = redispatch_results(imported, stub, network)

    assert results["delta"].tolist() == [-20.0, 10.0]
    assert any("preventive/- is not balanced: sum of deltas = -10.000 MW" in m for m in log_messages)
