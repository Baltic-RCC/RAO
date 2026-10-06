"""Unit tests for the central remedial action cost config (rao.crac.costs)."""
import pandas as pd
import pytest
from conftest import nc_unit, read_nc_profile, triangle_network
from rao.crac import models
from rao.crac.builder import CracBuilder
from rao.crac.costly_ra import import_crac
from rao.crac.costs import CostConfig

COSTS = CostConfig.from_dict({
    "defaults": {"activationCost": 10.0, "variationCosts": {"up": 20.0, "down": 30.0}},
    "remedialActions": {
        "RA_A": {"activationCost": 1.0, "variationCosts": {"up": 2.0, "down": 3.0}},
        "RA_B": {"variationCosts": {"down": 7.0}},
        "TOPO_BY_NAME": {"activationCost": 5.0},
    },
})


def _injection_action(action_id: str) -> models.InjectionRangeAction:
    return models.InjectionRangeAction(
        id=action_id, name=action_id, operator="AST", onInstantUsageRules=[{"instant": "curative"}],
        networkElementIdsAndKeys={f"_{action_id}": 1.0},
        ranges=[models.InjectionRange(rangeType="absolute", min=0.0, max=50.0)])


def _network_action(action_id: str, name: str | None = None, element: str = "L12") -> models.NetworkAction:
    return models.NetworkAction(
        id=action_id, name=name or action_id, operator="AST", onInstantUsageRules=[{"instant": "curative"}],
        terminalsConnectionActions=[models.TerminalsAction(networkElementId=element, actionType="open")])


def test_resolve_overrides_defaults_per_value():
    cost, defaulted = COSTS.resolve("RA_A")
    assert (cost.activation_cost, cost.up, cost.down, defaulted) == (1.0, 2.0, 3.0, [])
    cost, defaulted = COSTS.resolve("RA_B")
    assert (cost.activation_cost, cost.up, cost.down) == (10.0, 20.0, 7.0)
    assert defaulted == ["activationCost", "variationCosts.up"]
    cost, defaulted = COSTS.resolve("UNKNOWN")
    assert (cost.activation_cost, cost.up, cost.down) == (10.0, 20.0, 30.0)
    assert defaulted == ["activationCost", "variationCosts.up", "variationCosts.down"]


def test_resolve_by_name_when_id_is_not_configured():
    # Topology actions use the mRID as id, the config may use the readable name
    cost, defaulted = COSTS.resolve("8c8a9e59-mrid", name="TOPO_BY_NAME")
    assert cost.activation_cost == 5.0
    assert "activationCost" not in defaulted


def test_builtin_defaults():
    cost, defaulted = CostConfig().resolve("RA_A")
    assert (cost.activation_cost, cost.up, cost.down) == (0.0, 1.0, 1.0)
    assert len(defaulted) == 3


def test_apply_sets_activation_cost_on_network_actions_and_variation_costs_on_range_actions(log_messages):
    actions = [_network_action("8c8a9e59-mrid", name="TOPO_BY_NAME"), _network_action("TOPO_2"),
               _injection_action("RA_A"), _injection_action("RA_B")]

    defaulted = COSTS.apply(actions)

    topo, topo_2, ra_a, ra_b = (action.model_dump(exclude_none=True) for action in actions)
    assert topo["activationCost"] == 5.0 and "variationCosts" not in topo
    assert topo_2["activationCost"] == 10.0
    assert (ra_a["activationCost"], ra_a["variationCosts"]) == (1.0, {"up": 2.0, "down": 3.0})
    assert (ra_b["activationCost"], ra_b["variationCosts"]) == (10.0, {"up": 20.0, "down": 7.0})
    # Network actions only report activationCost as defaulted
    assert defaulted == {"TOPO_2": ["activationCost"], "RA_B": ["activationCost", "variationCosts.up"]}
    assert any("2 of 4 remedial actions use default costs" in m for m in log_messages)


def test_apply_warning_lists_at_most_ten_actions(log_messages):
    CostConfig().apply([_network_action(f"TOPO_{i:02d}") for i in range(12)], action_type="network action")
    warning = next(m for m in log_messages if "use default costs" in m)
    assert "12 of 12 network actions" in warning and "... (2 more)" in warning


def test_cost_config_from_yaml_file(tmp_path):
    path = tmp_path / "costs.yaml"
    path.write_text("remedialActions:\n  RA_A:\n    activationCost: 5\n    variationCosts: {up: 6, down: 7}\n")
    cost, defaulted = CostConfig.from_file(path).resolve("RA_A")
    assert (cost.activation_cost, cost.up, cost.down, defaulted) == (5.0, 6.0, 7.0, [])


@pytest.mark.parametrize("data, message", [
    ({"units": {}}, "Unknown sections"),
    ({"remedialActions": {"RA_A": {"activation": 1.0}}}, "Unknown keys"),
    ({"remedialActions": {"RA_A": {"activationCost": -1.0}}}, "must not be negative"),
    ({"remedialActions": {"RA_A": {"variationCosts": {"up": "cheap"}}}}, "must be a number"),
])
def test_cost_config_rejects_invalid_values(data, message):
    with pytest.raises(ValueError, match=message):
        CostConfig.from_dict(data).resolve("RA_A")


# ---------------------------------------------------------------- CracBuilder


def _builder(tmp_path, costs: CostConfig | None) -> CracBuilder:
    """CracBuilder with redispatch RAs and one topology action, other CRAC steps stubbed."""
    data = read_nc_profile(nc_unit("RA_A", "GEN_A", 0.0, 500.0), nc_unit("RA_B", "GEN_B", 0.0, 800.0),
                           tmp_path=tmp_path)
    builder = CracBuilder(data=data, network=pd.DataFrame(columns=["ID", "KEY", "VALUE", "INSTANCE_ID"]), costs=costs)
    for step in ("process_contingencies", "process_cnecs", "process_voltage_cnecs_from_network",
                 "update_limits_from_network"):
        setattr(builder, step, lambda *args, **kwargs: None)
    builder.process_remedial_actions = lambda: builder._crac.networkActions.append(
        _network_action("8c8a9e59-mrid", name="TOPO_BY_NAME"))
    return builder


def test_build_crac_assigns_costs_to_topology_and_redispatch_actions(tmp_path):
    crac = _builder(tmp_path, COSTS).build_crac(include_redispatch=True)

    assert crac["networkActions"][0]["activationCost"] == 5.0
    ranges = {a["id"]: a for a in crac["injectionRangeActions"]}
    assert (ranges["RA_A"]["activationCost"], ranges["RA_A"]["variationCosts"]) == (1.0, {"up": 2.0, "down": 3.0})
    assert (ranges["RA_B"]["activationCost"], ranges["RA_B"]["variationCosts"]) == (10.0, {"up": 20.0, "down": 7.0})


def test_build_crac_without_cost_config_writes_no_costs(tmp_path, log_messages):
    crac = _builder(tmp_path, None).build_crac(include_redispatch=True)

    assert "activationCost" not in crac["networkActions"][0]
    assert all("activationCost" not in a and "variationCosts" not in a for a in crac["injectionRangeActions"])
    assert not any("cost" in m for m in log_messages)


def test_costed_crac_is_imported_by_openrao(tmp_path):
    crac = _builder(tmp_path, COSTS).build_crac(include_redispatch=True)

    imported = import_crac(triangle_network(), crac)

    assert imported.get_network_actions().loc["8c8a9e59-mrid", "activation_cost"] == 5.0
    injection = imported.get_injection_range_actions()
    assert injection.loc["RA_A", "activation_cost"] == 1.0
    assert injection.loc["RA_B", "variation_cost_down"] == 7.0
