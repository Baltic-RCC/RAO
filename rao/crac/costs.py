"""
Remedial action costs for the CRAC (OpenRAO 7.3.0, used by the MIN_COST objective).

Costs are not part of the remedial action lists. They are applied to the CRAC actions once
they are built, optionally from a YAML or JSON file keyed by remedial action id (or name):
    - activationCost: every remedial action (network/topological and range actions)
    - variationCosts: range actions only (EUR/MW up and down)

Activation costs follow the remedial action type (see remedial_action_types), cheapest first:
    topologyUpAndDown < networkAction < countertrade < redispatch

    activationCosts:                      # per type, overrides ACTIVATION_COST_BY_TYPE
      redispatch: 1000.0
    defaults:
      activationCost: 0.0
      variationCosts: {up: 1.0, down: 1.0}
    remedialActions:
      RA_RD_KHES_G5:                      # injection range action (redispatch unit)
        activationCost: 100.0
        variationCosts: {up: 50.0, down: 50.0}
      8c8a9e59-a01-47d9-a9fe-d4fb23b...:  # network action by id (mRID) or by name
        activationCost: 20.0

Values missing for an action fall back to its type's activation cost, then to 'defaults' and then
to the built-in defaults below.
"""
import json
from dataclasses import dataclass, field
from pathlib import Path
from loguru import logger
from pydantic import BaseModel

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

# Built-in defaults, used when the cost config does not define a 'defaults' section.
# A non-zero variation cost keeps MIN_COST from treating range action volume as free.
DEFAULT_ACTIVATION_COST = 0.0
DEFAULT_VARIATION_COST = 1.0

# Activation cost per remedial action type, the order sets which actions the RAO prefers.
ACTIVATION_COST_BY_TYPE = {
    "topologyUpAndDown": 1.0,   # topology action with an 'upAndDown' range (id and id + OPPOSITE_DIRECTION_SUFFIX)
    "networkAction": 10.0,      # other network actions
    "countertrade": 100.0,      # injection range actions, see is_countertrade()
    "redispatch": 1000.0,       # injection range actions
}
# Suffix of the opposite network action created for 'upAndDown' topology actions (CracBuilder)
OPPOSITE_DIRECTION_SUFFIX = "-opposite-direction"

_ACTION_KEYS = {"activationCost", "variationCosts"}
_VARIATION_KEYS = {"up", "down"}


@dataclass(frozen=True)
class RemedialActionCost:
    activation_cost: float
    up: float
    down: float


@dataclass
class CostConfig:
    defaults: RemedialActionCost = field(default_factory=lambda: RemedialActionCost(
        DEFAULT_ACTIVATION_COST, DEFAULT_VARIATION_COST, DEFAULT_VARIATION_COST))
    remedial_actions: dict[str, dict] = field(default_factory=dict)
    activation_costs: dict[str, float] = field(default_factory=lambda: dict(ACTIVATION_COST_BY_TYPE))

    @classmethod
    def from_dict(cls, data: dict | None) -> "CostConfig":
        data = data or {}
        unknown = set(data) - {"defaults", "remedialActions", "activationCosts"}
        if unknown:
            raise ValueError(f"Unknown sections in cost config: {sorted(unknown)}, "
                             f"expected 'activationCosts', 'defaults' and 'remedialActions'")

        activation_costs = data.get("activationCosts") or {}
        if not isinstance(activation_costs, dict) or set(activation_costs) - set(ACTIVATION_COST_BY_TYPE):
            raise ValueError(f"Cost config 'activationCosts' must be a mapping with keys {list(ACTIVATION_COST_BY_TYPE)}")
        activation_costs = {**ACTIVATION_COST_BY_TYPE,
                            **{k: _cost(f"activationCosts.{k}", v) for k, v in activation_costs.items()}}

        builtin = cls().defaults
        defaults_section = data.get("defaults") or {}
        _check_action_section("defaults", defaults_section)
        variation = defaults_section.get("variationCosts") or {}
        defaults = RemedialActionCost(
            activation_cost=_cost("defaults.activationCost", defaults_section.get("activationCost", builtin.activation_cost)),
            up=_cost("defaults.variationCosts.up", variation.get("up", builtin.up)),
            down=_cost("defaults.variationCosts.down", variation.get("down", builtin.down)),
        )

        remedial_actions = data.get("remedialActions") or {}
        if not isinstance(remedial_actions, dict):
            raise ValueError("Cost config 'remedialActions' must be a mapping keyed by remedial action id or name")
        for action_id, section in remedial_actions.items():
            _check_action_section(f"remedialActions.{action_id}", section)

        return cls(defaults=defaults, remedial_actions={str(k): v or {} for k, v in remedial_actions.items()},
                   activation_costs=activation_costs)

    @classmethod
    def from_file(cls, path: str | Path) -> "CostConfig":
        path = Path(path)
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() in (".yaml", ".yml"):
            if yaml is None:
                raise RuntimeError("Install PyYAML to read YAML cost config or provide JSON")
            data = yaml.safe_load(text)
        else:
            data = json.loads(text)
        logger.info(f"Loaded remedial action cost config from: {path}")
        return cls.from_dict(data)

    def resolve(self, action_id: str, name: str | None = None,
                action_type: str | None = None) -> tuple[RemedialActionCost, list[str]]:
        """
        Return the costs of a remedial action (looked up by id, then name) and the values taken from defaults.
        The activation cost of an action with a known type falls back to its type cost, not reported as defaulted.
        """
        section = self.remedial_actions.get(action_id)
        if section is None and name is not None:
            section = self.remedial_actions.get(name)
        section = section or {}
        variation = section.get("variationCosts") or {}
        defaulted = []

        def _value(key: str, value, default: float) -> float:
            if value is None:
                defaulted.append(key)
                return default
            return _cost(f"remedialActions.{action_id}.{key}", value)

        activation_cost = section.get("activationCost")
        if activation_cost is None and action_type in self.activation_costs:
            activation_cost = self.activation_costs[action_type]
        cost = RemedialActionCost(
            activation_cost=_value("activationCost", activation_cost, self.defaults.activation_cost),
            up=_value("variationCosts.up", variation.get("up"), self.defaults.up),
            down=_value("variationCosts.down", variation.get("down"), self.defaults.down),
        )
        return cost, defaulted

    def apply(self, actions: list[BaseModel], action_type: str = "remedial action",
              types: dict[str, str] | None = None) -> dict[str, list[str]]:
        """
        Set the costs on CRAC action models in place: activationCost on every action and
        variationCosts on range actions (models with a 'variationCosts' field).
        types maps action id -> remedial action type (see remedial_action_types).

        Returns {action id: values taken from defaults}. One warning per action type
        summarizes the defaulted actions, the details are logged at debug level.
        """
        from rao.crac.models import VariationCosts

        defaulted_actions = {}
        for action in actions:
            cost, defaulted = self.resolve(action.id, getattr(action, "name", None), (types or {}).get(action.id))
            is_range_action = "variationCosts" in type(action).model_fields
            if not is_range_action:
                defaulted = [key for key in defaulted if not key.startswith("variationCosts")]
            action.activationCost = cost.activation_cost
            if is_range_action:
                action.variationCosts = VariationCosts(up=cost.up, down=cost.down)
            if defaulted:
                defaulted_actions[action.id] = defaulted
                logger.debug(f"{action_type} {action.id}: using default costs for {', '.join(defaulted)}")

        if defaulted_actions:
            ids = sorted(defaulted_actions)
            shown = ", ".join(ids[:10]) + (f", ... ({len(ids) - 10} more)" if len(ids) > 10 else "")
            # Without per-action costs configured every action is defaulted, nothing to warn about
            log = logger.warning if self.remedial_actions else logger.debug
            log(f"{len(ids)} of {len(actions)} {action_type}s use default costs (not in the cost config): {shown}")
        return defaulted_actions


def is_countertrade(action: BaseModel) -> bool:
    """Countertrade injection range action. TODO naming convention not defined yet, no action matches."""
    return False


def network_action_types(actions: list[BaseModel]) -> dict[str, str]:
    """'topologyUpAndDown' for both actions of an 'upAndDown' topology action pair, else 'networkAction'."""
    ids = {action.id for action in actions}
    return {action.id: "topologyUpAndDown" if (action.id.endswith(OPPOSITE_DIRECTION_SUFFIX)
                                               or f"{action.id}{OPPOSITE_DIRECTION_SUFFIX}" in ids)
            else "networkAction" for action in actions}


def injection_range_action_types(actions: list[BaseModel]) -> dict[str, str]:
    """'countertrade' or 'redispatch' per injection range action."""
    return {action.id: "countertrade" if is_countertrade(action) else "redispatch" for action in actions}


def _check_action_section(name: str, section) -> None:
    if not isinstance(section, dict):
        raise ValueError(f"Cost config '{name}' must be a mapping")
    unknown = set(section) - _ACTION_KEYS
    if unknown:
        raise ValueError(f"Unknown keys in cost config '{name}': {sorted(unknown)}")
    variation = section.get("variationCosts") or {}
    if not isinstance(variation, dict) or set(variation) - _VARIATION_KEYS:
        raise ValueError(f"Cost config '{name}.variationCosts' must be a mapping with 'up' and/or 'down'")


def _cost(name: str, value) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"Cost config '{name}' must be a number, got: {value!r}") from None
    if number < 0:
        raise ValueError(f"Cost config '{name}' must not be negative, got: {number}")
    return number
