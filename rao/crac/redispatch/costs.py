"""
Redispatching cost configuration.

Costs are not part of the remedial-action export, they are loaded from a separate YAML or
JSON file keyed by the unit action id (ra_name without the _UP/_DOWN suffix):

    defaults:
      activationCost: 0.0
      variationCosts: {up: 1.0, down: 1.0}
    units:
      RA_RD_KHES_G5:
        activationCost: 100.0
        variationCosts: {up: 50.0, down: 50.0}

Any value missing for a unit falls back to ``defaults`` (and then to the built-in defaults
below); every unit using a default is reported with a warning.
"""
import json
from dataclasses import dataclass, field
from pathlib import Path
from loguru import logger

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

# Built-in defaults, used when the cost config does not define a 'defaults' section.
# A non-zero variation cost keeps MIN_COST from treating redispatch volume as free.
DEFAULT_ACTIVATION_COST = 0.0
DEFAULT_VARIATION_COST = 1.0

_UNIT_KEYS = {"activationCost", "variationCosts"}
_VARIATION_KEYS = {"up", "down"}


@dataclass(frozen=True)
class UnitCost:
    activation_cost: float
    up: float
    down: float


@dataclass
class CostConfig:
    defaults: UnitCost = field(default_factory=lambda: UnitCost(
        DEFAULT_ACTIVATION_COST, DEFAULT_VARIATION_COST, DEFAULT_VARIATION_COST))
    units: dict[str, dict] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict | None) -> "CostConfig":
        data = data or {}
        unknown = set(data) - {"defaults", "units"}
        if unknown:
            raise ValueError(f"Unknown sections in cost config: {sorted(unknown)}, expected 'defaults' and 'units'")

        builtin = cls().defaults
        defaults_section = data.get("defaults") or {}
        _check_unit_section("defaults", defaults_section)
        variation = defaults_section.get("variationCosts") or {}
        defaults = UnitCost(
            activation_cost=_cost("defaults.activationCost", defaults_section.get("activationCost", builtin.activation_cost)),
            up=_cost("defaults.variationCosts.up", variation.get("up", builtin.up)),
            down=_cost("defaults.variationCosts.down", variation.get("down", builtin.down)),
        )

        units = data.get("units") or {}
        if not isinstance(units, dict):
            raise ValueError("Cost config 'units' must be a mapping keyed by unit action id")
        for unit_id, section in units.items():
            _check_unit_section(f"units.{unit_id}", section)

        return cls(defaults=defaults, units={str(k): v or {} for k, v in units.items()})

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
        logger.info(f"Loaded redispatch cost config from: {path}")
        return cls.from_dict(data)

    def resolve(self, unit_id: str) -> tuple[UnitCost, list[str]]:
        """Return the costs of a unit and the names of the values taken from defaults."""
        section = self.units.get(unit_id, {})
        variation = section.get("variationCosts") or {}
        defaulted = []

        def _value(name: str, value, default: float) -> float:
            if value is None:
                defaulted.append(name)
                return default
            return _cost(f"units.{unit_id}.{name}", value)

        cost = UnitCost(
            activation_cost=_value("activationCost", section.get("activationCost"), self.defaults.activation_cost),
            up=_value("variationCosts.up", variation.get("up"), self.defaults.up),
            down=_value("variationCosts.down", variation.get("down"), self.defaults.down),
        )
        return cost, defaulted


def _check_unit_section(name: str, section) -> None:
    if not isinstance(section, dict):
        raise ValueError(f"Cost config '{name}' must be a mapping")
    unknown = set(section) - _UNIT_KEYS
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
