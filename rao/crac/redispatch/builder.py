"""
CRAC builder for local redispatching.

Converts per-unit redispatching rows (one UP and one DOWN row per generating unit) into
OpenRAO ``injectionRangeActions`` and assembles/merges/validates a JSON CRAC for
pypowsybl 1.16.1 (OpenRAO 7.3.0).

OpenRAO's native NC CRAC importer is deliberately not used: it only maps
RotatingMachineAction with direction 'none' as a fixed set-point network action, so
up/down ranges would not become range actions.

Mapping rules:
    - only RotatingMachineAction rows on RotatingMachine.p are processed,
      value_kind must be 'absolute'
    - rows are grouped per grid element, ONE InjectionRangeAction is emitted per unit
      (OpenRAO does not allow two range actions on the same element)
    - action id = ra_name without the _UP/_DOWN suffix, operator = party
    - networkElementIdsAndKeys = {element: 1.0}, so the set-point is the generator MW
    - UP normal_value = Pmax, DOWN normal_value = Pmin
    - availability is expressed as intersected ranges (see _ranges())
"""
import json
from collections import Counter
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass, field
from io import BytesIO
import pandas as pd
import pypowsybl
from loguru import logger
from rao.crac import models
from rao.crac.redispatch.costs import CostConfig
from rao.crac.redispatch.sources import DIRECTION_DOWN, DIRECTION_UP, RedispatchRow

CRAC_VERSION = "2.10"
BIG = 100000.0

REDISPATCH_ALTERATION_TYPE = "RotatingMachineAction"
REDISPATCH_PROPERTY = "RotatingMachine.p"
ABSOLUTE_VALUE_KIND = "absolute"

DEFAULT_INSTANT = "curative"
DEFAULT_INSTANTS = [
    {"id": "preventive", "kind": "PREVENTIVE"},
    {"id": "outage", "kind": "OUTAGE"},
    {"id": "curative", "kind": "CURATIVE"},
]

# CRAC JSON keys holding remedial actions, all ids must be unique across them
REMEDIAL_ACTION_KEYS = (
    "networkActions", "pstRangeActions", "hvdcRangeActions", "injectionRangeActions", "counterTradeRangeActions",
)

# Range types as written in the CRAC JSON and as returned by pypowsybl Crac.get_ranges()
_RANGE_TYPE_NAMES = {
    "absolute": "ABSOLUTE",
    "relativeToInitialNetwork": "RELATIVE_TO_INITIAL_NETWORK",
    "relativeToPreviousInstant": "RELATIVE_TO_PREVIOUS_INSTANT",
}

_DIRECTION_SUFFIXES = ("_UP", "_DOWN")


class CracMergeError(ValueError):
    """Raised when injection range actions cannot be merged into an existing CRAC."""


class CracValidationError(ValueError):
    """Raised when the generated CRAC is not imported by OpenRAO as expected."""


@dataclass(frozen=True)
class SkippedUnit:
    unit_id: str
    grid_element_id: str
    reason: str


@dataclass
class RedispatchBuildResult:
    """Outcome of the row -> injection range action mapping."""
    actions: list[models.InjectionRangeAction] = field(default_factory=list)
    skipped: list[SkippedUnit] = field(default_factory=list)
    ignored_rows: list[RedispatchRow] = field(default_factory=list)
    defaulted_costs: dict[str, list[str]] = field(default_factory=dict)

    def to_dicts(self) -> list[dict]:
        return [action_to_dict(action) for action in self.actions]

    def summary(self) -> str:
        lines = [f"Injection range actions written: {len(self.actions)}"]
        for action in self.actions:
            element_id = next(iter(action.networkElementIdsAndKeys))
            ranges = ", ".join(f"{r.rangeType}[{r.min:g}, {r.max:g}]" for r in action.ranges)
            lines.append(f"  {action.id} ({action.operator}) -> {element_id}: {ranges}")
        lines.append(f"Units skipped: {len(self.skipped)}")
        for unit in self.skipped:
            lines.append(f"  {unit.unit_id} ({unit.grid_element_id}): {unit.reason}")
        if self.defaulted_costs:
            lines.append(f"Units using default costs: {len(self.defaulted_costs)}")
            for unit_id, names in sorted(self.defaulted_costs.items()):
                lines.append(f"  {unit_id}: {', '.join(names)}")
        if self.ignored_rows:
            lines.append(f"Rows ignored (not RotatingMachine.p up/down redispatch): {len(self.ignored_rows)}")
        return "\n".join(lines)


def action_to_dict(action: models.InjectionRangeAction | dict) -> dict:
    if isinstance(action, dict):
        return deepcopy(action)
    return action.model_dump(exclude_none=True, by_alias=True)


def unit_action_id(ra_name: str) -> str:
    """RA_RD_KHES_G5_UP / RA_RD_KHES_G5_DOWN -> RA_RD_KHES_G5"""
    for suffix in _DIRECTION_SUFFIXES:
        if ra_name.upper().endswith(suffix):
            return ra_name[: -len(suffix)]
    return ra_name


def _id_candidates(element_id: str) -> list[str]:
    # CGMES rdf:IDs may or may not keep the leading '_' after the IIDM import
    stripped = element_id.lstrip("_")
    return list(dict.fromkeys([element_id, stripped, f"_{stripped}"]))


def resolve_generator_id(element_id: str, generator_ids: Iterable[str]) -> str | None:
    """Return the network generator id matching element_id with or without leading '_'."""
    ids = generator_ids if isinstance(generator_ids, (set, frozenset, pd.Index)) else set(generator_ids)
    for candidate in _id_candidates(element_id):
        if candidate in ids:
            return candidate
    return None


def _ranges(p_min: float, p_max: float, up_available: bool, down_available: bool) -> list[models.InjectionRange]:
    """
    OpenRAO intersects all ranges of an action, so the current output P0 is not needed:
        - both directions -> [Pmin, Pmax]
        - UP only         -> [Pmin, Pmax] & [P0, P0 + BIG]  = [P0, Pmax]
        - DOWN only       -> [Pmin, Pmax] & [P0 - BIG, P0]  = [Pmin, P0]
    """
    ranges = [models.InjectionRange(rangeType="absolute", min=p_min, max=p_max)]
    if up_available and not down_available:
        ranges.append(models.InjectionRange(rangeType="relativeToInitialNetwork", min=0.0, max=BIG))
    elif down_available and not up_available:
        ranges.append(models.InjectionRange(rangeType="relativeToInitialNetwork", min=-BIG, max=0.0))
    return ranges


def _usage_rules(instant: str, contingency_ids: list[str] | None) -> dict:
    if contingency_ids:
        return {"onContingencyStateUsageRules": [
            {"instant": instant, "contingencyId": contingency_id}
            for contingency_id in dict.fromkeys(contingency_ids)
        ]}
    return {"onInstantUsageRules": [{"instant": instant}]}


def _is_redispatch_row(row: RedispatchRow) -> bool:
    return (row.alteration_type == REDISPATCH_ALTERATION_TYPE
            and row.property == REDISPATCH_PROPERTY
            and row.direction in (DIRECTION_UP, DIRECTION_DOWN))


def build_injection_range_actions(rows: Iterable[RedispatchRow],
                                  costs: CostConfig | None = None,
                                  network: pypowsybl.network.Network | None = None,
                                  instant: str = DEFAULT_INSTANT,
                                  contingency_ids: list[str] | None = None) -> RedispatchBuildResult:
    """
    Map redispatching rows to one InjectionRangeAction per generating unit.

    Args:
        rows: typed rows from any RowSource
        costs: cost configuration, built-in defaults are used if not given
        network: if given, element ids are resolved against its generators and missing
            UP/DOWN rows fall back to the generator max_p/min_p
        instant: instant of the usage rule (default 'curative')
        contingency_ids: if given, onContingencyStateUsageRules for these contingencies
            are emitted instead of an onInstantUsageRule
    """
    costs = costs or CostConfig()
    result = RedispatchBuildResult()

    relevant = []
    for row in rows:
        if _is_redispatch_row(row):
            relevant.append(row)
        else:
            logger.debug(f"Row ignored, not an up/down {REDISPATCH_ALTERATION_TYPE} on {REDISPATCH_PROPERTY}: {row.ra_name}")
            result.ignored_rows.append(row)
    if result.ignored_rows:
        logger.info(f"{len(result.ignored_rows)} rows ignored, not up/down {REDISPATCH_PROPERTY} redispatch rows")

    # Fail fast: relative values would need the current output and are not supported
    invalid = [row.ra_name for row in relevant if row.value_kind != ABSOLUTE_VALUE_KIND]
    if invalid:
        raise ValueError(f"Only value_kind '{ABSOLUTE_VALUE_KIND}' is supported for redispatching rows, "
                         f"got other value kinds for: {invalid}")

    generators = network.get_generators(attributes=["min_p", "max_p", "target_p"]) if network is not None else None
    if generators is None:
        logger.warning("No network given, element ids are not validated and missing UP/DOWN rows "
                       "fall back to Pmin = 0 / Pmax = BIG")

    # Group per unit; ids with and without leading '_' denote the same element
    groups: dict[str, list[RedispatchRow]] = {}
    for row in relevant:
        groups.setdefault(row.grid_element_id.lstrip("_"), []).append(row)

    actions = []
    for unit_rows in groups.values():
        action = _build_unit_action(unit_rows, costs, generators, instant, contingency_ids, result)
        if action is not None:
            actions.append(action)

    # Action ids must be unique in a CRAC; never pick one of two conflicting units silently
    duplicated_ids = {action_id for action_id, count in Counter(a.id for a in actions).items() if count > 1}
    for action in actions:
        if action.id in duplicated_ids:
            _skip(result, action.id, next(iter(action.networkElementIdsAndKeys)),
                  "action id is used by more than one grid element")

    result.actions = sorted((a for a in actions if a.id not in duplicated_ids), key=lambda a: a.id)
    result.skipped.sort(key=lambda unit: (unit.unit_id, unit.grid_element_id))
    logger.info(f"Built {len(result.actions)} injection range actions, skipped {len(result.skipped)} units")
    return result


def _skip(result: RedispatchBuildResult, unit_id: str, element_id: str, reason: str) -> None:
    logger.warning(f"Redispatch unit {unit_id} ({element_id}) skipped: {reason}")
    result.skipped.append(SkippedUnit(unit_id=unit_id, grid_element_id=element_id, reason=reason))


def _build_unit_action(rows: list[RedispatchRow],
                       costs: CostConfig,
                       generators: pd.DataFrame | None,
                       instant: str,
                       contingency_ids: list[str] | None,
                       result: RedispatchBuildResult) -> models.InjectionRangeAction | None:
    up_rows = [row for row in rows if row.direction == DIRECTION_UP]
    down_rows = [row for row in rows if row.direction == DIRECTION_DOWN]
    element_id = rows[0].grid_element_id
    unit_ids = sorted({unit_action_id(row.ra_name) for row in rows})
    unit_id = unit_ids[0]

    if len(up_rows) > 1 or len(down_rows) > 1:
        _skip(result, unit_id, element_id, f"{len(up_rows)} UP and {len(down_rows)} DOWN rows, expected at most one each")
        return None
    if len(unit_ids) > 1:
        _skip(result, unit_id, element_id, f"UP and DOWN rows give different action ids: {unit_ids}")
        return None
    parties = sorted({row.party for row in rows})
    if len(parties) > 1:
        _skip(result, unit_id, element_id, f"UP and DOWN rows have different parties: {parties}")
        return None
    kinds = sorted({row.kind for row in rows} - {instant})
    if kinds:
        logger.warning(f"Redispatch unit {unit_id}: row kind {kinds} differs from usage rule instant '{instant}'")

    up_row = up_rows[0] if up_rows else None
    down_row = down_rows[0] if down_rows else None

    # Resolve the element against network generators
    generator = None
    if generators is not None:
        network_id = resolve_generator_id(element_id, generators.index)
        if network_id is None:
            _skip(result, unit_id, element_id, "grid element not found among network generators")
            return None
        if network_id != element_id:
            logger.debug(f"Redispatch unit {unit_id}: element id {element_id} resolved to {network_id}")
        element_id = network_id
        generator = generators.loc[network_id]

    # UP row normal value is Pmax, DOWN row normal value is Pmin
    if up_row is not None:
        p_max = up_row.normal_value
    elif generator is not None:
        p_max = float(generator["max_p"])
        logger.warning(f"Redispatch unit {unit_id}: UP row missing, using network max_p = {p_max} as Pmax")
    else:
        p_max = BIG
        logger.warning(f"Redispatch unit {unit_id}: UP row missing and no network given, using Pmax = {BIG}")

    if down_row is not None:
        p_min = down_row.normal_value
    elif generator is not None:
        p_min = float(generator["min_p"])
        logger.warning(f"Redispatch unit {unit_id}: DOWN row missing, using network min_p = {p_min} as Pmin")
    else:
        p_min = 0.0
        logger.warning(f"Redispatch unit {unit_id}: DOWN row missing and no network given, using Pmin = 0")

    if p_min > p_max:
        _skip(result, unit_id, element_id, f"Pmin {p_min} is greater than Pmax {p_max}")
        return None

    # A missing row means the direction is not offered
    up_available = up_row is not None and up_row.available
    down_available = down_row is not None and down_row.available
    if not up_available and not down_available:
        _skip(result, unit_id, element_id, "neither UP nor DOWN direction is available")
        return None

    cost, defaulted = costs.resolve(unit_id)
    if defaulted:
        logger.warning(f"Redispatch unit {unit_id}: using default costs for {', '.join(defaulted)}")
        result.defaulted_costs[unit_id] = defaulted

    return models.InjectionRangeAction(
        id=unit_id,
        name=unit_id,
        operator=rows[0].party,
        activationCost=cost.activation_cost,
        variationCosts=models.VariationCosts(up=cost.up, down=cost.down),
        networkElementIdsAndKeys={element_id: 1.0},
        ranges=_ranges(p_min, p_max, up_available, down_available),
        **_usage_rules(instant, contingency_ids),
    )


"""
CRAC assembly
"""
def check_ra_usage_limits(ra_usage_limits: list[dict] | None, instants: list[dict]) -> None:
    """
    Warn about RA usage limit settings known to break redispatch in OpenRAO 7.3.0:
        - 'max-ra' counts range actions; redispatch always needs >= 2 actions (balance),
          so 'max-ra: 1' silently blocks all redispatch
        - limits are cumulative across curative instants
        - with several curative instants, 'max-topo-per-tso' throws a NullPointerException
          unless 'max-ra-per-tso' is also set for the same TSO
    """
    if not ra_usage_limits:
        return
    curative_instants = [i["id"] for i in instants if i.get("kind") == "CURATIVE"]
    multi_curative = len(curative_instants) > 1
    if multi_curative:
        logger.warning(f"RA usage limits are cumulative across curative instants {curative_instants}")

    for limit in ra_usage_limits:
        instant = limit.get("instant")
        max_ra = limit.get("max-ra")
        if max_ra is not None and max_ra < 2:
            logger.warning(f"RA usage limit 'max-ra: {max_ra}' at instant '{instant}' blocks redispatch, "
                           f"a balanced redispatch needs at least 2 range actions")
        for tso, value in (limit.get("max-ra-per-tso") or {}).items():
            if value < 2:
                logger.warning(f"RA usage limit 'max-ra-per-tso: {tso}: {value}' at instant '{instant}' "
                               f"blocks redispatch within {tso}")
        if multi_curative:
            missing = set(limit.get("max-topo-per-tso") or {}) - set(limit.get("max-ra-per-tso") or {})
            if missing:
                logger.warning(f"RA usage limit at instant '{instant}': 'max-topo-per-tso' without 'max-ra-per-tso' "
                               f"for {sorted(missing)} throws a NullPointerException in OpenRAO 7.3.0 "
                               f"with several curative instants")


def build_crac(injection_range_actions: Iterable[models.InjectionRangeAction | dict],
               crac_id: str = "RD_CRAC",
               name: str | None = None,
               version: str = CRAC_VERSION,
               ra_usage_limits: list[dict] | None = None) -> dict:
    """
    Build a standalone CRAC dict holding the injection range actions.

    'ra-usage-limits-per-instant' is not emitted unless ra_usage_limits is given, see
    check_ra_usage_limits() for the OpenRAO 7.3.0 pitfalls.
    """
    crac = {
        "type": "CRAC",
        "version": version,
        "id": crac_id,
        "name": name or crac_id,
        "instants": deepcopy(DEFAULT_INSTANTS),
    }
    if ra_usage_limits:
        check_ra_usage_limits(ra_usage_limits, crac["instants"])
        crac["ra-usage-limits-per-instant"] = deepcopy(ra_usage_limits)
    return merge_into_crac(crac, injection_range_actions)


def _range_action_elements(crac: dict) -> dict[str, str]:
    """Map normalized network element id -> id of the range action using it."""
    elements = {}
    for action in crac.get("injectionRangeActions") or []:
        for element_id in action.get("networkElementIdsAndKeys") or {}:
            elements[element_id.lstrip("_")] = action["id"]
    for key in ("pstRangeActions", "hvdcRangeActions"):
        for action in crac.get(key) or []:
            if element_id := action.get("networkElementId"):
                elements[element_id.lstrip("_")] = action["id"]
    return elements


def merge_into_crac(existing_crac: dict, injection_range_actions: Iterable[models.InjectionRangeAction | dict]) -> dict:
    """
    Return a copy of existing_crac with the injection range actions appended.

    Fails if a referenced instant or contingency does not exist, if an action id is
    already used, or if a network element is already used by another range action.
    Everything else (contingencies, flowCnecs, networkActions, ...) is preserved untouched.
    """
    crac = deepcopy(existing_crac)
    new_actions = sorted((action_to_dict(action) for action in injection_range_actions), key=lambda a: a["id"])

    instants = {instant["id"] for instant in crac.get("instants") or []}
    contingencies = {contingency["id"] for contingency in crac.get("contingencies") or []}
    action_ids = {action["id"] for key in REMEDIAL_ACTION_KEYS for action in crac.get(key) or []}
    elements = _range_action_elements(crac)

    for action in new_actions:
        for rule in action.get("onInstantUsageRules") or []:
            if rule["instant"] not in instants:
                raise CracMergeError(f"Action {action['id']}: instant '{rule['instant']}' is not defined in the CRAC "
                                     f"(available: {sorted(instants)})")
        for rule in action.get("onContingencyStateUsageRules") or []:
            if rule["instant"] not in instants:
                raise CracMergeError(f"Action {action['id']}: instant '{rule['instant']}' is not defined in the CRAC "
                                     f"(available: {sorted(instants)})")
            if rule["contingencyId"] not in contingencies:
                raise CracMergeError(f"Action {action['id']}: contingency '{rule['contingencyId']}' is not defined in the CRAC")

        if action["id"] in action_ids:
            raise CracMergeError(f"Remedial action id '{action['id']}' already exists in the CRAC")
        action_ids.add(action["id"])

        for element_id in action["networkElementIdsAndKeys"]:
            if (used_by := elements.get(element_id.lstrip("_"))) is not None:
                raise CracMergeError(f"Action {action['id']}: network element '{element_id}' is already used by "
                                     f"range action '{used_by}', OpenRAO allows one range action per element")
            elements[element_id.lstrip("_")] = action["id"]

    if new_actions:
        check_ra_usage_limits(crac.get("ra-usage-limits-per-instant"), crac.get("instants") or [])
        crac["injectionRangeActions"] = list(crac.get("injectionRangeActions") or []) + new_actions
    return crac


def crac_to_json(crac: dict) -> str:
    """Serialize deterministically, key order follows the CRAC dict (no key sorting)."""
    return json.dumps(crac, indent=2, ensure_ascii=False)


def import_crac(network: pypowsybl.network.Network, crac: dict):
    """Import a CRAC dict into pypowsybl/OpenRAO."""
    buffer = BytesIO(crac_to_json(crac).encode("utf-8"))
    return pypowsybl.rao.Crac.from_buffer_source(network, buffer, crac_file_name="crac.json")


def validate_crac(network: pypowsybl.network.Network, crac: dict):
    """
    Import the CRAC with OpenRAO and check its injection range actions and ranges.

    Also warns if a unit's initial set-point lies outside its absolute [Pmin, Pmax].
    Returns the imported pypowsybl Crac object.
    """
    imported = import_crac(network, crac)
    expected = {action["id"]: action for action in crac.get("injectionRangeActions") or []}

    injection_actions = imported.get_injection_range_actions()
    if len(injection_actions) != len(expected) or set(injection_actions.index) != set(expected):
        raise CracValidationError(f"Imported CRAC holds {len(injection_actions)} injection range actions, "
                                  f"expected {len(expected)}: missing {sorted(set(expected) - set(injection_actions.index))}")

    ranges = imported.get_ranges().reset_index()
    ranges = ranges[ranges["id"].isin(expected)]
    for action_id, action in expected.items():
        expected_ranges = sorted((_RANGE_TYPE_NAMES.get(r["rangeType"], r["rangeType"]), float(r["min"]), float(r["max"]))
                                 for r in action["ranges"])
        imported_ranges = sorted((row.range_type, float(row.min), float(row.max))
                                 for row in ranges[ranges["id"] == action_id].itertuples())
        if len(expected_ranges) != len(imported_ranges) or any(
                e[0] != i[0] or abs(e[1] - i[1]) > 1e-6 or abs(e[2] - i[2]) > 1e-6
                for e, i in zip(expected_ranges, imported_ranges)):
            raise CracValidationError(f"Action {action_id}: imported ranges {imported_ranges} differ from {expected_ranges}")

        absolute = [r for r in action["ranges"] if r["rangeType"] == "absolute"]
        initial = injection_actions.loc[action_id, "initial_set_point"]
        for r in absolute:
            if not r["min"] <= initial <= r["max"]:
                logger.warning(f"Action {action_id}: initial set-point {initial} MW lies outside its absolute "
                               f"range [{r['min']}, {r['max']}]")

    logger.info(f"CRAC validated by OpenRAO import: {len(expected)} injection range actions")
    return imported
