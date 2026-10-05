"""
Local redispatching: per-unit redispatching rows (one UP and one DOWN remedial action per
generating unit) -> OpenRAO ``injectionRangeActions``. The CRAC assembly is in
rao.crac.costly_ra.crac.

OpenRAO's native NC CRAC importer is deliberately not used: it only maps
RotatingMachineAction with direction 'none' as a fixed set-point network action, so
up/down ranges would not become range actions.

Mapping rules:
    - rows come from the remedial action list (NC RemedialAction profile or RCC export), Pmin,
      Pmax and availability are mapped from it only, the network model is never read
    - only RotatingMachineAction rows on RotatingMachine.p are processed,
      value_kind must be 'absolute'
    - rows are grouped per grid element, ONE InjectionRangeAction is emitted per unit
      (OpenRAO does not allow two range actions on the same element)
    - action id = ra_name without the _UP/_DOWN suffix, operator = party
    - networkElementIdsAndKeys = {_<element>: 1.0}, so the set-point is the generator MW; the id
      gets a single leading '_' like all CRAC elements (IIDM ids follow the CGMES rdf:ID)
    - UP normal_value = Pmax, DOWN normal_value = Pmin; without a DOWN value Pmin = 0, without
      an UP value the unit is left out of the CRAC (the shift cannot be bounded)
    - availability is expressed as intersected ranges (see _ranges())
"""
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from loguru import logger
from rao.crac import models
from rao.crac.costly_ra.costs import CostConfig
from rao.crac.costly_ra.crac import BIG, DEFAULT_INSTANT, action_to_dict, normalize_element_id, usage_rules
from rao.crac.costly_ra.sources import DIRECTION_DOWN, DIRECTION_UP, RedispatchRow

REDISPATCH_ALTERATION_TYPE = "RotatingMachineAction"
REDISPATCH_PROPERTY = "RotatingMachine.p"
ABSOLUTE_VALUE_KIND = "absolute"

_DIRECTION_SUFFIXES = ("_UP", "_DOWN")


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


def unit_action_id(ra_name: str) -> str:
    """RA_RD_KHES_G5_UP / RA_RD_KHES_G5_DOWN -> RA_RD_KHES_G5"""
    for suffix in _DIRECTION_SUFFIXES:
        if ra_name.upper().endswith(suffix):
            return ra_name[: -len(suffix)]
    return ra_name


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


def _is_redispatch_row(row: RedispatchRow) -> bool:
    return (row.alteration_type == REDISPATCH_ALTERATION_TYPE
            and row.property == REDISPATCH_PROPERTY
            and row.direction in (DIRECTION_UP, DIRECTION_DOWN))


def build_injection_range_actions(rows: Iterable[RedispatchRow],
                                  costs: CostConfig | None = None,
                                  instant: str = DEFAULT_INSTANT,
                                  contingency_ids: list[str] | None = None) -> RedispatchBuildResult:
    """
    Map redispatching rows to one InjectionRangeAction per generating unit.

    Args:
        rows: typed rows from any RowSource
        costs: cost configuration. Costs are disabled when not given: no activationCost /
            variationCosts are written and no cost warnings are logged (MAX_MIN_MARGIN runs
            do not use them). Pass a CostConfig to enable them, e.g. for MIN_COST.
        instant: instant of the usage rule (default 'curative')
        contingency_ids: if given, onContingencyStateUsageRules for these contingencies
            are emitted instead of an onInstantUsageRule
    """
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
    invalid = [row.ra_name for row in relevant
               if row.normal_value is not None and row.value_kind != ABSOLUTE_VALUE_KIND]
    if invalid:
        raise ValueError(f"Only value_kind '{ABSOLUTE_VALUE_KIND}' is supported for redispatching rows, "
                         f"got other value kinds for: {invalid}")

    # Group per unit; ids with and without leading '_' denote the same element
    groups: dict[str, list[RedispatchRow]] = {}
    for row in relevant:
        groups.setdefault(row.grid_element_id.lstrip("_"), []).append(row)

    actions = []
    for unit_rows in groups.values():
        action = _build_unit_action(unit_rows, costs, instant, contingency_ids, result)
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
                       costs: CostConfig | None,
                       instant: str,
                       contingency_ids: list[str] | None,
                       result: RedispatchBuildResult) -> models.InjectionRangeAction | None:
    up_rows = [row for row in rows if row.direction == DIRECTION_UP]
    down_rows = [row for row in rows if row.direction == DIRECTION_DOWN]
    element_id = normalize_element_id(rows[0].grid_element_id)
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

    # UP normal value is Pmax, DOWN normal value is Pmin, both from the remedial action list.
    # Without Pmax the shift cannot be bounded, so the unit is left out of the CRAC.
    if up_row is None or up_row.normal_value is None:
        _skip(result, unit_id, element_id, "no max range (UP normalValue) in the remedial action list, "
                                           "cannot determine how much to shift")
        return None
    p_max = up_row.normal_value

    if down_row is not None and down_row.normal_value is not None:
        p_min = down_row.normal_value
    else:
        p_min = 0.0
        logger.warning(f"Redispatch unit {unit_id}: no min range (DOWN normalValue) in the remedial action list, "
                       f"using Pmin = 0")

    if p_min > p_max:
        _skip(result, unit_id, element_id, f"Pmin {p_min} is greater than Pmax {p_max}")
        return None

    # A missing DOWN remedial action means DOWN is not offered
    up_available = up_row is not None and up_row.available
    down_available = down_row is not None and down_row.available
    if not up_available and not down_available:
        _skip(result, unit_id, element_id, "neither UP nor DOWN direction is available")
        return None

    cost_fields = {}
    if costs is not None:
        cost, defaulted = costs.resolve(unit_id)
        if defaulted:
            logger.warning(f"Redispatch unit {unit_id}: using default costs for {', '.join(defaulted)}")
            result.defaulted_costs[unit_id] = defaulted
        cost_fields = {"activationCost": cost.activation_cost,
                       "variationCosts": models.VariationCosts(up=cost.up, down=cost.down)}

    return models.InjectionRangeAction(
        id=unit_id,
        name=unit_id,
        operator=rows[0].party,
        **cost_fields,
        networkElementIdsAndKeys={element_id: 1.0},
        ranges=_ranges(p_min, p_max, up_available, down_available),
        **usage_rules(instant, contingency_ids),
    )
