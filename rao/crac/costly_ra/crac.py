"""
Generic CRAC helpers for costly remedial actions (redispatching, later countertrading):
assembly of a JSON CRAC for pypowsybl 1.16.1 (OpenRAO 7.3.0), merging into an existing
CRAC, usage rules/limits and validation by OpenRAO import.
"""
import json
from collections.abc import Iterable
from copy import deepcopy
from io import BytesIO
import pypowsybl
from loguru import logger
from pydantic import BaseModel

CRAC_VERSION = "2.10"
BIG = 100000.0

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


class CracMergeError(ValueError):
    """Raised when injection range actions cannot be merged into an existing CRAC."""


class CracValidationError(ValueError):
    """Raised when the generated CRAC is not imported by OpenRAO as expected."""


def action_to_dict(action: BaseModel | dict) -> dict:
    if isinstance(action, dict):
        return deepcopy(action)
    return action.model_dump(exclude_none=True, by_alias=True)


def normalize_element_id(element_id: str) -> str:
    """
    Single leading '_', as the IIDM ids imported from CGMES with rdf:ID ('source-for-iidm-id')
    and as the other CRAC elements are written: 'f157...' / '_f157...' / '__f157...' -> '_f157...'
    """
    return f"_{element_id.lstrip('_')}"


def usage_rules(instant: str, contingency_ids: list[str] | None) -> dict:
    if contingency_ids:
        return {"onContingencyStateUsageRules": [
            {"instant": instant, "contingencyId": contingency_id}
            for contingency_id in dict.fromkeys(contingency_ids)
        ]}
    return {"onInstantUsageRules": [{"instant": instant}]}


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


def build_crac(injection_range_actions: Iterable[BaseModel | dict],
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


def merge_into_crac(existing_crac: dict, injection_range_actions: Iterable[BaseModel | dict]) -> dict:
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

    This is an import check only, ranges are not checked against the network model.
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

    logger.info(f"CRAC validated by OpenRAO import: {len(expected)} injection range actions")
    return imported
