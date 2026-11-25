"""
Topological remedial actions that leave a substation weakly connected at transmission voltage.

A topology action is flagged when it opens lines so that a substation in scope loses a counted line
and ends on 0 or 1 of them, in the state the action is applied in. Flagged actions get the
'topologyWeakSubstation' activation cost (rao.crac.costs), they stay available to the RAO.
    - counted line: line, tie line or unpaired boundary line with nominal voltage >= min_kv at its
      ends and connected in the loaded model. An end outside the model (unpaired boundary line, also
      HVDC equivalents) or on a boundary voltage level (convert-boundary) counts for the substation
      inside only
    - substation in scope: holds a transformer, load or generator outside the boundary voltage levels,
      so tee points and the fictitious boundary substations are never flagged
    - state: curative actions after the outage of each CRAC contingency, preventive actions as loaded
Lines are counted, connectivity is not: two lines to the same neighbour count as two, and busbar
splitting or opened transformers are not assessed.
"""
from collections.abc import Iterable
from dataclasses import dataclass, field
import pandas as pd
from loguru import logger
from rao.crac.costly_ra.boundary_injection_lookup import BOUNDARY_VOLTAGE_LEVEL_NAME


def _norm(element_id) -> str:
    return str(element_id).lstrip("_")


def _count(lines: pd.DataFrame) -> pd.Series:
    """Counted lines per substation, ends outside the model (NaN) are not counted."""
    return pd.concat([lines["substation1"], lines["substation2"]]).value_counts()


@dataclass
class SubstationLines:
    lines: pd.DataFrame  # index: line id without '_'; substation1, substation2 (NaN: outside the model or boundary)
    substations: set[str]  # substations the rule applies to
    min_kv: float = 330.0
    aliases: dict[str, str] = field(default_factory=dict)  # tie line half (boundary line) id -> tie line id
    names: dict[str, str] = field(default_factory=dict)  # substation id -> name, for the log

    @classmethod
    def from_network(cls, network, min_kv: float = 330.0) -> "SubstationLines":
        voltage_levels = network.get_voltage_levels(attributes=["name", "substation_id", "nominal_v"])
        substation_of = voltage_levels["substation_id"].mask(voltage_levels["name"] == BOUNDARY_VOLTAGE_LEVEL_NAME)
        kv_of = voltage_levels["nominal_v"]

        lines = network.get_lines(attributes=["voltage_level1_id", "voltage_level2_id", "connected1", "connected2"])
        tie_lines = network.get_tie_lines(attributes=["boundary_line1_id", "boundary_line2_id", "connected1",
                                                      "connected2"])
        boundary_lines = network.get_boundary_lines(attributes=["voltage_level_id", "connected", "paired",
                                                                "tie_line_id"])
        unpaired = boundary_lines[~boundary_lines["paired"]]
        ends = pd.concat([
            pd.DataFrame({"vl1": lines["voltage_level1_id"], "vl2": lines["voltage_level2_id"],
                          "connected": lines["connected1"] & lines["connected2"]}),
            pd.DataFrame({"vl1": tie_lines["boundary_line1_id"].map(boundary_lines["voltage_level_id"]),
                          "vl2": tie_lines["boundary_line2_id"].map(boundary_lines["voltage_level_id"]),
                          "connected": tie_lines["connected1"] & tie_lines["connected2"]}),
            pd.DataFrame({"vl1": unpaired["voltage_level_id"], "vl2": None, "connected": unpaired["connected"]}),
        ])
        ends = ends[ends["connected"].astype(bool) & (ends["vl1"].map(kv_of) >= min_kv)
                    & (ends["vl2"].isna() | (ends["vl2"].map(kv_of) >= min_kv))]
        counted = pd.DataFrame({"substation1": ends["vl1"].map(substation_of),
                                "substation2": ends["vl2"].map(substation_of)})
        counted.index = counted.index.map(_norm)
        counted = counted[counted.notna().any(axis=1) & (counted["substation1"] != counted["substation2"])]

        equipment = pd.concat([
            network.get_2_windings_transformers(attributes=["voltage_level1_id"])["voltage_level1_id"],
            network.get_3_windings_transformers(attributes=["voltage_level1_id"])["voltage_level1_id"],
            network.get_loads(attributes=["voltage_level_id"])["voltage_level_id"],
            network.get_generators(attributes=["voltage_level_id"])["voltage_level_id"],
        ])
        paired = boundary_lines[boundary_lines["paired"]]
        return cls(lines=counted, substations=set(equipment.map(substation_of).dropna()), min_kv=min_kv,
                   aliases={_norm(half): _norm(tie) for half, tie in paired["tie_line_id"].items()},
                   names=network.get_substations(attributes=["name"])["name"].to_dict())

    def weakened(self, opened: Iterable[str], already_out: Iterable[str] = ()) -> dict[str, int]:
        """{substation: lines left} for substations in scope that lose a line to `opened` and end on <= 1 line."""
        base = self.lines[~self.lines.index.isin(self._ids(already_out))]
        after = base[~base.index.isin(self._ids(opened))]
        before_n = _count(base)
        after_n = _count(after).reindex(before_n.index, fill_value=0)
        hit = after_n[(after_n < before_n) & (after_n <= 1) & after_n.index.isin(self.substations)]
        return {substation: int(n) for substation, n in hit.items()}

    def _ids(self, element_ids: Iterable[str]) -> set[str]:
        """Ids without '_', a tie line half also stands for its tie line."""
        ids = {_norm(element_id) for element_id in element_ids}
        return ids | {self.aliases[i] for i in ids if i in self.aliases}


def weak_substation_actions(crac, topology: SubstationLines) -> dict[str, dict[str, int]]:
    """
    {network action id: {substation: lines left}} for the topology actions of the CRAC (rao.crac.models.Crac)
    that leave a substation on <= 1 counted line. Curative actions are checked after each contingency of
    the CRAC (flagged if any contingency triggers the rule), preventive actions in the network as loaded.
    """
    outages = [contingency.networkElementsIds for contingency in crac.contingencies or []]
    flagged = {}
    for action in crac.networkActions:
        opened = [t.networkElementId for t in action.terminalsConnectionActions or [] if t.actionType == "open"]
        if not opened:
            continue
        curative = any(rule.get("instant") == "curative" for rule in action.onInstantUsageRules or [])
        for outage in (outages if curative else []) or [()]:
            weakened = topology.weakened(opened, already_out=outage)
            if weakened:
                flagged[action.id] = weakened
                left = {topology.names.get(s) or s: n for s, n in weakened.items()}
                logger.info(f"Remedial action {action.name} ({action.id}) weakens substations, "
                            f"lines >= {topology.min_kv:g} kV left: {left}")
                break
    return flagged
