"""
Open switches isolating a branch from the rest of its voltage levels.

Some TSOs take equipment out of service by opening its bay breaker or disconnector. A terminals 'close' remedial
action alone does not reconnect it, the open bay switches have to be closed with it
(CracBuilder.attach_bay_switch_actions). The network is only read. Terminals disconnected in the SSH are looked up
too, the CGMES import keeps them in their bay:
    - node-breaker import: on their own node behind an open fictitious switch ('..._SW_fict'), closed with the others
    - bus-breaker import (import-node-breaker-as-bus-breaker): disconnected on the bay bus
Rules:
    - branches: lines, 2w transformers (also replaced 3w transformer legs) and boundary lines
    - rest of the voltage level: nearest bus or node holding other equipment (busbar sections included), shunt
      compensators and static var compensators excluded (line reactors can sit in the line bay)
    - path: fewest open switches, ties broken by switch id order
"""
import heapq
import pandas as pd

# Not counted as other equipment: line reactors can sit on the line side of the bay breaker
BAY_EQUIPMENT_TYPES = ("SHUNT_COMPENSATOR", "STATIC_VAR_COMPENSATOR")


class BaySwitches:
    """Open bay switches of the branches of one network, topology read once per voltage level."""

    def __init__(self, network):
        self.network = network
        self._node_breaker = None  # node-breaker voltage levels
        self._ends = None  # {branch id: [(voltage level, bus or node)] of its sides}
        self._topologies = {}  # {voltage level: (switches per bus or node, equipment per bus or node)}
        self._found = {}  # {branch id: (open switch ids or None, tie)}

    def find(self, element_id: str) -> tuple[list[str] | None, bool]:
        """
        (open switch ids, tie) between a branch and the rest of its voltage levels, connected terminals or not.
        Switch ids []: nothing to close (also unknown branches), None: no path to other equipment.
        tie: another set of open switches was as short, the pick follows switch id order.
        """
        if element_id not in self._found:
            found, tie = [], False
            for voltage_level, start in self._branch_ends().get(element_id, []):
                path, side_tie = self._search(voltage_level, start, element_id)
                if path is None:
                    found, tie = None, False
                    break
                found += path
                tie |= side_tie
            self._found[element_id] = (found, tie)
        found, tie = self._found[element_id]
        return (list(found) if found is not None else None), tie

    def __contains__(self, element_id: str) -> bool:
        """Branch looked up: line, 2w transformer or boundary line of the network."""
        return element_id in self._branch_ends()

    def _is_node_breaker(self, voltage_level: str) -> bool:
        if self._node_breaker is None:
            kinds = self.network.get_voltage_levels(attributes=["topology_kind"])["topology_kind"]
            self._node_breaker = set(kinds.index[kinds == "NODE_BREAKER"])
        return voltage_level in self._node_breaker

    def _branch_ends(self) -> dict[str, list[tuple[str, str | int]]]:
        if self._ends is None:
            columns = ["voltage_level", "bus", "node"]
            sides = []
            for get_branches in (self.network.get_lines, self.network.get_2_windings_transformers):
                for side in ("1", "2"):
                    attributes = [f"voltage_level{side}_id", f"bus_breaker_bus{side}_id", f"node{side}"]
                    sides.append(get_branches(attributes=attributes).set_axis(columns, axis=1))
            boundary_lines = self.network.get_boundary_lines(attributes=["voltage_level_id", "bus_breaker_bus_id",
                                                                         "node"])
            sides.append(boundary_lines.set_axis(columns, axis=1))
            self._ends = {}
            for element_id, voltage_level, bus, node in pd.concat(sides).itertuples():
                # Disconnected terminals too, they stay on their bay bus or node
                start = int(node) if self._is_node_breaker(voltage_level) else bus
                if start not in ("", -1) and not pd.isna(start):
                    self._ends.setdefault(element_id, []).append((voltage_level, start))
        return self._ends

    def _topology(self, voltage_level: str) -> tuple[dict, dict]:
        """(switches per vertex, equipment per vertex), vertices: nodes, or buses in bus-breaker voltage levels."""
        if voltage_level not in self._topologies:
            if self._is_node_breaker(voltage_level):
                topology = self.network.get_node_breaker_topology(voltage_level)
                switches = topology.switches.sort_index()
                edges = list(zip(switches.index, switches["node1"], switches["node2"], switches["open"]))
                # Internal connections: closed, without a switch
                connections = topology.internal_connections
                edges += [(None, *nodes, False) for nodes in zip(connections["node1"], connections["node2"])]
                elements = pd.DataFrame({"vertex": topology.nodes.index, "type": topology.nodes["connectable_type"]},
                                        index=topology.nodes["connectable_id"].to_numpy())
                elements = elements[elements.index != ""]
            else:
                topology = self.network.get_bus_breaker_topology(voltage_level)
                switches = topology.switches.sort_index()
                edges = list(zip(switches.index, switches["bus1_id"], switches["bus2_id"], switches["open"]))
                # Disconnected elements have no bus
                elements = topology.elements.rename(columns={"bus_id": "vertex"})
                elements = elements[elements["vertex"] != ""]
            adjacency = {}
            for switch_id, vertex1, vertex2, is_open in edges:
                adjacency.setdefault(vertex1, []).append((switch_id, vertex2, int(is_open)))
                adjacency.setdefault(vertex2, []).append((switch_id, vertex1, int(is_open)))
            equipment = {}
            for element_id, vertex in elements.loc[~elements["type"].isin(BAY_EQUIPMENT_TYPES), "vertex"].items():
                equipment.setdefault(vertex, set()).add(element_id)
            self._topologies[voltage_level] = (adjacency, equipment)
        return self._topologies[voltage_level]

    def _search(self, voltage_level: str, start: str | int, element_id: str) -> tuple[list[str] | None, bool]:
        """Fewest open switches from the branch to a bus or node with other equipment, (open switch ids, tie)."""
        switches, equipment = self._topology(voltage_level)

        def has_equipment(vertex) -> bool:
            return bool(equipment.get(vertex, set()) - {element_id})

        # 0-1 search over the switches keyed (open switch count, their ids), so ties go to the lowest switch ids.
        # Vertices with equipment end a path, the search goes on to the end of that cost for the tie check.
        cost_of, path = {}, None
        queue = [(0, (), start)]
        while queue:
            cost, opened, vertex = heapq.heappop(queue)
            if path is not None and cost > len(path):
                break
            if vertex in cost_of:
                continue
            cost_of[vertex] = cost
            if has_equipment(vertex):
                path = opened if path is None else path
                continue
            for switch_id, next_vertex, is_open in switches.get(vertex, []):
                if next_vertex not in cost_of:
                    heapq.heappush(queue, (cost + is_open, opened + (switch_id,) if is_open else opened, next_vertex))
        if path is None:
            return None, False

        # Tie: the shortest paths together hold more open switches than one of them
        ends = [vertex for vertex, cost in cost_of.items() if cost == len(path) and has_equipment(vertex)]
        seen, on_shortest = set(ends), set()
        while ends:
            vertex = ends.pop()
            for switch_id, previous, is_open in switches.get(vertex, []):
                if (previous in cost_of and not has_equipment(previous)
                        and cost_of[previous] + is_open == cost_of[vertex]):
                    if is_open:
                        on_shortest.add(switch_id)
                    if previous not in seen:
                        seen.add(previous)
                        ends.append(previous)
        return list(path), len(on_shortest) > len(path)


def open_bay_switches(network, element_id: str) -> list[str] | None:
    """Open switches between a branch and the rest of its voltage levels.
    []: nothing to close, None: no path to other equipment found."""
    return BaySwitches(network).find(element_id)[0]
