from pathlib import Path
import pandas as pd
import pypowsybl
import pytest
from loguru import logger
from rao.crac.redispatch.sources import RedispatchRow

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples" / "redispatch"
TC1_CGMES = REPO_ROOT / "test-data" / "tests" / "test-data" / "TC1_CGMES.zip"


@pytest.fixture
def log_messages():
    """Collect loguru messages of level WARNING and above."""
    messages = []
    handler_id = logger.add(lambda message: messages.append(message.record["message"]), level="WARNING")
    yield messages
    logger.remove(handler_id)


def make_row(ra_name: str, element_id: str, direction: str, normal_value: float, available: bool = True,
             party: str = "AST", kind: str = "curative", value_kind: str = "absolute",
             alteration_type: str = "RotatingMachineAction", property: str = "RotatingMachine.p") -> RedispatchRow:
    return RedispatchRow(kind=kind, ra_name=ra_name, available=available, area="Latvia", party=party,
                         alteration_type=alteration_type, alteration_name=ra_name.removeprefix("RA_"),
                         property=property, grid_element_id=element_id, normal_value=normal_value,
                         direction=direction, value_kind=value_kind)


def unit_rows(unit: str, element_id: str, p_min: float, p_max: float, up: bool = True, down: bool = True,
              **kwargs) -> list[RedispatchRow]:
    """UP and DOWN rows of one unit."""
    return [make_row(f"{unit}_UP", element_id, "up", p_max, available=up, **kwargs),
            make_row(f"{unit}_DOWN", element_id, "down", p_min, available=down, **kwargs)]


def _frame(**columns) -> pd.DataFrame:
    return pd.DataFrame(columns).set_index("id")


def create_network(lines: list[tuple[str, str, str, float]],
                   generators: list[tuple[str, str, float, float, float]],
                   loads: list[tuple[str, str, float]],
                   buses: tuple[str, ...] = ("B1", "B2", "B3")) -> pypowsybl.network.Network:
    """
    Bus-breaker test network, one 400 kV voltage level per bus.

    lines: (id, bus1, bus2, x), generators: (id, bus, min_p, max_p, target_p), loads: (id, bus, p0)
    """
    network = pypowsybl.network.create_empty("redispatch-test")
    substations = [f"S{bus}" for bus in buses]
    voltage_levels = [f"VL{bus}" for bus in buses]
    vl_of = dict(zip(buses, voltage_levels))
    network.create_substations(_frame(id=substations))
    network.create_voltage_levels(_frame(id=voltage_levels, substation_id=substations,
                                         topology_kind=["BUS_BREAKER"] * len(buses), nominal_v=[400.0] * len(buses)))
    network.create_buses(_frame(id=list(buses), voltage_level_id=voltage_levels))
    if lines:
        n = len(lines)
        network.create_lines(_frame(
            id=[line[0] for line in lines],
            voltage_level1_id=[vl_of[line[1]] for line in lines], bus1_id=[line[1] for line in lines],
            voltage_level2_id=[vl_of[line[2]] for line in lines], bus2_id=[line[2] for line in lines],
            r=[0.0] * n, x=[line[3] for line in lines], g1=[0.0] * n, b1=[0.0] * n, g2=[0.0] * n, b2=[0.0] * n))
        # OpenRAO needs current limits on branches monitored by FlowCNECs
        line_ids = [line[0] for line in lines]
        network.create_loading_limits(pd.DataFrame({
            "element_id": line_ids * 2, "side": ["ONE"] * n + ["TWO"] * n, "name": ["permanent"] * (2 * n),
            "type": ["CURRENT"] * (2 * n), "value": [3000.0] * (2 * n), "acceptable_duration": [-1] * (2 * n),
        }).set_index("element_id"))
    n = len(generators)
    network.create_generators(_frame(
        id=[g[0] for g in generators], voltage_level_id=[vl_of[g[1]] for g in generators],
        bus_id=[g[1] for g in generators], min_p=[g[2] for g in generators], max_p=[g[3] for g in generators],
        target_p=[g[4] for g in generators], target_v=[400.0] * n, voltage_regulator_on=[True] * n))
    if loads:
        network.create_loads(_frame(id=[load[0] for load in loads], voltage_level_id=[vl_of[load[1]] for load in loads],
                                    bus_id=[load[1] for load in loads], p0=[load[2] for load in loads], q0=[0.0] * len(loads)))
    return network


def triangle_network(parallel_l13: bool = False) -> pypowsybl.network.Network:
    """
    GEN_A 400 MW at B1, GEN_B 400 MW at B2, load 800 MW at B3, x = 10 ohm per line.
    With parallel_l13, L13 is split into L13a/L13b (x = 20 ohm each, same base flows).
    """
    if parallel_l13:
        lines = [("L12", "B1", "B2", 10.0), ("L13a", "B1", "B3", 20.0), ("L13b", "B1", "B3", 20.0),
                 ("L23", "B2", "B3", 10.0)]
    else:
        lines = [("L12", "B1", "B2", 10.0), ("L13", "B1", "B3", 10.0), ("L23", "B2", "B3", 10.0)]
    return create_network(lines=lines,
                          generators=[("GEN_A", "B1", 0.0, 500.0, 400.0), ("GEN_B", "B2", 0.0, 800.0, 400.0)],
                          loads=[("LOAD", "B3", 800.0)])
