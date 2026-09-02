"""
Tests for Voltage CNEC and Angle CNEC creation in the CRAC builder (OpenRAO NC CRAC format).
Uses synthetic NC profile / EQ fixtures in test-data/TC2_*.xml.
"""
import json
import os
import sys

import pandas as pd
import pytest
import triplets  # noqa: F401 (registers pandas accessors)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rao.crac.builder import CracBuilder
from rao.crac import models

TEST_DATA = os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture(scope="module")
def crac():
    data = pd.read_RDF([os.path.join(TEST_DATA, "TC2_monitoring_profiles.xml")])
    network = pd.read_RDF([os.path.join(TEST_DATA, "TC2_network_eq.xml")])

    builder = CracBuilder(data=data, network=network)
    builder._crac = models.Crac()
    builder.process_contingencies()
    builder.process_cnecs()
    builder.process_monitoring_cnecs()
    return builder


def test_flow_cnecs_untouched(crac):
    # Inspect pre-serialization model: fixture has no current limits, so serialization
    # would drop flow CNECs on the (pre-existing) zero-threshold filter
    ids = [c.id for c in crac._crac.flowCnecs]
    assert "ae-flow-1-preventive" in ids
    assert "ae-flow-1-curative" in ids
    # Limit-based assessed elements must not leak into FlowCNECs
    assert not any("voltage" in i or "angle" in i for i in ids)


def test_voltage_cnecs_created(crac):
    crac = crac.crac
    voltage_cnecs = {c["id"]: c for c in crac["voltageCnecs"]}
    assert set(voltage_cnecs) == {
        "ae-voltage-1-preventive", "ae-voltage-1-curative",
        "ae-voltage-2-preventive", "ae-voltage-2-curative",
    }

    high = voltage_cnecs["ae-voltage-1-preventive"]
    # BusBarSection resolved to its VoltageLevel, serialized with '_' prefix
    assert high["networkElementId"] == "_vl-330"
    assert high["thresholds"] == [{"unit": "kilovolt", "max": 362.0}]
    assert high["optimized"] is False and high["monitored"] is True
    assert high["instant"] == "preventive" and "contingencyId" not in high

    low_curative = voltage_cnecs["ae-voltage-2-curative"]
    assert low_curative["thresholds"] == [{"unit": "kilovolt", "min": 300.0}]
    assert low_curative["instant"] == "curative"
    assert low_curative["contingencyId"] == "co-1"


def test_angle_cnecs_created(crac):
    crac = crac.crac
    angle_cnecs = {c["id"]: c for c in crac["angleCnecs"]}
    assert set(angle_cnecs) == {"ae-angle-1-preventive", "ae-angle-1-curative"}

    cnec = angle_cnecs["ae-angle-1-preventive"]
    # isFlowToRefTerminal=true -> exporting = AngleReferenceTerminal (terminal-1 -> busbar-1)
    assert cnec["exportingNetworkElementId"] == "_busbar-1"
    assert cnec["importingNetworkElementId"] == "_busbar-2"
    assert cnec["thresholds"] == [{"unit": "degree", "min": -30.0, "max": 30.0}]
    assert cnec["optimized"] is False and cnec["monitored"] is True


def test_crac_is_json_serializable(crac):
    crac = crac.crac
    dumped = json.dumps(crac)
    assert "voltageCnecs" in dumped and "angleCnecs" in dumped
