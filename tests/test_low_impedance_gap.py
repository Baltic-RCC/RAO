"""Low-impedance pre-processing run before every optimisation (rao/find_low_impedance_gap.py)."""
from unittest.mock import MagicMock

import pandas as pd
import pypowsybl
import pytest

from rao.find_low_impedance_gap import find_threshold_gap_branches, zero_out

TC1_LINE = "_ffbabc27-1ccd-4fdc-b037-e341706c8d29"
TC1_3W = "_84ed55f4-61f5-4d9d-8755-bba7b877a246"


def set_line_impedance_pu(network, line_id, r, x):
    network.per_unit = True
    network.update_lines(id=[line_id], r=[r], x=[x])
    network.per_unit = False


@pytest.mark.integration
class TestOnTc1Network:
    def test_branch_inventory(self, tc1_network):
        _, negative_x, branches = find_threshold_gap_branches(tc1_network)
        kinds = branches["kind"].value_counts().to_dict()
        assert kinds["tie_line"] == 5  # built from paired boundary lines
        assert {f"{TC1_3W}-leg{n}" for n in (1, 2, 3)} <= set(branches.index)
        assert any(kind.startswith("2wt (") for kind in kinds)  # tap-step corrected transformers
        assert set(negative_x["kind"]) == {"line"}  # TC1 contains a series-compensated (x < 0) line
        assert tc1_network.per_unit is False

    def test_gap_detected_and_zeroed(self, tc1_network):
        set_line_impedance_pu(tc1_network, TC1_LINE, 1e-6, 5e-6)
        gap, _, _ = find_threshold_gap_branches(tc1_network, lo=1e-8, hi=3e-5)
        assert gap.index.tolist() == [TC1_LINE]
        assert gap.loc[TC1_LINE, "z_pu"] == pytest.approx((1e-6 ** 2 + 5e-6 ** 2) ** 0.5)

        zero_out(tc1_network, gap)
        tc1_network.per_unit = True
        assert tc1_network.get_lines().loc[TC1_LINE, ["r", "x"]].tolist() == [0.0, 0.0]
        tc1_network.per_unit = False

    def test_default_network_has_no_gap(self, tc1_network):
        assert find_threshold_gap_branches(tc1_network)[0].empty


def test_only_branches_inside_the_gap_are_reported():
    network = pypowsybl.network.create_ieee14()
    lines = network.get_lines().index[:3].tolist()
    network.per_unit = True
    network.update_lines(id=lines, r=[0.0, 0.0, 0.0], x=[2e-8, 3.1e-5, 5e-9])
    network.per_unit = False
    gap, _, _ = find_threshold_gap_branches(network, lo=1e-8, hi=3e-5)
    assert gap.index.tolist() == [lines[0]]  # above hi and below lo are both outside the gap


def test_disconnected_branch_ignored():
    network = pypowsybl.network.create_ieee14()
    line = network.get_lines().index[0]
    set_line_impedance_pu(network, line, 0.0, 1e-6)
    network.update_lines(id=line, connected2=False)
    assert line not in find_threshold_gap_branches(network)[0].index


def test_per_unit_restored_on_error():
    network = MagicMock()
    network.per_unit = False
    network.get_lines.side_effect = RuntimeError("broken network")
    with pytest.raises(RuntimeError):
        find_threshold_gap_branches(network)
    assert network.per_unit is False


def test_zero_out_only_lines_and_two_winding_transformers(capsys):
    network = MagicMock()
    network.per_unit = False
    gap = pd.DataFrame({"kind": ["line", "2wt", "2wt (rtc step applied)", "tie_line", "3wt_leg1"],
                        "r": 0.0, "x": 1e-6, "z_pu": 1e-6},
                       index=["l1", "t1", "t2", "tie", "3w-leg1"])
    zero_out(network, gap)
    network.update_lines.assert_called_once_with(id=["l1"], r=[0.0], x=[0.0])
    network.update_2_windings_transformers.assert_called_once_with(id=["t1", "t2"], r=[0.0, 0.0], x=[0.0, 0.0])
    assert "Not auto-fixed, handle manually" in capsys.readouterr().out
    assert network.per_unit is False


def test_zero_out_empty_gap_is_a_no_op():
    network = MagicMock()
    network.per_unit = False
    zero_out(network, pd.DataFrame(columns=["kind", "r", "x", "z_pu"]))
    network.update_lines.assert_not_called()
    network.update_2_windings_transformers.assert_not_called()
