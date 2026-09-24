"""
Find every branch-like element that OpenLoadFlow treats as zero-impedance with the
load flow threshold (e.g. 3e-5) but as a real (tiny) impedance with the sensitivity
default (1e-8). Those elements are exactly what differs between your converging LF
and your diverging sensitivity base case.

|Z| is computed in per unit (Sbase = 100 MVA, like OLF), including the r/x %
correction of the *current* tap step for 2W transformers. Legs of 3W transformers
are checked individually (OLF models them as 3 branches to a star bus).
"""
import numpy as np
import pandas as pd


def _branch_frame(df, r, x, c1, c2, kind, index=None):
    return pd.DataFrame(
        {"r": df[r].values, "x": df[x].values,
         "connected1": df[c1].values, "connected2": df[c2].values},
        index=df.index if index is None else index,
    ).assign(kind=kind)


def find_threshold_gap_branches(network, lo=1e-8, hi=3e-5):
    was_pu = network.per_unit
    network.per_unit = True
    try:
        frames = []

        # Lines (all of them, fictitious included: OLF does not care about the flag)
        lines = network.get_lines(all_attributes=True)
        frames.append(_branch_frame(lines, "r", "x", "connected1", "connected2", "line"))

        # Tie lines are NOT returned by get_lines(); build them from their boundary-line halves.
        # pypowsybl >= 1.15 renamed dangling lines to boundary lines (old names are deprecated).
        if hasattr(network, "get_boundary_lines"):
            bl = network.get_boundary_lines(all_attributes=True)
            side1, side2 = "boundary_line1_id", "boundary_line2_id"
        else:
            bl = network.get_dangling_lines(all_attributes=True)
            side1, side2 = "dangling_line1_id", "dangling_line2_id"
        tl = network.get_tie_lines(all_attributes=True)
        paired = set()
        if len(tl):
            d1 = bl.loc[tl[side1]]
            d2 = bl.loc[tl[side2]]
            paired = set(d1.index) | set(d2.index)
            frames.append(pd.DataFrame({
                "r": d1["r"].values + d2["r"].values,   # series approximation
                "x": d1["x"].values + d2["x"].values,
                "connected1": d1["connected"].values,
                "connected2": d2["connected"].values,
            }, index=tl.index).assign(kind="tie_line"))

        # Unpaired boundary lines (branch to a fictitious boundary bus in OLF)
        unpaired = bl[~bl.index.isin(paired)]
        frames.append(_branch_frame(unpaired, "r", "x", "connected", "connected", "boundary_line"))

        # 2W transformers, with the current tap step r/x correction (in %)
        t2 = network.get_2_windings_transformers(all_attributes=True)
        t2f = _branch_frame(t2, "r", "x", "connected1", "connected2", "2wt")
        for get_tc, get_steps, name in (
            (network.get_ratio_tap_changers, network.get_ratio_tap_changer_steps, "rtc"),
            (network.get_phase_tap_changers, network.get_phase_tap_changer_steps, "ptc"),
        ):
            tc = get_tc()
            if tc.empty:
                continue
            steps = get_steps()  # MultiIndex (id, position)
            keys = [k for k in zip(tc.index, tc["tap"]) if k in steps.index]
            cur = steps.loc[keys]
            cur.index = cur.index.get_level_values(0)
            ids = cur.index.intersection(t2f.index)
            t2f.loc[ids, "r"] = t2f.loc[ids, "r"] * (1 + cur.loc[ids, "r"] / 100)
            t2f.loc[ids, "x"] = t2f.loc[ids, "x"] * (1 + cur.loc[ids, "x"] / 100)
            t2f.loc[ids, "kind"] = f"2wt ({name} step applied)"
        frames.append(t2f)

        # 3W transformer legs (not replaced by >330kV base voltage flag)
        t3 = network.get_3_windings_transformers(all_attributes=True)
        for leg in (1, 2, 3):
            frames.append(_branch_frame(
                t3, f"r{leg}", f"x{leg}", f"connected{leg}", f"connected{leg}",
                f"3wt_leg{leg}", index=t3.index + f"-leg{leg}"))

        allb = pd.concat(frames)
        allb = allb[allb["connected1"] & allb["connected2"]].copy()
        allb["z_pu"] = np.hypot(allb["r"], allb["x"])

        gap = allb[(allb["z_pu"] >= lo) & (allb["z_pu"] < hi)].sort_values("z_pu")
        negative_x = allb[allb["x"] < 0].sort_values("x")
        return gap, negative_x, allb
    finally:
        network.per_unit = was_pu


def zero_out(network, gap):
    """Set r = x = 0 on gap elements so both thresholds treat them as zero-impedance,
    i.e. the sensitivity sees the same merged topology as your converging LF.
    do not apply this to CNECs, PSTs or contingency elements."""
    was_pu = network.per_unit
    network.per_unit = True
    try:
        lines = gap.index[gap["kind"] == "line"].tolist()
        t2 = gap.index[gap["kind"].str.startswith("2wt")].tolist()
        if lines:
            network.update_lines(id=lines, r=[0.0] * len(lines), x=[0.0] * len(lines))
        if t2:
            network.update_2_windings_transformers(id=t2, r=[0.0] * len(t2), x=[0.0] * len(t2))
        # tie lines / boundary lines / 3wt legs: print and handle individually
        # (for those, update_boundary_lines(id=..., r=..., x=...) on the halves)
        rest = gap[~gap.index.isin(lines + t2)]
        if len(rest):
            print("Not auto-fixed, handle manually:\n", rest)
    finally:
        network.per_unit = was_pu


if __name__ == "__main__":
    pass
