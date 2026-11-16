# Implementation guide: activation cost for remedial actions that weaken a 330 kV substation

Handoff for a fresh session. Start with: "Read `weak-substation-ra-cost-guide.md` and implement Phase 1."

## Goal

When the virtual operator builds a CRAC, give an `activationCost` of **50** to every topological remedial
action (RA) whose activation would leave a substation either

- **islanded on the 330 kV side** (no 330 kV line left in service), or
- **hanging on a single 330 kV line**.

All other RAs keep no activation cost. The RA stays available to the RAO; the cost only ranks it behind
equivalent RAs that do not weaken a substation. It must never be turned into a filter (owner decision, based
on SOGL Art. 21(2)(c) and Art. 35(5): this is a risk preference, not an operational security limit).

## Read this first: what the cost does and does not do

- OpenRAO uses activation costs **only under the `MIN_COST` objective**. `main` runs `MAX_MIN_MARGIN`
  (`rao/parameters/rao_v34.json`), so after Phase 1 the cost is written to the CRAC and to Elastic but
  **does not change which RA is chosen**. That is expected, not a bug.
- Phase 1 (this guide) is the detection and the cost in the CRAC. Phase 2 (switching the objective) is a
  separate owner decision; see the last section. Do not start Phase 2 without the owner's go-ahead.

## Repo state you are building on

- Base: `main` at `b4436da` (operator/optimizer split, pydantic settings). Stack: pypowsybl 1.16.1
  (OpenRAO 7.3.0), CRAC JSON 2.11, RAO parameters 3.4.
- Flow: `HandlerVirtualOperator.handle()` in `rao/handlers.py` loads the merged model into pypowsybl
  (`self.network`), applies the 3w→2w and low-Z workarounds, creates one `CracBuilder`
  (`rao/crac/builder.py`) and calls `crac_service.build_crac(contingency_ids=[mrid])` once per contingency.
  The optimizer worker only downloads and runs that CRAC.
- `CracBuilder.process_remedial_actions()` builds `models.NetworkAction` objects with
  `terminalsConnectionActions` (`actionType` `open`/`close`). An `upAndDown` RA produces a second action
  `<mRID>-opposite-direction` with the action types flipped. The instant comes from `RemedialAction.kind`
  (`curative` or `preventive`).
- IDs: the builder and the models hold equipment ids **without** a leading underscore; the models add `_`
  on serialisation, which is the pypowsybl id. Compare ids with the underscore stripped on both sides.

Related work, for context only:

- `LN-CRAC-activation-costs` ([Baltic-RCC/RAO#37](https://github.com/Baltic-RCC/RAO/issues/37)): flat cost
  50 for every `TopologyAction` with direction `none`/`up`. Stale (pre-refactor) and type-based. **Do not
  merge or port it**; this feature replaces it with a consequence-based rule.
- `LN-costly-rao-wip`: `rao/crac/costs.py` (config-driven costs per RA type), `MIN_COST` parameter file,
  redispatch. Not merged. Phase 1 must not depend on it. The only overlap is the
  `NetworkAction.activationCost` field, which that branch declares the same way as below.

## The rule

Definitions, all evaluated on the pypowsybl network the operator already holds:

- **Counted line**: an AC line or tie line with both ends at nominal voltage ≥ 330 kV, both ends in
  different substations, and both terminals connected in the loaded model.
- **Substation in scope**: a pypowsybl substation with at least one transformer, load or generator. This
  drops tee points and intermediate nodes of segmented lines, which would otherwise be flagged falsely.
- **State the RA is applied in**: for a `curative` RA, the network with the CRAC contingency's elements
  already out; for a `preventive` RA, the network as loaded.
- **Flag the RA** if, for any substation in scope, the number of counted lines **drops because of the RA**
  and ends at **0 or 1**.

Consequences of "drops because of the RA":

- A `close` action is never flagged.
- Opening a line that is already out (in the model or by the contingency) changes nothing and is not flagged.
- A substation the contingency already left on one line does not flag RAs that do not touch it.
- A substation that is radial by design (one line) is flagged when the RA opens that line (1 → 0).

## Phase 1 changes

### 1. New module `rao/crac/substation_security.py`

Reference implementation. It was run against pypowsybl 1.16.1 sample networks (results in the test section).

```python
"""Flag topological remedial actions that leave a substation weakly connected at transmission voltage."""
from dataclasses import dataclass
import pandas as pd
from loguru import logger


def _norm(element_id) -> str:
    return str(element_id).lstrip("_")


@dataclass
class SubstationLines:
    lines: pd.DataFrame   # index: normalized line id; columns: substation1, substation2 (in-service lines only)
    substations: set      # substations the rule applies to (have a transformer, load or generator)

    @classmethod
    def from_network(cls, network, min_kv: float = 330.0) -> "SubstationLines":
        vl = network.get_voltage_levels()[["substation_id", "nominal_v"]]
        frames = [network.get_lines()[["voltage_level1_id", "voltage_level2_id", "connected1", "connected2"]]]
        tie = network.get_tie_lines()
        if not tie.empty:  # cross-border lines of a merged model
            dl = network.get_dangling_lines()
            frames.append(pd.DataFrame({
                "voltage_level1_id": tie["dangling_line1_id"].map(dl["voltage_level_id"]),
                "voltage_level2_id": tie["dangling_line2_id"].map(dl["voltage_level_id"]),
                "connected1": tie["connected1"], "connected2": tie["connected2"]}, index=tie.index))
        lines = pd.concat(frames)
        for side in ("1", "2"):
            lines[f"substation{side}"] = lines[f"voltage_level{side}_id"].map(vl["substation_id"])
            lines[f"kv{side}"] = lines[f"voltage_level{side}_id"].map(vl["nominal_v"])
        lines = lines[(lines.kv1 >= min_kv) & (lines.kv2 >= min_kv)
                      & (lines.substation1 != lines.substation2)
                      & lines.connected1 & lines.connected2]
        lines.index = lines.index.map(_norm)
        equipment_vl = pd.concat([network.get_2_windings_transformers()["voltage_level1_id"],
                                  network.get_3_windings_transformers()["voltage_level1_id"],
                                  network.get_loads()["voltage_level_id"],
                                  network.get_generators()["voltage_level_id"]])
        return cls(lines=lines[["substation1", "substation2"]], substations=set(equipment_vl.map(vl["substation_id"])))

    def weakened(self, opened, already_out=()) -> dict:
        """{substation: lines left} for substations that lose a line to `opened` and end on <= 1 line."""
        base = self.lines[~self.lines.index.isin({_norm(i) for i in already_out})]
        after = base[~base.index.isin({_norm(i) for i in opened})]
        before_n = pd.concat([base.substation1, base.substation2]).value_counts()
        after_n = pd.concat([after.substation1, after.substation2]).value_counts().reindex(before_n.index, fill_value=0)
        hit = after_n[(after_n < before_n) & (after_n <= 1) & after_n.index.isin(self.substations)]
        return hit.to_dict()


def apply_weak_substation_costs(crac, topology: SubstationLines, cost: float) -> list[str]:
    """Set activationCost on network actions (rao.crac.models.Crac, in place). Returns the flagged action ids."""
    flagged = []
    for action in crac.networkActions:
        opened = [t.networkElementId for t in action.terminalsConnectionActions or [] if t.actionType == "open"]
        if not opened:
            continue
        curative = any(rule.get("instant") == "curative" for rule in action.onInstantUsageRules)
        states = [c.networkElementsIds for c in crac.contingencies] if curative else [()]
        for outage in states or [()]:
            weakened = topology.weakened(opened, already_out=outage)
            if weakened:
                action.activationCost = cost
                flagged.append(action.id)
                logger.info(f"Remedial action {action.name} gets activation cost {cost}: "
                            f"substations left on <= 1 line of the monitored voltage: {weakened}")
                break
    return flagged
```

### 2. `rao/crac/models.py`

Add one optional field to `NetworkAction`, after `operator`:

```python
activationCost: Optional[float] = None
```

`CracBuilder.crac` dumps with `exclude_none=True`, so unflagged actions have no `activationCost` key and the
CRAC of an unflagged run is byte-identical to today's. CRAC 2.11 import accepts the field on a network
action (checked: `Crac.get_network_actions()` returns `activation_cost 50.0`).

### 3. `rao/settings.py`

Add to `RaoHandlerSettings`, in the virtual operator block:

```python
weak_substation_activation_cost: float = 50.0   # 0 disables the check
weak_substation_min_voltage_kv: float = 330.0
```

Env vars are the upper-case field names. Add both to `config/.example.env` only if that file lists the
other operator settings.

### 4. `rao/crac/builder.py`

- `CracBuilder.__init__`: add keyword arguments `substation_lines: SubstationLines | None = None` and
  `weak_substation_cost: float = 0.0`, stored on `self`. Defaults keep the `__main__` block and any other
  caller working with the check off.
- `build_crac()`: after the `remedial_actions_3w_workaround()` block and before
  `update_limits_from_network()`, add:

```python
if self.substation_lines is not None and self.weak_substation_cost > 0:
    apply_weak_substation_costs(self._crac, self.substation_lines, self.weak_substation_cost)
```

It has to run after both `process_contingencies()` and `process_remedial_actions()`, because it reads
`self._crac.contingencies` and has to see the `-opposite-direction` actions.

### 5. `rao/handlers.py`

In `HandlerVirtualOperator.handle()`, build the topology once per message from `self.network`, after the
3w replacement and the initial load flow, and pass it to the builder:

```python
substation_lines = SubstationLines.from_network(self.network, min_kv=conf.weak_substation_min_voltage_kv)
crac_service = CracBuilder(data=input_files_data, network=network_triplets, workaround=workaround_ctx,
                           substation_lines=substation_lines,
                           weak_substation_cost=conf.weak_substation_activation_cost)
```

Nothing changes in the optimizer worker. `HandlerOptimizer.post_process_results()` already merges every
CRAC network action field as `action.<field>`, so flagged actions get `action.activationCost` in the
`rao-results` Elastic documents with no code change. Check that the index mapping accepts a new float field.

### 6. Tests: new `tests/test_substation_security.py`

No test data is needed; use pypowsybl sample networks. Verified results to assert:

| Network | Call | Expected |
|---|---|---|
| `create_eurostag_tutorial_example1_network()` (two parallel 380 kV lines P1–P2) | `weakened(["_NHV1_NHV2_1"])` | `{"P1": 1, "P2": 1}` |
| same | `weakened(["NHV1_NHV2_1"], already_out=["NHV1_NHV2_2"])` | `{"P1": 0, "P2": 0}` |
| same | `weakened(["NHV1_NHV2_1"], already_out=["NHV1_NHV2_1"])` | `{}` |
| `create_four_substations_node_breaker_network()` (400 kV S2–S3–S4) | `weakened(["LINE_S2S3"])` | `{"S3": 1, "S2": 0}` |
| same | `weakened(["LINE_S2S3"], already_out=["LINE_S3S4"])` | `{"S2": 0, "S3": 0}` |

Add these as well:

- `apply_weak_substation_costs` on a `models.Crac` built in the test (eurostag network, contingency
  `NHV1_NHV2_2`): a curative `open` of `NHV1_NHV2_1` and a preventive `open` are flagged with 50, a curative
  `close` is not, and the dumped CRAC has `activationCost` only on the flagged actions.
- A line disconnected in the model (`network.disconnect("NHV1_NHV2_2")` before `from_network`) is not counted.
- `min_kv` above the network's voltage flags nothing.

Run with `uv run pytest tests/test_substation_security.py`. There is no test job in CI, and
`tests/test_lf_settings_manager.py` is a script with known unrelated problems; leave it.

## Acceptance

1. Tests above pass.
2. With `WEAK_SUBSTATION_ACTIVATION_COST=0` the CRAC JSON is identical to the one `main` produces.
3. On a real merged model, the INFO log lists each flagged RA with the affected substations and line counts,
   and the owner confirms a sample of them by hand (the owner tests offline; hand over a patch if asked).
4. The CRAC with costs loads in `pypowsybl.rao.Crac.from_buffer_source` and a `MAX_MIN_MARGIN` run gives the
   same result as before.

## Not verified, check on a real model

- **Tie lines.** The tie-line branch of `from_network` uses the pypowsybl 1.16.1 column names but was not
  run on a model that has tie lines. Confirm cross-border 330 kV lines appear in `SubstationLines.lines`.
- **"In service" test.** `connected1 & connected2` was checked on bus-breaker and node-breaker samples. On
  the merged CGMES model, confirm a line known to be out is excluded; if not, also require non-empty
  `bus1_id`/`bus2_id`.
- **RA equipment ids.** The rule only sees RAs whose `TopologyAction.Equipment` is the line itself. If TSOs
  model line switching through a breaker or disconnector id, those RAs are silently not flagged. Count how
  many `open` action element ids are missing from `SubstationLines.lines` and from the transformer tables,
  and report it.
- **Substation boundaries.** pypowsybl's CGMES import can merge substations joined by transformers or
  switches. Check a few known substations are not merged with a neighbour.

## Known limits (accepted for Phase 1)

- Lines are counted, connectivity is not. Two lines to the same neighbour count as two. A substation fed
  through a radial chain is only caught at the first substation of the chain, which is enough to flag the RA.
- HVDC links and 330/110 kV transformers do not count as 330 kV connections.
- Busbar splitting inside a substation and RAs that open transformers are not assessed.
- 400 kV substations are included by the `>= 330` threshold. Ask the owner if 330 kV only is wanted.
- Outside a per-contingency CRAC (`build_crac()` with no ids), a curative RA is flagged if any contingency
  in the CRAC triggers the rule.

## Out of scope

- Any change to `rao_v34.json`, `RAO_PROFILES` or the objective function.
- `rao/crac/costs.py`, redispatch and countertrade from `LN-costly-rao-wip`.
- Porting the direction-based cost of `LN-CRAC-activation-costs`.
- Extra keys in the CRAC JSON to explain the flag; the reason goes to the log only.

## Phase 2 (owner decision, do not start unasked)

To make the cost change RAO choices, the optimizer has to run `MIN_COST`:

- Starting point: `rao/parameters/rao_v34_min_cost.json` on `LN-costly-rao-wip`. It needs the
  `costly-min-margin-parameters` extension (`shifted-violation-penalty`, `shifted-violation-threshold`) and
  `pst-model: APPROXIMATED_INTEGERS`.
- Apply it as an override of the default entry in `rao/parameters/profiles.py`, not as a global default.
- `MIN_COST` stops maximising margin and only secures the grid at least cost, so results change for every
  contingency, not only the ones with flagged RAs. That is why it needs a separate decision and test round.
- Keep 50 well below the cheapest real costly RA (100 for countertrade on that branch's scale) and far below
  the violation penalty (10000), so the RAO never buys redispatch or leaves a violation to avoid a flagged RA.
- Report virtual costs separately from real RA costs in results.
