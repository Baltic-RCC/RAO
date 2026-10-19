# rao-costly-ra-wip: handoff

Work-in-progress context for the costly remedial actions (redispatch, later countertrade) in the RAO worker.
Start a new session with: "Read `rao-costly-ra-wip.md` and continue from the Next steps."

- Branch: `claude/sweet-turing-g1wjzf` (base `main` 9d3a777, PR #68 merge). No PR opened yet.
- Previous session: https://claude.ai/code/session_01ByiHW56ZdRBSchGsKjf9mM
- Stack: pypowsybl **1.16.1** (OpenRAO 7.3.0), CRAC JSON 2.10/2.11 (both import), RAO parameters JSON **3.4** (3.3 is rejected).
- Offline work: the owner tests on an offline machine and moves changes there as git patches
  (`git diff --no-renames origin/main...HEAD -- . ':(exclude)tests'`, applied with PyCharm "Apply Patch").
- Tests: `uv run pytest tests/test_costly_ra_crac.py tests/test_costly_ra_rao.py tests/test_crac_costs.py`
  (71 pass; the TC1 test needs the `test-data` submodule). `tests/test_lf_settings_manager.py` was already broken on main
  (`connected_component_mode` vs `component_mode` in pypowsybl 1.16.1) and is unrelated.

## Target vision (owner)

Sequential RAO runs per contingency:
1. Run 1: topological remedial actions only.
2. If run 1 does not meet the optimization condition (negative margin left / cost condition): run 2 with countertrade
   actions (not implemented).
3. Then run 3 with redispatch actions (built on this branch).

Costs per RA type must be mapped separately, each in its own mapping function (network actions, injection range actions,
later countertrade). Costs are not used yet; the worker runs MAX_MIN_MARGIN.

## Owner decisions so far

- Redispatch RAs come from the NC RemedialAction profile (`RA` keyword) already loaded as triplets by `CracBuilder`,
  the same data as the topology actions. **No network model is read** for ranges, limits or ids.
- Each unit has separate `RA_RD_<unit>_UP` and `RA_RD_<unit>_DOWN` remedial actions (alterations `RD_<unit>_UP/DOWN`) on the
  same RotatingMachine rdfID → merged into one `InjectionRangeAction` `RA_RD_<unit>`, element `{"_<rdfID>": 1.0}`.
- Pmax = UP `normalValue`; no UP value → unit left out with a warning. Pmin = DOWN `normalValue`, or 0 with a warning.
- Instant is always `curative` (kept as a parameter; a separate instant like `curative2` was an earlier idea, superseded by
  the sequential runs).
- Operator: `RemedialAction.RemedialActionSystemOperator` as is, same handling as topology actions.
- Redispatch is scoped to the TSO of the violation: SAR `PowerFlowResult.ACDCTerminal` → equipment → matching CNEC
  `operator`; only that TSO's redispatch RAs go in the CRAC (most loaded violation wins if several TSOs).
- Costs: centralised in `rao/crac/costs.py`, off unless `CRAC_COSTS_PATH` is set. No CLI (removed on request).
- Owner prefers minimal changes ("ponytail" mode): smallest complete change, short replies.

## Code map

| File | Role |
|---|---|
| `rao/crac/builder.py` | `CracBuilder.build_crac(contingency_ids, include_redispatch, redispatch_instant, violated_element_ids)` → `process_redispatch_actions()`, `_violation_operator()`, `apply_costs()` |
| `rao/crac/costly_ra/sources.py` | `NcRemedialActionRowSource` (NC triplets → `RedispatchRow`), `CsvRowSource`/`DataFrameRowSource` (unused alternative input) |
| `rao/crac/costly_ra/redispatch.py` | `build_injection_range_actions(rows, instant)`: filter, group per unit, ranges, skip reasons |
| `rao/crac/costly_ra/crac.py` | Generic: `normalize_element_id`, `usage_rules`, `build_crac`, `merge_into_crac`, `validate_crac`, `import_crac`, `check_ra_usage_limits` |
| `rao/crac/costs.py` | `CostConfig` (`defaults` + `remedialActions` keyed by id or name), `.apply(actions)` |
| `rao/crac/models.py` | `InjectionRangeAction`, `InjectionRange`, `VariationCosts`, `NetworkAction.activationCost` |
| `rao/costly_ra.py` | Standalone MIN_COST helpers (`load_min_cost_parameters`, `run_rao`, `redispatch_results`, `apply_redispatch`), not used by the worker |
| `rao/parameters/rao_v34_min_cost.json` | MIN_COST parameters (needs `costly-min-margin-parameters` and `pst-model: APPROXIMATED_INTEGERS`) |
| `rao/handlers.py` | Passes `CRAC_INCLUDE_REDISPATCH`, `CRAC_COSTS_PATH` and the violated element ids to `CracBuilder` |
| `rao/config.properties` | `CRAC_INCLUDE_REDISPATCH = False`, `CRAC_COSTS_PATH = None` |

Availability → ranges: both directions `absolute [Pmin, Pmax]`; UP only adds `relativeToInitialNetwork [0, 100000]`;
DOWN only adds `[-100000, 0]`; neither → skipped.

## Next steps (agreed direction, not built)

1. **Fix result post-processing** (`handlers.post_process_results` / `build_applied_actions`): only `networkActionResults`
   are read. A range-only result raises `KeyError: 'activatedStates'`; redispatch results are not stored in Elastic.
   Blocks any redispatch run.
2. **Staged runs** in `handlers.py`: `build_crac(stage="topology" | "redispatch")` (redispatch stage = contingencies + CNECs +
   redispatch, no topology actions); run 1 → stop if secure, else (countertrade placeholder) → run 3.
   Security check verified: under MAX_MIN_MARGIN, `RaoResult.to_json()["costResults"][<last instant>]["functionalCost"] > 0`
   means unsecure (= −min margin).
3. **Per-type cost mapping** in `costs.py`: one config section + one function per RA type; `apply_costs()` calls them.
4. Open questions for the owner: does run 3 start from the network with run 1's topology actions applied (RaoResult does not
   modify the network)? Does run 3 use MIN_COST or MAX_MIN_MARGIN? Stop condition = no negative margin, or a cost threshold?

## Known risks / unverified

- AE `AssessedSystemOperator` and RA `RemedialActionSystemOperator` must be the same EIC URI string for the TSO scoping;
  watch the log line `Redispatch scoped to violation TSO …: 0 remedial action rows`.
- MAX_MIN_MARGIN pushes redispatch to Pmin/Pmax (maximizes margin, ignores volume/cost).
- A DOWN action without value gets Pmin = 0; a pumping unit with negative output would start outside its range (untested).
- `rao_v34.json` `solver-specific-parameters` set to `""`: CBC via OR-Tools never supported `THREADS 10 MAXTIME 3000`.
- Existing `process_cnecs` / `process_remedial_actions` fail on TC1 test data (missing description, `.unique().item()` on
  string arrays); not touched.
