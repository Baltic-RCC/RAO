# RAO testing implementation process flowchart

```mermaid
flowchart TD
    TSOs["TSOs"] --> Rabbit1(("Rabbit (from TSOs)"))
    Rabbit1 --> ELKMinio["ELK/Minio"]
    ELKMinio --> CGMBA["CGM_BA"] & COAE["Input data (CO/AE/RA lists in CSA)"]
    COAE --> Conv["Input data conversion (NC profile → internal JSON format)"]
    SAR["SAR (security assessment result) profile"] --> Conv
    Conv -- CRAC, GLSK, CNEC input --> RAO["RAO (Deployed in Kubernetes, runs in parallel with CSA)"]
    RAOParams["RAO Parameters (separate config)"] --> RAO
    RAO --> JSON2NC["Output data conversion (Internal JSON → NC profiles)"]
    RAO -- RAO process logs --> ELK["ELK"]
    JSON2NC -- RAO Output (SAR) --> ELK
    CGMBA --> RAO
    ELK --> ResultsDash["RAO results dashboard"] & LogsDash["RAO logs dashboard"]
    Operator(["Operator"]) -. Assess and validate optimized results,<br>perform RA proposal and coordination .-> ResultsDash
    Operator -. Monitor status, observe errors in logs,<br>report on failure .-> RAO & LogsDash
    CSA["CSA (D-1/ID)"] --> RabbitCSA(("Rabbit"))
    RabbitCSA -- Output (SAR) --> BMS["BMS"] & SAR
    ELKMinio@{ shape: cyl}
    CGMBA@{ shape: lean-r}
    COAE@{ shape: lean-r}
    SAR@{ shape: lean-r}
    ELK@{ shape: cyl}
    BMS@{ shape: cyl}
     ELKMinio:::db
     ELK:::db
     BMS:::db
    classDef db fill:#f9f,stroke:#333,stroke-width:1px
    style ELKMinio fill:transparent,stroke:#000000
    style ELK fill:#FFFFFF
    style Operator color:#000000
    style BMS fill:#FFFFFF





## Local redispatching CRAC builder

`rao/crac/redispatch` converts per-unit redispatching remedial-action rows (one UP row and one DOWN row per
generating unit, from the RCC remedial-action export) into OpenRAO `injectionRangeActions`. The JSON CRAC it
produces imports into pypowsybl 1.16.1 (OpenRAO 7.3.0), and `rao/redispatch.py` runs a MIN_COST RAO on it.

OpenRAO's native NC CRAC importer is not used for these rows. It maps a `RotatingMachineAction` only with
direction `none`, as a fixed set-point network action, so up/down ranges would not become range actions.

### Usage

```bash
build-rd-crac --rows examples/redispatch/rd_rows.csv --costs examples/redispatch/costs.yaml \
              [--network model.xiidm] [--base-crac crac.json] --out crac_out.json \
              [--instant curative] [--contingency CO_ID ...]
```

`build-rd-crac` is installed by `uv sync`; `python -m rao.crac.redispatch.cli` is equivalent. It prints the
actions written and the units skipped, with reasons. With `--network`, element IDs are resolved against the
network generators, and the CRAC is validated by importing it with OpenRAO. CGMES `.zip` models are loaded
with the repository CGMES import parameters. With `--base-crac`, the actions are merged into an existing CRAC.
With `--contingency`, `onContingencyStateUsageRules` are written instead of an `onInstantUsageRule`; this
needs `--base-crac`, because the contingencies must exist.

```python
from rao.crac.redispatch import CostConfig, CsvRowSource, build_injection_range_actions, merge_into_crac, validate_crac
from rao.redispatch import load_min_cost_parameters, run_rao, redispatch_results, apply_redispatch

rows = CsvRowSource("export.csv").read()                     # any RowSource adapter: rows -> RedispatchRow
result = build_injection_range_actions(rows, costs=CostConfig.from_file("costs.yaml"), network=network)
print(result.summary())                                      # actions, skipped units, defaulted costs
crac = merge_into_crac(base_crac_dict, result.actions)       # or build_crac(result.actions)
imported = validate_crac(network, crac)                      # OpenRAO import + action/range checks
rao_result = run_rao(network, imported, load_min_cost_parameters(dc=True))
results = redispatch_results(imported, rao_result, network)  # action, generator, instant, contingency, P0, P, delta
apply_redispatch(network, results, instant="curative", contingency="CO_ID")  # optional, RaoResult never edits the network
```

Input sources implement `RowSource.read() -> list[RedispatchRow]`. `CsvRowSource` (and `DataFrameRowSource`)
read the export columns `kind, ra_name, available, area, party, alteration_type, alteration_name, property,
grid_element_id, normal_value, direction, value_kind`. Other sources, such as the NC/CSA RemedialAction XML
profile, can be added as further adapters.

### Mapping rules

- Only rows with `alteration_type == RotatingMachineAction`, `property == RotatingMachine.p` and direction
  `up`/`down` are processed; other rows are counted as ignored. A `value_kind` other than `absolute` is
  rejected with an error.
- Rows are grouped by `grid_element_id`, and **one** `InjectionRangeAction` is emitted per unit, because
  OpenRAO does not allow two range actions on the same element. Rows whose IDs differ only by the leading
  `_` belong to the same unit.
- Action id and name = `ra_name` without the `_UP`/`_DOWN` suffix (e.g. `RA_RD_KHES_G5`); `operator` = `party`.
- `networkElementIdsAndKeys = {<generator id>: 1.0}`: exactly one element with key 1.0. The set-point is then
  the generator MW (`targetP`), and OpenRAO's initial set-point consistency check cannot fail.
- UP row `normal_value` = Pmax, DOWN row `normal_value` = Pmin.
- A missing UP or DOWN row means that direction is not offered. Its bound falls back to the network
  generator's `max_p` / `min_p` when a network is given, otherwise to Pmax = 100000 / Pmin = 0, with a warning.
- Element ID resolution (with a network): `id`, `id.lstrip("_")` and `"_" + id.lstrip("_")` are tried against
  the network generators, because CGMES rdf:IDs may or may not keep the leading `_` after the IIDM import.
  Unresolved units are skipped and reported.
- Units are also skipped and reported for: duplicate UP/DOWN rows, inconsistent `ra_name`/`party` between UP
  and DOWN, Pmin > Pmax, the same action id on two elements, or no direction available.
- Usage rule: `onInstantUsageRules: [{"instant": "curative"}]` by default (configurable), or
  `onContingencyStateUsageRules` for a given contingency list.
- Output is deterministic: actions are sorted by id and keys always come in the same order.

### Availability → ranges

OpenRAO intersects all ranges of an action, so the current output P0 is not needed at build time
(`BIG = 100000.0`):

| UP available | DOWN available | ranges | effective range |
|---|---|---|---|
| yes | yes | `absolute [Pmin, Pmax]` | [Pmin, Pmax] |
| yes | no | `absolute [Pmin, Pmax]` + `relativeToInitialNetwork [0, +BIG]` | [P0, Pmax] |
| no | yes | `absolute [Pmin, Pmax]` + `relativeToInitialNetwork [-BIG, 0]` | [Pmin, P0] |
| no | no | unit skipped and reported | — |

`validate_crac` warns when a unit's initial set-point lies outside its absolute [Pmin, Pmax].

### Cost config

Costs are not part of the export. They are loaded from YAML or JSON, keyed by unit action id (see
`examples/redispatch/costs.yaml`):

```yaml
defaults:
  activationCost: 100.0          # EUR per activation
  variationCosts: {up: 50.0, down: 50.0}   # EUR/MW
units:
  RA_RD_KHES_G5:
    activationCost: 100.0
    variationCosts: {up: 50.0, down: 50.0}
  RA_RD_PHES_G1:
    variationCosts: {down: 40.0}  # missing values fall back to defaults
```

Any value missing for a unit falls back to `defaults`, and a warning is logged for every unit that uses a
default. Without a `defaults` section, the built-in defaults are `activationCost: 0.0` and
`variationCosts: {up: 1.0, down: 1.0}`.

### Example output

Unit with both directions available:

```json
{
  "id": "RA_RD_KHES_G5",
  "name": "RA_RD_KHES_G5",
  "operator": "AST",
  "activationCost": 100.0,
  "variationCosts": { "up": 50.0, "down": 50.0 },
  "onInstantUsageRules": [{ "instant": "curative" }],
  "networkElementIdsAndKeys": { "_f157276b-ba01-4a30-a510-d5939c71018b": 1.0 },
  "ranges": [{ "rangeType": "absolute", "min": 0.0, "max": 56.0 }]
}
```

UP-only unit (`ranges` only):

```json
"ranges": [
  { "rangeType": "absolute", "min": 0.0, "max": 93.0 },
  { "rangeType": "relativeToInitialNetwork", "min": 0.0, "max": 100000.0 }
]
```

A standalone CRAC has the header `{"type": "CRAC", "version": "2.10", "id": ..., "name": ...}` and the
instants `preventive` (PREVENTIVE), `outage` (OUTAGE) and `curative` (CURATIVE). `merge_into_crac` fails if
a referenced instant or contingency is missing, if an action id already exists, or if an element is already
used by another range action. Everything else in the base CRAC is left untouched.

### Known OpenRAO 7.3.0 constraints (pypowsybl 1.16.1)

- Formats: CRAC JSON `"version": "2.10"`, RAO parameters JSON `"version": "3.4"` (3.3 is rejected).
- MIN_COST fails without the `costly-min-margin-parameters` block, and without
  `pst-model: APPROXIMATED_INTEGERS`. pypowsybl's Python `Parameters()` object cannot set the costly block,
  so `load_min_cost_parameters()` always loads `rao/parameters/rao_v34_min_cost.json`. DC or AC is set
  with `dc=`.
- `ra-usage-limits-per-instant` is not written by default. If you configure limits, `check_ra_usage_limits`
  warns about these pitfalls:
  - `max-ra` counts range actions, and a balanced redispatch always needs at least 2 actions, so
    `max-ra: 1` silently blocks all redispatch;
  - limits are cumulative across curative instants;
  - with several curative instants, `max-topo-per-tso` throws a NullPointerException unless
    `max-ra-per-tso` is also set for the same TSO.
- Multi-element (GSK-style) injection actions are out of scope; `redispatch_results` rejects them.

Tests: `uv run pytest tests/test_redispatch_crac.py tests/test_redispatch_rao.py`. The TC1 CGMES round trip
needs the `test-data` submodule (`git submodule update --init test-data`) and is skipped without it.
