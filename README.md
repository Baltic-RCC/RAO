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





## Costly remedial actions: local redispatching CRAC builder

`rao/crac/costly_ra` holds the costly remedial actions. The generic CRAC helpers (assembly, merge, usage
rules and limits, validation) are in `costly_ra/crac.py`, so countertrading can reuse them later; the
redispatch mapping is in `costly_ra/redispatch.py`, and the MIN_COST run helpers are in `rao/costly_ra.py`.

The redispatch builder converts redispatching remedial actions (one UP and one DOWN `RotatingMachineAction`
per generating unit) into OpenRAO `injectionRangeActions`. The JSON CRAC it produces imports into
pypowsybl 1.16.1 (OpenRAO 7.3.0), and `rao/costly_ra.py` runs a MIN_COST RAO on it.

The remedial actions are retrieved the same way as for the topology actions: the NC RemedialAction profile
(`RA`) from object storage, loaded into triplets with `pd.read_RDF`. Pmin, Pmax and availability are mapped
**only from that remedial action list**. The network model is never loaded or read for limits or ranges.

OpenRAO's native NC CRAC importer is not used for these actions. It maps a `RotatingMachineAction` only
with direction `none`, as a fixed set-point network action, so up/down ranges would not become range actions.

### In the CRAC building process

`CracBuilder.build_crac(contingency_ids, include_redispatch=True)` calls
`CracBuilder.process_redispatch_actions()`, which adds the injection range actions to the CRAC built from the
same `data` triplets (CO/AE/RA). In the RAO worker this is switched off by default (`rao/config.properties`):

```properties
CRAC_INCLUDE_REDISPATCH = False   # True adds redispatch injection range actions to the CRAC
CRAC_COSTS_PATH = None            # remedial action cost config (YAML/JSON); None = no costs written (default)
```

Each unit has a separate `RA_RD_<unit>_UP` and `RA_RD_<unit>_DOWN` remedial action (alterations
`RD_<unit>_UP` / `RD_<unit>_DOWN`) on the same RotatingMachine rdf:ID. They are merged into one injection
range action `RA_RD_<unit>`, with the `curative` usage rule. The instant stays a parameter
(`redispatch_instant`), so the redispatch actions can later be moved to a separate curative instant.

With the worker's default RAO parameters (MAX_MIN_MARGIN), costs are not used. The redispatch stays
balanced, but the RAO maximizes the margin instead of minimizing the volume, so activated units can go
all the way to their Pmin/Pmax.

Mapping from the NC RemedialAction profile (`NcRemedialActionRowSource`):

| NC object / attribute | used as |
|---|---|
| `GridStateAlterationRemedialAction` `IdentifiedObject.name` | action id/name, without `_UP`/`_DOWN` |
| `RemedialAction.RemedialActionSystemOperator` | `operator`, as is (same as for the topology network actions) |
| `RemedialAction.normalAvailable` and `GridStateAlteration.normalEnabled` | availability of the direction |
| `RemedialAction.kind` | checked against the usage rule instant (warning only) |
| `RotatingMachineAction.RotatingMachine` | network element, written as `_<mRID>` |
| `StaticPropertyRange` `PropertyReference` | must be `RotatingMachine.p` |
| `StaticPropertyRange` `RangeConstraint.direction` | `up` or `down` (`RelativeDirectionKind`) |
| `StaticPropertyRange` `RangeConstraint.normalValue` | UP: Pmax, DOWN: Pmin |
| `StaticPropertyRange` `RangeConstraint.valueKind` | must be `absolute` (`ValueOffsetKind`) |

### Python API

```python
from rao.crac.costly_ra import NcRemedialActionRowSource, build_injection_range_actions, merge_into_crac, import_crac
from rao.crac.costs import CostConfig
from rao.costly_ra import load_min_cost_parameters, run_rao, redispatch_results, apply_redispatch

rows = NcRemedialActionRowSource(pd.read_RDF(ra_profiles)).read()  # or CsvRowSource("export.csv").read()
result = build_injection_range_actions(rows)
print(result.summary())                                      # actions written, skipped units
CostConfig.from_file("crac_costs.yaml").apply(result.actions)  # optional, needed for MIN_COST
crac = merge_into_crac(base_crac_dict, result.actions)       # or build_crac(result.actions)
# Building the CRAC above needs no network model; the network is only needed to run the RAO
imported = import_crac(network, crac)                        # pypowsybl Crac
rao_result = run_rao(network, imported, load_min_cost_parameters(dc=True))
results = redispatch_results(imported, rao_result, network)  # action, generator, instant, contingency, P0, P, delta
apply_redispatch(network, results, instant="curative", contingency="CO_ID")  # optional, RaoResult never edits the network
```

Sources implement `RowSource.read() -> list[RedispatchRow]`: `NcRemedialActionRowSource` (NC profile
triplets), `CsvRowSource` and `DataFrameRowSource` (RCC export columns `kind, ra_name, available, area, party,
alteration_type, alteration_name, property, grid_element_id, normal_value, direction, value_kind`).

### Mapping rules

- Only `RotatingMachineAction` rows on `RotatingMachine.p` with direction `up`/`down` are processed; other
  rows (e.g. `RotatingMachine.q`, `none`, `upAndDown`) are counted as ignored. A `valueKind` other than
  `absolute` is rejected with an error.
- Rows are grouped by grid element, and **one** `InjectionRangeAction` is emitted per unit, because OpenRAO
  does not allow two range actions on the same element.
- Action id and name = remedial action name without the `_UP`/`_DOWN` suffix (`RA_RD_ME_G1_DOWN` →
  `RA_RD_ME_G1`); the alteration name (`RD_ME_G1_DOWN`) is not used.
- `networkElementIdsAndKeys = {"_<RotatingMachine mRID>": 1.0}`: exactly one element with key 1.0, so the
  set-point is the generator MW. The element id always gets a single leading `_`, like the other CRAC
  elements and the IIDM ids imported with `source-for-iidm-id = rdfID`.
- UP `normalValue` = Pmax, DOWN `normalValue` = Pmin.
- No min range in the remedial action list (no DOWN action, or a DOWN action without `StaticPropertyRange`
  / `normalValue`): Pmin = 0, with a warning. Without a DOWN action, DOWN is not offered.
- No max range (no UP action, or an UP action without a value): the unit is **left out of the CRAC** with a
  warning, because the shift cannot be bounded.
- A `RotatingMachineAction` without `StaticPropertyRange` takes its direction from the `_UP`/`_DOWN` suffix
  of the RA name.
- Units are also skipped and reported for: no direction available, duplicate UP/DOWN actions, inconsistent
  name/operator between UP and DOWN, Pmin > Pmax, or the same action id on two elements.
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

`validate_crac(network, crac)` only checks that OpenRAO imports the actions with these ranges; it does not
compare them with the network model.

### Remedial action costs (`rao/crac/costs.py`)

Costs are central to the CRAC, not specific to costly remedial actions: `CracBuilder.build_crac()` calls
`CracBuilder.apply_costs()` once all remedial actions are built. That step sets `activationCost` on the
network (topological) actions and the injection range actions, and `variationCosts` on the range actions.
The redispatch mapping itself writes no costs.

Costs are **disabled by default**: without a cost config (`CRAC_COSTS_PATH = None`, `CracBuilder(costs=None)`)
nothing is written and no cost warnings are logged. They are only needed for the MIN_COST objective. The
config is YAML or JSON, keyed by remedial action id, or by name when the id isn't listed (topology actions
use their mRID as id). See `examples/crac_costs.yaml`:

```yaml
defaults:
  activationCost: 100.0                    # EUR per activation, all remedial actions
  variationCosts: {up: 50.0, down: 50.0}   # EUR/MW, range actions only
remedialActions:
  RA_RD_KHES_G5:                           # redispatch unit
    activationCost: 100.0
    variationCosts: {up: 50.0, down: 50.0}
  RA_RD_PHES_G1:
    variationCosts: {down: 40.0}           # missing values fall back to defaults
  RA_LN316LV:                              # topological network action
    activationCost: 20.0
```

Values missing for an action fall back to `defaults`. One warning per action type lists the actions that use
defaults, and the details are logged at debug level. Without a `defaults` section, the built-in defaults are
`activationCost: 0.0` and `variationCosts: {up: 1.0, down: 1.0}`.

### Example output

Unit with both directions available, after costs are applied (without a cost config, `activationCost` and
`variationCosts` are left out):

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

Tests: `uv run pytest tests/test_costly_ra_crac.py tests/test_costly_ra_rao.py tests/test_crac_costs.py`. The TC1 CGMES round trip
needs the `test-data` submodule (`git submodule update --init test-data`) and is skipped without it.
