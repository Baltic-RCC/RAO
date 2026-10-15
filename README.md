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





# RAO workers

The optimization runs as two deployments built from the same `rao` image:

| Worker | Entrypoint | Queue in | Does |
|---|---|---|---|
| preparer | `rao/worker.py` | `rao.violations` | Filters SAR violations, prepares network (3w trafo + low-Z workarounds, initial LF), builds a CRAC per contingency, stores network (XIIDM) and CRACs in S3, publishes one task per contingency to `rao.optimization` |
| optimizer | `rao/optimizer_worker.py` | `rao.optimization` | Loads prepared network and CRAC from S3, runs RAO trying the profiles in `rao/parameters/profiles.py` in order until one succeeds, post-processes and sends results to Elastic |

Optimization task headers carry the original message headers plus `contingency-id`, `batch-id` and `batch-size`.
Results are published per contingency, the `settings.profile` field shows which RAO profile produced them.

Infrastructure required outside this repository:
- exchange `rao.optimization` bound to queue `rao.optimization`
- KEDA ScaledJob for the optimizer with `command: ["python", "rao/optimizer_worker.py"]`
- S3 lifecycle rule expiring prepared networks under `RAO/PREPARED/`
