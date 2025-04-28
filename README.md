# RAO testing implementation process flowchart

```mermaid
flowchart TD
    TSOs["TSOs"] --> Rabbit1(("Rabbit (from TSOs)"))
    Rabbit1 --> ELKMinio["ELK/Minio"]
    ELKMinio --> CGMBA["CGM_BA"] & COAE["Input data (CO/AE/RA lists in CSA)"]
    COAE --> Conv["Input data conversion (NC profile → internal JSON format)"]
    SAR["SAR (security assessment result) profile"] --> Conv
    Conv -- CRAC, GLSK, CNEC input --> RAO["RAO (Jenkins job, runs in parallel with CSA)"]
    RAOParams["RAO Parameters (separate config)"] --> RAO
    RAO --> JSON2NC["Output data conversion (Internal JSON → NC profiles)"]
    RAO -- RAO process logs --> ELK["ELK"]
    JSON2NC -- RAO Output (SAR) --> ELK
    CGMBA --> AmpMW["Amp>MW conversion"]
    AmpMW -- TATL, etc. --> RAO
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


