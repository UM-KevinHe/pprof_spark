# ADR-0008: Spikes deferred at the Phase 0 gate

- Status: Accepted
- Date: 2026-10-06
- Decisions: D-14 (no Databricks deployment work), D-01 (Classic Spark for v1)

| Spike | Question | Status | Reason | Reopen when |
|---|---|---|---|---|
| S-01 | Does a JAR built for Scala 2.13.16 run on DBR 18 LTS while CI tests on open-source Spark 4.1.x? | Deferred | D-14 | Databricks work resumes. Meanwhile the linkage compile against scala-library 2.13.16 runs on every push (PLAT-3) |
| S-02, Databricks legs | Does the Dataset backend run unchanged on standard and serverless compute? | Deferred | D-14 | Databricks work resumes. The local part passed (ADR-0002) |
| S-03 | What does table materialization cost against persist, on Classic and serverless? | Deferred | D-14; serverless is the motivating case | Serverless becomes a target |
| S-04 | Can third-party Spark ML models be used through Spark Connect ML? | Deferred | D-01 keeps v1 on Classic Spark, and the `ml` adapters do not exist yet | The `ml` adapters exist and Connect clients need them |
| S-07 | Which block sizes and partition counts work at 10^8 to 10^9 rows? | Deferred | Needs a cluster (D-14); DIST-4's default of 4 MB blocks stands | A cluster is available and the reduction volume at large p (OI-02) is designed |
