# ADR-0002: Spark Connect locally for test layer T8 (spike S-02, local part)

- Status: Proposed
- Date: 2026-10-05
- Decision: D-13 in DECISIONS.md
- Spike: S-02, local part; the Databricks standard and serverless legs remain open

## Context
S-02 asks whether the Dataset-only backend (DIST-3) runs unchanged under Spark Connect. PLAT-2
requires `engine` to stay Connect-compatible, and layer T8 runs the engine suites under local
Spark Connect. Databricks standard and serverless compute are the real Connect targets, but they
need the deployment path of round 3.

## Options considered
1. In-process: a Connect server on the local Classic session's SparkContext and the Scala Connect
   client in the same test JVM.
2. Separate server JVM: the test JVM holds only the client; a server process holds Classic Spark,
   the Connect server, and the test classes that deserialized closures need.
3. Databricks only: standard or serverless JAR tasks.

## Decision (proposed)
Option 1 for CI now, as an informational job until this ADR is accepted. Option 3 completes S-02
once round 3 exists. Option 2 remains the fallback if option 1 proves fragile.

## Evidence (assistant sandbox, 2026-10-05; not CI)
- Spark 4.1.0 with spark-connect-client-jvm 4.1.0 on OpenJDK 17.0.20 and 21.0.12: the 9 engine
  tests passed through the Connect client. The backend-shape pipeline (createDataset,
  repartition, groupByKey and mapGroups, persist, mapPartitions, collect, block-ordered
  reduction) produced one bit pattern, 0xc2d2fb3a186d793c, for partition counts 1, 3 and 8, two
  input orders and ANSI on and off. It equals the Spark-free evaluation and therefore the Classic
  result. `SoftwareInfo.capture` classified the session as Connect.
- The client is an uber jar of 12,282 classes. It also contains 1,228 classes of spark-sql-api, 4
  of which differ, and 1,883 classes of the Connect server jar, 1,713 of which differ (protobuf
  and gRPC classes built against other relocations). With the client first on the classpath, the
  in-process server fails to start (`NoSuchMethodError` in `SparkConnectServiceGrpc.bindService`)
  and 7 of 9 engine tests fail; with the client last, everything above passes. build.sbt
  therefore pins the client to the end of Test classpaths.
- Pending: the first CI run of the T8 job.

## Consequences
- T8 sends every engine operation through the Connect protocol: the client serializes typed
  closures and the server runs them; configuration and caching are server calls.
- T8 cannot detect client-side use of Classic-only classes, because the test JVM also holds
  Classic Spark. The compile restriction (ARCH-2) and the API check cover that statically; the
  Databricks legs of S-02 cover it at run time.
- In-process correctness depends on the classpath order of two uber jars. The order is explicit
  in build.sbt and exercised by T8; re-check it on every Spark upgrade.
