# flink-kotlyn

Experiment with Apache Flink using **Kotlin**, with a small web GUI for submitting jobs and watching how they execute.

Flink runs embedded (local mini-cluster per job), so no Flink installation is needed. Requires JDK 17.

## Run

```
./gradlew run          # then open http://localhost:8080
./gradlew run --args=9000   # custom port (or PORT env var; HOST env var to change bind address, default 127.0.0.1)
./gradlew test
```

## What the GUI offers

Pick a scenario, tweak the input, parallelism, runtime mode (STREAMING/BATCH) and watermark delay, then submit.
For every job you can see the results, the Flink execution plan (operators, parallelism, shipping strategies such as
`HASH`/`REBALANCE`), a timeline of job state changes and the input used.

Scenarios (`src/main/kotlin/dev/flinklab/Scenarios.kt`): word count (streaming vs batch), parallelism & partitioning,
tumbling / sliding / session event-time windows, late data with side outputs, keyed state (`ValueState`) and
side-output routing. Add your own by appending a `Scenario` to `Scenarios.all`.

Note: Flink user functions must be serializable, so the build compiles Kotlin lambdas to classes
(`-Xlambdas=class -Xsam-conversions=class`) and the scenarios use explicit function classes plus `.returns(Types...)`.
