# JFR Memory Monitoring

The driver can publish DuckDB memory-usage statistics as periodic Java
Flight Recorder (JFR) events. This feature is documented on the DuckDB
website:

- **[Memory Monitoring with JFR](https://duckdb.org/docs/stable/clients/java/profiling#memory-monitoring-with-jfr)**

That page covers enabling emission (`jdbc_jfr_memory_monitor`),
controlling the sampling period, the `duckdb.MemoryUsage` event schema,
the attribution model, JVM requirements, and inspecting a recording.

Only the repo-specific verification workflow is documented below.

## Manual verification

Two shell scripts reproduce the feature end-to-end and are the
recommended way to sanity-check a new build:

```
./scripts/verify-jfr.sh          # Java >= 9
./scripts/verify-jfr-java8.sh    # Java 8 (covers both JFR and no-JFR paths)
```

Switch the active JDK first (for example
`sdk u java 25.0.3-amzn` or `sdk u java 8.0.462-amzn`). Each script
builds any missing artifacts, runs the four `test_jfr_memory_event*`
unit tests, and — on Java ≥ 9 — captures a live recording and
verifies the event with `jfr summary` and `jfr print`. The Java 8
script additionally asserts the JFR-less fallback by running the
driver with `jfr.jar` stripped from the bootclasspath.
