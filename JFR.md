# JFR Memory Monitoring

The driver can publish DuckDB memory-usage statistics as a periodic
Java Flight Recorder (JFR) event, so any JFR-aware tool (JMC, `jfr`
CLI, async-profiler, Datadog/New Relic continuous profilers, …) can
ingest DuckDB memory metrics alongside the rest of the JVM signal.

The feature is **strictly opt-in** per connection and is a silent
no-op on JVMs without JFR support. Nothing is emitted unless the
application both sets the JDBC property on a connection and has an
active JFR recording that enables the event.

## Enabling emission

Pass `jdbc_jfr_memory_monitor=<component-id>` when opening a
connection:

```java
Properties props = new Properties();
props.setProperty(DuckDBDriver.JDBC_JFR_MEMORY_MONITOR, "pricing-service");
Connection conn = DriverManager.getConnection("jdbc:duckdb:/tmp/pricing.db", props);
```

…or in the URL:

```
jdbc:duckdb:/tmp/pricing.db;jdbc_jfr_memory_monitor=pricing-service
```

The `<component-id>` is an arbitrary label chosen by the application;
it is attached to every event as the `component` field so operators
can attribute memory to logical components in dashboards and queries.

Rules:

| Property value            | Effect                                         |
| ------------------------- | ---------------------------------------------- |
| absent                    | no events emitted for this connection          |
| empty string              | no events emitted (same as absent)             |
| non-empty string          | events emitted, tagged with the given value    |

The JDBC property is purely an opt-in switch and a label. It does
**not** control whether JFR is actually recording, nor the sampling
period — those are governed by JFR recording settings (see below).

## Controlling period and enabled state

Sampling rate and enabled state are JFR-native settings. Configure
them in a `.jfc` profile, via JMC, or programmatically:

```java
try (Recording r = new Recording()) {
    r.enable("duckdb.MemoryUsage").withPeriod(Duration.ofSeconds(1));
    r.start();
    // ... application work ...
    r.stop();
    r.dump(Path.of("app.jfr"));
}
```

Equivalent `.jfc` snippet:

```xml
<event name="duckdb.MemoryUsage">
  <setting name="enabled">true</setting>
  <setting name="period">1 s</setting>
</event>
```

When no recording enables the event, the driver performs zero work —
the DuckDB `duckdb_memory()` query is never issued.

## Event schema

Event name: **`duckdb.MemoryUsage`** — one event per memory tag per
JFR tick.

| Field                   | Type   | Meaning                                                           |
| ----------------------- | ------ | ----------------------------------------------------------------- |
| `component`             | String | Application-supplied identifier (the JDBC property value).        |
| `tag`                   | String | DuckDB memory tag (e.g. `IN_MEMORY_TABLE`, `HASH_TABLE`, `ALLOCATOR`). |
| `dbAddress`             | long   | Native address of the DuckDB instance — stable per-instance id.   |
| `memoryUsageBytes`      | long   | Bytes currently allocated for this tag.                           |
| `temporaryStorageBytes` | long   | Bytes spilled to temporary storage for this tag.                  |

Plus the standard JFR fields `startTime`, `duration`, `eventThread`
(stack traces are disabled for this event).

## Attribution model

The monitor is keyed on the **native DuckDB instance address**, not
on the JDBC connection. This matters when multiple connections share
a DuckDB instance (multiple `DriverManager.getConnection` calls
against the same file DB, or `conn.duplicate()`):

- One sample stream per distinct DuckDB instance — no double-counting
  of shared memory.
- `component` is captured from the first opted-in connection to an
  instance; later opted-in connections to the same instance do not
  change the label.
- The monitor is created when the first opted-in connection opens and
  torn down when the last one closes; a subsequent `getConnection`
  starts a fresh monitor.

To attribute memory to distinct logical components, open each against
a distinct DuckDB instance and give each one a unique
`jdbc_jfr_memory_monitor` value.

## Requirements

A JFR-capable JVM:

- OpenJDK/HotSpot 11 and newer: JFR is included.
- Amazon Corretto 8, OpenJDK 8u272+, and several other Java 8
  distributions: JFR backport included (`jdk.jfr` package).
- JVMs without `jdk.jfr` (e.g. some stripped Java 8 builds): the
  feature is a silent no-op; the `jdbc_jfr_memory_monitor` property
  is ignored and no classes that depend on `jdk.jfr` are loaded.

No additional JVM flags are required.

## Inspecting a recording

With the `jfr` CLI bundled with the JDK:

```
jfr summary app.jfr | grep duckdb.MemoryUsage
jfr print --events duckdb.MemoryUsage app.jfr | head -12
jfr metadata app.jfr | sed -n '/class MemoryUsage/,/^}/p'
```

Or open `app.jfr` in JMC for an interactive view.

## Manual verification

Two shell scripts reproduce the above end-to-end and are the
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
