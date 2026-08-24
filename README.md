It's required to have a JDK installed to build.
Make sure the `JAVA_HOME` environment variable is set.


### Development

To build the driver, run `make release`. 


This produces multiple jars in the build folder:
`build/release/duckdb_jdbc.jar` - the pure-Java artifact (no native library)
`build/release/duckdb_jdbc_native_<os>_<arch>.jar` - the native library for the current platform
`build/release/duckdb_jdbc_tests.jar` - the tests

The native library is not embedded in the Java jar. When publishing to Maven, each
platform's native library is shipped as a separate classifier artifact
(`duckdb_jdbc-<version>-<classifier>.jar`) and pulled in by the pure-Java artifact
via OS/arch-activated profiles. The glibc builds are the default for Linux; the musl
variants are published but must be declared explicitly (Maven cannot detect musl at
resolve time). See `pom.xml.template`.

The tests can be ran using using `make test` (the Makefile puts the native jar on the
classpath automatically) or this command:
```
java -cp "build/release/duckdb_jdbc_tests.jar:build/release/duckdb_jdbc.jar:build/release/duckdb_jdbc_native_linux_amd64.jar" org/duckdb/TestDuckDBJDBC
```

This optionally takes an argument to only run a single test, for example:
```
java -cp "build/release/duckdb_jdbc_tests.jar:build/release/duckdb_jdbc.jar:build/release/duckdb_jdbc_native_linux_amd64.jar"  org/duckdb/TestDuckDBJDBC test_valid_but_local_config_throws_exception
```

The string write performance benchmark is optional and can be run with:
```
make stress
```
Override its workload with `PERF_ROWS` and `PERF_SAMPLES` if needed. The
benchmark is intentionally not part of the regular test suite.

Scalar function usage examples: [UDF.MD](UDF.MD)

JFR memory monitoring usage: [JFR.md](JFR.md)

### Publishing

The driver is published as a set of `org.duckdb` artifacts, each with its own
groupId/artifactId/version (GAV), instead of one jar carrying multiple classifier
variants. Every artifact is self-contained; the native library is resolved as a
first-class dependency.

Published artifacts (version `V`):

[options="header",cols="1,2,1"]
|===
| Artifact | Content | Notes
| duckdb_jdbc | empty JAR | backward-compatible aggregate: a thin POM that transitively pulls `duckdb_jdbc_java` + the 4 core platform natives
| duckdb_jdbc_java | Java classes only | ships `-sources` and `-javadoc`; declares no native dependency (pick one yourself)
| duckdb_jdbc_linux_amd64 | Linux x86_64 (glibc) | one of the 4 aggregate defaults
| duckdb_jdbc_linux_arm64 | Linux ARM64 (glibc) | one of the 4 aggregate defaults
| duckdb_jdbc_macos_universal | macOS (x86_64 + ARM64) | one of the 4 aggregate defaults
| duckdb_jdbc_windows_amd64 | Windows x86_64 | one of the 4 aggregate defaults
| duckdb_jdbc_linux_amd64_musl | Linux x86_64 (musl) | published, opt-in (not in the aggregate)
| duckdb_jdbc_linux_arm64_musl | Linux ARM64 (musl) | published, opt-in (not in the aggregate)
| duckdb_jdbc_windows_arm64 | Windows ARM64 | published, opt-in (not in the aggregate)
|===

All `packaging` is `jar`; groupId is `org.duckdb`.

Recommended usage:
- Simplest: depend on `duckdb_jdbc`. Its thin POM pulls java + the 4 core platform
  natives, and the driver loader picks the matching `.so` at runtime.
- Pin the platform yourself: depend on `duckdb_jdbc_java` plus exactly the native
  artifact you target (e.g. `duckdb_jdbc_linux_amd64`).
- Alpine/musl: declare `duckdb_jdbc_linux_amd64_musl` (or `duckdb_jdbc_linux_arm64_musl`)
  explicitly; Maven cannot detect musl, so it is not auto-selected.

The poms are generated from checked-in templates at publish time (the version is the
git tag, or a commit-derived SNAPSHOT):
- `pom.xml.template` produces the `duckdb_jdbc` aggregate POM (thin: unconditional
  dependencies on `duckdb_jdbc_java` + the 4 core OS natives).
- `classifier-pom.xml.template` produces each module POM (`duckdb_jdbc_java` and every
  native artifact) via the `${ARTIFACT_ID}` placeholder.

Release flow (GitHub Actions, `.github/workflows/Java.yml`):
- Each platform job builds and uploads `duckdb_jdbc.jar` and its
  `duckdb_jdbc_native_<os>_<arch>.jar`.
- `maven-deploy` (on tags) runs `scripts/jdbc_maven_deploy.py` to publish all artifacts
  to Maven Central.
- `maven-snapshot-deploy` (manual dispatch on `main`) runs
  `scripts/jdbc_maven_deploy_s3.py` to publish SNAPSHOTs to the DuckDB staging S3 repo.

Example - depend on everything (recommended):

```
<dependency>
  <groupId>org.duckdb</groupId>
  <artifactId>duckdb_jdbc</artifactId>
  <version>V</version>
</dependency>
```

Example - pin the platform yourself:

```
<dependency>
  <groupId>org.duckdb</groupId>
  <artifactId>duckdb_jdbc_java</artifactId>
  <version>V</version>
</dependency>
<dependency>
  <groupId>org.duckdb</groupId>
  <artifactId>duckdb_jdbc_linux_amd64_musl</artifactId>
  <version>V</version>
</dependency>
```
