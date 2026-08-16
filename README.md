It's required to have a JDK installed to build.
Make sure the `JAVA_HOME` environment variable is set.


### Development

To build the driver, run `make release`. 


This will produce two jars in the build folder:
`build/release/duckdb_jdbc.jar`
`build/release/duckdb_jdbc_tests.jar`

The tests can be ran using using `make test` or this command
```
java -cp "build/release/duckdb_jdbc_tests.jar:build/release/duckdb_jdbc.jar" org/duckdb/TestDuckDBJDBC
```

This optionally takes an argument to only run a single test, for example:
```
java -cp "build/release/duckdb_jdbc_tests.jar:build/release/duckdb_jdbc.jar"  org/duckdb/TestDuckDBJDBC test_valid_but_local_config_throws_exception
```

The string write performance benchmark is optional and can be run with:
```
make stress
```
Override its workload with `PERF_ROWS` and `PERF_SAMPLES` if needed. The
benchmark is intentionally not part of the regular test suite.

Scalar function usage examples: [UDF.MD](UDF.MD)

JFR memory monitoring usage: [JFR.md](JFR.md)
