# Java Scalar and Table Functions (UDF)

The JDBC driver can register user-defined functions (UDFs) written in
Java. This feature is documented in full on the DuckDB website:

- **[Define Functions](https://duckdb.org/docs/stable/clients/java/functions)**

That page covers building and registering scalar functions (the
functional-interface overloads, variadic arguments, NULL handling,
volatile functions, error handling, and the vectorized
`DuckDBScalarFunction` interface), table functions (`DuckDBTableFunction`,
resource cleanup via `DuckDBTableFunctionState`, parallel execution, and
projection pushdown), inspecting the Java-side function registry, and
type mapping.

## Repo-specific reference

The exercised examples live in the driver's own test suite:

- `src/test/java/org/duckdb/TestScalarFunctions.java`
- `src/test/java/org/duckdb/TestTableFunctions.java`
