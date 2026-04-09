# Java Scalar Functions (UDF)

Use `DuckDBFunctions.scalarFunction()` to build and register scalar functions.

## Recommended API (Functional Interfaces)

Use these overloads for simple functions:

- `withFunction(Supplier<?>)` for zero arguments
- `withFunction(Function<?, ?>)` for one argument
- `withFunction(BiFunction<?, ?, ?>)` for two arguments
- `withIntFunction(IntUnaryOperator | IntBinaryOperator)` for `INTEGER` unary/binary functions
- `withLongFunction(LongUnaryOperator | LongBinaryOperator)` for `BIGINT` unary/binary functions
- `withDoubleFunction(DoubleUnaryOperator | DoubleBinaryOperator)` for `DOUBLE` unary/binary functions

### Simple example (`withIntFunction`)

```java
try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
    DuckDBFunctions.scalarFunction()
        .withName("java_add_one")
        .withParameter(int.class)
        .withReturnType(int.class)
        .withIntFunction(x -> x + 1)
        .register(conn);
}
```

```sql
SELECT java_add_one(41);
```

### Slightly more complex example (`withDoubleFunction`)

```java
try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
    DuckDBFunctions.scalarFunction()
        .withName("java_weighted_sum")
        .withParameters(double.class, double.class)
        .withReturnType(double.class)
        .withDoubleFunction((x, w) -> x * w + 10.0)
        .register(conn);
}
```

```sql
SELECT java_weighted_sum(2.5, 4.0);
```

NULL handling behavior:

- Java functions that take object arguments (`Function` and `BiFunction` callbacks) are registered with
  `duckdb_scalar_function_set_special_handling()` C API option enabled - `null` Java arguments are passed for `NULL` input values
- when `withNullInNullOut()` option is set (skips the `duckdb_scalar_function_set_special_handling()` call on function registration),
  then DuckDB engine may skip the function call completely (and set `NULL` to the call result automatically),
  but this does not happen in all cases (for example, when the function is applied to a relation where some rows are `NULL`),
  so the function still can be passed `null` arguments and need to check them and return `null` accordingly
- Java functions that take primitive arguments (`withIntFunction(...)`, `withLongFunction(...)`, and `withDoubleFunction(...)`)
  are never passed `null` arguments as an input - even when the DuckDB engine calls such function with `NULL` value - 
  the call will be skipped on Java side and `null` value will be returned (returning `null` writes NULL output)

Runtime error model:

- Callback-time reader/writer/context type and value failures throw `DuckDBFunctions.CallException extends RuntimeException`.
- Invalid row/column indexes throw `IndexOutOfBoundsException`.
- `SQLException` is used for registration-time API usage and type declaration/validation.

## Type declaration and mapping

`withParameter(...)` and `withReturnType(...)` accept:

- `Class<?>` (Object or primitive class, like `int.class` or `Integer.TYPE`)
- `DuckDBColumnType`
- `DuckDBLogicalType`

Common class mappings include:

- `int` -> `INTEGER`
- `long` -> `BIGINT`
- `float` -> `FLOAT`
- `double` -> `DOUBLE`
- `String` -> `VARCHAR`
- `BigDecimal` -> `DECIMAL`
- `BigInteger` -> `HUGEINT`
- `LocalDate` and `java.sql.Date` -> `DATE`
- `LocalDateTime`, `java.sql.Timestamp`, and `java.util.Date` -> `TIMESTAMP`

Notes:

- `UHUGEINT` is supported through explicit `DuckDBColumnType.UHUGEINT`/`DuckDBLogicalType` declarations.
- Java class auto-mapping for `BigInteger` remains `HUGEINT`.

Use `DuckDBLogicalType.decimal(width, scale)` for explicit DECIMAL precision/scale.

## Varargs

Declare varargs type with `withVarArgs(DuckDBLogicalType)`.

For functional varargs, use `withVarArgsFunction(Function<Object[], ?>)`:

```java
try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
     DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
    DuckDBFunctions.scalarFunction()
        .withName("java_sum_varargs")
        .withParameter(Integer.class) // fixed argument(s)
        .withVarArgs(intType)         // variadic argument type
        .withReturnType(Integer.class)
        .withVarArgsFunction(args -> {
            int sum = 0;
            for (Object arg : args) {
                sum += (Integer) arg;
            }
            return sum;
        })
        .register(conn);
}
```

```sql
SELECT java_sum_varargs(1, 2, 3, 4);
```

Notes:

- `withFunction(Function)` and `withFunction(BiFunction)` reject varargs.
- `withVarArgsFunction(...)` requires `withVarArgs(...)` first.

## Builder methods

- `withName(String)`
- `withParameter(Class<?> | DuckDBColumnType | DuckDBLogicalType)`
- `withParameters(Class<?>...)`
- `withReturnType(Class<?> | DuckDBColumnType | DuckDBLogicalType)`
- `withFunction(Supplier | Function | BiFunction)`
- `withIntFunction(IntUnaryOperator | IntBinaryOperator)`
- `withLongFunction(LongUnaryOperator | LongBinaryOperator)`
- `withDoubleFunction(DoubleUnaryOperator | DoubleBinaryOperator)`
- `withVarArgs(DuckDBLogicalType)`
- `withVarArgsFunction(Function<Object[], ?>)`
- `withVectorizedFunction(DuckDBScalarFunction)`
- `withVolatile()`
- `withNullInNullOut()`
- `register(java.sql.Connection)`

## Registered Function Metadata And Registry

Registered functions, returned from `.register()` call, are additionally tracked in a Java-side registry exposed by `DuckDBDriver.registeredFunctions()`.
This registry provides bookkeeping for functions registered through the JDBC API, not an authoritative view of the DuckDB catalog.
`DuckDBDriver.clearFunctionsRegistry()` clears only the Java-side registry and does not de-register functions from DuckDB.

`DuckDBFunctions.RegisteredFunction` exposes immutable metadata about the successful registration result:

- `name()`
- `functionKind()`
- `isScalar()`
- parameter and return type metadata
- callback and flags used at registration time

To inspect Java-side registrations:

```java
List<DuckDBFunctions.RegisteredFunction> functions = DuckDBDriver.registeredFunctions();
```

The returned list is read-only. Duplicate function names may appear in the registry.

## Advanced API (`DuckDBScalarFunction`)

Use `withVectorizedFunction(...)` to access multiple input rows (when the scalar function is applied
to a row set from a relation) in a single call. DuckDB engine splits the input row set into "data chunks"
(represented in Java as `DuckDBDataChunkReader`) each chunk containing up to 2048 rows.

`DuckDBScalarFunction` callback receives the `DuckDBDataChunkReader` as an `input` argument.
Data chunk contains a number of "data vectors" (`DuckDBReadableVector`) - single vector for each input column.
Vectors can be accessed using `input.vector(columnIndex)`. It writes the results into  the `output` `DuckDBWritableVector`.
Results in the `output` vector must be set **on the same `row` indices** that are used to read `input`.

`input.stream()` call returns a `LongStream` of `row` indices that can be used with Java Streams API. 

- `DuckDBDataChunkReader`, `DuckDBReadableVector`, and `DuckDBWritableVector` are valid only during callback execution.

Example with multiple input types (`TIMESTAMP`, `VARCHAR`, `DOUBLE`) and `VARCHAR` output:

```
try (Connection conn = DriverManager.getConnection("jdbc:duckdb:"); Statement stmt = conn.createStatement();
     DuckDBLogicalType tsType = DuckDBLogicalType.of(DuckDBColumnType.TIMESTAMP);
     DuckDBLogicalType strType = DuckDBLogicalType.of(DuckDBColumnType.VARCHAR);
     DuckDBLogicalType dblType = DuckDBLogicalType.of(DuckDBColumnType.DOUBLE)) {
    DuckDBFunctions.scalarFunction()
        .withName("java_event_label")
        .withParameters(tsType, strType, dblType)
        .withReturnType(strType)
        .withVectorizedFunction((input, output) -> {
            input.stream().forEach(row -> {
                String value = input.vector(0).getLocalDateTime(row) + " | " +
                               String.valueOf(input.vector(1).getString(row)).trim().toUpperCase() + " | " +
                               input.vector(2).getDouble(row, 0.0d);
                output.setString(row, value);
            });
        })
        .register(conn);
    ...
}
```

```sql
SELECT java_event_label(TIMESTAMP '2026-04-04 12:00:00', 'launch', 4.5);
```
