# Java Scalar Functions (UDF)

Use `DuckDBFunctions.scalarFunction()` to build and register scalar functions.
`register(java.sql.Connection)` returns a `DuckDBRegisteredFunction` with metadata about the registered scalar function.

Registered functions are also tracked in a Java-side registry exposed by `DuckDBDriver.registeredFunctions()`.
This registry is bookkeeping for functions registered through the JDBC API, not an authoritative view of the DuckDB catalog.
`DuckDBDriver.clearFunctionsRegistry()` clears only the Java-side registry and does not de-register functions from DuckDB.

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
    DuckDBRegisteredFunction function = DuckDBFunctions.scalarFunction()
                                            .withName("java_add_one")
                                            .withParameter(Integer.class)
                                            .withReturnType(Integer.class)
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
        .withParameter(Double.class)
        .withParameter(Double.class)
        .withReturnType(Double.class)
        .withDoubleFunction((x, w) -> x * w + 10.0)
        .register(conn);
}
```

```sql
SELECT java_weighted_sum(2.5, 4.0);
```

Behavior:

- `Function` and `BiFunction` callbacks receive `null` for NULL inputs; implement null handling in Java callback logic.
- `withIntFunction(...)`, `withLongFunction(...)`, and `withDoubleFunction(...)` run with null propagation enabled.
- For `withVectorizedFunction(...)`, use `ctx.propagateNulls(true)` when you want stream-level null skipping and automatic NULL output.
- For `Supplier`, returning `null` writes NULL output.
- `Function` and `BiFunction` are fixed arity only (no varargs).

Runtime error model:

- Callback-time reader/writer/context type and value failures throw `DuckDBFunctionException`.
- Invalid row/column indexes throw `IndexOutOfBoundsException`.
- `SQLException` remains for registration-time API usage and type declaration/validation.

## Type declaration and mapping

`withParameter(...)` and `withReturnType(...)` accept:

- `Class<?>`
- `DuckDBColumnType`
- `DuckDBLogicalType`

Common class mappings include:

- `Integer` -> `INTEGER`
- `Long` -> `BIGINT`
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
- `withSpecialHandling()`
- `register(java.sql.Connection)`

## Registered Function Metadata And Registry

`DuckDBRegisteredFunction` exposes immutable metadata about the successful registration result:

- `name()`
- `functionKind()`
- `isScalar()`
- parameter and return type metadata
- callback and flags used at registration time

To inspect Java-side registrations:

```java
List<DuckDBRegisteredFunction> functions = DuckDBDriver.registeredFunctions();
```

The returned list is read-only. Duplicate function names may appear in the registry.

## Advanced API (`DuckDBScalarFunction`)

Use `withVectorizedFunction(...)` for full context control through `DuckDBScalarContext`.

Example with multiple input types (`TIMESTAMP`, `VARCHAR`, `DOUBLE`) and `VARCHAR` output:

```java
try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
     DuckDBLogicalType tsType = DuckDBLogicalType.of(DuckDBColumnType.TIMESTAMP);
     DuckDBLogicalType strType = DuckDBLogicalType.of(DuckDBColumnType.VARCHAR);
     DuckDBLogicalType dblType = DuckDBLogicalType.of(DuckDBColumnType.DOUBLE)) {
    DuckDBFunctions.scalarFunction()
        .withName("java_event_label")
        .withParameter(tsType)
        .withParameter(strType)
        .withParameter(dblType)
        .withReturnType(strType)
        .withVectorizedFunction(ctx -> {
            ctx.propagateNulls(true).stream().forEachOrdered(row -> {
                String value = row.getLocalDateTime(0) + " | "
                    + row.getString(1).trim().toUpperCase()
                    + " | " + row.getDouble(2);
                row.setString(value);
            });
        })
        .register(conn);
}
```

```sql
SELECT java_event_label(TIMESTAMP '2026-04-04 12:00:00', 'launch', 4.5);
```

Lifecycle rules:

- `DuckDBScalarContext`, `DuckDBScalarRow`, `DuckDBReadableVector`, and `DuckDBWritableVector` are valid only during callback execution.
- `DuckDBReadableVector` and `DuckDBWritableVector` are abstract callback runtime types (not interfaces).
- Write exactly one output value per input row for each callback invocation.
- With `propagateNulls(true)`, `DuckDBScalarContext.stream()` skips rows that contain NULL in any input column and writes NULL to the output for those rows.
