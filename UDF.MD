# Java Scalar Functions (UDF)

Use `DuckDBFunctions.scalarFunction()` to build and register scalar functions.

## Recommended API (Functional Interfaces)

Use these overloads for simple functions:

- `withFunction(Supplier<?>)` for zero arguments
- `withFunction(Function<?, ?>)` for one argument
- `withFunction(BiFunction<?, ?, ?>)` for two arguments

### Simple example (`Function`)

```java
try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class)) {
    DuckDBFunctions.scalarFunction()
        .withName("java_add_one")
        .withParameter(Integer.class)
        .withReturnType(Integer.class)
        .withFunction((Function<Integer, Integer>) x -> x + 1)
        .register(conn);
}
```

### Slightly more complex example (`BiFunction`)

```java
try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class)) {
    DuckDBFunctions.scalarFunction()
        .withName("java_weighted_sum")
        .withParameter(Double.class)
        .withParameter(Double.class)
        .withReturnType(Double.class)
        .withFunction((BiFunction<Double, Double, Double>) (x, w) -> x * w + 10.0)
        .register(conn);
}
```

Behavior:

- For `Function`/`BiFunction`, NULL input propagates to NULL output.
- For `Supplier`, returning `null` writes NULL output.
- `Function` and `BiFunction` are fixed arity only (no varargs).

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
- `LocalDate` and `java.sql.Date` -> `DATE`
- `LocalDateTime`, `java.sql.Timestamp`, and `java.util.Date` -> `TIMESTAMP`

Use `DuckDBLogicalType.decimal(width, scale)` for explicit DECIMAL precision/scale.

## Varargs

Declare varargs type with `withVarArgs(DuckDBLogicalType)`.

For functional varargs, use `withVarArgsFunction(Function<Object[], ?>)`:

```java
try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
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

Notes:

- `withFunction(Function)` and `withFunction(BiFunction)` reject varargs.
- `withVarArgsFunction(...)` requires `withVarArgs(...)` first.

## Builder methods

- `withName(String)`
- `withParameter(Class<?> | DuckDBColumnType | DuckDBLogicalType)`
- `withReturnType(Class<?> | DuckDBColumnType | DuckDBLogicalType)`
- `withFunction(Supplier | Function | BiFunction)`
- `withVarArgs(DuckDBLogicalType)`
- `withVarArgsFunction(Function<Object[], ?>)`
- `withVectorFunction(DuckDBScalarVectorFunction)`
- `withVolatile()`
- `withSpecialHandling()`
- `register(DuckDBConnection)`

## Advanced API (`DuckDBScalarVectorFunction`)

Use `withVectorFunction(...)` for full chunk/vector control.

Example with multiple input types (`TIMESTAMP`, `VARCHAR`, `DOUBLE`) and `VARCHAR` output:

```java
try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
     DuckDBLogicalType tsType = DuckDBLogicalType.of(DuckDBColumnType.TIMESTAMP);
     DuckDBLogicalType strType = DuckDBLogicalType.of(DuckDBColumnType.VARCHAR);
     DuckDBLogicalType dblType = DuckDBLogicalType.of(DuckDBColumnType.DOUBLE)) {
    DuckDBFunctions.scalarFunction()
        .withName("java_event_label")
        .withParameter(tsType)
        .withParameter(strType)
        .withParameter(dblType)
        .withReturnType(strType)
        .withVectorFunction((input, out) -> {
            DuckDBReadableVector ts = input.vector(0);
            DuckDBReadableVector tag = input.vector(1);
            DuckDBReadableVector score = input.vector(2);
            int rowCount = input.rowCount();
            for (int i = 0; i < rowCount; i++) {
                if (ts.isNull(i) || tag.isNull(i) || score.isNull(i)) {
                    out.setNull(i);
                    continue;
                }
                String value = ts.getLocalDateTime(i) + " | "
                    + tag.getString(i).trim().toUpperCase()
                    + " | " + score.getDouble(i);
                out.setString(i, value);
            }
        })
        .register(conn);
}
```

Lifecycle rules:

- `DuckDBDataChunkReader`, `DuckDBReadableVector`, and `DuckDBWritableVector` are valid only during callback execution.
- Write exactly one output value per input row for each callback invocation.
