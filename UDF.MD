# Java Scalar Functions (UDF)

Use `DuckDBConnection.registerScalarFunction` to register a vectorized scalar function in Java.

```java
void registerScalarFunction(
    String name,
    String[] parameterTypes,
    String returnType,
    DuckDBVectorizedScalarFunction function
) throws SQLException
```

Notes:
- `parameterTypes` and `returnType` are SQL type strings (for example: `INTEGER`, `VARCHAR`, `TIMESTAMP`).
- The callback is vectorized: process `rowCount` rows from the input chunk and write one value per row into `out`.
- `DuckDBDataChunkReader` / `DuckDBReadableVector` / `DuckDBWritableVector` are valid only during the callback.

## Simple example

```java
try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class)) {
    conn.registerScalarFunction("java_add_one", new String[] {"INTEGER"}, "INTEGER", (input, rowCount, out) -> {
        DuckDBReadableVector in = input.vector(0);
        for (int i = 0; i < rowCount; i++) {
            if (in.isNull(i)) {
                out.setNull(i);
            } else {
                out.setInt(i, in.getInt(i) + 1);
            }
        }
    });
}
```

## More complete example

Build a label from `TIMESTAMP` + `VARCHAR` + `DOUBLE`, preserving `NULL` behavior:

```java
try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class)) {
    conn.registerScalarFunction(
        "java_event_label",
        new String[] {"TIMESTAMP", "VARCHAR", "DOUBLE"},
        "VARCHAR",
        (input, rowCount, out) -> {
            DuckDBReadableVector ts = input.vector(0);
            DuckDBReadableVector tag = input.vector(1);
            DuckDBReadableVector score = input.vector(2);

            for (int i = 0; i < rowCount; i++) {
                if (ts.isNull(i) || tag.isNull(i) || score.isNull(i)) {
                    out.setNull(i);
                    continue;
                }
                String value =
                    ts.getLocalDateTime(i) + " | " + tag.getString(i).trim().toUpperCase() + " | " + score.getDouble(i);
                out.setString(i, value);
            }
        }
    );
}
```
