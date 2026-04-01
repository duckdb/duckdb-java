# Java Scalar Functions (UDF)

Use `DuckDBConnection.registerScalarFunction` to register a vectorized scalar function in Java.

```java
void registerScalarFunction(
    String name,
    DuckDBLogicalType[] parameterTypes,
    DuckDBLogicalType returnType,
    DuckDBVectorizedScalarFunction function
) throws SQLException
```

Notes:
- The API uses typed logical types (`DuckDBLogicalType`) instead of SQL type strings.
- The callback is vectorized: process `rowCount` rows from the input chunk and write one value per row into `out`.
- `DuckDBDataChunkReader` / `DuckDBReadableVector` / `DuckDBWritableVector` are valid only during the callback.

## Simple example

```java
try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class)) {
    try (DuckDBLogicalType intType = DuckDBLogicalType.of(DuckDBColumnType.INTEGER)) {
        conn.registerScalarFunction("java_add_one", new DuckDBLogicalType[] {intType}, intType, (input, rowCount, out) -> {
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
}
```

## More complete example

Build a label from `TIMESTAMP` + `VARCHAR` + `DOUBLE`, preserving `NULL` behavior:

```java
try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class)) {
    try (DuckDBLogicalType tsType = DuckDBLogicalType.of(DuckDBColumnType.TIMESTAMP);
         DuckDBLogicalType strType = DuckDBLogicalType.of(DuckDBColumnType.VARCHAR);
         DuckDBLogicalType dblType = DuckDBLogicalType.of(DuckDBColumnType.DOUBLE)) {
        conn.registerScalarFunction(
            "java_event_label",
            new DuckDBLogicalType[] {tsType, strType, dblType},
            strType,
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
}
```
