# User-Defined Functions (Java)

This guide shows how to use Java Scalar UDFs and Table Functions with `DuckDBConnection`.

## Scalar UDF

Scalar UDF callbacks use a vectorized contract:

```java
ScalarUdf.apply(UdfContext ctx, UdfReader[] args, UdfScalarWriter out, int rowCount)
```

Use `rowCount` loops and write one output value per row.

### Basic example

```java
try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection.class);
     Statement stmt = conn.createStatement()) {

    conn.registerScalarUdf("add_one", DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                           (ctx, args, out, rowCount) -> {
                               for (int row = 0; row < rowCount; row++) {
                                   out.setInt(row, args[0].getInt(row) + 1);
                               }
                           });

    try (ResultSet rs = stmt.executeQuery("SELECT add_one(41)")) {
        rs.next();
        System.out.println(rs.getInt(1)); // 42
    }
}
```

### Registration forms

You can register scalar UDFs with:

- `DuckDBColumnType` signatures (`registerScalarUdf`)
- `Class<?>` signatures (`registerScalarUdf`)
- explicit `UdfLogicalType` signatures (`registerScalarUdf`)
- varargs signatures (`registerScalarUdfVarArgs`)

For decimal precision/scale, prefer explicit logical types:

```java
conn.registerScalarUdf(
    "mul_decimal",
    new UdfLogicalType[] {UdfLogicalType.decimal(20, 4), UdfLogicalType.decimal(20, 4)},
    UdfLogicalType.decimal(38, 8),
    (ctx, args, out, rowCount) -> {
        for (int row = 0; row < rowCount; row++) {
            out.setBigDecimal(row, args[0].getBigDecimal(row).multiply(args[1].getBigDecimal(row)));
        }
    }
);
```

### Options

`UdfOptions` controls scalar behavior:

- `deterministic(true|false)`: marks whether equal inputs always produce equal output. Use `false` for non-deterministic logic (for example random/time-based behavior).
- `nullSpecialHandling(true|false)`: when `true`, your callback receives rows that contain `NULL` input values; when `false`, DuckDB handles null propagation before callback execution.
- `returnNullOnException(true|false)`: when `true`, Java exceptions in callback rows are returned as `NULL`; when `false`, the query fails with an error.
- `varArgs(true|false)`: enables varargs registration (normally used via `registerScalarUdfVarArgs`).

Example:

```java
UdfOptions options = new UdfOptions()
    .deterministic(true)
    .nullSpecialHandling(true)
    .returnNullOnException(false);

conn.registerScalarUdf("safe_add", DuckDBColumnType.INTEGER, DuckDBColumnType.INTEGER,
                       (ctx, args, out, rowCount) -> {
                           for (int row = 0; row < rowCount; row++) {
                               if (args[0].isNull(row)) {
                                   out.setNull(row);
                               } else {
                                   out.setInt(row, args[0].getInt(row) + 1);
                               }
                           }
                       }, options);
```

## UdfReader / UdfScalarWriter object mappings

| DuckDB type | Reader object | Writer object |
| --- | --- | --- |
| `BOOLEAN` | `Boolean` | `Boolean` |
| `TINYINT`, `SMALLINT`, `INTEGER`, `UTINYINT`, `USMALLINT` | `Integer` | `Integer` |
| `BIGINT`, `UINTEGER`, `UBIGINT` | `Long` | `Long` |
| `FLOAT` | `Float` | `Float` |
| `DOUBLE` | `Double` | `Double` |
| `DECIMAL` | `BigDecimal` | `BigDecimal` |
| `VARCHAR` | `String` | `String` |
| `BLOB` | `byte[]` | `byte[]` |
| `DATE` | `LocalDate` or `Date` | `LocalDate` or `Date` |
| `TIME`, `TIME_NS` | `LocalTime` | `LocalTime` |
| `TIME_WITH_TIME_ZONE` | `OffsetTime` | `OffsetTime` |
| `TIMESTAMP`, `TIMESTAMP_S`, `TIMESTAMP_MS`, `TIMESTAMP_NS` | `LocalDateTime` | `LocalDateTime` or `Date` |
| `TIMESTAMP_WITH_TIME_ZONE` | `OffsetDateTime` | `OffsetDateTime` or `Date` |
| `UUID` | `UUID` | `UUID` |
| `HUGEINT`, `UHUGEINT` | `byte[]` | `byte[]` |

`UdfScalarWriter` supports explicit setters and `setObject(...)`.

## Table Function

Table function callbacks use:

- `bind(BindContext ctx, Object[] parameters) -> TableBindResult`
- `init(InitContext ctx, TableBindResult bind) -> TableState`
- `produce(TableState state, UdfOutputAppender out) -> int`

What each callback does:

- `bind`: runs once per invocation to validate/interpret parameters, define output schema, and create bind state.
- `init`: runs after bind to initialize execution state (cursor/counters/chunk state).
- `produce`: runs repeatedly to emit rows in chunks; return the number of rows produced in that call.

### Basic example

```java
conn.registerTableFunction(
    "range_java",
    new TableFunction() {
        @Override
        public TableBindResult bind(BindContext ctx, Object[] parameters) {
            long end = ((Number) parameters[0]).longValue();
            return new TableBindResult(
                new String[] {"i"},
                new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.BIGINT)},
                new long[] {0L, end}
            );
        }

        @Override
        public TableState init(InitContext ctx, TableBindResult bind) {
            return new TableState(bind.getBindState());
        }

        @Override
        public int produce(TableState state, UdfOutputAppender out) {
            long[] st = (long[]) state.getState();
            long current = st[0];
            long end = st[1];
            int produced = 0;

            while (produced < 256 && current < end) {
                out.beginRow().append(current).endRow();
                current++;
                produced++;
            }

            st[0] = current;
            return produced;
        }
    },
    new TableFunctionDefinition().withParameterTypes(new DuckDBColumnType[] {DuckDBColumnType.BIGINT}),
    new TableFunctionOptions().threadSafe(false).maxThreads(1)
);
```

### Bind parameter object mappings

In `bind`, parameters are materialized as Java objects. Common mappings:

- `DECIMAL -> BigDecimal`
- `DATE -> LocalDate`
- `TIME`, `TIME_NS -> LocalTime`
- `TIMESTAMP* -> LocalDateTime`
- `TIME_WITH_TIME_ZONE -> OffsetTime`
- `TIMESTAMP_WITH_TIME_ZONE -> OffsetDateTime`
- `UUID -> UUID`

### Output writing with UdfOutputAppender

`UdfOutputAppender` supports:

- primitive/object `append(...)` for one column at a time
- `setObject(...)` and typed setters (`setBigDecimal`, `setLocalDate`, etc.)
- nested output objects for container types:
  - `LIST`/`ARRAY`: Java arrays or `Collection`
  - `MAP`: `Map`
  - `STRUCT`: positional `List`/array or named `Map<String, Object>`
  - `UNION`: `AbstractMap.SimpleEntry<String, Object>`
  - `ENUM`: `String`

## Table function options

`TableFunctionOptions`:

- `threadSafe(false|true)`
- `maxThreads(int >= 1)`

`TableFunctionDefinition`:

- `withParameterTypes(...)`
- `withProjectionPushdown(true|false)`

## Unsupported in scalar signatures

Scalar UDF signatures do not support nested/container logical types (`LIST`, `STRUCT`, `MAP`, `ARRAY`, `UNION`, `ENUM`) and `INTERVAL`.

## Practical recommendations

- Use chunk-oriented loops (`rowCount`) for scalar UDF throughput.
- Avoid executing SQL on the same `DuckDBConnection` from inside callbacks.
- Use explicit logical types for decimal-sensitive workloads.
