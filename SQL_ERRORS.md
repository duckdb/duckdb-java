# SQL_ERRORS

Every `java.sql.SQLException` thrown by the DuckDB JDBC driver carries two extra attributes:

- a **vendor error code**, returned by `SQLException#getErrorCode()`, and
- a **categorized SQLState**, returned by `SQLException#getSQLState()`.

The authoritative list of codes is the public `org.duckdb.ErrorCode` enum; the categorized states live in
`org.duckdb.SQLState`. Each `ErrorCode` constant identifies a single, fixed call site, so a caller that
sees a code can pinpoint the exact origin of the failure.

## Error codes

Codes are allocated deterministically and are **stable** across releases:

- each distinct throwing class (the `origin`) is assigned a 100-wide block starting at `1000`
  (1000, 1100, 1200, ...), in order of first appearance in `ErrorCode`; and
- constants within an origin increment the code sequentially by declaration order.

Codes are always non-negative and unique (uniqueness is asserted at class-load time and in tests).

`ErrorCode#getCode()`, `#getSQLState()`, `#getOrigin()` and `#getDescription()` expose the code, state,
throwing class, and a short description respectively. A ready-to-throw `SQLException` can be built with
`ErrorCode#asException()`.

## SQLState assignment rules

States follow the standard JDBC / SQLSTATE class taxonomy (the first two characters select the SQLSTATE
class). The state assigned to an error is a function of the **origin and nature** of the failure:

| SQLState | Class meaning | Driver usage |
| --- | --- | --- |
| `08001` | Connection rejected / unable to establish | connection startup / session-init failures |
| `08003` | Connection closed | any closed-object error (closed connection, statement, result set, appender) |
| `08006` | Connection I/O failure | I/O failures |
| `22xxx` | Data / type conversion exception | value conversion and type-check failures (`22002`, `22003`, `22007`, `22018`, `22000`) |
| `23000` / `23505` | Integrity constraint / unique violation | native constraint errors |
| `24000` | Invalid cursor state | missing row in context |
| `42xxx` | Syntax error / access rule violation | parser, binder, table/column-not-found and function-registration errors |
| `0700x` | Data exception (parameters) | parameter index / null parameter errors (`07009`, `07000`) |
| `0A000` | Feature not supported | `SQLFeatureNotSupportedException` and unsupported operations |
| `HYxxx` | ODBC/JDBC CLI-specific condition | driver housekeeping fallbacks (`HY000` general, `HY010` function sequence, `HY024` invalid argument, `HY001` memory) |

Closed-object failures always use `08003`; everything else without a precise SQL class defaults to
`HY000`.

## Native DuckDB errors

DuckDB's C API reports errors as free text (for example from `duckdb_result_error` or
`duckdb_appender_error`). These are surfaced through
`JdbcUtils#createSQLExceptionFromNativeError(String, Throwable)`, which:

- keeps the native message text verbatim, and
- maps a recognised error prefix to a categorized SQLState.

The prefix-to-state mapping is case-insensitive and first-match-wins:

| Native error prefix | SQLState |
| --- | --- |
| `Parser Error`, `Syntax Error`, `Parse Error` | `37000` |
| `Binder Error`, `Catalog Error`, `table ... does not exist` | `42xxx` |
| `Conversion Error`, `Cast Error`, `Invalid Type` | `22018` |
| `Out of Range`, `Overflow` | `22003` |
| `Constraint Error`, `duplicate key`, `NOT NULL constraint` | `23000` |
| `Connection Error` | `08001` |
| `Not Supported` | `0A000` |
| anything else | `HY000` (error code `ErrorCode.NATIVE_UNDECODED`) |

## Guidelines for contributors

- Prefer `JdbcUtils#createSQLException(String, ErrorCode[, Throwable])` over constructing
  `SQLException` directly so that every thrown exception carries a code and state.
- Prefer `JdbcUtils#createSQLExceptionFromNativeError(String[, Throwable])` when wrapping native text.
- Closed-object errors must use the `08003` state.