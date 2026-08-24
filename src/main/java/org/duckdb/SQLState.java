package org.duckdb;

/**
 * Categorized SQLState codes used by every {@link java.sql.SQLException} thrown by the DuckDB JDBC
 * driver. Values follow the standard JDBC / SQLSTATE class taxonomy (first two characters select the
 * SQLSTATE class, the last three the subclass) as listed in the IBM SQLSTATE value documentation and
 * the JDBC 4.3 specification, extended where the driver needs a vendor-specific condition.
 *
 * <p>The state assigned to a given error is a function of the <em>origin</em> of the failure, not of
 * the message text: closed-object errors map to {@code 08003}, data/type-conversion failures map into
 * the {@code 22xxx} class, parameter problems map into {@code 0700x}, and everything else that has no
 * precise SQL class defaults to {@link #HY000}.
 */
public enum SQLState {
    /** Connection closed / server rejected the connection ({@code 08001}). */
    CONNECTION_REJECTED("08001"),
    /** Connection closed / client could not be established ({@code 08S01}). */
    CONNECTION_TERMINATED("08S01"),
    /** The connection was closed ({@code 08003}). */
    CONNECTION_CLOSED("08003"),
    /** Connection exception - I/O failure traveling the network ({@code 08006}). */
    CONNECTION_IO("08006"),
    /** Communication link failure ({@code 08S01}). */
    CONNECTION_COMMUNICATION("08S01"),
    /** Network access to the server failed ({@code 08001}). */
    CONNECTION_UNABLE_ESTABLISH("08001"),
    /** Invalid authorization specification ({@code 28000}). */
    AUTH_FAILED("28000"),
    /** Null value not allowed / NOT NULL constraint violated ({@code 23000}). */
    INTEGRITY_CONSTRAINT("23000"),
    /** Unique constraint / duplicate key violated ({@code 23505}). */
    DUPLICATE_KEY("23505"),
    /** Data exception - no row found ({@code 22002}). */
    NO_ROW_FOUND("22002"),
    /** Data exception - numeric value out of range ({@code 22003}). */
    NUMERIC_VALUE_OUT_OF_RANGE("22003"),
    /** Data exception - datetime field overflow ({@code 22007}). */
    DATETIME_FIELD_OVERFLOW("22007"),
    /** Data exception - invalid character value for cast ({@code 22018}). */
    INVALID_CHARACTER_VALUE_FOR_CAST("22018"),
    /** Data exception - string data right truncation ({@code 22001}). */
    STRING_DATA_RIGHT_TRUNCATION("22001"),
    /** Data exception - substring error ({@code 22011}). */
    SUBSTRING_ERROR("22011"),
    /** Data exception (fallback within the {@code 22xxx} class) ({@code 22000}). */
    DATA_EXCEPTION("22000"),
    /** Feature not supported ({@code 0A000}). */
    NOT_SUPPORTED("0A000"),
    /** Invalid cursor state, e.g. no row in context ({@code 24000}). */
    INVALID_CURSOR_STATE("24000"),
    /** Syntax error or access rule violation (parser) ({@code 42000}). */
    SYNTAX_ERROR("42000"),
    /** Base table or view not found ({@code 42S02}). */
    TABLE_NOT_FOUND("42S02"),
    /** Column not found ({@code 42S22}). */
    COLUMN_NOT_FOUND("42S22"),
    /** Parameter index out of range / invalid descriptor index ({@code 07009}). */
    PARAMETER_INDEX_OUT_OF_RANGE("07009"),
    /** Invalid use of null parameter / non-null parameter ({@code 07000} fallback). */
    INVALID_PARAMETER_DATA_EXCEPTION("07000"),
    /** Invalid attribute value (ODBC/JDBC-specific) ({@code HY024}). */
    INVALID_ATTRIBUTE_VALUE("HY024"),
    /** Memory allocation error ({@code HY001}). */
    MEMORY_ALLOCATION("HY001"),
    /** Function sequence error, e.g. method used out of order ({@code HY010}). */
    FUNCTION_SEQUENCE_ERROR("HY010"),
    /**
     * General / fallback error ({@code HY000}): used for driver "housekeeping" failures that have no
     * precise SQL class. Anything resembling a closed object uses {@link #CONNECTION_CLOSED} instead.
     */
    HY000("HY000");

    private final String code;

    SQLState(String code) {
        this.code = code;
    }

    /** The five-character SQLSTATE string. */
    public String getCode() {
        return code;
    }

    @Override
    public String toString() {
        return code;
    }
}