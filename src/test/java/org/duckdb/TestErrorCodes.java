package org.duckdb;

import static org.duckdb.test.Assertions.assertEquals;
import static org.duckdb.test.Assertions.assertThrows;
import static org.duckdb.test.Assertions.assertTrue;
import static org.duckdb.test.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Verifies that every SQLException thrown by the driver carries a categorized SQLState and a unique,
 * stable vendor {@link ErrorCode}.
 */
public class TestErrorCodes {

    private static final String JDBC_URL = "jdbc:duckdb:";

    // ---- ErrorCode invariants -------------------------------------------------

    public static void test_error_codes_are_unique() throws Exception {
        Map<Integer, String> byCode = new HashMap<>();
        for (ErrorCode ec : ErrorCode.values()) {
            Integer code = ec.getCode();
            assertEquals(null, byCode.put(code, ec.name()), "error code " + code + " must be unique");
            assertTrue(code >= 0, "error code must be non-negative: " + ec);
        }
        assertEquals(ErrorCode.values().length, ErrorCode.count());
    }

    public static void test_error_code_states_are_valid() throws Exception {
        for (ErrorCode ec : ErrorCode.values()) {
            assertTrue(ec.getSQLState() != null && ec.getSQLState().getCode().length() == 5,
                       "every code must have a 5-char SQLState: " + ec);
        }
    }

    public static void test_error_code_as_exception() throws Exception {
        SQLException e = ErrorCode.RESULT_SET_IS_CLOSED.asException();
        assertEquals(e.getErrorCode(), ErrorCode.RESULT_SET_IS_CLOSED.getCode());
        assertEquals(e.getSQLState(), ErrorCode.RESULT_SET_IS_CLOSED.getSQLState().getCode());
    }

    // ---- connection -----------------------------------------------------------

    public static void test_connection_closed_state() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        conn.close();
        try {
            conn.createStatement();
            fail("expected SQLException");
        } catch (SQLException e) {
            assertStateAndCode(e, ErrorCode.CONNECTION_CLOSED);
        }
    }

    // ---- closed result set ----------------------------------------------------

    public static void test_result_set_closed_state() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 42 as a")) {
            rs.next();
            rs.close();
            try {
                rs.getInt(1);
                fail("expected SQLException");
            } catch (SQLException e) {
                assertStateAndCode(e, ErrorCode.RESULT_SET_IS_CLOSED);
            }
        }
    }

    // ---- column index out of bounds -------------------------------------------

    public static void test_meta_column_oob_state() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 42 as a")) {
            ResultSetMetaData meta = rs.getMetaData();
            try {
                meta.getColumnName(2);
                fail("expected SQLException");
            } catch (SQLException e) {
                assertStateAndCode(e, ErrorCode.META_COLUMN_OOB);
            }
        }
    }

    // ---- type conversion ------------------------------------------------------

    public static void test_conversion_state() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 'hello' as v")) {
            rs.next();
            try {
                rs.getObject(1, Boolean.class);
                fail("expected SQLException");
            } catch (SQLException e) {
                assertStateAndCode(e, ErrorCode.RESULT_SET_CONVERSION);
            }
        }
    }

    // ---- native error prefix → classified SQLState ----------------------------

    public static void test_native_error_state() throws Exception {
        // Catalog / table-not-found prefix maps to the 42xxx table-not-found class.
        assertEquals("42S02", JdbcUtils.nativeState("Catalog Error: Table with name foo does not exist!").getCode());
        // Parser prefix maps to the syntax-error class.
        assertEquals("42000", JdbcUtils.nativeState("Parser Error: syntax error at or near \"bogus\"").getCode());
        // Conversion prefix maps to the invalid-character-for-cast class.
        assertEquals("22018",
                     JdbcUtils.nativeState("Conversion Error: could not convert string 'x' to INTEGER").getCode());
        // Unknown prefix falls back to HY000.
        assertEquals("HY000", JdbcUtils.nativeState("Oops: something odd happened").getCode());

        // A native error surfaced via createSQLExceptionFromNativeError carries the parsed state and the
        // NATIVE_UNDECODED vendor code, preserving the message verbatim.
        SQLException e =
            JdbcUtils.createSQLExceptionFromNativeError("Catalog Error: Table with name foo does not exist!");
        assertEquals(e.getErrorCode(), ErrorCode.NATIVE_UNDECODED.getCode(), "error code");
        assertEquals(e.getSQLState(), "42S02", "SQLState");
        assertTrue(e.getMessage().startsWith("Catalog Error:"), "message must be preserved");
    }

    // ---- param index out of bounds --------------------------------------------

    public static void test_param_oob_state() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement ps = conn.prepareStatement("SELECT ?")) {
            try {
                ps.setInt(5, 1);
                fail("expected SQLException");
            } catch (SQLException e) {
                assertStateAndCode(e, ErrorCode.PREPARED_PARAM_OOB);
            }
        }
    }

    // ---- helper ---------------------------------------------------------------

    private static void assertStateAndCode(SQLException e, ErrorCode expected) throws Exception {
        assertEquals(e.getErrorCode(), expected.getCode(), "error code");
        assertEquals(e.getSQLState(), expected.getSQLState().getCode(), "SQLState");
    }
}