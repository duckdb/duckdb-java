package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Arrays;

public class TestCAPIAppender {

    public static void test_appender_basic() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 VARCHAR)");
            try (DuckDBCAPIAppender appender = conn.createCAPIAppender("tab1")) {
                for (int i = 0; i < 1; i++) {
                    appender.beginRow().append(Integer.MAX_VALUE - i).append("foo" + i).endRow();
                }
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), Integer.MAX_VALUE);
                assertEquals(rs.getString(2), "foo0");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_null() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 VARCHAR)");
            try (DuckDBCAPIAppender appender = conn.createCAPIAppender("tab1")) {
                for (int i = 0; i < 1; i++) {
                    String str = null;
                    appender.beginRow().appendNull().append(str).endRow();
                }
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), null);
                assertEquals(rs.getString(2), null);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_default() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 VARCHAR)");
            try (DuckDBCAPIAppender appender = conn.createCAPIAppender("tab1")) {
                for (int i = 0; i < 1; i++) {
                    appender.beginRow().appendDefault().appendDefault().endRow();
                }
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 0);
                assertEquals(rs.getString(2), null);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_basic_auto_flush() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 12; // two flushes
            int tail = 16;       // flushed on close

            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 VARCHAR)");
            try (DuckDBCAPIAppender appender = conn.createCAPIAppender(DuckDBConnection.DEFAULT_SCHEMA, "tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    appender.beginRow().append(Integer.MAX_VALUE - i).append("foo" + i).endRow();
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1 DESC")) {
                    for (int i = 0; i < count; i++) {
                        assertTrue(rs.next());
                        assertEquals(rs.getInt(1), Integer.MAX_VALUE - i);
                        assertEquals(rs.getString(2), "foo" + i);
                    }
                    assertFalse(rs.next());
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getInt(1), count + tail);
            }
        }
    }

    public static void test_appender_numbers() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            // int8, int4, int2, int1, float8, float4
            stmt.execute("CREATE TABLE numbers (a BIGINT, b INTEGER, c SMALLINT, d TINYINT, e DOUBLE, f FLOAT)");
            try (DuckDBCAPIAppender appender = conn.createCAPIAppender("numbers")) {
                for (int i = 0; i < 50; i++) {
                    appender.beginRow()
                        .append(Long.MAX_VALUE - i)
                        .append(Integer.MAX_VALUE - i)
                        .append((short) (Short.MAX_VALUE - i))
                        .append((byte) (Byte.MAX_VALUE - i))
                        .append((double) i)
                        .append((float) i)
                        .endRow();
                }
                appender.flush();
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT max(a), max(b), max(c), max(d), max(e), max(f) FROM numbers")) {
                assertFalse(rs.isClosed());
                assertTrue(rs.next());

                long resA = rs.getLong(1);
                assertEquals(resA, Long.MAX_VALUE);

                int resB = rs.getInt(2);
                assertEquals(resB, Integer.MAX_VALUE);

                short resC = rs.getShort(3);
                assertEquals(resC, Short.MAX_VALUE);

                byte resD = rs.getByte(4);
                assertEquals(resD, Byte.MAX_VALUE);

                double resE = rs.getDouble(5);
                assertEquals(resE, 49.0d);

                float resF = rs.getFloat(6);
                assertEquals(resF, 49.0f);
            }
        }
    }

    public static void test_appender_date() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            LocalDate ld1 = LocalDate.now();
            // TODO: https://github.com/duckdb/duckdb-java/issues/200
            LocalDate ld2 = LocalDate.of(/*-*/ 23434, 3, 5);
            LocalDate ld3 = LocalDate.of(1970, 1, 1);
            LocalDate ld4 = LocalDate.of(11111, 12, 31);
            LocalDate ld5 = LocalDate.of(999999999, 12, 31);

            stmt.execute("CREATE TABLE date_only (id INT4, a DATE)");
            try (DuckDBCAPIAppender appender = conn.createCAPIAppender("date_only")) {
                appender.beginRow()
                    .append(1)
                    .appendEpochDays((int) ld1.toEpochDay())
                    .endRow()
                    .beginRow()
                    .append(2)
                    .append(ld2)
                    .endRow()
                    .beginRow()
                    .append(3)
                    .append(ld3)
                    .endRow()
                    .beginRow()
                    .append(4)
                    .append(ld4)
                    .endRow()
                    .beginRow()
                    .append(5);
                assertThrows(() -> { appender.append(ld5); }, SQLException.class);
                appender.append(ld4).endRow().flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT a FROM date_only ORDER BY id")) {
                assertFalse(rs.isClosed());

                assertTrue(rs.next());
                LocalDate res1 = rs.getObject(1, LocalDate.class);
                assertEquals(res1, ld1);

                assertTrue(rs.next());
                LocalDate res2 = rs.getObject(1, LocalDate.class);
                assertEquals(res2, ld2);

                assertTrue(rs.next());
                LocalDate res3 = rs.getObject(1, LocalDate.class);
                assertEquals(res3, ld3);

                assertTrue(rs.next());
                LocalDate res4 = rs.getObject(1, LocalDate.class);
                assertEquals(res4, ld4);

                assertTrue(rs.next());
                LocalDate res5 = rs.getObject(1, LocalDate.class);
                assertEquals(res5, ld4);

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_string_with_emoji() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE data (id INTEGER, str_value VARCHAR(10))");
            String expectedValue = "\u4B54\uD86D\uDF7C\uD83D\uDD25\uD83D\uDE1C";
            char cjk1 = '\u4b54';
            char cjk2 = '\u5b57';

            try (DuckDBCAPIAppender appender = conn.createCAPIAppender("data")) {
                appender.beginRow().append(1).append(expectedValue).endRow();
                // append char
                appender.beginRow().append(2).append(cjk1).endRow();
                // append char array
                appender.beginRow().append(3).append(new char[] {cjk1, cjk2}).endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT str_value FROM data ORDER BY id")) {
                assertTrue(rs.next());
                String row1 = rs.getString(1);
                assertEquals(row1, expectedValue);

                assertTrue(rs.next());
                String row2 = rs.getString(1);
                assertEquals(row2, String.valueOf(cjk1));

                assertTrue(rs.next());
                String row3 = rs.getString(1);
                assertEquals(row3, String.valueOf(new char[] {cjk1, cjk2}));

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_table_does_not_exist() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class)) {
            assertThrows(
                () -> { conn.createCAPIAppender(DuckDBConnection.DEFAULT_SCHEMA, "data"); }, SQLException.class);
        }
    }

    public static void test_appender_table_deleted() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");
            try (DuckDBCAPIAppender appender =
                     conn.createCAPIAppender(DuckDBConnection.DEFAULT_SCHEMA, "data").beginRow().append(1).endRow()) {
                stmt.execute("DROP TABLE data");
                appender.beginRow().append(2).endRow();
                assertThrows(appender::flush, SQLException.class);
            }
        }
    }

    public static void test_appender_append_too_many_columns() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");

            try (DuckDBCAPIAppender appender = conn.createCAPIAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                assertThrows(() -> { appender.beginRow().append(1).append(2).flush(); }, SQLException.class);
            }
        }
    }

    public static void test_appender_append_too_few_columns() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER, b INTEGER)");
            try (DuckDBCAPIAppender appender = conn.createCAPIAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                assertThrows(() -> { appender.beginRow().append(1).endRow(); }, SQLException.class);
            }
        }
    }

    public static void test_appender_type_mismatch() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");
            try (DuckDBCAPIAppender appender = conn.createCAPIAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                assertThrows(() -> { appender.beginRow().append("str"); }, SQLException.class);
            }
        }
    }

    public static void test_appender_null_integer() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");

            try (DuckDBCAPIAppender appender = conn.createCAPIAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                appender.beginRow().appendNull().endRow().flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());
                // java.sql.ResultSet.getInt(int) returns 0 if the value is NULL
                assertEquals(0, results.getInt(1));
                assertTrue(results.wasNull());
            }
        }
    }
}
