package org.duckdb;

import static java.time.ZoneOffset.UTC;
import static org.duckdb.DuckDBHugeInt.HUGE_INT_MAX;
import static org.duckdb.DuckDBHugeInt.HUGE_INT_MIN;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.*;
import java.time.*;
import java.util.*;

public class TestAppender {

    public static void test_appender_basic() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 VARCHAR)");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(Integer.MAX_VALUE).append("foo").endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), Integer.MAX_VALUE);
                assertEquals(rs.getString(2), "foo");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_null_basic() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 VARCHAR)");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                String str = null;
                appender.beginRow().append(41).append(str).endRow();
                appender.beginRow().append(42).appendNull().endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), 41);
                assertEquals(rs.getString(2), null);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), 42);
                assertEquals(rs.getString(2), null);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_default() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INTEGER DEFAULT 42, col2 VARCHAR DEFAULT 'foo')");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().appendDefault().appendDefault().endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getString(2), "foo");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_boolean() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 BOOLEAN)");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(41).append(true).endRow();
                appender.beginRow().append(42).append(false).endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getBoolean(2), true);
                assertTrue(rs.next());
                assertEquals(rs.getBoolean(2), false);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_unsigned() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 UTINYINT, col3 USMALLINT, col4 UINTEGER, col5 UBIGINT)");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(41)
                    .append((byte) -1)
                    .append((short) -1)
                    .append(-1)
                    .append((long) -1)
                    .endRow();
                appender.beginRow()
                    .append(42)
                    .append((byte) -2)
                    .append((short) -2)
                    .append(-2)
                    .append((long) -2)
                    .endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getShort(2), (short) ((1 << 8) - 1));
                assertEquals(rs.getInt(3), (1 << 16) - 1);
                assertEquals(rs.getLong(4), (1L << 32) - 1);
                assertEquals(rs.getObject(5, BigInteger.class),
                             BigInteger.ONE.shiftLeft(64).subtract(BigInteger.valueOf(1)));
                assertTrue(rs.next());
                assertEquals(rs.getShort(2), (short) ((1 << 8) - 2));
                assertEquals(rs.getInt(3), (1 << 16) - 2);
                assertEquals(rs.getLong(4), (1L << 32) - 2);
                assertEquals(rs.getObject(5, BigInteger.class),
                             BigInteger.ONE.shiftLeft(64).subtract(BigInteger.valueOf(2)));
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_long_string() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 VARCHAR)");

            String inlineStr = "foobar";
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 1 << 10; i++) {
                sb.append(i);
            }
            String longStr = sb.toString();
            String emptyStr = "";

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(41).append(inlineStr).endRow();
                appender.beginRow().append(42).append(longStr).endRow();
                appender.beginRow().append(43).append(emptyStr).endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(2), inlineStr);
                assertTrue(rs.next());
                assertEquals(rs.getString(2), longStr);
                assertTrue(rs.next());
                assertEquals(rs.getString(2), emptyStr);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_huge_integer() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 HUGEINT, col3 UHUGEINT)");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(41).append(HUGE_INT_MIN).append(BigInteger.ZERO).endRow();
                appender.beginRow().append(42).append(HUGE_INT_MIN.add(BigInteger.ONE)).append(BigInteger.ONE).endRow();
                appender.beginRow().append(43).append(HUGE_INT_MAX).append(HUGE_INT_MAX).endRow();
                appender.beginRow()
                    .append(44)
                    .append(HUGE_INT_MAX.subtract(BigInteger.ONE))
                    .append(HUGE_INT_MAX.subtract(BigInteger.ONE))
                    .endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(2, BigInteger.class), HUGE_INT_MIN);
                assertEquals(rs.getObject(3, BigInteger.class), BigInteger.ZERO);
                assertTrue(rs.next());
                assertEquals(rs.getObject(2, BigInteger.class), HUGE_INT_MIN.add(BigInteger.ONE));
                assertEquals(rs.getObject(3, BigInteger.class), BigInteger.ONE);
                assertTrue(rs.next());
                assertEquals(rs.getObject(2, BigInteger.class), HUGE_INT_MAX);
                assertEquals(rs.getObject(3, BigInteger.class), HUGE_INT_MAX);
                assertTrue(rs.next());
                assertEquals(rs.getObject(2, BigInteger.class), HUGE_INT_MAX.subtract(BigInteger.ONE));
                assertEquals(rs.getObject(3, BigInteger.class), HUGE_INT_MAX.subtract(BigInteger.ONE));
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_timestamp_local() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE TABLE tab1(col1 INT, col2 TIMESTAMP_S, col3 TIMESTAMP_MS, col4 TIMESTAMP, col5 TIMESTAMP_NS)");

            LocalDateTime ldt = LocalDateTime.of(2025, 6, 25, 12, 34, 45, 678901234);
            java.util.Date dt = java.util.Date.from(ldt.toInstant(UTC));

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tab1 VALUES(?, ?, ?, ?, ?)")) {
                ps.setInt(1, 41);
                ps.setObject(2, ldt);
                ps.setObject(3, ldt);
                ps.setObject(4, ldt);
                ps.setObject(5, ldt);
                ps.execute();
            }

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(42).append(ldt).append(ldt).append(ldt).append(ldt).endRow();
                appender.beginRow().append(43).append(dt).append(dt).append(dt).append(dt).endRow();
                appender.flush();
            }

            // todo: check rounding rules
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 41);
                assertEquals(rs.getObject(2, LocalDateTime.class).toString(), "2025-06-25T12:34:46");
                assertEquals(rs.getObject(3, LocalDateTime.class).toString(), "2025-06-25T12:34:45.679");
                assertEquals(rs.getObject(4, LocalDateTime.class).toString(), "2025-06-25T12:34:45.678901");
                assertEquals(rs.getObject(5, LocalDateTime.class).toString(), "2025-06-25T12:34:45.678901");
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getObject(2, LocalDateTime.class).toString(), "2025-06-25T12:34:45");
                assertEquals(rs.getObject(3, LocalDateTime.class).toString(), "2025-06-25T12:34:45.678");
                assertEquals(rs.getObject(4, LocalDateTime.class).toString(), "2025-06-25T12:34:45.678901");
                assertEquals(rs.getObject(5, LocalDateTime.class).toString(), "2025-06-25T12:34:45.678901234");
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 43);
                assertEquals(rs.getObject(2, LocalDateTime.class).toString(), "2025-06-25T12:34:45");
                assertEquals(rs.getObject(3, LocalDateTime.class).toString(), "2025-06-25T12:34:45.678");
                assertEquals(rs.getObject(4, LocalDateTime.class).toString(), "2025-06-25T12:34:45.678");
                assertEquals(rs.getObject(5, LocalDateTime.class).toString(), "2025-06-25T12:34:45.678");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_timestamp_tz() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone activeTimeZone = TimeZone.getTimeZone("Europe/Sofia");
        TimeZone.setDefault(activeTimeZone);
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 TIMESTAMP WITH TIME ZONE)");

            OffsetDateTime odt = OffsetDateTime.of(2025, 6, 25, 12, 34, 45, 678901234, ZoneOffset.ofHours(8));

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tab1 VALUES(?, ?)")) {
                ps.setInt(1, 41);
                ps.setObject(2, odt);
                ps.execute();
            }

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(42).append(odt).endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 41);
                assertEquals(rs.getObject(2, LocalDateTime.class).toString(), "2025-06-25T07:34:45.678901");
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getObject(2, LocalDateTime.class).toString(), "2025-06-25T07:34:45.678901");
                assertFalse(rs.next());
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_appender_time_local() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 TIME)");

            LocalTime lt = LocalTime.of(12, 34, 45, 678901234);

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tab1 VALUES(?, ?)")) {
                ps.setInt(1, 41);
                ps.setObject(2, lt);
                ps.execute();
            }

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(42).append(lt).endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 41);
                assertEquals(rs.getObject(2, Time.class).toString(), "12:34:45");
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getObject(2, Time.class).toString(), "12:34:45");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_time_tz() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone activeTimeZone = TimeZone.getTimeZone("Europe/Sofia");
        TimeZone.setDefault(activeTimeZone);
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 TIME WITH TIME ZONE)");

            ZoneId zoneId = ZoneId.of("Australia/Melbourne");
            LocalDateTime ldt = LocalDateTime.of(2025, 6, 25, 12, 34, 45, 678901234);
            ZoneOffset zoneOffset = zoneId.getRules().getOffset(ldt.toInstant(UTC));
            OffsetTime ot = ldt.toLocalTime().atOffset(zoneOffset);
            Instant instant = ldt.toInstant(zoneOffset);
            Calendar cal = new GregorianCalendar();
            cal.setTimeZone(TimeZone.getTimeZone(zoneId));
            cal.setTime(java.util.Date.from(instant));

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tab1 VALUES(?, ?)")) {
                ps.setInt(1, 41);
                ps.setTime(2, Time.valueOf(ldt.toLocalTime()), cal);
                ps.execute();
            }

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(42).append(ot).endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 41);
                assertEquals(rs.getObject(2, Time.class).toString(), "20:34:45");
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getObject(2, Time.class).toString(), "12:34:45");
                assertFalse(rs.next());
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_appender_basic_auto_flush() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 12; // two flushes
            int tail = 16;       // flushed on close

            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 VARCHAR)");
            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "tab1")) {
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
            try (DuckDBAppender appender = conn.createAppender("numbers")) {
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
            try (DuckDBAppender appender = conn.createAppender("date_only")) {
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

            try (DuckDBAppender appender = conn.createAppender("data")) {
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
            assertThrows(() -> { conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data"); }, SQLException.class);
        }
    }

    public static void test_appender_table_deleted() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");
            try (DuckDBAppender appender =
                     conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data").beginRow().append(1).endRow()) {
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

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                assertThrows(() -> { appender.beginRow().append(1).append(2).flush(); }, SQLException.class);
            }
        }
    }

    public static void test_appender_append_too_few_columns() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER, b INTEGER)");
            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                assertThrows(() -> { appender.beginRow().append(1).endRow(); }, SQLException.class);
            }
        }
    }

    public static void test_appender_type_mismatch() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");
            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
                assertThrows(() -> { appender.beginRow().append("str"); }, SQLException.class);
            }
        }
    }

    public static void test_appender_null_integer() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE data (a INTEGER)");

            try (DuckDBAppender appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "data")) {
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

    public static void test_appender_decimal() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            BigDecimal bigdec16 = new BigDecimal("12.34").setScale(2);
            BigDecimal bigdec32 = new BigDecimal("1234.5678").setScale(4);
            BigDecimal bigdec64 = new BigDecimal("123456789012.345678").setScale(6);
            BigDecimal bigdec128 = new BigDecimal("123456789012345678.90123456789012345678").setScale(20);
            BigDecimal negbigdec16 = new BigDecimal("-12.34").setScale(2);
            BigDecimal negbigdec32 = new BigDecimal("-1234.5678").setScale(4);
            BigDecimal negbigdec64 = new BigDecimal("-123456789012.345678").setScale(6);
            BigDecimal negbigdec128 = new BigDecimal("-123456789012345678.90123456789012345678").setScale(20);
            BigDecimal smallbigdec16 = new BigDecimal("-1.34").setScale(2);
            BigDecimal smallbigdec32 = new BigDecimal("-123.5678").setScale(4);
            BigDecimal smallbigdec64 = new BigDecimal("-12345678901.345678").setScale(6);
            BigDecimal smallbigdec128 = new BigDecimal("-12345678901234567.90123456789012345678").setScale(20);
            BigDecimal intbigdec16 = new BigDecimal("-1").setScale(2);
            BigDecimal intbigdec32 = new BigDecimal("-123").setScale(4);
            BigDecimal intbigdec64 = new BigDecimal("-12345678901").setScale(6);
            BigDecimal intbigdec128 = new BigDecimal("-12345678901234567").setScale(20);
            BigDecimal onebigdec16 = new BigDecimal("1").setScale(2);
            BigDecimal onebigdec32 = new BigDecimal("1").setScale(4);
            BigDecimal onebigdec64 = new BigDecimal("1").setScale(6);
            BigDecimal onebigdec128 = new BigDecimal("1").setScale(20);

            stmt.execute(
                "CREATE TABLE decimals (id INT4, a DECIMAL(4,2), b DECIMAL(8,4), c DECIMAL(18,6), d DECIMAL(38,20))");

            try (DuckDBAppender appender = conn.createAppender("decimals")) {
                appender.beginRow()
                    .append(1)
                    .append(bigdec16)
                    .append(bigdec32)
                    .append(bigdec64)
                    .append(bigdec128)
                    .endRow()
                    .beginRow()
                    .append(2)
                    .append(negbigdec16)
                    .append(negbigdec32)
                    .append(negbigdec64)
                    .append(negbigdec128)
                    .endRow()
                    .beginRow()
                    .append(3)
                    .append(smallbigdec16)
                    .append(smallbigdec32)
                    .append(smallbigdec64)
                    .append(smallbigdec128)
                    .endRow()
                    .beginRow()
                    .append(4)
                    .append(intbigdec16)
                    .append(intbigdec32)
                    .append(intbigdec64)
                    .append(intbigdec128)
                    .endRow()
                    .beginRow()
                    .append(5)
                    .append(onebigdec16)
                    .append(onebigdec32)
                    .append(onebigdec64)
                    .append(onebigdec128)
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT a,b,c,d FROM decimals ORDER BY id")) {
                assertFalse(rs.isClosed());
                assertTrue(rs.next());

                BigDecimal rs1 = rs.getObject(1, BigDecimal.class);
                BigDecimal rs2 = rs.getObject(2, BigDecimal.class);
                BigDecimal rs3 = rs.getObject(3, BigDecimal.class);
                BigDecimal rs4 = rs.getObject(4, BigDecimal.class);

                assertEquals(rs1, bigdec16);
                assertEquals(rs2, bigdec32);
                assertEquals(rs3, bigdec64);
                assertEquals(rs4, bigdec128);
                assertTrue(rs.next());

                BigDecimal nrs1 = rs.getObject(1, BigDecimal.class);
                BigDecimal nrs2 = rs.getObject(2, BigDecimal.class);
                BigDecimal nrs3 = rs.getObject(3, BigDecimal.class);
                BigDecimal nrs4 = rs.getObject(4, BigDecimal.class);

                assertEquals(nrs1, negbigdec16);
                assertEquals(nrs2, negbigdec32);
                assertEquals(nrs3, negbigdec64);
                assertEquals(nrs4, negbigdec128);
                assertTrue(rs.next());

                BigDecimal srs1 = rs.getObject(1, BigDecimal.class);
                BigDecimal srs2 = rs.getObject(2, BigDecimal.class);
                BigDecimal srs3 = rs.getObject(3, BigDecimal.class);
                BigDecimal srs4 = rs.getObject(4, BigDecimal.class);

                assertEquals(srs1, smallbigdec16);
                assertEquals(srs2, smallbigdec32);
                assertEquals(srs3, smallbigdec64);
                assertEquals(srs4, smallbigdec128);
                assertTrue(rs.next());

                BigDecimal irs1 = rs.getObject(1, BigDecimal.class);
                BigDecimal irs2 = rs.getObject(2, BigDecimal.class);
                BigDecimal irs3 = rs.getObject(3, BigDecimal.class);
                BigDecimal irs4 = rs.getObject(4, BigDecimal.class);

                assertEquals(irs1, intbigdec16);
                assertEquals(irs2, intbigdec32);
                assertEquals(irs3, intbigdec64);
                assertEquals(irs4, intbigdec128);
                assertTrue(rs.next());

                BigDecimal oners1 = rs.getObject(1, BigDecimal.class);
                BigDecimal oners2 = rs.getObject(2, BigDecimal.class);
                BigDecimal oners3 = rs.getObject(3, BigDecimal.class);
                BigDecimal oners4 = rs.getObject(4, BigDecimal.class);

                assertEquals(oners1, onebigdec16);
                assertEquals(oners2, onebigdec32);
                assertEquals(oners3, onebigdec64);
                assertEquals(oners4, onebigdec128);
            }
        }
    }

    public static void test_appender_decimal_wrong_scale() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute(
                "CREATE TABLE decimals (id INT4, a DECIMAL(4,2), b DECIMAL(8,4), c DECIMAL(18,6), d DECIMAL(38,20))");

            assertThrows(() -> {
                try (DuckDBAppender appender = conn.createAppender("decimals")) {
                    appender.append(1).beginRow().append(new BigDecimal("121.14").setScale(2));
                }
            }, SQLException.class);

            assertThrows(() -> {
                try (DuckDBAppender appender = conn.createAppender("decimals")) {
                    appender.beginRow()
                        .append(2)
                        .append(new BigDecimal("21.1").setScale(2))
                        .append(new BigDecimal("12111.1411").setScale(4));
                }
            }, SQLException.class);

            assertThrows(() -> {
                try (DuckDBAppender appender = conn.createAppender("decimals")) {
                    appender.beginRow()
                        .append(3)
                        .append(new BigDecimal("21.1").setScale(2))
                        .append(new BigDecimal("21.1").setScale(4))
                        .append(new BigDecimal("1234567890123.123456").setScale(6));
                }
            }, SQLException.class);
        }
    }

    public static void test_appender_array_basic() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 INTEGER[3])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(41).append(new int[] {41, 42, 43}).endRow();
                appender.beginRow()
                    .append(42)
                    .append(new int[] {44, 45, 46}, new boolean[] {false, true, false})
                    .endRow();
                appender.beginRow().append(43).appendNull().endRow();
                appender.flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = 41")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 41);
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 43);
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 44);
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 0);
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 46);
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col2 FROM tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_array_bool() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;         // auto flush
            int tail = 16;               // flushed on close
            int arrayLen = (1 << 6) + 6; // increase this for stress tests

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 BOOLEAN[" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    boolean[] arr = new boolean[arrayLen];
                    for (byte j = 0; j < arrayLen; j++) {
                        arr[j] = true;
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getBoolean(2), true);
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = " + row)) {
                for (byte j = 0; j < arrayLen; j++) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getBoolean(1), true);
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_nested_array_bool() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;         // auto flush
            int tail = 17;               // flushed on close
            int arrayLen = (1 << 6) + 5; // increase this for stress tests
            int childLen = (1 << 6) + 7;

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 BOOLEAN[" + childLen + "][" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    boolean[][] arr = new boolean[arrayLen][childLen];
                    for (int j = 0; j < arrayLen; j++) {
                        for (int k = 0; k < childLen; k++) {
                            arr[j][k] = 0 == (i + j + k) % 2;
                        }
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "][" + (childLen - 1) +
                                                  "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getBoolean(2), 0 == (i + arrayLen + childLen - 4) % 2);
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery(
                     "SELECT unnest(ucol2) FROM (SELECT unnest(col2) as ucol2 FROM tab1 WHERE col1 = " + row + ")")) {
                for (int j = 0; j < arrayLen; j++) {
                    for (int k = 0; k < childLen; k++) {
                        assertTrue(rs2.next());
                        assertEquals(rs2.getBoolean(1), 0 == (row + j + k) % 2);
                    }
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_array_tinyint() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;         // auto flush
            int tail = 16;               // flushed on close
            int arrayLen = (1 << 6) + 6; // increase this for stress tests

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 TINYINT[" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    byte[] arr = new byte[arrayLen];
                    for (byte j = 0; j < arrayLen; j++) {
                        arr[j] = (byte) ((i % 8) + j);
                    }
                    appender.beginRow().append(i).appendByteArray(arr).endRow();
                }
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getInt(2), (i % 8) + arrayLen - 2);
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = " + row)) {
                for (byte j = 0; j < arrayLen; j++) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getByte(1), (byte) ((row % 8) + j));
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_nested_array_tinyint() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;         // auto flush
            int tail = 17;               // flushed on close
            int arrayLen = (1 << 6) + 5; // increase this for stress tests
            int childLen = (1 << 6) + 7;

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 TINYINT[" + childLen + "][" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    byte[][] arr = new byte[arrayLen][childLen];
                    for (int j = 0; j < arrayLen; j++) {
                        for (int k = 0; k < childLen; k++) {
                            arr[j][k] = (byte) (i + j + k);
                        }
                    }
                    appender.beginRow().append(i).appendByteArray(arr).endRow();
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "][" + (childLen - 1) +
                                                  "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getByte(2), (byte) (i + arrayLen + childLen - 4));
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery(
                     "SELECT unnest(ucol2) FROM (SELECT unnest(col2) as ucol2 FROM tab1 WHERE col1 = " + row + ")")) {
                for (int j = 0; j < arrayLen; j++) {
                    for (int k = 0; k < childLen; k++) {
                        assertTrue(rs2.next());
                        assertEquals(rs2.getByte(1), (byte) (row + j + k));
                    }
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_array_smallint() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;          // auto flush
            int tail = 16;                // flushed on close
            int arrayLen = (1 << 12) + 6; // increase this for stress tests

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 SMALLINT[" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    short[] arr = new short[arrayLen];
                    for (int j = 0; j < arrayLen; j++) {
                        arr[j] = (short) (i + j);
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getShort(2), (short) (i + arrayLen - 2));
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = " + row)) {
                for (int j = 0; j < arrayLen; j++) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getShort(1), (short) (row + j));
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_nested_array_smallint() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;         // auto flush
            int tail = 17;               // flushed on close
            int arrayLen = (1 << 6) + 5; // increase this for stress tests
            int childLen = (1 << 6) + 7;

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 SMALLINT[" + childLen + "][" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    short[][] arr = new short[arrayLen][childLen];
                    for (int j = 0; j < arrayLen; j++) {
                        for (int k = 0; k < childLen; k++) {
                            arr[j][k] = (short) (i + j + k);
                        }
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "][" + (childLen - 1) +
                                                  "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getShort(2), (short) (i + arrayLen + childLen - 4));
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery(
                     "SELECT unnest(ucol2) FROM (SELECT unnest(col2) as ucol2 FROM tab1 WHERE col1 = " + row + ")")) {
                for (int j = 0; j < arrayLen; j++) {
                    for (int k = 0; k < childLen; k++) {
                        assertTrue(rs2.next());
                        assertEquals(rs2.getShort(1), (short) (row + j + k));
                    }
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_array_integer() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;          // auto flush
            int tail = 16;                // flushed on close
            int arrayLen = (1 << 12) + 6; // increase this for stress tests

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 INTEGER[" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    int[] arr = new int[arrayLen];
                    for (int j = 0; j < arrayLen; j++) {
                        arr[j] = i + j;
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getInt(2), i + arrayLen - 2);
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = " + row)) {
                for (int j = 0; j < arrayLen; j++) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getInt(1), row + j);
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_nested_array_basic_integer() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 INTEGER[2][3])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(41).append(new int[][] {{42, 43}, {44, 45}, {46, 47}}).endRow();
                appender.beginRow().append(48).append(new int[][] {{49, 50}, null, {53, 54}}).endRow();
                appender.beginRow()
                    .append(55)
                    .append(new int[][] {{56, 57}, {58, 59}, {60, 61}},
                            new boolean[][] {{false, true}, {false, false}, {true, false}})
                    .endRow();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, unnest(col2) FROM tab1 WHERE col1 = 41")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 41);
                Object[] array1 = (Object[]) rs.getArray(2).getArray();
                assertEquals(array1.length, 2);
                assertEquals(array1[0], 42);
                assertEquals(array1[1], 43);

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 41);
                Object[] array2 = (Object[]) rs.getArray(2).getArray();
                assertEquals(array2.length, 2);
                assertEquals(array2[0], 44);
                assertEquals(array2[1], 45);

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 41);
                Object[] array3 = (Object[]) rs.getArray(2).getArray();
                assertEquals(array3.length, 2);
                assertEquals(array3[0], 46);
                assertEquals(array3[1], 47);

                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, unnest(col2) FROM tab1 WHERE col1 = 48")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 48);
                Object[] array1 = (Object[]) rs.getArray(2).getArray();
                assertEquals(array1.length, 2);
                assertEquals(array1[0], 49);
                assertEquals(array1[1], 50);

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 48);
                assertNull(rs.getObject(2));

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 48);
                Object[] array3 = (Object[]) rs.getArray(2).getArray();
                assertEquals(array3.length, 2);
                assertEquals(array3[0], 53);
                assertEquals(array3[1], 54);

                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, unnest(col2) FROM tab1 WHERE col1 = 55")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 55);
                Object[] array1 = (Object[]) rs.getArray(2).getArray();
                assertEquals(array1.length, 2);
                assertEquals(array1[0], 56);
                assertNull(array1[1]);

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 55);
                Object[] array2 = (Object[]) rs.getArray(2).getArray();
                assertEquals(array2.length, 2);
                assertEquals(array2[0], 58);
                assertEquals(array2[1], 59);

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 55);
                Object[] array3 = (Object[]) rs.getArray(2).getArray();
                assertEquals(array3.length, 2);
                assertNull(array3[0]);
                assertEquals(array3[1], 61);

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_nested_array_integer() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 12;         // auto flush twice
            int tail = 17;               // flushed on close
            int arrayLen = (1 << 6) + 5; // increase this for stress tests
            int childLen = (1 << 6) + 7;

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 INTEGER[" + childLen + "][" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    int[][] arr = new int[arrayLen][childLen];
                    for (int j = 0; j < arrayLen; j++) {
                        for (int k = 0; k < childLen; k++) {
                            arr[j][k] = (i + 1) * (j + 1) * (k + 1);
                        }
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "][" + (childLen - 1) +
                                                  "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getInt(2), (i + 1) * (arrayLen - 1) * (childLen - 1));
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery(
                     "SELECT unnest(ucol2) FROM (SELECT unnest(col2) as ucol2 FROM tab1 WHERE col1 = " + row + ")")) {
                for (int j = 0; j < arrayLen; j++) {
                    for (int k = 0; k < childLen; k++) {
                        assertTrue(rs2.next());
                        assertEquals(rs2.getInt(1), (row + 1) * (j + 1) * (k + 1));
                    }
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_array_bigint() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;          // auto flush
            int tail = 16;                // flushed on close
            int arrayLen = (1 << 12) + 6; // increase this for stress tests

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 BIGINT[" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    long[] arr = new long[arrayLen];
                    for (int j = 0; j < arrayLen; j++) {
                        arr[j] = i + j;
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getLong(2), (long) (i + arrayLen - 2));
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = " + row)) {
                for (int j = 0; j < arrayLen; j++) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getLong(1), (long) (row + j));
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_nested_array_bigint() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;         // auto flush
            int tail = 17;               // flushed on close
            int arrayLen = (1 << 6) + 5; // increase this for stress tests
            int childLen = (1 << 6) + 7;

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 BIGINT[" + childLen + "][" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    long[][] arr = new long[arrayLen][childLen];
                    for (int j = 0; j < arrayLen; j++) {
                        for (int k = 0; k < childLen; k++) {
                            arr[j][k] = (long) (i + 1) * (j + 1) * (k + 1);
                        }
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "][" + (childLen - 1) +
                                                  "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getLong(2), (long) ((i + 1) * (arrayLen - 1) * (childLen - 1)));
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery(
                     "SELECT unnest(ucol2) FROM (SELECT unnest(col2) as ucol2 FROM tab1 WHERE col1 = " + row + ")")) {
                for (int j = 0; j < arrayLen; j++) {
                    for (int k = 0; k < childLen; k++) {
                        assertTrue(rs2.next());
                        assertEquals(rs2.getLong(1), (long) (row + 1) * (j + 1) * (k + 1));
                    }
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_array_float() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;          // auto flush
            int tail = 16;                // flushed on close
            int arrayLen = (1 << 12) + 6; // increase this for stress tests

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 FLOAT[" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    float[] arr = new float[arrayLen];
                    for (int j = 0; j < arrayLen; j++) {
                        arr[j] = (float) (i + j + 0.001);
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getFloat(2), (float) (i + arrayLen - 2 + 0.001));
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = " + row)) {
                for (int j = 0; j < arrayLen; j++) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getFloat(1), (float) (row + j + 0.001));
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_nested_array_float() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 12;         // auto flush twice
            int tail = 17;               // flushed on close
            int arrayLen = (1 << 6) + 5; // increase this for stress tests
            int childLen = (1 << 6) + 7;

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 FLOAT[" + childLen + "][" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    float[][] arr = new float[arrayLen][childLen];
                    for (int j = 0; j < arrayLen; j++) {
                        for (int k = 0; k < childLen; k++) {
                            arr[j][k] = (i + 1) * (j + 1) * (k + 1) + 0.001f;
                        }
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "][" + (childLen - 1) +
                                                  "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getFloat(2), (i + 1) * (arrayLen - 1) * (childLen - 1) + 0.001f);
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery(
                     "SELECT unnest(ucol2) FROM (SELECT unnest(col2) as ucol2 FROM tab1 WHERE col1 = " + row + ")")) {
                for (int j = 0; j < arrayLen; j++) {
                    for (int k = 0; k < childLen; k++) {
                        assertTrue(rs2.next());
                        assertEquals(rs2.getFloat(1), (row + 1) * (j + 1) * (k + 1) + 0.001f);
                    }
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_array_double() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;          // auto flush
            int tail = 16;                // flushed on close
            int arrayLen = (1 << 12) + 6; // increase this for stress tests

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 DOUBLE[" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    double[] arr = new double[arrayLen];
                    for (int j = 0; j < arrayLen; j++) {
                        arr[j] = (i + j + 0.001);
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs =
                     stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getDouble(2), (i + arrayLen - 2 + 0.001));
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = " + row)) {
                for (int j = 0; j < arrayLen; j++) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getDouble(1), (row + j + 0.001));
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_nested_array_double() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 11;         // auto flush
            int tail = 17;               // flushed on close
            int arrayLen = (1 << 6) + 5; // increase this for stress tests
            int childLen = (1 << 6) + 7;

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 DOUBLE[" + childLen + "][" + arrayLen + "])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    double[][] arr = new double[arrayLen][childLen];
                    for (int j = 0; j < arrayLen; j++) {
                        for (int k = 0; k < childLen; k++) {
                            arr[j][k] = (i + 1) * (j + 1) * (k + 1) + 0.001;
                        }
                    }
                    appender.beginRow().append(i).append(arr).endRow();
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "][" + (childLen - 1) +
                                                  "] FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    assertEquals(rs.getDouble(2), (i + 1) * (arrayLen - 1) * (childLen - 1) + 0.001);
                }
                assertFalse(rs.next());
            }

            int row = count - 2;
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery(
                     "SELECT unnest(ucol2) FROM (SELECT unnest(col2) as ucol2 FROM tab1 WHERE col1 = " + row + ")")) {
                for (int j = 0; j < arrayLen; j++) {
                    for (int k = 0; k < childLen; k++) {
                        assertTrue(rs2.next());
                        assertEquals(rs2.getDouble(1), (row + 1) * (j + 1) * (k + 1) + 0.001);
                    }
                }
                assertFalse(rs2.next());
            }
        }
    }

    public static void test_appender_roundtrip_blob() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            SecureRandom random = SecureRandom.getInstanceStrong();
            byte[] data = new byte[512];
            random.nextBytes(data);

            stmt.execute("CREATE TABLE data (a BLOB)");

            try (DuckDBAppender appender = conn.createAppender("data")) {
                appender.beginRow().append(data).endRow().flush();
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());

                Blob resultBlob = results.getBlob(1);
                byte[] resultBytes = resultBlob.getBytes(1, (int) resultBlob.length());
                assertTrue(Arrays.equals(resultBytes, data), "byte[] data is round tripped untouched");
            }
        }
    }

    public static void test_appender_struct_basic() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 STRUCT(s1 INTEGER, s2 VARCHAR))");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .beginStruct()
                    .append(43)
                    .append("foo")
                    .endStruct()
                    .endRow()

                    .beginRow()
                    .append(44)
                    .beginStruct()
                    .append(45)
                    .appendNull()
                    .endStruct()
                    .endRow()

                    .beginRow()
                    .append(46)
                    .appendNull()
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());

                assertEquals(rs.getInt(1), 42);
                DuckDBStruct struct = (DuckDBStruct) rs.getObject(2);
                Map<String, Object> map = struct.getMap();
                assertEquals(map.get("s1"), 43);
                assertEquals(map.get("s2"), "foo");

                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());

                assertEquals(rs.getInt(1), 44);
                DuckDBStruct struct = (DuckDBStruct) rs.getObject(2);
                Map<String, Object> map = struct.getMap();
                assertEquals(map.get("s1"), 45);
                assertNull(map.get("s2"));

                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 WHERE col1 = 46")) {
                assertTrue(rs.next());

                assertEquals(rs.getInt(1), 46);
                assertNull(rs.getObject(2));

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_struct_with_array() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 STRUCT(s1 INTEGER, s2 INTEGER[2]))");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .beginStruct()
                    .append(43)
                    .append(new int[] {44, 45})
                    .endStruct()
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1")) {
                assertTrue(rs.next());

                assertEquals(rs.getInt(1), 42);
                DuckDBStruct struct = (DuckDBStruct) rs.getObject(2);
                Map<String, Object> map = struct.getMap();
                assertEquals(map.get("s1"), 43);
                DuckDBArray arrayWrapper = (DuckDBArray) map.get("s2");
                Object[] array = (Object[]) arrayWrapper.getArray();
                assertEquals(array.length, 2);
                assertEquals(array[0], 44);
                assertEquals(array[1], 45);

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_struct_flush() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 12; // auto flush twice
            int tail = 17;       // flushed on close

            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 STRUCT(s1 INTEGER, s2 INTEGER[2], s3 VARCHAR))");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    appender.beginRow().append(i);
                    appender.beginStruct().append(i + 1);
                    if (0 == i % 7) {
                        appender.append(new int[] {i + 2, i + 3}, new boolean[] {false, true});
                    } else {
                        appender.append(new int[] {i + 2, i + 3});
                    }
                    if (0 == i % 13) {
                        appender.appendNull();
                    } else {
                        appender.append("foo" + i);
                    }
                    appender.endStruct();
                    appender.endRow();
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    DuckDBStruct struct = (DuckDBStruct) rs.getObject(2);
                    Map<String, Object> map = struct.getMap();
                    assertEquals(map.get("s1"), i + 1);
                    DuckDBArray arrayWrapper = (DuckDBArray) map.get("s2");
                    Object[] array = (Object[]) arrayWrapper.getArray();
                    assertEquals(array.length, 2);
                    assertEquals(array[0], i + 2);
                    if (0 == i % 7) {
                        assertNull(array[1]);
                    } else {
                        assertEquals(array[1], i + 3);
                    }
                    if (0 == i % 13) {
                        assertNull(map.get("s3"));
                    } else {
                        assertEquals(map.get("s3"), "foo" + i);
                    }
                }

                assertFalse(rs.next());
            }
        }
    }
}
