package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.nCopies;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class TestAppender {

    public static void test_appender_basic() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 VARCHAR)");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(Integer.MAX_VALUE).append("foo").endRow();
                assertEquals(appender.flush(), 1L);
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
                assertEquals(appender.flush(), 2L);
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
                assertEquals(appender.flush(), 1L);
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
                assertEquals(appender.flush(), 2L);
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
                assertEquals(appender.flush(), 2L);
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

    public static void test_appender_uuid() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 UUID)");
            UUID uuid1 = UUID.fromString("777dfbdb-83e7-40f5-ae1b-e12215bdd798");
            UUID uuid2 = UUID.fromString("b8708825-3b58-45a1-9a6e-dab053c3f387");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(1).append(uuid1).endRow();
                appender.beginRow().append(2).append(uuid2).endRow();
                assertEquals(appender.flush(), 2L);
            }

            try (DuckDBResultSet rs =
                     stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1").unwrap(DuckDBResultSet.class)) {
                assertTrue(rs.next());
                assertEquals(rs.getUuid(2), uuid1);
                assertEquals(rs.getObject(2, UUID.class), uuid1);
                assertTrue(rs.next());
                assertEquals(rs.getUuid(2), uuid2);
                assertEquals(rs.getObject(2, UUID.class), uuid2);
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
                assertEquals(appender.flush(), 3L);
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
                assertEquals(appender.flush(), 4L);
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
                assertEquals(appender.flush(), 2L);
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
                assertEquals(appender.flush(), 1L);
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
                assertEquals(appender.flush(), 1L);
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
                assertEquals(appender.flush(), 1L);
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
                int count = 50;
                for (int i = 0; i < count; i++) {
                    appender.beginRow()
                        .append(Long.MAX_VALUE - i)
                        .append(Integer.MAX_VALUE - i)
                        .append((short) (Short.MAX_VALUE - i))
                        .append((byte) (Byte.MAX_VALUE - i))
                        .append((double) i)
                        .append((float) i)
                        .endRow();
                }
                assertEquals(appender.flush(), (long) count);
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
                long count = appender.append(ld4).endRow().flush();
                assertEquals(count, 5L);
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
                assertEquals(appender.flush(), 3L);
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
                long count = appender.beginRow().appendNull().endRow().flush();
                assertEquals(count, 1L);
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
                long count = appender.beginRow()
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
                assertEquals(count, 5L);
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
                    appender.beginRow().append(1).append(new BigDecimal("121.14").setScale(2));
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

    public static void test_appender_roundtrip_blob() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            SecureRandom random = SecureRandom.getInstanceStrong();
            byte[] data = new byte[512];
            random.nextBytes(data);

            stmt.execute("CREATE TABLE data (a BLOB)");

            try (DuckDBAppender appender = conn.createAppender("data")) {
                long count = appender.beginRow().append(data).endRow().flush();
                assertEquals(count, 1L);
            }

            try (ResultSet results = stmt.executeQuery("SELECT * FROM data")) {
                assertTrue(results.next());

                Blob resultBlob = results.getBlob(1);
                byte[] resultBytes = resultBlob.getBytes(1, (int) resultBlob.length());
                assertTrue(Arrays.equals(resultBytes, data), "byte[] data is round tripped untouched");
            }
        }
    }

    public static void test_appender_begin_misuse() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 STRUCT(s1 INTEGER, s2 VARCHAR))");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                assertThrows(() -> { appender.beginRow().beginRow(); }, SQLException.class);
            }
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                assertThrows(() -> { appender.beginStruct().beginStruct(); }, SQLException.class);
            }
        }
    }

    public static void test_appender_incomplete_flush() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 STRUCT(s1 INTEGER, s2 VARCHAR))");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                assertThrows(() -> { appender.beginRow().append(42).flush(); }, SQLException.class);
            }
        }
    }

    public static void test_appender_varchar_as_bytes() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 VARCHAR)");
            String cjkValue = "\u4B54\uD86D\uDF7C\uD83D\uDD25\uD83D\uDE1C";

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(41)
                    .append("foo".getBytes(UTF_8))
                    .endRow()
                    .beginRow()
                    .append(42)
                    .append(cjkValue.getBytes(UTF_8))
                    .endRow();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col2 FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "foo");

                assertTrue(rs.next());
                assertEquals(rs.getString(1), cjkValue);

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_basic_enum() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');");
            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 mood)");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(41).append("sad").endRow();
                appender.beginRow().append(42).append("happy").endRow();
                appender.beginRow().append(43).appendDefault().endRow();
                appender.beginRow().append(44).appendNull().endRow();
                appender.beginRow().append(45).append("ok").endRow();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT CAST(col2 AS VARCHAR) FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "sad");

                assertTrue(rs.next());
                assertEquals(rs.getString(1), "happy");

                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());

                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());

                assertTrue(rs.next());
                assertEquals(rs.getString(1), "ok");

                assertFalse(rs.next());
            }

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                assertThrows(() -> { appender.beginRow().append(44).append("foobar").endRow(); }, SQLException.class);
            }
        }
    }

    public static void test_lots_appender_concurrent_flush() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1 (col1 INTEGER[], col2 VARCHAR[])");
            AtomicBoolean completed = new AtomicBoolean(false);
            AtomicLong concurrentlyFlushed = new AtomicLong(0);

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                AtomicBoolean flushThrown = new AtomicBoolean(false);
                Thread thFail = new Thread(() -> {
                    try {
                        assertThrows(appender::flush, SQLException.class);
                        flushThrown.set(true);
                    } catch (Exception e) {
                        flushThrown.set(false);
                    }
                });
                thFail.start();
                thFail.join();
                assertTrue(flushThrown.get());

                Lock appenderLock = appender.unsafeBreakThreadConfinement();

                Thread th = new Thread(() -> {
                    long count = 0;
                    while (!completed.get()) {
                        appenderLock.lock();
                        try {
                            count += appender.flush();
                        } catch (SQLException e) {
                            // suppress
                        } finally {
                            appenderLock.unlock();
                        }
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    concurrentlyFlushed.set(count);
                });
                th.start();

                for (int i = 0; i < 1 << 18; i++) {
                    int[] arr1 = new int[i % 128];
                    List<String> arr2 = new ArrayList<>();
                    for (int j = 0; j < arr1.length; j++) {
                        arr1[j] = j;
                        arr2.add(String.join("", nCopies(j % 32, String.valueOf(i))));
                    }
                    appenderLock.lock();
                    try {
                        appender.beginRow().append(arr1).append(arr2).endRow();
                    } finally {
                        appenderLock.unlock();
                    }
                }

                completed.set(true);
                th.join();
                assertTrue(concurrentlyFlushed.get() > 0);
            }
        }
    }
}
