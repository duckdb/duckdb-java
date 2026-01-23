package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Assertions.assertFalse;

import java.sql.*;
import java.time.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

public class TestTimestamp {

    public static void test_timestamp_ms() throws Exception {
        String expectedString = "2022-08-17 12:11:10.999";
        String sql = "SELECT '2022-08-17T12:11:10.999'::TIMESTAMP_MS as ts_ms";
        assert_timestamp_match(sql, expectedString, "TIMESTAMP_MS");
    }

    public static void test_timestamp_ns() throws Exception {
        String expectedString = "2022-08-17 12:11:10.999999999";
        String sql = "SELECT '2022-08-17T12:11:10.999999999'::TIMESTAMP_NS as ts_ns";
        assert_timestamp_match(sql, expectedString, "TIMESTAMP_NS");
    }

    public static void test_timestamp_s() throws Exception {
        String expectedString = "2022-08-17 12:11:10";
        String sql = "SELECT '2022-08-17T12:11:10'::TIMESTAMP_S as ts_s";
        assert_timestamp_match(sql, expectedString, "TIMESTAMP_S");
    }

    private static void assert_timestamp_match(String fetchSql, String expectedString, String expectedTypeName)
        throws Exception {
        String originalTzProperty = System.getProperty("user.timezone");
        TimeZone originalTz = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            System.setProperty("user.timezone", "UTC");
            try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(fetchSql)) {
                assertTrue(rs.next());
                Timestamp actual = rs.getTimestamp(1);

                Timestamp expected = Timestamp.valueOf(expectedString);

                assertEquals(expected.getTime(), actual.getTime());
                assertEquals(expected.getNanos(), actual.getNanos());

                assertEquals(Types.TIMESTAMP, rs.getMetaData().getColumnType(1));
                assertEquals(expectedTypeName, rs.getMetaData().getColumnTypeName(1));
            }
        } finally {
            TimeZone.setDefault(originalTz);
            System.setProperty("user.timezone", originalTzProperty);
        }
    }

    public static void test_timestamp_tz() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE t (id INT, t1 TIMESTAMPTZ)");
            stmt.execute("INSERT INTO t (id, t1) VALUES (1, '2022-01-01T12:11:10+02')");
            stmt.execute("INSERT INTO t (id, t1) VALUES (2, '2022-01-01T12:11:10Z')");

            PreparedStatement ps = conn.prepareStatement("INSERT INTO T (id, t1) VALUES (?, ?)");

            OffsetDateTime odt1 = OffsetDateTime.of(2020, 10, 7, 13, 15, 7, 12345, ZoneOffset.ofHours(7));
            OffsetDateTime odt1Rounded = OffsetDateTime.of(2020, 10, 7, 13, 15, 7, 12000, ZoneOffset.ofHours(7));
            OffsetDateTime odt2 = OffsetDateTime.of(1878, 10, 2, 1, 15, 7, 12345, ZoneOffset.ofHours(-5));
            OffsetDateTime odt2Rounded = OffsetDateTime.of(1878, 10, 2, 1, 15, 7, 13000, ZoneOffset.ofHours(-5));
            OffsetDateTime odt3 = OffsetDateTime.of(2022, 1, 1, 12, 11, 10, 0, ZoneOffset.ofHours(2));
            OffsetDateTime odt4 = OffsetDateTime.of(2022, 1, 1, 12, 11, 10, 0, ZoneOffset.ofHours(0));
            OffsetDateTime odt5 = OffsetDateTime.of(1900, 11, 27, 23, 59, 59, 0, ZoneOffset.ofHours(1));

            ps.setObject(1, 3);
            ps.setObject(2, odt1);
            ps.execute();
            ps.setObject(1, 4);
            ps.setObject(2, odt5, Types.TIMESTAMP_WITH_TIMEZONE);
            ps.execute();
            ps.setObject(1, 5);
            ps.setObject(2, odt2);
            ps.execute();

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM t ORDER BY id")) {
                ResultSetMetaData meta = rs.getMetaData();
                rs.next();
                assertTrue(rs.getObject(2, OffsetDateTime.class).isEqual(odt3));
                rs.next();
                assertEquals(rs.getObject(2, OffsetDateTime.class), odt4);
                rs.next();
                assertTrue(rs.getObject(2, OffsetDateTime.class).isEqual(odt1Rounded));
                rs.next();
                assertTrue(rs.getObject(2, OffsetDateTime.class).isEqual(odt5));
                rs.next();
                assertTrue(rs.getObject(2, OffsetDateTime.class).isEqual(odt2Rounded));
                assertTrue(((OffsetDateTime) rs.getObject(2)).isEqual(odt2Rounded));

                // Metadata tests
                assertEquals(Types.TIMESTAMP_WITH_TIMEZONE,
                             DuckDBResultSetMetaData.type_to_int(DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE));
                assertEquals(OffsetDateTime.class.getName(), meta.getColumnClassName(2));
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_timestamp_as_long() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE t (id INT, t1 TIMESTAMP)");
            stmt.execute("INSERT INTO t (id, t1) VALUES (1, '2022-01-01T12:11:10')");
            stmt.execute("INSERT INTO t (id, t1) VALUES (2, '2022-01-01T12:11:11')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM t ORDER BY id")) {
                rs.next();
                assertEquals(rs.getLong(2), 1641039070000000L);
                rs.next();
                assertEquals(rs.getLong(2), 1641039071000000L);
            }
        }
    }

    public static void test_timestamptz_as_long() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {

            stmt.execute("SET CALENDAR='gregorian'");
            stmt.execute("SET TIMEZONE='America/Los_Angeles'");
            stmt.execute("CREATE TABLE t (id INT, t1 TIMESTAMPTZ)");
            stmt.execute("INSERT INTO t (id, t1) VALUES (1, '2022-01-01T12:11:10Z')");
            stmt.execute("INSERT INTO t (id, t1) VALUES (2, '2022-01-01T12:11:11Z')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM t ORDER BY id")) {
                rs.next();
                assertEquals(rs.getLong(2), 1641039070000000L);
                rs.next();
                assertEquals(rs.getLong(2), 1641039071000000L);
            }
        }
    }

    public static void test_consecutive_timestamps() throws Exception {
        long expected = 986860800000L;
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                     "select range from range(TIMESTAMP '2001-04-10', TIMESTAMP '2001-04-11', INTERVAL 30 MINUTE)")) {
                Calendar cal = GregorianCalendar.getInstance();
                cal.setTimeZone(TimeZone.getTimeZone("UTC"));
                while (rs.next()) {
                    Timestamp actual = rs.getTimestamp(1, cal);
                    assertEquals(expected, actual.getTime());
                    expected += 30 * 60 * 1_000;
                }
            }
        }
    }

    public static void test_timestamp_getters() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone activeTimeZone = TimeZone.getTimeZone("Europe/Sofia");
        TimeZone.setDefault(activeTimeZone);
        try {
            try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement s = conn.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                         "SELECT '2020-01-01 01:23:45.678901 Australia/Darwin'::TIMESTAMP WITH TIME ZONE")) {
                    rs.next();
                    assertEquals("2019-12-31 17:53:45.678901", rs.getTimestamp(1).toString());
                    assertEquals(1577807625678L, rs.getTimestamp(1).getTime());
                    assertEquals("2019-12-31", rs.getDate(1).toString());
                    assertEquals("Tue Dec 31 00:00:00 EET 2019",
                                 new java.util.Date(rs.getDate(1).getTime()).toString());
                    assertEquals("17:53:45", rs.getTime(1).toString());
                    assertEquals("2019-12-31T17:53:45.678901", rs.getTimestamp(1).toLocalDateTime().toString());
                    Calendar cal = GregorianCalendar.getInstance();
                    cal.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                    assertEquals("2019-12-31 10:53:45.678901", rs.getTimestamp(1, cal).toString());
                    assertEquals(1577782425678L, rs.getTimestamp(1, cal).getTime());
                }
                try (ResultSet rs =
                         s.executeQuery("SELECT '2020-01-01 01:23:45.678901'::TIMESTAMP WITHOUT TIME ZONE")) {
                    rs.next();
                    assertEquals("2020-01-01 01:23:45.678901", rs.getTimestamp(1).toString());
                    assertEquals(1577834625678L, rs.getTimestamp(1).getTime());
                    Calendar cal = GregorianCalendar.getInstance();
                    cal.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                    assertEquals("2020-01-01 08:23:45.678901", rs.getTimestamp(1, cal).toString());
                    assertEquals(1577859825678L, rs.getTimestamp(1, cal).getTime());
                }
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_duckdb_timestamp() throws Exception {
        duckdb_timestamp_test();

        // Store default time zone
        TimeZone defaultTZ = TimeZone.getDefault();

        // Test with different time zones
        TimeZone.setDefault(TimeZone.getTimeZone("America/Lima"));
        duckdb_timestamp_test();

        // Test with different time zones
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));
        duckdb_timestamp_test();

        // Restore default time zone
        TimeZone.setDefault(defaultTZ);
    }

    public static void duckdb_timestamp_test() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE a (ts TIMESTAMP)");

                // Generate tests without database
                Timestamp ts0 = Timestamp.valueOf("1970-01-01 00:00:00");
                Timestamp ts1 = Timestamp.valueOf("2021-07-29 21:13:11");
                Timestamp ts2 = Timestamp.valueOf("2021-07-29 21:13:11.123456");
                Timestamp ts3 = Timestamp.valueOf("1921-07-29 21:13:11");
                Timestamp ts4 = Timestamp.valueOf("1921-07-29 21:13:11.123456");

                Timestamp cts0 = new DuckDBTimestamp(ts0).toSqlTimestamp();
                Timestamp cts1 = new DuckDBTimestamp(ts1).toSqlTimestamp();
                Timestamp cts2 = new DuckDBTimestamp(ts2).toSqlTimestamp();
                Timestamp cts3 = new DuckDBTimestamp(ts3).toSqlTimestamp();
                Timestamp cts4 = new DuckDBTimestamp(ts4).toSqlTimestamp();

                assertTrue(ts0.getTime() == cts0.getTime());
                assertTrue(ts0.compareTo(cts0) == 0);
                assertTrue(ts1.getTime() == cts1.getTime());
                assertTrue(ts1.compareTo(cts1) == 0);
                assertTrue(ts2.getTime() == cts2.getTime());
                assertTrue(ts2.compareTo(cts2) == 0);
                assertTrue(ts3.getTime() == cts3.getTime());
                assertTrue(ts3.compareTo(cts3) == 0);
                assertTrue(ts4.getTime() == cts4.getTime());
                assertTrue(ts4.compareTo(cts4) == 0);

                DuckDBTimestamp dts4 = new DuckDBTimestamp(ts1);
                assertTrue(dts4.toSqlTimestamp().compareTo(ts1) == 0);
                DuckDBTimestamp dts5 = new DuckDBTimestamp(ts2);
                assertTrue(dts5.toSqlTimestamp().compareTo(ts2) == 0);

                // Insert and read a timestamp
                stmt.execute("INSERT INTO a (ts) VALUES ('2005-11-02 07:59:58')");
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM a")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject("ts"), Timestamp.valueOf("2005-11-02 07:59:58"));
                    assertEquals(rs.getTimestamp("ts"), Timestamp.valueOf("2005-11-02 07:59:58"));
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(ts) FROM a WHERE ts = ?")) {
                ps.setTimestamp(1, Timestamp.valueOf("2005-11-02 07:59:58"));
                try (ResultSet rs2 = ps.executeQuery()) {
                    assertTrue(rs2.next());
                    assertEquals(rs2.getInt(1), 1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(ts) FROM a WHERE ts = ?")) {
                ps.setObject(1, Timestamp.valueOf("2005-11-02 07:59:58"));
                try (ResultSet rs3 = ps.executeQuery()) {
                    assertTrue(rs3.next());
                    assertEquals(rs3.getInt(1), 1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(ts) FROM a WHERE ts = ?")) {
                ps.setObject(1, Timestamp.valueOf("2005-11-02 07:59:58"), Types.TIMESTAMP);
                ResultSet rs4 = ps.executeQuery();
                assertTrue(rs4.next());
                assertEquals(rs4.getInt(1), 1);
                rs4.close();
            }

            try (Statement stmt2 = conn.createStatement()) {
                stmt2.execute("INSERT INTO a (ts) VALUES ('1905-11-02 07:59:58.12345')");
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(ts) FROM a WHERE ts = ?")) {
                ps.setTimestamp(1, Timestamp.valueOf("1905-11-02 07:59:58.12345"));
                try (ResultSet rs5 = ps.executeQuery()) {
                    assertTrue(rs5.next());
                    assertEquals(rs5.getInt(1), 1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT ts FROM a WHERE ts = ?")) {
                ps.setTimestamp(1, Timestamp.valueOf("1905-11-02 07:59:58.12345"));
                try (ResultSet rs6 = ps.executeQuery()) {
                    assertTrue(rs6.next());
                    assertEquals(rs6.getTimestamp(1), Timestamp.valueOf("1905-11-02 07:59:58.12345"));
                }
            }
        }
    }

    public static void test_duckdb_localdatetime() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE x (ts TIMESTAMP)");

            LocalDateTime ldt = LocalDateTime.of(2021, 1, 18, 21, 20, 7);

            try (PreparedStatement ps1 = conn.prepareStatement("INSERT INTO x VALUES (?)")) {
                ps1.setObject(1, ldt);
                ps1.execute();
            }

            try (PreparedStatement ps2 = conn.prepareStatement("SELECT * FROM x"); ResultSet rs2 = ps2.executeQuery()) {

                rs2.next();
                assertEquals(rs2.getTimestamp(1), rs2.getObject(1, Timestamp.class));
                assertEquals(rs2.getObject(1, LocalDateTime.class), ldt);
            }
        }
    }

    public static void test_duckdb_localdate() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE x (dt Date)");

            LocalDate ld = LocalDate.of(2024, 7, 22);
            Date date = Date.valueOf(ld);

            try (PreparedStatement ps1 = conn.prepareStatement("INSERT INTO x VALUES (?)")) {
                ps1.setObject(1, date);
                ps1.execute();

                ps1.setObject(1, ld);
                ps1.execute();
            }

            try (PreparedStatement ps2 = conn.prepareStatement("SELECT * FROM x"); ResultSet rs2 = ps2.executeQuery()) {
                rs2.next();
                assertEquals(rs2.getDate(1), rs2.getObject(1, Date.class));
                assertEquals(rs2.getObject(1, LocalDate.class), ld);
                assertEquals(rs2.getObject("dt", LocalDate.class), ld);

                rs2.next();
                assertEquals(rs2.getDate(1), rs2.getObject(1, Date.class));
                assertEquals(rs2.getObject(1, LocalDate.class), ld);
                assertEquals(rs2.getObject("dt", LocalDate.class), ld);
            }
        }
    }

    // Longer, resource intensive test - might be commented out for a quick test run
    public static void test_lots_of_timestamps() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            Timestamp ts = Timestamp.valueOf("1970-01-01 01:01:01");

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE a (ts TIMESTAMP)");
                for (long i = 134234533L; i < 13423453300L; i = i + 735127) {
                    ts.setTime(i);
                    stmt.execute("INSERT INTO a (ts) VALUES ('" + ts + "')");
                }
            }

            for (long i = 134234533L; i < 13423453300L; i = i + 735127) {
                try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(ts) FROM a WHERE ts = ?")) {
                    ps.setTimestamp(1, ts);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(rs.getInt(1), 1);
                    }
                }
            }
        }
    }

    public static void test_set_date() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Sofia"));
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement stmt = conn.prepareStatement("SELECT ?::DATE")) {

            try (Statement st = conn.createStatement()) {
                st.execute("SET TimeZone = 'Europe/Sofia'");
            }

            // default time zone
            Date date = Date.valueOf(LocalDate.of(1969, 1, 1));
            stmt.setDate(1, date);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                assertEquals(rs.getDate(1), date);
            }

            // custom time zone
            stmt.clearParameters();
            Calendar cal = new GregorianCalendar();
            cal.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
            Date dateWithCal = Date.valueOf(LocalDate.of(2000, 1, 1));
            stmt.setDate(1, dateWithCal, cal);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                assertEquals(rs.getDate(1), Date.valueOf(LocalDate.of(1999, 12, 31)));
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_set_time() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Sofia"));
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement stmt = conn.prepareStatement("SELECT ?::TIME")) {

            // default time zone
            Time time = Time.valueOf(LocalTime.of(12, 40, 0));
            stmt.setTime(1, time);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                assertEquals(rs.getTime(1), time);
            }

            // custom time zone
            stmt.clearParameters();
            Calendar cal = new GregorianCalendar();
            cal.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
            Time timeWithCal = Time.valueOf(LocalTime.of(0, 0, 0));
            stmt.setTime(1, timeWithCal, cal);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                assertEquals(rs.getTime(1), Time.valueOf(LocalTime.of(14, 0, 0)));
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_set_timestamp() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Sofia"));
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement stmt = conn.prepareStatement("SELECT ?::TIMESTAMP")) {

            // default time zone
            Timestamp timestamp = Timestamp.valueOf(LocalDateTime.of(2021, 7, 16, 12, 34, 45));
            stmt.setTimestamp(1, timestamp);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                assertEquals(rs.getTimestamp(1), timestamp);
            }

            // custom time zone
            stmt.clearParameters();
            Calendar cal = new GregorianCalendar();
            cal.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
            Timestamp timestampWithCal = Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 0, 0));
            stmt.setTimestamp(1, timestampWithCal, cal);
            try (ResultSet rs = stmt.executeQuery()) {
                rs.next();
                assertEquals(rs.getTimestamp(1), Timestamp.valueOf(LocalDateTime.of(1999, 12, 31, 14, 0, 0)));
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_calendar_types() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/Sofia"));
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();

             ResultSet rs = stmt.executeQuery(
                 "SELECT '2019-11-26 21:11:43.123456'::timestamp ts, '2019-11-26'::date dt, '21:11:00'::time te")) {
            Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("America/Los_Angeles"), Locale.US);

            assertTrue(rs.next());
            assertEquals(rs.getTimestamp("ts", cal), Timestamp.valueOf("2019-11-27 07:11:43.123456"));
            assertEquals(rs.getDate("dt", cal), Date.valueOf("2019-11-26"));
            assertEquals(rs.getTime("te", cal), Time.valueOf("21:11:00"));
            assertFalse(rs.next());
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_temporal_nulls() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();

             ResultSet rs = stmt.executeQuery("SELECT NULL::timestamp ts, NULL::date dt, NULL::time te")) {
            assertTrue(rs.next());
            assertNull(rs.getObject("ts"));
            assertNull(rs.getTimestamp("ts"));

            assertNull(rs.getObject("dt"));
            assertNull(rs.getDate("dt"));

            assertNull(rs.getObject("te"));
            assertNull(rs.getTime("te"));

            assertFalse(rs.next());
        }
    }

    public static void test_evil_date() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT '5131-08-05 (BC)'::date d")) {
            assertTrue(rs.next());
            assertEquals(rs.getDate("d"), Date.valueOf(LocalDate.of(-5130, 8, 5)));

            assertFalse(rs.next());
        }
    }

    public static void test_time_tz() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement s = conn.createStatement()) {
            s.execute("set timezone = 'UTC'");
            s.executeUpdate("create table t (i time with time zone)");
            try (ResultSet rs = conn.getMetaData().getColumns(null, "%", "t", "i");) {
                rs.next();

                assertEquals(rs.getString("TYPE_NAME"), "TIME WITH TIME ZONE");
                assertEquals(rs.getInt("DATA_TYPE"), Types.TIME_WITH_TIMEZONE);
            }

            s.execute(
                "INSERT INTO t VALUES ('01:01:00'), ('01:02:03+12:30:45'), ('04:05:06-03:10'), ('07:08:09+15:59:59');");
            try (ResultSet rs = s.executeQuery("SELECT * FROM t")) {
                rs.next();
                assertEquals(rs.getObject(1), OffsetTime.of(LocalTime.of(1, 1), ZoneOffset.UTC));
                rs.next();
                assertEquals(rs.getObject(1),
                             OffsetTime.of(LocalTime.of(1, 2, 3), ZoneOffset.ofHoursMinutesSeconds(12, 30, 45)));
                rs.next();
                assertEquals(rs.getObject(1),
                             OffsetTime.of(LocalTime.of(4, 5, 6), ZoneOffset.ofHoursMinutesSeconds(-3, -10, 0)));
                rs.next();
                assertEquals(rs.getObject(1),
                             OffsetTime.of(LocalTime.of(7, 8, 9), ZoneOffset.ofHoursMinutesSeconds(15, 59, 59)));
            }
        }
    }

    public static void test_time_ns() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement s = conn.createStatement()) {
            s.executeUpdate("create table t (i time_ns)");
            try (ResultSet rs = conn.getMetaData().getColumns(null, "%", "t", "i");) {
                rs.next();

                assertEquals(rs.getString("TYPE_NAME"), "TIME_NS");
                assertEquals(rs.getInt("DATA_TYPE"), Types.OTHER);
            }

            s.execute("INSERT INTO t VALUES ('01:01:00'), ('01:02:03.456789012')");
            try (ResultSet rs = s.executeQuery("SELECT * FROM t")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalTime.class), LocalTime.of(1, 1));
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalTime.class), LocalTime.of(1, 2, 3, 456789012));
                assertFalse(rs.next());
            }
        }
    }

    public static void test_bug532_timestamp() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE t0(c0 DATETIME);");
            stmt.execute("INSERT INTO t0 VALUES(DATE '1-1-1');");
            try (ResultSet rs = stmt.executeQuery("SELECT t0.c0 FROM t0; ")) {
                rs.next();
                rs.getObject(1);
            }
        }
    }

    public static void test_timestamp_before_epoch() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone activeTimeZone = TimeZone.getTimeZone("Europe/Sofia");
        TimeZone.setDefault(activeTimeZone);
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("SELECT TIMESTAMP '1969-01-01 00:00:00.123456'")) {
                rs.next();
                assertEquals(rs.getObject(1, LocalDateTime.class), LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456000));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT TIMESTAMP_NS '1969-01-01 00:00:00.123456789'")) {
                rs.next();
                assertEquals(rs.getObject(1, LocalDateTime.class), LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT TIMESTAMP WITH TIME ZONE '1969-01-01 00:00:00.123456Z'")) {
                rs.next();
                assertEquals(rs.getObject(1, LocalDateTime.class), LocalDateTime.of(1969, 1, 1, 2, 0, 0, 123456000));
            }

        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_timestamp_read_ts_from_date() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT '2020-01-02'::DATE")) {
            assertTrue(rs.next());
            assertEquals(rs.getTimestamp(1).toLocalDateTime().toLocalDate(), LocalDate.of(2020, 1, 2));
            assertFalse(rs.next());
        }
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT NULL::DATE")) {
            assertTrue(rs.next());
            assertNull(rs.getTimestamp(1));
            assertTrue(rs.wasNull());
            assertFalse(rs.next());
        }
    }

    public static void test_timestamp_read_ldt_from_date() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT '2020-01-02'::DATE")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1, LocalDateTime.class).toLocalDate(), LocalDate.of(2020, 1, 2));
            assertFalse(rs.next());
        }
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT NULL::DATE")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1, LocalDateTime.class));
            assertTrue(rs.wasNull());
            assertFalse(rs.next());
        }
    }
}
