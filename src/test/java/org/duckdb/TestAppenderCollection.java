package org.duckdb;

import static java.util.Arrays.asList;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Assertions.assertFalse;
import static org.duckdb.test.Helpers.createMap;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.Date;

public class TestAppenderCollection {
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
                assertEquals(appender.flush(), 3L);
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
        int count = 1 << 11;         // auto flush
        int tail = 16;               // flushed on close
        int arrayLen = (1 << 6) + 6; // increase this for stress tests

        for (String ddl : new String[] {"CREATE TABLE tab1(col1 INT, col2 BOOLEAN[" + arrayLen + "])",
                                        "CREATE TABLE tab1(col1 INT, col2 BOOLEAN[])"}) {
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {

                stmt.execute(ddl);

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
    }

    public static void test_appender_array_tinyint() throws Exception {
        int count = 1 << 11;         // auto flush
        int tail = 16;               // flushed on close
        int arrayLen = (1 << 6) + 6; // increase this for stress tests

        for (String ddl : new String[] {"CREATE TABLE tab1(col1 INT, col2 TINYINT[" + arrayLen + "])",
                                        "CREATE TABLE tab1(col1 INT, col2 TINYINT[])"}) {
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {

                stmt.execute(ddl);

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
    }

    public static void test_appender_array_smallint() throws Exception {
        int count = 1 << 11;          // auto flush
        int tail = 16;                // flushed on close
        int arrayLen = (1 << 12) + 6; // increase this for stress tests

        for (String ddl : new String[] {"CREATE TABLE tab1(col1 INT, col2 SMALLINT[" + arrayLen + "])",
                                        "CREATE TABLE tab1(col1 INT, col2 SMALLINT[])"}) {
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {

                stmt.execute(ddl);

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
    }

    public static void test_appender_array_integer() throws Exception {
        int count = 1 << 11;          // auto flush
        int tail = 16;                // flushed on close
        int arrayLen = (1 << 12) + 6; // increase this for stress tests

        for (String ddl : new String[] {"CREATE TABLE tab1(col1 INT, col2 INTEGER[" + arrayLen + "])",
                                        "CREATE TABLE tab1(col1 INT, col2 INTEGER[])"}) {
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {

                stmt.execute(ddl);

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
    }

    public static void test_appender_array_bigint() throws Exception {
        int count = 1 << 11;          // auto flush
        int tail = 16;                // flushed on close
        int arrayLen = (1 << 12) + 6; // increase this for stress tests

        for (String ddl : new String[] {"CREATE TABLE tab1(col1 INT, col2 BIGINT[" + arrayLen + "])",
                                        "CREATE TABLE tab1(col1 INT, col2 BIGINT[])"}) {
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {

                stmt.execute(ddl);

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
    }

    public static void test_appender_array_float() throws Exception {
        int count = 1 << 11;          // auto flush
        int tail = 16;                // flushed on close
        int arrayLen = (1 << 12) + 6; // increase this for stress tests

        for (String ddl : new String[] {"CREATE TABLE tab1(col1 INT, col2 FLOAT[" + arrayLen + "])",
                                        "CREATE TABLE tab1(col1 INT, col2 FLOAT[])"}) {
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {

                stmt.execute(ddl);

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
    }

    public static void test_appender_array_double() throws Exception {
        int count = 1 << 11;          // auto flush
        int tail = 16;                // flushed on close
        int arrayLen = (1 << 12) + 6; // increase this for stress tests

        for (String ddl : new String[] {"CREATE TABLE tab1(col1 INT, col2 DOUBLE[" + arrayLen + "])",
                                        "CREATE TABLE tab1(col1 INT, col2 DOUBLE[])"}) {
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {

                stmt.execute(ddl);

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
    }

    public static void test_appender_list_basic() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 INTEGER[])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(41)
                    .append(new int[] {42, 43})
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(new int[0])
                    .endRow()
                    .beginRow()
                    .append(45)
                    .appendNull()
                    .endRow()
                    .beginRow()
                    .append(46)
                    .append(new int[] {47, 48, 49}, new boolean[] {false, true, false})
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = 41")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 43);
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT len(col2) FROM tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 0);
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col2 FROM tab1 WHERE col1 = 45")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = 46")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 47);
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 49);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_array_basic_varchar() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 VARCHAR[2])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList("foo", "barbazboo0123456789"))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, "bar"))
                    .endRow()
                    .flush();
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "foo");
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "barbazboo0123456789");
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "bar");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_list_basic_varchar() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 VARCHAR[])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList("foo", "barbazboo0123456789", "bar"))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, "boo"))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "foo");
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "barbazboo0123456789");
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "bar");
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "boo");
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_array_varchar() throws Exception {
        int count = 1 << 11;         // auto flush
        int tail = 16;               // flushed on close
        int arrayLen = (1 << 6) + 7; // increase this for stress tests

        for (String ddl : new String[] {"CREATE TABLE tab1(col1 INT, col2 VARCHAR[" + arrayLen + "])",
                                        "CREATE TABLE tab1(col1 INT, col2 VARCHAR[])"}) {
            for (boolean inlined : new boolean[] {true, false}) {
                try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                     Statement stmt = conn.createStatement()) {

                    stmt.execute(ddl);

                    try (DuckDBAppender appender = conn.createAppender("tab1")) {
                        appender.setWriteInlinedStrings(inlined);
                        for (int i = 0; i < count + tail; i++) {
                            List<String> list = new ArrayList<>();
                            for (int j = 0; j < arrayLen; j++) {
                                list.add(String.valueOf(i + j));
                            }
                            appender.beginRow().append(i).append(list).endRow();
                        }
                    }

                    try (ResultSet rs =
                             stmt.executeQuery("SELECT col1, col2[" + (arrayLen - 1) + "] FROM tab1 ORDER BY col1")) {
                        for (int i = 0; i < count + tail; i++) {
                            assertTrue(rs.next());
                            assertEquals(rs.getInt(1), i);
                            assertEquals(rs.getString(2), String.valueOf(i + arrayLen - 2));
                        }
                        assertFalse(rs.next());
                    }

                    int row = count - 2;
                    try (Statement stmt2 = conn.createStatement();
                         ResultSet rs2 = stmt2.executeQuery("SELECT unnest(col2) FROM tab1 WHERE col1 = " + row)) {
                        for (int j = 0; j < arrayLen; j++) {
                            assertTrue(rs2.next());
                            assertEquals(rs2.getString(1), String.valueOf(row + j));
                        }
                        assertFalse(rs2.next());
                    }
                }
            }
        }
    }

    public static void test_appender_list_basic_uuid() throws Exception {
        UUID uid1 = UUID.fromString("6b9ec4d3-a5e1-45e5-b696-1e599a0e2058");
        UUID uid2 = UUID.fromString("7bf979fc-0c09-4864-bf40-233e7b1fdea1");
        UUID uid3 = UUID.fromString("2f2ac3eb-9105-4864-a2e5-e6d9a7311ba6");
        UUID uid4 = UUID.fromString("3b56451f-691a-4a8d-90ca-1e03479fc38a");
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 UUID[])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(uid1, uid2, uid3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, uid4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), uid1);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), uid2);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), uid3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), uid4);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_list_basic_date() throws Exception {
        LocalDate ld1 = LocalDate.of(2020, 12, 1);
        LocalDate ld2 = LocalDate.of(2020, 12, 2);
        LocalDate ld3 = LocalDate.of(2020, 12, 3);
        LocalDate ld4 = LocalDate.of(2020, 12, 4);
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 DATE[])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(ld1, ld2, ld3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, ld4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), ld1);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), ld2);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), ld3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), ld4);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_list_basic_time() throws Exception {
        LocalTime lt1 = LocalTime.of(23, 59, 1);
        LocalTime lt2 = LocalTime.of(23, 59, 2);
        LocalTime lt3 = LocalTime.of(23, 59, 3);
        LocalTime lt4 = LocalTime.of(23, 59, 4);
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 TIME[])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(lt1, lt2, lt3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, lt4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), lt1);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), lt2);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), lt3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), lt4);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_list_basic_tztime() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone activeTimeZone = TimeZone.getTimeZone("Europe/Sofia");
        TimeZone.setDefault(activeTimeZone);
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            OffsetTime ot1 = LocalTime.of(23, 59, 1).atOffset(ZoneOffset.ofHours(1));
            OffsetTime ot2 = LocalTime.of(23, 59, 2).atOffset(ZoneOffset.ofHours(2));
            OffsetTime ot3 = LocalTime.of(23, 59, 3).atOffset(ZoneOffset.ofHours(3));
            OffsetTime ot4 = LocalTime.of(23, 59, 4).atOffset(ZoneOffset.ofHours(4));

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 TIME WITH TIME ZONE[])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(ot1, ot2, ot3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, ot4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), ot1);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), ot2);
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), ot3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), ot4);
                assertFalse(rs.next());
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_appender_list_basic_local_date_time() throws Exception {
        LocalDateTime ldt1 = LocalDateTime.of(2020, 12, 31, 23, 59, 1);
        LocalDateTime ldt2 = LocalDateTime.of(2020, 12, 31, 23, 59, 2);
        LocalDateTime ldt3 = LocalDateTime.of(2020, 12, 31, 23, 59, 3);
        LocalDateTime ldt4 = LocalDateTime.of(2020, 12, 31, 23, 59, 4);

        for (String ddl : new String[] {"CREATE TABLE tab1(col1 INT, col2 TIMESTAMP[])",
                                        "CREATE TABLE tab1(col1 INT, col2 TIMESTAMP_S[])",
                                        "CREATE TABLE tab1(col1 INT, col2 TIMESTAMP_MS[])",
                                        "CREATE TABLE tab1(col1 INT, col2 TIMESTAMP_NS[])"}) {
            try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                 Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
                try (DuckDBAppender appender = conn.createAppender("tab1")) {
                    appender.beginRow()
                        .append(42)
                        .append(asList(ldt1, ldt2, ldt3))
                        .endRow()
                        .beginRow()
                        .append(43)
                        .append((List<Object>) null)
                        .endRow()
                        .beginRow()
                        .append(44)
                        .append(asList(null, ldt4))
                        .endRow()
                        .flush();
                }

                try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getObject(1, LocalDateTime.class), ldt1);
                    assertTrue(rs.next());
                    assertEquals(rs.getObject(1, LocalDateTime.class), ldt2);
                    assertTrue(rs.next());
                    assertEquals(rs.getObject(1, LocalDateTime.class), ldt3);
                    assertFalse(rs.next());
                }
                try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                    assertTrue(rs.next());
                    assertNull(rs.getObject(1, LocalDateTime.class));
                    assertTrue(rs.wasNull());
                    assertFalse(rs.next());
                }
                try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                    assertTrue(rs.next());
                    assertNull(rs.getObject(1, LocalDateTime.class));
                    assertTrue(rs.wasNull());
                    assertTrue(rs.next());
                    assertEquals(rs.getObject(1, LocalDateTime.class), ldt4);
                    assertFalse(rs.next());
                }
            }
        }
    }

    public static void test_appender_list_basic_local_date_time_date() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        TimeZone activeTimeZone = TimeZone.getTimeZone("Europe/Sofia");
        TimeZone.setDefault(activeTimeZone);
        try {
            java.util.Date dt1 = Date.from(LocalDateTime.of(2020, 12, 31, 23, 59, 1).toInstant(ZoneOffset.UTC));
            java.util.Date dt2 = Date.from(LocalDateTime.of(2020, 12, 31, 23, 59, 2).toInstant(ZoneOffset.UTC));
            java.util.Date dt3 = Date.from(LocalDateTime.of(2020, 12, 31, 23, 59, 3).toInstant(ZoneOffset.UTC));
            java.util.Date dt4 = Date.from(LocalDateTime.of(2020, 12, 31, 23, 59, 4).toInstant(ZoneOffset.UTC));

            for (String ddl : new String[] {"CREATE TABLE tab1(col1 INT, col2 TIMESTAMP[])",
                                            "CREATE TABLE tab1(col1 INT, col2 TIMESTAMP_S[])",
                                            "CREATE TABLE tab1(col1 INT, col2 TIMESTAMP_MS[])",
                                            "CREATE TABLE tab1(col1 INT, col2 TIMESTAMP_NS[])"}) {
                try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute(ddl);
                    try (DuckDBAppender appender = conn.createAppender("tab1")) {
                        appender.beginRow()
                            .append(42)
                            .append(asList(dt1, dt2, dt3))
                            .endRow()
                            .beginRow()
                            .append(43)
                            .append((List<Object>) null)
                            .endRow()
                            .beginRow()
                            .append(44)
                            .append(asList(null, dt4))
                            .endRow()
                            .flush();
                    }

                    try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                        assertTrue(rs.next());
                        assertEquals(Date.from(rs.getObject(1, LocalDateTime.class).toInstant(ZoneOffset.UTC)), dt1);
                        assertTrue(rs.next());
                        assertEquals(Date.from(rs.getObject(1, LocalDateTime.class).toInstant(ZoneOffset.UTC)), dt2);
                        assertTrue(rs.next());
                        assertEquals(Date.from(rs.getObject(1, LocalDateTime.class).toInstant(ZoneOffset.UTC)), dt3);
                        assertFalse(rs.next());
                    }
                    try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                        assertTrue(rs.next());
                        assertNull(rs.getObject(1, LocalDateTime.class));
                        assertTrue(rs.wasNull());
                        assertFalse(rs.next());
                    }
                    try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                        assertTrue(rs.next());
                        assertNull(rs.getObject(1, LocalDateTime.class));
                        assertTrue(rs.wasNull());
                        assertTrue(rs.next());
                        assertEquals(Date.from(rs.getObject(1, LocalDateTime.class).toInstant(ZoneOffset.UTC)), dt4);
                        assertFalse(rs.next());
                    }
                }
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public static void test_appender_list_basic_offset_date_time() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            OffsetDateTime odt1 = LocalDateTime.of(2020, 12, 31, 23, 59, 1).atOffset(ZoneOffset.UTC);
            OffsetDateTime odt2 = LocalDateTime.of(2020, 12, 31, 23, 59, 2).atOffset(ZoneOffset.UTC);
            OffsetDateTime odt3 = LocalDateTime.of(2020, 12, 31, 23, 59, 3).atOffset(ZoneOffset.UTC);
            OffsetDateTime odt4 = LocalDateTime.of(2020, 12, 31, 23, 59, 4).atOffset(ZoneOffset.UTC);

            stmt.execute("CREATE TABLE tab1(col1 INT, col2 TIMESTAMP WITH TIME ZONE[])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(odt1, odt2, odt3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, odt4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalDateTime.class).getSecond(), odt1.getSecond());
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalDateTime.class).getSecond(), odt2.getSecond());
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalDateTime.class).getSecond(), odt3.getSecond());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertEquals(rs.getObject(1, LocalDateTime.class).getSecond(), odt4.getSecond());
                assertFalse(rs.next());
            }
        }
    }

    private static void assertFetchedArrayEquals(Array dba, boolean[] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            assertEquals(objArr[i], arr[i]);
        }
    }

    public static void test_appender_list_basic_array_bool() throws Exception {
        boolean[] arr1 = new boolean[] {true, false, true};
        boolean[] arr2 = new boolean[] {false, true, false};
        boolean[] arr3 = new boolean[] {false, false, true};
        boolean[] arr4 = new boolean[] {true, true, false};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 BOOL[3][])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(arr1, arr2, arr3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, arr4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr1);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr2);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr4);
                assertFalse(rs.next());
            }
        }
    }

    private static void assertFetchedArrayEquals(Array dba, byte[] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            assertEquals(objArr[i], arr[i]);
        }
    }

    public static void test_appender_list_basic_array_tinyint() throws Exception {
        byte[] arr1 = new byte[] {41, 42, 43};
        byte[] arr2 = new byte[] {45, 46, 47};
        byte[] arr3 = new byte[] {48, 49, 50};
        byte[] arr4 = new byte[] {51, 52, 53};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 TINYINT[3][])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(arr1, arr2, arr3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, arr4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr1);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr2);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr4);
                assertFalse(rs.next());
            }
        }
    }

    private static void assertFetchedArrayEquals(Array dba, short[] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            assertEquals(objArr[i], arr[i]);
        }
    }

    public static void test_appender_list_basic_array_smallint() throws Exception {
        short[] arr1 = new short[] {41, 42, 43};
        short[] arr2 = new short[] {45, 46, 47};
        short[] arr3 = new short[] {48, 49, 50};
        short[] arr4 = new short[] {51, 52, 53};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 SMALLINT[3][])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(arr1, arr2, arr3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, arr4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr1);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr2);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr4);
                assertFalse(rs.next());
            }
        }
    }

    private static void assertFetchedArrayEquals(Array dba, int[] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            assertEquals(objArr[i], arr[i]);
        }
    }

    public static void test_appender_list_basic_array_int() throws Exception {
        int[] arr1 = new int[] {41, 42, 43};
        int[] arr2 = new int[] {45, 46, 47};
        int[] arr3 = new int[] {48, 49, 50};
        int[] arr4 = new int[] {51, 52, 53};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 INTEGER[3][])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(arr1, arr2, arr3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, arr4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr1);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr2);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr4);
                assertFalse(rs.next());
            }
        }
    }

    private static void assertFetchedArrayEquals(Array dba, long[] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            assertEquals(objArr[i], arr[i]);
        }
    }

    public static void test_appender_list_basic_array_bigint() throws Exception {
        long[] arr1 = new long[] {41, 42, 43};
        long[] arr2 = new long[] {45, 46, 47};
        long[] arr3 = new long[] {48, 49, 50};
        long[] arr4 = new long[] {51, 52, 53};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 BIGINT[3][])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(arr1, arr2, arr3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, arr4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr1);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr2);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr4);
                assertFalse(rs.next());
            }
        }
    }

    private static void assertFetchedArrayEquals(Array dba, float[] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            assertEquals(objArr[i], arr[i]);
        }
    }

    public static void test_appender_list_basic_array_float() throws Exception {
        float[] arr1 = new float[] {41, 42, 43};
        float[] arr2 = new float[] {45, 46, 47};
        float[] arr3 = new float[] {48, 49, 50};
        float[] arr4 = new float[] {51, 52, 53};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 FLOAT[3][])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(arr1, arr2, arr3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, arr4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr1);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr2);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr4);
                assertFalse(rs.next());
            }
        }
    }

    private static void assertFetchedArrayEquals(Array dba, double[] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            assertEquals(objArr[i], arr[i]);
        }
    }

    public static void test_appender_list_basic_array_double() throws Exception {
        double[] arr1 = new double[] {41, 42, 43};
        double[] arr2 = new double[] {45, 46, 47};
        double[] arr3 = new double[] {48, 49, 50};
        double[] arr4 = new double[] {51, 52, 53};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 DOUBLE[3][])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(arr1, arr2, arr3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, arr4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr1);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr2);
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertFetchedArrayEquals(rs.getArray(1), arr4);
                assertFalse(rs.next());
            }
        }
    }

    private static void assertFetchedListEquals(Array dba, List<?> list) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, list.size());
        for (int i = 0; i < objArr.length; i++) {
            assertEquals(objArr[i], list.get(i));
        }
    }

    public static void test_appender_list_basic_nested_list() throws Exception {
        List<String> list1 = asList("foo1", "bar1", "baz1", "boo1");
        List<String> list2 = asList("foo2", null, "bar2");
        List<String> list3 = new ArrayList<>();
        List<String> list4 = asList("foo4", "bar4");
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 VARCHAR[][])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(list1, list2, list3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, list4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedListEquals(rs.getArray(1), list1);
                assertTrue(rs.next());
                assertFetchedListEquals(rs.getArray(1), list2);
                assertTrue(rs.next());
                assertFetchedListEquals(rs.getArray(1), list3);
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT col2 from tab1 WHERE col1 = 43")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertFalse(rs.next());
            }
            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
                assertTrue(rs.next());
                assertFetchedListEquals(rs.getArray(1), list4);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_list_bigint() throws Exception {
        int count = 1 << 12;        // auto flush twice
        int tail = 7;               // flushed on close
        int listLen = (1 << 6) + 7; // increase this for stress tests

        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INTEGER, col2 BIGINT[])");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    List<Long> list = new ArrayList<>();
                    for (long j = 0; j < Math.min(i, listLen); j++) {
                        if (0 == (i + j) % 13) {
                            list.add(null);
                        } else {
                            list.add(i + j);
                        }
                    }
                    appender.beginRow().append(i).append(list).endRow();
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), count + tail);
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery(
                     "SELECT count(*) FROM (SELECT unnest(col2) FROM tab1 WHERE col1 = " + (listLen - 7) + ")")) {
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), listLen - 7);
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col1, unnest(col2) FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    for (long j = 0; j < Math.min(i, listLen); j++) {
                        assertTrue(rs.next());
                        assertEquals(rs.getInt(1), i);
                        if (0 == (i + j) % 13) {
                            assertNull(rs.getObject(2));
                            assertTrue(rs.wasNull());
                        } else {
                            assertEquals(rs.getLong(2), i + j);
                        }
                    }
                }
            }
        }
    }
}
