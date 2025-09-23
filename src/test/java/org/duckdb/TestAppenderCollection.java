package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Assertions.assertFalse;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

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
}
