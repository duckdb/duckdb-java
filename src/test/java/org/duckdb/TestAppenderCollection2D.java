package org.duckdb;

import static java.util.Arrays.asList;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Assertions.assertFalse;

import java.sql.Array;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class TestAppenderCollection2D {

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

    private static void assertFetchedArrayEquals(Array dba, boolean[][] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            Array wrapper = (Array) objArr[i];
            Object[] objArrInner = (Object[]) wrapper.getArray();
            for (int j = 0; j < objArrInner.length; j++) {
                assertEquals(objArrInner[j], arr[i][j]);
            }
        }
    }

    public static void test_appender_list_basic_array_bool_2d() throws Exception {
        boolean[][] arr1 = new boolean[][] {{true, false, true}, {false, true, false}};
        boolean[][] arr2 = new boolean[][] {{false, true, false}, {true, false, true}};
        boolean[][] arr3 = new boolean[][] {{true, true, false}, {false, true, true}};
        boolean[][] arr4 = new boolean[][] {{false, false, true}, {true, false, false}};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 BOOL[3][2][])");
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

    private static void assertFetchedArrayEquals(Array dba, byte[][] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            Array wrapper = (Array) objArr[i];
            Object[] objArrInner = (Object[]) wrapper.getArray();
            for (int j = 0; j < objArrInner.length; j++) {
                assertEquals(objArrInner[j], arr[i][j]);
            }
        }
    }

    public static void test_appender_list_basic_array_tinyint_2d() throws Exception {
        byte[][] arr1 = new byte[][] {{41, 42, 43}, {44, 45, 46}};
        byte[][] arr2 = new byte[][] {{51, 52, 53}, {54, 55, 56}};
        byte[][] arr3 = new byte[][] {{61, 62, 63}, {64, 65, 66}};
        byte[][] arr4 = new byte[][] {{71, 72, 73}, {74, 75, 76}};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 TINYINT[3][2][])");
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

    private static void assertFetchedArrayEquals(Array dba, short[][] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            Array wrapper = (Array) objArr[i];
            Object[] objArrInner = (Object[]) wrapper.getArray();
            for (int j = 0; j < objArrInner.length; j++) {
                assertEquals(objArrInner[j], arr[i][j]);
            }
        }
    }

    public static void test_appender_list_basic_array_smallint_2d() throws Exception {
        short[][] arr1 = new short[][] {{41, 42, 43}, {44, 45, 46}};
        short[][] arr2 = new short[][] {{51, 52, 53}, {54, 55, 56}};
        short[][] arr3 = new short[][] {{61, 62, 63}, {64, 65, 66}};
        short[][] arr4 = new short[][] {{71, 72, 73}, {74, 75, 76}};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 SMALLINT[3][2][])");
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

    private static void assertFetchedArrayEquals(Array dba, int[][] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            Array wrapper = (Array) objArr[i];
            Object[] objArrInner = (Object[]) wrapper.getArray();
            for (int j = 0; j < objArrInner.length; j++) {
                assertEquals(objArrInner[j], arr[i][j]);
            }
        }
    }

    public static void test_appender_list_basic_array_int_2d() throws Exception {
        int[][] arr1 = new int[][] {{41, 42, 43}, {44, 45, 46}};
        int[][] arr2 = new int[][] {{51, 52, 53}, {54, 55, 56}};
        int[][] arr3 = new int[][] {{61, 62, 63}, {64, 65, 66}};
        int[][] arr4 = new int[][] {{71, 72, 73}, {74, 75, 76}};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 INTEGER[3][2][])");
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

    private static void assertFetchedArrayEquals(Array dba, long[][] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            Array wrapper = (Array) objArr[i];
            Object[] objArrInner = (Object[]) wrapper.getArray();
            for (int j = 0; j < objArrInner.length; j++) {
                assertEquals(objArrInner[j], arr[i][j]);
            }
        }
    }

    public static void test_appender_list_basic_array_bigint_2d() throws Exception {
        long[][] arr1 = new long[][] {{41, 42, 43}, {44, 45, 46}};
        long[][] arr2 = new long[][] {{51, 52, 53}, {54, 55, 56}};
        long[][] arr3 = new long[][] {{61, 62, 63}, {64, 65, 66}};
        long[][] arr4 = new long[][] {{71, 72, 73}, {74, 75, 76}};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 BIGINT[3][2][])");
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

    private static void assertFetchedArrayEquals(Array dba, float[][] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            Array wrapper = (Array) objArr[i];
            Object[] objArrInner = (Object[]) wrapper.getArray();
            for (int j = 0; j < objArrInner.length; j++) {
                assertEquals(objArrInner[j], arr[i][j]);
            }
        }
    }

    public static void test_appender_list_basic_array_float_2d() throws Exception {
        float[][] arr1 = new float[][] {{41.1F, 42.1F, 43.1F}, {44.1F, 45.1F, 46.1F}};
        float[][] arr2 = new float[][] {{51.1F, 52.1F, 53.1F}, {54.1F, 55.1F, 56.1F}};
        float[][] arr3 = new float[][] {{61.1F, 62.1F, 63.1F}, {64.1F, 65.1F, 66.1F}};
        float[][] arr4 = new float[][] {{71.1F, 72.1F, 73.1F}, {74.1F, 75.1F, 76.1F}};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 FLOAT[3][2][])");
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

    private static void assertFetchedArrayEquals(Array dba, double[][] arr) throws Exception {
        Object[] objArr = (Object[]) dba.getArray();
        assertEquals(objArr.length, arr.length);
        for (int i = 0; i < objArr.length; i++) {
            Array wrapper = (Array) objArr[i];
            Object[] objArrInner = (Object[]) wrapper.getArray();
            for (int j = 0; j < objArrInner.length; j++) {
                assertEquals(objArrInner[j], arr[i][j]);
            }
        }
    }

    public static void test_appender_list_basic_array_double_2d() throws Exception {
        double[][] arr1 = new double[][] {{41.1D, 42.1D, 43.1D}, {44.1D, 45.1D, 46.1D}};
        double[][] arr2 = new double[][] {{51.1D, 52.1D, 53.1D}, {54.1D, 55.1D, 56.1D}};
        double[][] arr3 = new double[][] {{61.1D, 62.1D, 63.1D}, {64.1D, 65.1D, 66.1D}};
        double[][] arr4 = new double[][] {{71.1D, 72.1D, 73.1D}, {74.1D, 75.1D, 76.1D}};
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 DOUBLE[3][2][])");
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
}
