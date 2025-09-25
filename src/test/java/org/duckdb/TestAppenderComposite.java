package org.duckdb;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Helpers.createMap;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class TestAppenderComposite {

    public static void test_appender_struct_basic() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 STRUCT(s1 INTEGER, s2 VARCHAR))");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                long count = appender.beginRow()
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
                assertEquals(count, 3L);
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

    public static void test_appender_struct_nested() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE TABLE tab1 (col1 INTEGER, col2 STRUCT(s1 INTEGER, s2 STRUCT(ns1 INTEGER, ns2 VARCHAR)), col3 VARCHAR)");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .beginStruct()
                    .append(43)
                    .beginStruct()
                    .append(44)
                    .append("foo")
                    .endStruct()
                    .endStruct()
                    .append("bar")
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());

                assertEquals(rs.getInt(1), 42);
                DuckDBStruct struct = (DuckDBStruct) rs.getObject(2);
                Map<String, Object> map = struct.getMap();
                assertEquals(map.get("s1"), 43);
                DuckDBStruct nested = (DuckDBStruct) map.get("s2");
                Map<String, Object> nestedMap = nested.getMap();
                assertEquals(nestedMap.get("ns1"), 44);
                assertEquals(nestedMap.get("ns2"), "foo");
                assertEquals(rs.getString(3), "bar");

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_struct_with_array() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 STRUCT(s1 INTEGER, s2 INTEGER[2]))");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                long count = appender.beginRow()
                                 .append(42)
                                 .beginStruct()
                                 .append(43)
                                 .append(new int[] {44, 45})
                                 .endStruct()
                                 .endRow()

                                 .beginRow()
                                 .append(46)
                                 .beginStruct()
                                 .append(47)
                                 .append((int[]) null)
                                 .endStruct()
                                 .endRow()

                                 .beginRow()
                                 .append(48)
                                 .beginStruct()
                                 .append(49)
                                 .append(new int[] {50, 51}, new boolean[] {true, false})
                                 .endStruct()
                                 .endRow()

                                 .flush();
                assertEquals(count, 3L);
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col2 FROM tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                DuckDBStruct struct = (DuckDBStruct) rs.getObject(1);
                Map<String, Object> map = struct.getMap();
                assertEquals(map.get("s1"), 43);
                DuckDBArray arrayWrapper = (DuckDBArray) map.get("s2");
                Object[] array = (Object[]) arrayWrapper.getArray();
                assertEquals(array.length, 2);
                assertEquals(array[0], 44);
                assertEquals(array[1], 45);
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col2 FROM tab1 WHERE col1 = 46")) {
                assertTrue(rs.next());
                DuckDBStruct struct = (DuckDBStruct) rs.getObject(1);
                Map<String, Object> map = struct.getMap();
                assertEquals(map.get("s1"), 47);
                assertNull(map.get("s2"));
                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT col2 FROM tab1 WHERE col1 = 48")) {
                assertTrue(rs.next());
                DuckDBStruct struct = (DuckDBStruct) rs.getObject(1);
                Map<String, Object> map = struct.getMap();
                assertEquals(map.get("s1"), 49);
                DuckDBArray arrayWrapper = (DuckDBArray) map.get("s2");
                Object[] array = (Object[]) arrayWrapper.getArray();
                assertEquals(array.length, 2);
                assertNull(array[0]);
                assertEquals(array[1], 51);
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

    public static void test_appender_struct_incomplete() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 STRUCT(s1 INTEGER, s2 VARCHAR))");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                // underflow
                assertThrows(() -> {
                    appender.beginRow()
                        .append(42)
                        .beginStruct()
                        .append(43)
                        //                            .append("foo")
                        .endStruct();
                }, SQLException.class);
            }
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                // overflow
                assertThrows(() -> {
                    appender.beginRow()
                        .append(42)
                        .beginStruct()
                        .append(43)
                        .append("foo")
                        .append(44) // extra field
                        .endStruct();
                }, SQLException.class);
            }
        }
    }

    public static void test_appender_union_basic() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 UNION(u1 INTEGER, u2 VARCHAR))");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                long count = appender.beginRow()
                                 .append(42)
                                 .beginUnion("u1")
                                 .append(43)
                                 .endUnion()
                                 .endRow()

                                 .beginRow()
                                 .append(44)
                                 .beginUnion("u1")
                                 .appendNull()
                                 .endUnion()
                                 .endRow()

                                 .beginRow()
                                 .append(45)
                                 .beginUnion("u2")
                                 .append("foo")
                                 .endUnion()
                                 .endRow()

                                 .flush();
                assertEquals(count, 3L);
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());

                assertEquals(rs.getInt(1), 42);
                Object obj = rs.getObject(2);
                assertTrue(obj instanceof Integer);
                assertEquals(obj, 43);

                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 WHERE col1 = 44")) {
                assertTrue(rs.next());

                assertEquals(rs.getInt(1), 44);
                Object obj = rs.getObject(2);
                assertNull(obj);
                //                assertTrue(rs.wasNull());

                assertFalse(rs.next());
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 WHERE col1 = 45")) {
                assertTrue(rs.next());

                assertEquals(rs.getInt(1), 45);
                Object obj = rs.getObject(2);
                assertTrue(obj instanceof String);
                assertEquals(obj, "foo");

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_union_flush() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {

            int count = 1 << 12; // auto flush twice
            int tail = 17;       // flushed on close

            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 UNION(u1 INTEGER, u2 INTEGER[2], u3 VARCHAR))");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                for (int i = 0; i < count + tail; i++) {
                    appender.beginRow().append(i);
                    switch (i % 3) {
                    case 0:
                        appender.beginUnion("u1").append(i + 1);
                        break;
                    case 1:
                        appender.beginUnion("u2").append(new int[] {i + 2, i + 3});
                        break;
                    default:
                        appender.beginUnion("u3").append("foo" + i);
                    }
                    appender.endUnion();
                    appender.endRow();
                }
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                for (int i = 0; i < count + tail; i++) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), i);
                    Object obj = rs.getObject(2);
                    switch (i % 3) {
                    case 0:
                        assertTrue(obj instanceof Integer);
                        assertEquals(obj, i + 1);
                        break;
                    case 1:
                        assertTrue(obj instanceof DuckDBArray);
                        DuckDBArray arrayWrapper = (DuckDBArray) obj;
                        Object[] array = (Object[]) arrayWrapper.getArray();
                        assertEquals(array.length, 2);
                        assertEquals(array[0], i + 2);
                        assertEquals(array[1], i + 3);
                        break;
                    default:
                        assertTrue(obj instanceof String);
                        assertEquals(obj, "foo" + i);
                    }
                }

                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_union_nested() throws Exception {
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 ("
                         + "col1 INTEGER, "
                         + "col2 STRUCT( "
                         + "   s1 INTEGER, "
                         + "   s2 UNION("
                         + "       u1 INTEGER,"
                         + "       u2 STRUCT("
                         + "           us1 INTEGER, "
                         + "           us2 INTEGER[2],"
                         + "           us3 VARCHAR"
                         + "       )"
                         + "   )"
                         + "), "
                         + "col3 VARCHAR)");

            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .beginStruct()
                    .append(43)
                    .beginUnion("u1")
                    .append(44)
                    .endUnion()
                    .endStruct()
                    .append("foo")
                    .endRow()

                    .beginRow()
                    .append(45)
                    .beginStruct()
                    .append(46)
                    .beginUnion("u2")
                    .beginStruct()
                    .append(47)
                    .append(new int[] {48, 49})
                    .append("bar")
                    .endStruct()
                    .endUnion()
                    .endStruct()
                    .append("baz")
                    .endRow();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());

                assertEquals(rs.getInt(1), 42);
                DuckDBStruct struct = (DuckDBStruct) rs.getObject(2);
                Map<String, Object> map = struct.getMap();
                assertEquals(map.size(), 2);
                assertEquals(map.get("s1"), 43);
                assertEquals(map.get("s2"), 44);
                assertEquals(rs.getString(3), "foo");

                assertTrue(rs.next());

                assertEquals(rs.getInt(1), 45);
                DuckDBStruct struct1 = (DuckDBStruct) rs.getObject(2);
                Map<String, Object> map1 = struct1.getMap();
                assertEquals(map1.size(), 2);
                assertEquals(map1.get("s1"), 46);
                DuckDBStruct struct2 = (DuckDBStruct) map1.get("s2");
                Map<String, Object> map2 = struct2.getMap();
                assertEquals(map2.size(), 3);
                assertEquals(map2.get("us1"), 47);
                DuckDBArray arrWrapper = (DuckDBArray) map2.get("us2");
                Object[] arr = (Object[]) arrWrapper.getArray();
                assertEquals(arr.length, 2);
                assertEquals(arr[0], 48);
                assertEquals(arr[1], 49);
                assertEquals(map2.get("us3"), "bar");
                assertEquals(rs.getString(3), "baz");

                assertFalse(rs.next());
            }
        }
    }

    private static void assertFetchedStructEquals(Object dbs, Collection<Object> struct) throws Exception {
        DuckDBStruct dbStruct = (DuckDBStruct) dbs;
        Map<String, Object> map = dbStruct.getMap();
        Collection<Object> fetched = map.values();
        assertEquals(fetched.size(), struct.size());
        List<Object> structList = new ArrayList<>(struct);
        int i = 0;
        for (Object f : fetched) {
            assertEquals(f, structList.get(i));
            i++;
        }
    }

    public static void test_appender_list_basic_struct() throws Exception {
        Collection<Object> struct1 = asList(42, "foo");
        Collection<Object> struct2 = asList(null, "bar");
        Collection<Object> struct3 = asList(43, null);
        Collection<Object> struct4 = asList(44, "baz");
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 STRUCT(s1 INT, s2 VARCHAR)[])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(struct1, struct2, struct3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, struct4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedStructEquals(rs.getObject(1), struct1);
                assertTrue(rs.next());
                assertFetchedStructEquals(rs.getObject(1), struct2);
                assertTrue(rs.next());
                assertFetchedStructEquals(rs.getObject(1), struct3);
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
                assertFetchedStructEquals(rs.getObject(1), struct4);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_list_basic_struct_as_map() throws Exception {
        LinkedHashMap<Object, Object> struct1 = createMap("key1", 42, "key2", "foo");
        LinkedHashMap<Object, Object> struct2 = createMap("key1", null, "key2", "bar");
        LinkedHashMap<Object, Object> struct3 = createMap("key1", 43, "key2", null);
        LinkedHashMap<Object, Object> struct4 = createMap("key1", 44, "key2", "baz");
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 STRUCT(s1 INT, s2 VARCHAR)[])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(struct1, struct2, struct3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, struct4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedStructEquals(rs.getObject(1), struct1.values());
                assertTrue(rs.next());
                assertFetchedStructEquals(rs.getObject(1), struct2.values());
                assertTrue(rs.next());
                assertFetchedStructEquals(rs.getObject(1), struct3.values());
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
                assertFetchedStructEquals(rs.getObject(1), struct4.values());
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_list_basic_struct_with_primitives() throws Exception {
        Collection<Object> struct1 = asList(true, (byte) 42, (short) 43, 44, 45L, BigInteger.valueOf(46), 47.1F, 48.1D,
                                            BigDecimal.valueOf(49.123));
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 STRUCT("
                         + "s1 BOOL,"
                         + "s2 TINYINT,"
                         + "s3 SMALLINT,"
                         + "s4 INTEGER,"
                         + "s5 BIGINT,"
                         + "s6 HUGEINT,"
                         + "s7 FLOAT,"
                         + "s8 DOUBLE,"
                         + "s9 DECIMAL"
                         + ")[])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow().append(42).append(singletonList(struct1)).endRow().flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertFetchedStructEquals(rs.getObject(1), struct1);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_appender_list_basic_union() throws Exception {
        Map.Entry<String, Object> union1 = new AbstractMap.SimpleEntry<>("u1", 42);
        Map.Entry<String, Object> union2 = new AbstractMap.SimpleEntry<>("u2", "foo");
        Map.Entry<String, Object> union3 = new AbstractMap.SimpleEntry<>("u1", null);
        Map.Entry<String, Object> union4 = new AbstractMap.SimpleEntry<>("u2", "bar");
        try (DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 UNION(u1 INT, u2 VARCHAR)[])");
            try (DuckDBAppender appender = conn.createAppender("tab1")) {
                appender.beginRow()
                    .append(42)
                    .append(asList(union1, union2, union3))
                    .endRow()
                    .beginRow()
                    .append(43)
                    .append((List<Object>) null)
                    .endRow()
                    .beginRow()
                    .append(44)
                    .append(asList(null, union4))
                    .endRow()
                    .flush();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT unnest(col2) from tab1 WHERE col1 = 42")) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), union1.getValue());
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), union2.getValue());
                assertTrue(rs.next());
                assertEquals(rs.getObject(1), union3.getValue());
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
                assertEquals(rs.getObject(1), union4.getValue());
                assertFalse(rs.next());
            }
        }
    }
}
