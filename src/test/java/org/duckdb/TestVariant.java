package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestVariant {

    public static void test_variant_varchar() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 'foo'::VARCHAR::VARIANT AS col1")) {
            assertTrue(rs.next());
            assertEquals(rs.getMetaData().getColumnType(1), Types.OTHER);
            assertEquals(rs.getObject(1).getClass(), String.class);
            assertEquals(rs.getObject(1), "foo");
            assertFalse(rs.next());
        }
    }

    public static void test_variant_bool() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT TRUE::BOOL::VARIANT AS col1")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), Boolean.class);
            assertEquals(rs.getObject(1), true);
            assertFalse(rs.next());
        }
    }

    public static void test_variant_integrals() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 41::TINYINT::VARIANT AS col1"
                                              + " UNION ALL "
                                              + "SELECT 42::SMALLINT::VARIANT AS col1"
                                              + " UNION ALL "
                                              + "SELECT 43::INTEGER::VARIANT AS col1"
                                              + " UNION ALL "
                                              + "SELECT 44::BIGINT::VARIANT AS col1"
                                              + " UNION ALL "
                                              + "SELECT 45::HUGEINT::VARIANT AS col1")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), Byte.class);
            assertEquals(rs.getObject(1), (byte) 41);
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), Short.class);
            assertEquals(rs.getObject(1), (short) 42);
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), Integer.class);
            assertEquals(rs.getObject(1), 43);
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), Long.class);
            assertEquals(rs.getObject(1), (long) 44);
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), BigInteger.class);
            assertEquals(rs.getObject(1), BigInteger.valueOf(45));
            assertFalse(rs.next());
        }
    }

    public static void test_variant_floats() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 41.1::FLOAT::VARIANT AS col1"
                                              + " UNION ALL "
                                              + "SELECT 42.2::DOUBLE::VARIANT AS col1")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), Float.class);
            assertEquals(rs.getObject(1), (float) 41.1);
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), Double.class);
            assertEquals(rs.getObject(1), 42.2);
            assertFalse(rs.next());
        }
    }

    public static void test_variant_decimals() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 41.1::DECIMAL(8,1)::VARIANT AS col1"
                                              + " UNION ALL "
                                              + "SELECT 42.2::DECIMAL(38,1)::VARIANT AS col1")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), BigDecimal.class);
            assertEquals(rs.getObject(1), BigDecimal.valueOf(41.1));
            //            assertEquals(rs.getMetaData().getPrecision(1), 8);
            //            assertEquals(rs.getMetaData().getScale(1), 1);
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), BigDecimal.class);
            assertEquals(rs.getObject(1), BigDecimal.valueOf(42.2));
            //            assertEquals(rs.getMetaData().getPrecision(1), 38);
            //            assertEquals(rs.getMetaData().getScale(1), 1);
            assertFalse(rs.next());
        }
    }

    public static void test_variant_null() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 'foo'::VARCHAR::VARIANT AS col1"
                                              + " UNION ALL "
                                              + "SELECT NULL::VARIANT AS col1"
                                              + " UNION ALL "
                                              + "SELECT 42::INTEGER::VARIANT AS col1")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), String.class);
            assertEquals(rs.getObject(1), "foo");
            assertTrue(rs.next());
            assertEquals(rs.getObject(1), null);
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), Integer.class);
            assertEquals(rs.getObject(1), 42);
            assertFalse(rs.next());
        }
    }

    public static void test_variant_query_params() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement ps = conn.prepareStatement("SELECT ?::VARCHAR::VARIANT AS col1"
                                                          + " UNION ALL "
                                                          + "SELECT ?::INTEGER::VARIANT AS col1")) {
            ps.setString(1, "foo");
            ps.setInt(2, 42);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getObject(1).getClass(), String.class);
                assertEquals(rs.getObject(1), "foo");
                assertTrue(rs.next());
                assertEquals(rs.getObject(1).getClass(), Integer.class);
                assertEquals(rs.getObject(1), 42);
                assertFalse(rs.next());
            }
        }
    }

    public static void test_variant_columns() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 'foo'::VARCHAR::VARIANT AS col1, 42::INTEGER::VARIANT AS col2")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), String.class);
            assertEquals(rs.getObject(1), "foo");
            assertEquals(rs.getObject(2).getClass(), Integer.class);
            assertEquals(rs.getObject(2), 42);
            assertFalse(rs.next());
        }
    }

    public static void test_variant_array() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT [41, 42, 43]::INTEGER[3]::VARIANT AS col1")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), DuckDBArray.class);
            Array arrayWrapper = (Array) rs.getObject(1);
            Object[] array = (Object[]) arrayWrapper.getArray();
            assertEquals(array.length, 3);
            assertEquals(array[0].getClass(), Integer.class);
            assertEquals(array[0], 41);
            assertEquals(array[1].getClass(), Integer.class);
            assertEquals(array[1], 42);
            assertEquals(array[2].getClass(), Integer.class);
            assertEquals(array[2], 43);
            assertFalse(rs.next());
        }
    }

    public static void test_variant_list() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT [41, 42, 43]::INTEGER[]::VARIANT AS col1")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), DuckDBArray.class);
            Array arrayWrapper = (Array) rs.getObject(1);
            Object[] array = (Object[]) arrayWrapper.getArray();
            assertEquals(array.length, 3);
            assertEquals(array[0].getClass(), Integer.class);
            assertEquals(array[0], 41);
            assertEquals(array[1].getClass(), Integer.class);
            assertEquals(array[1], 42);
            assertEquals(array[2].getClass(), Integer.class);
            assertEquals(array[2], 43);
            assertFalse(rs.next());
        }
    }

    public static void test_variant_list_of_variants() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs =
                 stmt.executeQuery("SELECT [41::VARIANT, NULL::VARIANT, 'foo'::VARIANT]::VARIANT[]::VARIANT AS col1")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), DuckDBArray.class);
            Array arrayWrapper = (Array) rs.getObject(1);
            Object[] array = (Object[]) arrayWrapper.getArray();
            assertEquals(array.length, 3);
            assertEquals(array[0].getClass(), Integer.class);
            assertEquals(array[0], 41);
            assertNull(array[1]);
            assertEquals(array[2].getClass(), String.class);
            assertEquals(array[2], "foo");
            assertFalse(rs.next());
        }
    }

    public static void test_variant_map() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAP {'foo': 41, 'bar': 42}::VARIANT AS col1")) {
            assertTrue(rs.next());
            assertEquals(rs.getObject(1).getClass(), DuckDBArray.class);
            Array arrayWrapper = (Array) rs.getObject(1);
            Object[] array = (Object[]) arrayWrapper.getArray();
            assertEquals(array.length, 2);
            {
                DuckDBStruct struct = (DuckDBStruct) array[0];
                Map<?, ?> map = struct.getMap();
                assertEquals(map.size(), 2);
                assertEquals(map.get("key"), "foo");
                assertEquals(map.get("value"), 41);
            }
            {
                DuckDBStruct struct = (DuckDBStruct) array[1];
                Map<?, ?> map = struct.getMap();
                assertEquals(map.size(), 2);
                assertEquals(map.get("key"), "bar");
                assertEquals(map.get("value"), 42);
            }
            assertFalse(rs.next());
        }
    }

    public static void test_variant_struct() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT {'foo': 41, 'bar': 42}::VARIANT AS col1")) {
            assertTrue(rs.next());
            DuckDBStruct struct = (DuckDBStruct) rs.getObject(1);
            Map<?, ?> map = struct.getMap();
            assertEquals(map.size(), 2);
            assertEquals(map.get("foo"), 41);
            assertEquals(map.get("bar"), 42);
            assertFalse(rs.next());
        }
    }

    public static void test_variant_struct_with_variant() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE tab1 (col1 INTEGER, col2 STRUCT(s1 INTEGER, s2 VARIANT))");
            stmt.execute("INSERT INTO tab1 VALUES(41, row(42, 43))");
            stmt.execute("INSERT INTO tab1 VALUES(44, row(45, 'foo'))");

            try (ResultSet rs = stmt.executeQuery("SELECT col2 FROM tab1 ORDER BY col1")) {
                assertTrue(rs.next());
                {
                    DuckDBStruct struct = (DuckDBStruct) rs.getObject(1);
                    Map<?, ?> map = struct.getMap();
                    assertEquals(map.size(), 2);
                    assertEquals(map.get("s1"), 42);
                    assertEquals(map.get("s2"), 43);
                }
                assertTrue(rs.next());
                {
                    DuckDBStruct struct = (DuckDBStruct) rs.getObject(1);
                    Map<?, ?> map = struct.getMap();
                    assertEquals(map.size(), 2);
                    assertEquals(map.get("s1"), 45);
                    assertEquals(map.get("s2"), "foo");
                }
                assertFalse(rs.next());
            }
        }
    }
}
