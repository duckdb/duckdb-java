package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;
import static org.duckdb.test.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.*;
import java.util.LinkedHashMap;

public class TestParameterMetadata {

    public static void test_parameter_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            for (DuckDBColumnType colType : DuckDBColumnType.values()) {
                final String colTypeName;
                switch (colType) {
                case ENUM:
                case MAP:
                case STRUCT:
                case UNION:
                case UNKNOWN:
                    continue;
                case DECIMAL:
                    colTypeName = "DECIMAL(18,3)";
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    colTypeName = "TIMESTAMP WITH TIME ZONE";
                    break;
                case TIME_WITH_TIME_ZONE:
                    colTypeName = "TIME WITH TIME ZONE";
                    break;
                case LIST:
                case ARRAY:
                    colTypeName = "VARCHAR[]";
                    break;
                default:
                    colTypeName = colType.name();
                }
                String tblName = "metadata_test_" + colType.name();
                stmt.execute("CREATE TABLE " + tblName + " (col1 " + colTypeName + ")");
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tblName + " VALUES(?)")) {
                    ParameterMetaData meta = ps.getParameterMetaData();
                    assertEquals(meta.getParameterTypeName(1), colTypeName);
                }
            }
        }
    }

    public static void test_parameter_metadata_decimal() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE test_meta_decimal (id DECIMAL(3,0), dec16 DECIMAL(4,1), "
                             + "dec32 DECIMAL(9,4), dec64 DECIMAL(18,7), "
                             + "dec128 DECIMAL(38,10), int INTEGER, uint UINTEGER)");
            }
            try (
                PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO test_meta_decimal (id, dec16, dec32, dec64, dec128, int, uint) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                ParameterMetaData meta = ps.getParameterMetaData();
                assertEquals(3, meta.getPrecision(1));
                assertEquals(0, meta.getScale(1));
                assertEquals(4, meta.getPrecision(2));
                assertEquals(1, meta.getScale(2));
                assertEquals(9, meta.getPrecision(3));
                assertEquals(4, meta.getScale(3));
                assertEquals(18, meta.getPrecision(4));
                assertEquals(7, meta.getScale(4));
                assertEquals(38, meta.getPrecision(5));
                assertEquals(10, meta.getScale(5));
                assertEquals(0, meta.getPrecision(6));
                assertEquals(0, meta.getScale(6));
                assertEquals(0, meta.getPrecision(7));
                assertEquals(0, meta.getScale(7));

                assertTrue(meta.isSigned(5));
                assertTrue(meta.isSigned(6));
                assertFalse(meta.isSigned(7));

                assertTrue(BigDecimal.class.getName().equals(meta.getParameterClassName(1)));
                assertTrue(Integer.class.getName().equals(meta.getParameterClassName(6)));
                assertTrue(Long.class.getName().equals(meta.getParameterClassName(7)));
            }
        }
    }

    public static void test_parameter_metadata_struct() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE metadata_test_struct_1 (col1 STRUCT(v VARCHAR, i INTEGER))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO metadata_test_struct_1 VALUES(?)")) {
                ParameterMetaData meta = ps.getParameterMetaData();
                assertEquals(meta.getParameterTypeName(1), "STRUCT(v VARCHAR, i INTEGER)");
                assertEquals(meta.getParameterClassName(1), DuckDBStruct.class.getName());
                assertEquals(meta.getPrecision(1), 0);
                assertEquals(meta.getScale(1), 0);
            }
        }
    }

    public static void test_parameter_metadata_enum() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TYPE metadata_test_enum_type_1 as ENUM('foo', 'bar')");
            stmt.execute("CREATE TABLE metadata_test_enum_1 (col1 metadata_test_enum_type_1)");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO metadata_test_enum_1 VALUES(?)")) {
                ParameterMetaData meta = ps.getParameterMetaData();
                assertEquals(meta.getParameterTypeName(1), "ENUM");
                assertEquals(meta.getParameterClassName(1), String.class.getName());
                assertEquals(meta.getPrecision(1), 0);
                assertEquals(meta.getScale(1), 0);
            }
        }
    }

    public static void test_parameter_metadata_map() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE metadata_test_map_1 (col1 MAP(INTEGER, DOUBLE))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO metadata_test_map_1 VALUES(?)")) {
                ParameterMetaData meta = ps.getParameterMetaData();
                assertEquals(meta.getParameterTypeName(1), "MAP(INTEGER, DOUBLE)");
                assertEquals(meta.getParameterClassName(1), LinkedHashMap.class.getName());
                assertEquals(meta.getPrecision(1), 0);
                assertEquals(meta.getScale(1), 0);
            }
        }
    }

    public static void test_parameter_metadata_union() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE metadata_test_union_1 (col1 UNION(num INTEGER, str VARCHAR))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO metadata_test_union_1 VALUES(?)")) {
                ParameterMetaData meta = ps.getParameterMetaData();
                assertEquals(meta.getParameterTypeName(1), "UNION(num INTEGER, str VARCHAR)");
                assertEquals(meta.getParameterClassName(1), String.class.getName());
                assertEquals(meta.getPrecision(1), 0);
                assertEquals(meta.getScale(1), 0);
            }
        }
    }
}
