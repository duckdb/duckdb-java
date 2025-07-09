package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.assertEquals;
import static org.duckdb.test.Assertions.assertListsEqual;
import static org.duckdb.test.Assertions.assertTrue;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TestSpatial {

    public static void test_spatial_POINT_2D() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial");
            stmt.executeUpdate("LOAD spatial");

            // POINT_2D literal
            try (ResultSet rs = stmt.executeQuery("SELECT ST_Point2D(41.1, 42.2)")) {
                rs.next();
                Object obj = rs.getObject(1);
                DuckDBStruct struct = (DuckDBStruct) obj;
                assertEquals(41.1d, struct.getMap().get("x"));
                assertEquals(42.2d, struct.getMap().get("y"));
            }

            // POINT_2D parameter
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::POINT_2D")) {
                Struct param = conn.createStruct("POINT_2D", new Object[] {41.1d, 42.2d});
                ps.setObject(1, param);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    Object obj = rs.getObject(1);
                    DuckDBStruct struct = (DuckDBStruct) obj;
                    assertEquals(41.1d, struct.getMap().get("x"));
                    assertEquals(42.2d, struct.getMap().get("y"));
                }
            }
        }
    }

    public static void test_spatial_POINT_3D() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial");
            stmt.executeUpdate("LOAD spatial");

            // POINT_3D literal
            try (ResultSet rs = stmt.executeQuery("SELECT ST_Point3D(41.1, 42.2, 43.3)")) {
                rs.next();
                Object obj = rs.getObject(1);
                DuckDBStruct struct = (DuckDBStruct) obj;
                assertEquals(41.1d, struct.getMap().get("x"));
                assertEquals(42.2d, struct.getMap().get("y"));
                assertEquals(43.3d, struct.getMap().get("z"));
            }

            // POINT_3D parameter
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::POINT_3D")) {
                Struct param = conn.createStruct("POINT_3D", new Object[] {41.1d, 42.2d, 43.3d});
                ps.setObject(1, param);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    Object obj = rs.getObject(1);
                    DuckDBStruct struct = (DuckDBStruct) obj;
                    assertEquals(41.1d, struct.getMap().get("x"));
                    assertEquals(42.2d, struct.getMap().get("y"));
                    assertEquals(43.3d, struct.getMap().get("z"));
                }
            }
        }
    }

    public static void test_spatial_POINT_4D() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial");
            stmt.executeUpdate("LOAD spatial");

            // POINT_4D literal
            try (ResultSet rs = stmt.executeQuery("SELECT ST_Point4D(41.1, 42.2, 43.3, 44.4)")) {
                rs.next();
                Object obj = rs.getObject(1);
                DuckDBStruct struct = (DuckDBStruct) obj;
                assertEquals(41.1d, struct.getMap().get("x"));
                assertEquals(42.2d, struct.getMap().get("y"));
                assertEquals(43.3d, struct.getMap().get("z"));
                assertEquals(44.4d, struct.getMap().get("m"));
            }

            // POINT_4D parameter
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::POINT_4D")) {
                Struct param = conn.createStruct("POINT_4D", new Object[] {41.1d, 42.2d, 43.3d, 44.4d});
                ps.setObject(1, param);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    Object obj = rs.getObject(1);
                    DuckDBStruct struct = (DuckDBStruct) obj;
                    assertEquals(41.1d, struct.getMap().get("x"));
                    assertEquals(42.2d, struct.getMap().get("y"));
                    assertEquals(43.3d, struct.getMap().get("z"));
                    assertEquals(44.4d, struct.getMap().get("m"));
                }
            }
        }
    }

    public static void test_spatial_BOX_2D() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial");
            stmt.executeUpdate("LOAD spatial");

            // BOX_2D literal
            try (ResultSet rs = stmt.executeQuery("SELECT ST_Extent(ST_Point(41.1, 42.2))")) {
                rs.next();
                Object obj = rs.getObject(1);
                DuckDBStruct struct = (DuckDBStruct) obj;
                assertEquals(41.1d, struct.getMap().get("min_x"));
                assertEquals(42.2d, struct.getMap().get("min_y"));
                assertEquals(41.1d, struct.getMap().get("max_x"));
                assertEquals(42.2d, struct.getMap().get("max_y"));
            }

            // BOX_2D parameter
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::BOX_2D")) {
                Struct param = conn.createStruct("BOX_2D", new Object[] {41.1d, 42.2d, 43.3d, 44.4d});
                ps.setObject(1, param);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    Object obj = rs.getObject(1);
                    DuckDBStruct struct = (DuckDBStruct) obj;
                    assertEquals(41.1d, struct.getMap().get("min_x"));
                    assertEquals(42.2d, struct.getMap().get("min_y"));
                    assertEquals(43.3d, struct.getMap().get("max_x"));
                    assertEquals(44.4d, struct.getMap().get("max_y"));
                }
            }
        }
    }

    public static void test_spatial_BOX_2DF() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial");
            stmt.executeUpdate("LOAD spatial");

            // BOX_2DF literal
            try (ResultSet rs = stmt.executeQuery(
                     "SELECT STRUCT_PACK(min_x := 41.1, min_y := 42.2, max_x := 43.3, max_y := 44.4)::BOX_2DF")) {
                rs.next();
                Object obj = rs.getObject(1);
                DuckDBStruct struct = (DuckDBStruct) obj;
                assertEquals(41.1f, struct.getMap().get("min_x"));
                assertEquals(42.2f, struct.getMap().get("min_y"));
                assertEquals(43.3f, struct.getMap().get("max_x"));
                assertEquals(44.4f, struct.getMap().get("max_y"));
            }

            // BOX_2DF parameter
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::BOX_2DF")) {
                Struct param = conn.createStruct("BOX_2DF", new Object[] {41.1f, 42.2f, 43.3f, 44.4f});
                ps.setObject(1, param);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    Object obj = rs.getObject(1);
                    DuckDBStruct struct = (DuckDBStruct) obj;
                    assertEquals(41.1f, struct.getMap().get("min_x"));
                    assertEquals(42.2f, struct.getMap().get("min_y"));
                    assertEquals(43.3f, struct.getMap().get("max_x"));
                    assertEquals(44.4f, struct.getMap().get("max_y"));
                }
            }
        }
    }

    public static void test_spatial_LINESTRING_2D() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial");
            stmt.executeUpdate("LOAD spatial");

            // LINESTRING_2D literal
            try (ResultSet rs = stmt.executeQuery("SELECT ARRAY["
                                                  + "STRUCT_PACK(x := 41.1, y := 42.2),"
                                                  + "STRUCT_PACK(x := 43.3, y := 44.4)"
                                                  + "]::LINESTRING_2D")) {
                rs.next();
                Array array = rs.getArray(1);
                Object[] arr = (Object[]) array.getArray();
                DuckDBStruct struct1 = (DuckDBStruct) arr[0];
                assertEquals(41.1d, struct1.getMap().get("x"));
                assertEquals(42.2d, struct1.getMap().get("y"));
                DuckDBStruct struct2 = (DuckDBStruct) arr[1];
                assertEquals(43.3d, struct2.getMap().get("x"));
                assertEquals(44.4d, struct2.getMap().get("y"));
            }

            // LINESTRING_2D parameter
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::LINESTRING_2D")) {
                Struct point1 = conn.createStruct("POINT_2D", new Object[] {41.1d, 42.2d});
                Struct point2 = conn.createStruct("POINT_2D", new Object[] {43.3d, 44.4d});
                Array param = conn.createArrayOf("POINT_2D", new Object[] {point1, point2});
                ps.setObject(1, param);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    Array array = rs.getArray(1);
                    Object[] arr = (Object[]) array.getArray();
                    DuckDBStruct struct1 = (DuckDBStruct) arr[0];
                    assertEquals(41.1d, struct1.getMap().get("x"));
                    assertEquals(42.2d, struct1.getMap().get("y"));
                    DuckDBStruct struct2 = (DuckDBStruct) arr[1];
                    assertEquals(43.3d, struct2.getMap().get("x"));
                    assertEquals(44.4d, struct2.getMap().get("y"));
                }
            }
        }
    }

    public static void test_spatial_POLYGON_2D() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial");
            stmt.executeUpdate("LOAD spatial");

            // POLYGON_2D literal
            try (ResultSet rs = stmt.executeQuery("SELECT ARRAY[ARRAY["
                                                  + "STRUCT_PACK(x := 41.1, y := 42.2),"
                                                  + "STRUCT_PACK(x := 43.3, y := 44.4),"
                                                  + "STRUCT_PACK(x := 45.5, y := 46.6),"
                                                  + "STRUCT_PACK(x := 47.7, y := 48.8)"
                                                  + "]]::POLYGON_2D")) {
                rs.next();
                Array array = rs.getArray(1);
                Object[] arrOuterObj = (Object[]) array.getArray();
                Array arrOuter = (Array) arrOuterObj[0];
                Object[] arr = (Object[]) arrOuter.getArray();
                DuckDBStruct struct1 = (DuckDBStruct) arr[0];
                assertEquals(41.1d, struct1.getMap().get("x"));
                assertEquals(42.2d, struct1.getMap().get("y"));
                DuckDBStruct struct2 = (DuckDBStruct) arr[1];
                assertEquals(43.3d, struct2.getMap().get("x"));
                assertEquals(44.4d, struct2.getMap().get("y"));
                DuckDBStruct struct3 = (DuckDBStruct) arr[2];
                assertEquals(45.5d, struct3.getMap().get("x"));
                assertEquals(46.6d, struct3.getMap().get("y"));
                DuckDBStruct struct4 = (DuckDBStruct) arr[3];
                assertEquals(47.7d, struct4.getMap().get("x"));
                assertEquals(48.8d, struct4.getMap().get("y"));
            }

            // POLYGON_2D parameter
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::POLYGON_2D")) {
                Struct point1 = conn.createStruct("POINT_2D", new Object[] {41.1d, 42.2d});
                Struct point2 = conn.createStruct("POINT_2D", new Object[] {43.3d, 44.4d});
                Struct point3 = conn.createStruct("POINT_2D", new Object[] {45.5d, 46.6d});
                Struct point4 = conn.createStruct("POINT_2D", new Object[] {47.7d, 48.8d});
                Array arrWrapper = conn.createArrayOf("POINT_2D", new Object[] {point1, point2, point3, point4});
                Array param = conn.createArrayOf("POINT_2D[]", new Object[] {arrWrapper});
                ps.setObject(1, param);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    Array array = rs.getArray(1);
                    Object[] arrOuterObj = (Object[]) array.getArray();
                    Array arrOuter = (Array) arrOuterObj[0];
                    Object[] arr = (Object[]) arrOuter.getArray();
                    DuckDBStruct struct1 = (DuckDBStruct) arr[0];
                    assertEquals(41.1d, struct1.getMap().get("x"));
                    assertEquals(42.2d, struct1.getMap().get("y"));
                    DuckDBStruct struct2 = (DuckDBStruct) arr[1];
                    assertEquals(43.3d, struct2.getMap().get("x"));
                    assertEquals(44.4d, struct2.getMap().get("y"));
                    DuckDBStruct struct3 = (DuckDBStruct) arr[2];
                    assertEquals(45.5d, struct3.getMap().get("x"));
                    assertEquals(46.6d, struct3.getMap().get("y"));
                    DuckDBStruct struct4 = (DuckDBStruct) arr[3];
                    assertEquals(47.7d, struct4.getMap().get("x"));
                    assertEquals(48.8d, struct4.getMap().get("y"));
                }
            }
        }
    }

    public static void test_spatial_GEOMETRY() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial");
            stmt.executeUpdate("LOAD spatial");

            byte[] geometryBytes =
                new byte[] {0,   0,   0,   0,   0,   0,    0,  0,  0,    0,    0,    0,    1,    0,  0,  0,
                            -51, -52, -52, -52, -52, -116, 68, 64, -102, -103, -103, -103, -103, 25, 69, 64};

            // GEOMETRY literal
            try (ResultSet rs = stmt.executeQuery("SELECT ST_GeomFromText('POINT(41.1 42.2)')")) {
                rs.next();
                Blob blob = rs.getBlob(1);
                byte[] bytes = blob.getBytes(1, (int) blob.length());
                List<Byte> list = new ArrayList<>();
                for (byte b : bytes) {
                    list.add(b);
                }
                List<Byte> expected = new ArrayList<>();
                for (byte b : geometryBytes) {
                    expected.add(b);
                }
                assertListsEqual(expected, list);
            }

            // GEOMETRY parameter
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?::POINT_2D::GEOMETRY")) {
                Struct param = conn.createStruct("POINT_2D", new Object[] {41.1d, 42.2d});
                ps.setObject(1, param);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());
                    List<Byte> list = new ArrayList<>();
                    for (byte b : bytes) {
                        list.add(b);
                    }
                    List<Byte> expected = new ArrayList<>();
                    for (byte b : geometryBytes) {
                        expected.add(b);
                    }
                    assertListsEqual(expected, list);
                }
            }
        }
    }

    public static void test_spatial_WKB_BLOB() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial");
            stmt.executeUpdate("LOAD spatial");

            byte[] wkbBytes = new byte[] {1,  1,  0,    0,    0,    -51,  -52,  -52, -52, -52, -116,
                                          68, 64, -102, -103, -103, -103, -103, 25,  69,  64};

            // WKB_BLOB literal
            try (ResultSet rs = stmt.executeQuery("SELECT ST_AsWKB(ST_GeomFromText('POINT(41.1 42.2)'))")) {
                rs.next();
                Blob blob = rs.getBlob(1);
                byte[] bytes = blob.getBytes(1, (int) blob.length());
                List<Byte> list = new ArrayList<>();
                for (byte b : bytes) {
                    list.add(b);
                }
                List<Byte> expected = new ArrayList<>();
                for (byte b : wkbBytes) {
                    expected.add(b);
                }
                assertListsEqual(expected, list);
            }

            // WKB_BLOB parameter - not implemented
        }
    }

    public static void test_geometry_deserialisation() throws Exception {
        String QUERY = "select ST_GeomFromGeoJSON('{\"type\": \"Point\", \"coordinates\": [30.0, 10.0]}') as p;";
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial;");
            stmt.executeUpdate("LOAD spatial;");

            try (ResultSet rs = stmt.executeQuery(QUERY)) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "POINT (30 10)");
            }
        }
    }

    public static void test_geometry_array_deserialisation() throws Exception {
        String QUERY =
            "WITH example AS (\n"
            + "  SELECT ST_GEOMFROMTEXT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 2, 2 1))') AS geography)\n"
            + "SELECT\n"
            + "  geography AS original_geography,\n"
            + "  ST_DUMP(geography) AS dumped_geographies\n"
            + "FROM example;";

        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSTALL spatial;");
            stmt.executeUpdate("LOAD spatial;");

            try (ResultSet rs = stmt.executeQuery(QUERY)) {
                assertTrue(rs.next());
                assertEquals(rs.getString(1), "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (1 2, 2 1))");
                assertEquals(rs.getString(2), "[{geom=POINT (0 0), path=[1]}, {geom=LINESTRING (1 2, 2 1), path=[2]}]");
            }
        }
    }
}
