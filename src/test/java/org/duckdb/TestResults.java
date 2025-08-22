package org.duckdb;

import static org.duckdb.DuckDBDriver.JDBC_STREAM_RESULTS;
import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;

public class TestResults {

    public static void test_result() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT CAST(42 AS INTEGER) as a, CAST(4.2 AS DOUBLE) as b")) {
                ResultSetMetaData meta = rs.getMetaData();
                assertEquals(meta.getColumnCount(), 2);
                assertEquals(meta.getColumnName(1), "a");
                assertEquals(meta.getColumnName(2), "b");
                assertEquals(meta.getColumnTypeName(1), "INTEGER");
                assertEquals(meta.getColumnTypeName(2), "DOUBLE");

                assertThrows(() -> meta.getColumnName(0), ArrayIndexOutOfBoundsException.class);

                assertThrows(() -> meta.getColumnTypeName(0), ArrayIndexOutOfBoundsException.class);

                assertThrows(() -> meta.getColumnName(3), SQLException.class);

                assertThrows(() -> meta.getColumnTypeName(3), SQLException.class);

                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 42);
                assertEquals(rs.getString(1), "42");
                assertEquals(rs.getDouble(1), 42.0, 0.001);
                assertTrue(rs.getObject(1).equals(42));

                assertEquals(rs.getInt("a"), 42);
                assertEquals(rs.getString("a"), "42");
                assertEquals(rs.getDouble("a"), 42.0, 0.001);
                assertTrue(rs.getObject("a").equals(42));

                assertEquals(rs.getInt(2), 4);
                assertEquals(rs.getString(2), "4.2");
                assertEquals(rs.getDouble(2), 4.2, 0.001);
                assertTrue(rs.getObject(2).equals(4.2));

                assertEquals(rs.getInt("b"), 4);
                assertEquals(rs.getString("b"), "4.2");
                assertEquals(rs.getDouble("b"), 4.2, 0.001);
                assertTrue(rs.getObject("b").equals(4.2));

                assertFalse(rs.next());
            }
            // test duplication
            try (Connection conn2 = conn.unwrap(DuckDBConnection.class).duplicate();
                 Statement stmt2 = conn2.createStatement(); ResultSet rs_conn2 = stmt2.executeQuery("SELECT 42")) {
                rs_conn2.next();
                assertEquals(42, rs_conn2.getInt(1));
            }
        }
    }

    public static void test_empty_table() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE a (i iNTEGER)");
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM a")) {
                assertFalse(rs.next());

                assertEquals(assertThrows(() -> rs.getObject(1), SQLException.class), "No row in context");
            }
        }
    }

    public static void test_broken_next() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE t0(c0 INT8, c1 VARCHAR)");
            stmt.execute(
                "INSERT INTO t0(c1, c0) VALUES (-315929644, 1), (-315929644, -315929644), (-634993846, -1981637379)");
            stmt.execute("INSERT INTO t0(c0, c1) VALUES (-433000283, -433000283)");
            stmt.execute("INSERT INTO t0(c0) VALUES (-995217820)");
            stmt.execute("INSERT INTO t0(c1, c0) VALUES (-315929644, -315929644)");

            try (ResultSet rs = stmt.executeQuery("SELECT c0 FROM t0")) {
                while (rs.next()) {
                    assertTrue(!rs.getObject(1).equals(null));
                }
            }
        }
    }

    public static void test_duckdb_get_object_with_class() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE TABLE b (vchar VARCHAR, bo BOOLEAN, sint SMALLINT, nint INTEGER, bigi BIGINT,"
                + " flt FLOAT, dbl DOUBLE, dte DATE, tme TIME, ts TIMESTAMP, dec16 DECIMAL(3,1),"
                + " dec32 DECIMAL(9,8), dec64 DECIMAL(16,1), dec128 DECIMAL(30,10), tint TINYINT, utint UTINYINT,"
                + " usint USMALLINT, uint UINTEGER, ubig UBIGINT, hin HUGEINT, uhin UHUGEINT, blo BLOB, uid UUID)");
            stmt.execute(
                "INSERT INTO b VALUES ('varchary', true, 6, 42, 666, 42.666, 666.42,"
                +
                " '1970-01-02', '01:00:34', '1970-01-03 03:42:23', 42.2, 1.23456789, 987654321012345.6, 111112222233333.44444, "
                + " -4, 200, 50001, 4000111222, 18446744073709551615, 18446744073709551616, "
                + " 170141183460469231731687303715884105728, 'yeah'::BLOB, "
                + "'5b7bce70-4238-43d1-81d2-6e62f23bf9bd'::UUID)");

            try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM b"); ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(rs.getString(1), rs.getObject(1, String.class));
                assertEquals(rs.getBoolean(2), rs.getObject(2, Boolean.class));
                assertEquals(rs.getShort(3), rs.getObject(3, Short.class));
                assertEquals(rs.getInt(4), rs.getObject(4, Integer.class));
                assertEquals(rs.getLong(5), rs.getObject(5, Long.class));
                assertEquals(rs.getFloat(6), rs.getObject(6, Float.class));
                assertEquals(rs.getDouble(7), rs.getObject(7, Double.class));
                assertEquals(rs.getDate(8), rs.getObject(8, Date.class));
                assertEquals(rs.getTime(9), rs.getObject(9, Time.class));
                assertEquals(rs.getTimestamp(10), rs.getObject(10, Timestamp.class));
                assertEquals(rs.getObject(10, LocalDateTime.class), LocalDateTime.parse("1970-01-03T03:42:23"));
                assertEquals(rs.getObject(10, LocalDateTime.class), LocalDateTime.of(1970, 1, 3, 3, 42, 23));
                assertEquals(rs.getBigDecimal(11), rs.getObject(11, BigDecimal.class));
                assertEquals(rs.getBigDecimal(12), rs.getObject(12, BigDecimal.class));
                assertEquals(rs.getBigDecimal(13), rs.getObject(13, BigDecimal.class));
                assertEquals(rs.getBigDecimal(14), rs.getObject(14, BigDecimal.class));
                assertEquals(rs.getObject(23), rs.getObject(23, UUID.class));
            }
        }

        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT '5b7bce70-4238-43d1-81d2-6e62f23bf9bd'")) {
            rs.next();
            assertEquals(UUID.fromString(rs.getString(1)), rs.getObject(1, UUID.class));
        }
    }

    public static void test_duckdb_get_object_with_class_null() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT NULL")) {
            rs.next();
            assertNull(rs.getObject(1, Integer.class));
        }
    }

    public static void test_throw_wrong_datatype() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE t (id INT, t1 TIMESTAMPTZ, t2 TIMESTAMP)");
            stmt.execute("INSERT INTO t (id, t1, t2) VALUES (1, '2022-01-01T12:11:10+02', '2022-01-01T12:11:10')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
                rs.next();

                assertThrows(() -> rs.getShort(2), IllegalArgumentException.class);
            }
        }
    }

    public static void test_lots_of_big_data() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            int rows = 10000;
            stmt.execute("CREATE TABLE a (i iNTEGER)");
            for (int i = 0; i < rows; i++) {
                stmt.execute("INSERT INTO a VALUES (" + i + ")");
            }

            try (
                ResultSet rs = stmt.executeQuery(
                    "SELECT CAST(i AS SMALLINT), CAST(i AS INTEGER), CAST(i AS BIGINT), CAST(i AS FLOAT), CAST(i AS DOUBLE), CAST(i as STRING), NULL FROM a")) {
                int count = 0;
                while (rs.next()) {
                    for (int col = 1; col <= 6; col++) {
                        assertEquals(rs.getShort(col), (short) count);
                        assertFalse(rs.wasNull());
                        assertEquals(rs.getInt(col), count);
                        assertFalse(rs.wasNull());
                        assertEquals(rs.getLong(col), (long) count);
                        assertFalse(rs.wasNull());
                        assertEquals(rs.getFloat(col), (float) count, 0.001);
                        assertFalse(rs.wasNull());
                        assertEquals(rs.getDouble(col), count, 0.001);
                        assertFalse(rs.wasNull());
                        assertEquals(Double.parseDouble(rs.getString(col)), (double) count, 0.001);
                        assertFalse(rs.wasNull());
                        Object o = rs.getObject(col);
                        assertFalse(rs.wasNull());
                    }
                    short null_short = rs.getShort(7);
                    assertTrue(rs.wasNull());
                    int null_int = rs.getInt(7);
                    assertTrue(rs.wasNull());
                    long null_long = rs.getLong(7);
                    assertTrue(rs.wasNull());
                    float null_float = rs.getFloat(7);
                    assertTrue(rs.wasNull());
                    double null_double = rs.getDouble(7);
                    assertTrue(rs.wasNull());
                    String null_string = rs.getString(7);
                    assertTrue(rs.wasNull());
                    Object null_object = rs.getObject(7);
                    assertTrue(rs.wasNull());

                    count++;
                }

                assertEquals(rows, count);
            }
        }
    }

    // https://github.com/duckdb/duckdb/issues/7218
    public static void test_unknown_result_type() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             PreparedStatement p = connection.prepareStatement(
                 "select generate_series.generate_series from generate_series(?, ?) order by 1")) {
            p.setInt(1, 0);
            p.setInt(2, 1);

            try (ResultSet rs = p.executeQuery()) {
                rs.next();
                assertEquals(rs.getInt(1), 0);
                rs.next();
                assertEquals(rs.getInt(1), 1);
            }
        }
    }

    public static void test_result_streaming() throws Exception {
        Properties props = new Properties();
        props.setProperty(JDBC_STREAM_RESULTS, String.valueOf(true));

        try (Connection conn = DriverManager.getConnection(JDBC_URL, props);
             PreparedStatement stmt1 = conn.prepareStatement("SELECT * FROM range(100000)");
             ResultSet rs = stmt1.executeQuery()) {
            while (rs.next()) {
                rs.getInt(1);
            }
            assertFalse(rs.next()); // is exhausted
        }
    }

    public static void test_stream_multiple_open_results() throws Exception {
        Properties props = new Properties();
        props.setProperty(JDBC_STREAM_RESULTS, String.valueOf(true));

        String QUERY = "SELECT * FROM range(100000)";
        try (Connection conn = DriverManager.getConnection(JDBC_URL, props); Statement stmt1 = conn.createStatement();
             Statement stmt2 = conn.createStatement()) {

            try (ResultSet rs1 = stmt1.executeQuery(QUERY); ResultSet rs2 = stmt2.executeQuery(QUERY)) {
                assertNotNull(rs2);
                assertThrows(rs1::next, SQLException.class);
            }
        }
    }

    public static void test_results_strings_cast() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {

            // BLOB
            try (ResultSet rs = stmt.executeQuery("SELECT 'foo'::BLOB")) {
                rs.next();
                assertEquals(rs.getString(1), "foo");
            }

            // LIST
            try (ResultSet rs = stmt.executeQuery("SELECT ['foo', 'bar', 'baz']")) {
                rs.next();
                assertEquals(rs.getString(1), "[foo, bar, baz]");
            }

            // STRUCT
            try (ResultSet rs = stmt.executeQuery("SELECT struct_pack(s1 := 'foo', s2 := 42)")) {
                rs.next();
                assertEquals(rs.getString(1), "{'s1': foo, 's2': 42}");
            }

            // MAP
            try (ResultSet rs = stmt.executeQuery("SELECT MAP{'foo': 42}")) {
                rs.next();
                assertEquals(rs.getString(1), "{foo=42}");
            }

            // UNION
            try (ResultSet rs = stmt.executeQuery("SELECT union_value(foo := 42)")) {
                rs.next();
                assertEquals(rs.getString(1), "42");
            }

            // ARRAY
            try (ResultSet rs = stmt.executeQuery("SELECT ARRAY['foo', 'bar', 'baz']")) {
                rs.next();
                assertEquals(rs.getString(1), "[foo, bar, baz]");
            }
        }
    }
}
