package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.sql.*;

public class TestPrepare {

    public static void test_prepare_exception() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            assertThrows(() -> stmt.execute("this is no SQL;"), SQLException.class);
        }
    }

    public static void test_prepare_types() throws Exception {
        try (
            Connection conn = DriverManager.getConnection(JDBC_URL);
            PreparedStatement ps = conn.prepareStatement(
                "SELECT CAST(? AS BOOLEAN) c1, CAST(? AS TINYINT) c2, CAST(? AS SMALLINT) c3, CAST(? AS INTEGER) c4, CAST(? AS BIGINT) c5, CAST(? AS FLOAT) c6, CAST(? AS DOUBLE) c7, CAST(? AS STRING) c8")) {

            ps.setBoolean(1, true);
            ps.setByte(2, (byte) 42);
            ps.setShort(3, (short) 43);
            ps.setInt(4, 44);
            ps.setLong(5, 45);
            ps.setFloat(6, (float) 4.6);
            ps.setDouble(7, 4.7);
            ps.setString(8, "four eight");

            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getBoolean(1), true);
                assertEquals(rs.getByte(2), (byte) 42);
                assertEquals(rs.getShort(3), (short) 43);
                assertEquals(rs.getInt(4), 44);
                assertEquals(rs.getLong(5), (long) 45);
                assertEquals(rs.getFloat(6), 4.6, 0.001);
                assertEquals(rs.getDouble(7), 4.7, 0.001);
                assertEquals(rs.getString(8), "four eight");
            }

            ps.setBoolean(1, false);
            ps.setByte(2, (byte) 82);
            ps.setShort(3, (short) 83);
            ps.setInt(4, 84);
            ps.setLong(5, (long) 85);
            ps.setFloat(6, (float) 8.6);
            ps.setDouble(7, 8.7);
            ps.setString(8, "eight eight\n\t");

            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getBoolean(1), false);
                assertEquals(rs.getByte(2), (byte) 82);
                assertEquals(rs.getShort(3), (short) 83);
                assertEquals(rs.getInt(4), 84);
                assertEquals(rs.getLong(5), (long) 85);
                assertEquals(rs.getFloat(6), 8.6, 0.001);
                assertEquals(rs.getDouble(7), 8.7, 0.001);
                assertEquals(rs.getString(8), "eight eight\n\t");
            }

            ps.setObject(1, false);
            ps.setObject(2, (byte) 82);
            ps.setObject(3, (short) 83);
            ps.setObject(4, 84);
            ps.setObject(5, (long) 85);
            ps.setObject(6, (float) 8.6);
            ps.setObject(7, 8.7);
            ps.setObject(8, "𫝼🔥😜䭔🟢");

            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(rs.getBoolean(1), false);
                assertEquals(rs.getByte(2), (byte) 82);
                assertEquals(rs.getShort(3), (short) 83);
                assertEquals(rs.getInt(4), 84);
                assertEquals(rs.getLong(5), (long) 85);
                assertEquals(rs.getFloat(6), 8.6, 0.001);
                assertEquals(rs.getDouble(7), 8.7, 0.001);
                assertEquals(rs.getString(8), "𫝼🔥😜䭔🟢");

                ps.setNull(1, 0);
                ps.setNull(2, 0);
                ps.setNull(3, 0);
                ps.setNull(4, 0);
                ps.setNull(5, 0);
                ps.setNull(6, 0);
                ps.setNull(7, 0);
                ps.setNull(8, 0);
            }

            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(8, rs.getMetaData().getColumnCount());
                for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                    assertNull(rs.getObject(c));
                    assertTrue(rs.wasNull());
                    assertNull(rs.getString(c));
                    assertTrue(rs.wasNull());
                }
            }
        }
    }

    public static void test_prepare_insert() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("create table ctstable1 (TYPE_ID int, TYPE_DESC varchar(32), primary key(TYPE_ID))");
            }

            try (PreparedStatement pStmt1 = conn.prepareStatement("insert into ctstable1 values(?, ?)")) {
                for (int j = 1; j <= 10; j++) {
                    String sTypeDesc = "Type-" + j;
                    int newType = j;
                    pStmt1.setInt(1, newType);
                    pStmt1.setString(2, sTypeDesc);
                    int count = pStmt1.executeUpdate();
                    assertEquals(count, 1);
                }
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(
                    "create table ctstable2 (KEY_ID int, COF_NAME varchar(32), PRICE float, TYPE_ID int, primary key(KEY_ID) )");
            }

            try (PreparedStatement pStmt = conn.prepareStatement("insert into ctstable2 values(?, ?, ?, ?)")) {
                for (int i = 1; i <= 10; i++) {
                    // Perform the insert(s)
                    int newKey = i;
                    String newName = "xx"
                                     + "-" + i;
                    float newPrice = i + (float) .00;
                    int newType = i % 5;
                    if (newType == 0)
                        newType = 5;
                    pStmt.setInt(1, newKey);
                    pStmt.setString(2, newName);
                    pStmt.setFloat(3, newPrice);
                    pStmt.setInt(4, newType);
                    pStmt.executeUpdate();
                }
            }

            try (Statement stmt = conn.createStatement()) {

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM ctstable1")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), 10);
                }

                stmt.executeUpdate("DELETE FROM ctstable1");

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM ctstable1")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1), 0);
                }
            }
        }
    }

    public static void test_prepared_statement_metadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement stmt = conn.prepareStatement("SELECT 'hello' as world")) {
            ResultSetMetaData metadata = stmt.getMetaData();
            assertEquals(metadata.getColumnCount(), 1);
            assertEquals(metadata.getColumnName(1), "world");
            assertEquals(metadata.getColumnType(1), Types.VARCHAR);
        }
    }

    public static void test_statement_creation_bug1268() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            Statement stmt;

            stmt = conn.createStatement();
            stmt.close();

            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.close();

            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 0);
            stmt.close();

            PreparedStatement pstmt;
            pstmt = conn.prepareStatement("SELECT 42");
            pstmt.close();

            pstmt = conn.prepareStatement("SELECT 42", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            pstmt.close();

            pstmt = conn.prepareStatement("SELECT 42", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 0);
            pstmt.close();
        }
    }

    public static void test_bug4218_prepare_types() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            String query = "SELECT ($1 || $2)";
            conn.prepareStatement(query);
            assertTrue(true);
        }
    }

    public static void test_unbindable_query() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement stmt = conn.prepareStatement("SELECT ?, ?")) {
            stmt.setString(1, "word1");
            stmt.setInt(2, 42);

            ResultSetMetaData meta = stmt.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnName(1), "unknown");
            assertEquals(meta.getColumnTypeName(1), "UNKNOWN");
            assertEquals(meta.getColumnType(1), Types.OTHER);

            try (ResultSet resultSet = stmt.executeQuery()) {
                ResultSetMetaData metadata = resultSet.getMetaData();

                assertEquals(metadata.getColumnCount(), 2);

                assertEquals(metadata.getColumnName(1), "$1");
                assertEquals(metadata.getColumnTypeName(1), "VARCHAR");
                assertEquals(metadata.getColumnType(1), Types.VARCHAR);

                assertEquals(metadata.getColumnName(2), "$2");
                assertEquals(metadata.getColumnTypeName(2), "INTEGER");
                assertEquals(metadata.getColumnType(2), Types.INTEGER);

                resultSet.next();
                assertEquals(resultSet.getString(1), "word1");
                assertEquals(resultSet.getInt(2), 42);
            }
        }
    }

    public static void test_labels_with_prepped_statement() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ? as result")) {
                stmt.setString(1, "Quack");
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        assertEquals(rs.getObject("result"), "Quack");
                    }
                }
            }
        }
    }

    public static void test_execute_updated_on_prep_stmt() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement s = conn.createStatement()) {
            s.executeUpdate("create table t (i int)");

            try (PreparedStatement p = conn.prepareStatement("insert into t (i) select ?")) {
                p.setInt(1, 41);
                assertEquals(p.executeUpdate(), 1);
            }

            try (PreparedStatement p = conn.prepareStatement("insert into t (i) select ?")) {
                p.setInt(1, 42);
                assertEquals(p.executeLargeUpdate(), 1L);
            }
        }
    }

    public static void test_invalid_execute_calls() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement s = conn.createStatement()) {
                s.execute("create table test (id int)");
            }
            try (PreparedStatement s = conn.prepareStatement("select 1")) {
                String msg = assertThrows(s::executeUpdate, SQLException.class);
                assertTrue(msg.contains("can only be used with queries that return nothing") &&
                           msg.contains("or update rows"));
            }
            try (PreparedStatement s = conn.prepareStatement("insert into test values (1)")) {
                String msg = assertThrows(s::executeQuery, SQLException.class);
                assertTrue(msg.contains("can only be used with queries that return a ResultSet"));
            }
        }
    }

    public static void test_update_count() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             Statement s = connection.createStatement()) {
            // check that updateCount does not throw when called
            // before running the query
            assertEquals(s.getUpdateCount(), -1);
            s.execute("create table t (i int)");
            assertEquals(s.getUpdateCount(), -1);
            assertEquals(s.getLargeUpdateCount(), -1L);
            assertEquals(s.executeUpdate("insert into t values (1)"), 1);
            assertFalse(s.execute("insert into t values (1)"));
            assertEquals(s.getUpdateCount(), 1);

            // result is invalidated after a call
            assertEquals(s.getUpdateCount(), -1);
            assertEquals(s.getLargeUpdateCount(), -1L);
        }
    }

    public static void test_execute_autogen_keys() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 INT)");

            // check that the following method do not throw SQLFeatureNotSupportedException
            String sql = "INSERT INTO tab1 VALUES (42)";
            assertFalse(stmt.execute(sql, Statement.NO_GENERATED_KEYS));
            assertFalse(stmt.execute(sql, new int[0]));
            assertFalse(stmt.execute(sql, new String[0]));
            assertEquals(stmt.executeUpdate(sql, Statement.NO_GENERATED_KEYS), 1);
            assertEquals(stmt.executeLargeUpdate(sql, Statement.NO_GENERATED_KEYS), 1L);
            assertEquals(stmt.executeUpdate(sql, new int[0]), 1);
            assertEquals(stmt.executeLargeUpdate(sql, new int[0]), 1L);
            assertEquals(stmt.executeUpdate(sql, new String[0]), 1);
            assertEquals(stmt.executeLargeUpdate(sql, new String[0]), 1L);
        }
    }

    public static void test_max_rows() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             Statement stmt = connection.createStatement()) {
            stmt.setMaxRows(42);
            stmt.setLargeMaxRows(42);
            assertEquals(stmt.getMaxRows(), 0);
            assertEquals(stmt.getLargeMaxRows(), 0L);
        }
    }
}
