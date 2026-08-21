package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.sql.*;
import org.duckdb.user.DuckDBUserArray;

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

    private static void test_prepare_statement_unsupported_types() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            conn.prepareStatement("SELECT 42", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).close();
            assertThrows(
                ()
                    -> conn.prepareStatement("SELECT 42", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY),
                SQLException.class);
            assertThrows(
                ()
                    -> conn.prepareStatement("SELECT 42", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE),
                SQLException.class);
        }
        try (Connection conn = DriverManager.getConnection(JDBC_URL + ";access_mode=READ_ONLY")) {
            conn.prepareStatement("SELECT 42", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE).close();
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

    public static void test_prepare_autogen_keys() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab2 (col1 INT)");

            String sql = "INSERT INTO tab2 VALUES (42)";

            // NO_GENERATED_KEYS should transparently fall through to the plain overload
            try (PreparedStatement ps = conn.prepareStatement(sql, Statement.NO_GENERATED_KEYS)) {
                assertEquals(ps.executeUpdate(), 1);
                assertEquals(ps.executeLargeUpdate(), 1L);
            }

            // RETURN_GENERATED_KEYS on a table without generated columns yields an empty result set
            try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                assertEquals(ps.executeUpdate(), 1);
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertFalse(keys.next());
                }
            }
        }
    }

    public static void test_prepare_return_generated_keys() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            // A table with a sequence-backed auto-increment identifier
            stmt.execute(
                "CREATE SEQUENCE seq_return_tab START 5");
            stmt.execute(
                "CREATE TABLE return_tab (id BIGINT DEFAULT nextval('seq_return_tab') PRIMARY KEY, name VARCHAR)");

            String sql = "INSERT INTO return_tab (name) VALUES ('duck')";

            // prepareStatement(sql, RETURN_GENERATED_KEYS) + executeUpdate + getGeneratedKeys
            try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                assertEquals(ps.executeUpdate(), 1);
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 5L);
                    assertFalse(keys.next());
                }
            }

            // execute(sql, RETURN_GENERATED_KEYS)
            try (
                Statement s = conn.createStatement()) {
                assertTrue(s.execute("INSERT INTO return_tab (name) VALUES ('goose')", Statement.RETURN_GENERATED_KEYS));
                try (ResultSet keys = s.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 6L);
                }
            }
        }
    }

    public static void test_prepare_return_generated_keys_cols() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE seq_generated_cols START WITH 10");
            stmt.execute(
                "CREATE TABLE return_cols (id BIGINT DEFAULT nextval('seq_generated_cols'), name VARCHAR, val INT)");

            String sql = "INSERT INTO return_cols (name, val) VALUES ('duck', 42)";

            // column names overload
            try (PreparedStatement ps = conn.prepareStatement(sql, new String[] {"id"})) {
                assertEquals(ps.executeUpdate(), 1);
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 10L);
                    assertFalse(keys.next());
                }
            }

            // column indexes overload (id is column 1)
            try (PreparedStatement ps = conn.prepareStatement(sql, new int[] {1})) {
                assertEquals(ps.executeUpdate(), 1);
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 11L);
                    assertFalse(keys.next());
                }
            }
        }
    }

    public static void test_execute_update_generated_keys() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE seq_update_gen START WITH 1");
            stmt.execute(
                "CREATE TABLE update_gen (id BIGINT DEFAULT nextval('seq_update_gen'), payload VARCHAR)");

            String sql = "INSERT INTO update_gen (payload) VALUES ('x')";

            // executeLargeUpdate(sql, RETURN_GENERATED_KEYS)
            try (Statement s = conn.createStatement()) {
                assertEquals(s.executeLargeUpdate(sql, Statement.RETURN_GENERATED_KEYS), 1L);
                try (ResultSet keys = s.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 1L);
                }
            }

            // executeUpdate(sql, int[]) and executeUpdate(sql, String[])
            try (Statement s = conn.createStatement()) {
                assertEquals(s.executeUpdate(sql, new int[] {1}), 1);
                try (ResultSet keys = s.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 2L);
                }
                assertEquals(s.executeUpdate(sql, new String[] {"id"}), 1);
                try (ResultSet keys = s.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 3L);
                }
            }
        }
    }

    public static void test_generated_keys_multiple_rows() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE seq_multiple START WITH 1");
            stmt.execute("CREATE TABLE multiple_gen (id BIGINT DEFAULT nextval('seq_multiple'), payload VARCHAR)");

            String sql = "INSERT INTO multiple_gen (payload) SELECT 'a' UNION ALL SELECT 'b' UNION ALL SELECT 'c'";

            try (Statement s = conn.createStatement()) {
                assertEquals(s.executeLargeUpdate(sql, Statement.RETURN_GENERATED_KEYS), 3L);
                try (ResultSet keys = s.getGeneratedKeys()) {
                    for (long i = 1; i <= 3; i++) {
                        assertTrue(keys.next());
                        assertEquals(keys.getLong(1), i);
                    }
                    assertFalse(keys.next());
                }
            }
        }
    }

    public static void test_generated_keys_update_and_delete() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE upd_del (id BIGINT, payload VARCHAR, flag INT)");
            stmt.execute("INSERT INTO upd_del VALUES (1, 'a', 0), (2, 'b', 0), (3, 'c', 1)");

            // UPDATE ... RETURNING id uses an explicit column selector
            try (Statement s = conn.createStatement()) {
                String sql = "UPDATE upd_del SET flag = 1 WHERE id = 1";
                assertEquals(s.executeUpdate(sql, new String[] {"id"}), 1);
                try (ResultSet keys = s.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 1L);
                    assertFalse(keys.next());
                }
            }

            // DELETE ... RETURNING id uses an explicit column selector
            try (Statement s = conn.createStatement()) {
                String sql = "DELETE FROM upd_del WHERE id = 2";
                assertEquals(s.executeLargeUpdate(sql, new int[] {1}), 1L);
                try (ResultSet keys = s.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 2L);
                    assertFalse(keys.next());
                }
            }
        }
    }

    public static void test_generated_keys_non_dml() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            // SELECT with RETURN_GENERATED_KEYS must not be rewritten and must yield empty keys
            assertTrue(stmt.execute("SELECT 1", Statement.RETURN_GENERATED_KEYS));
            try (ResultSet keys = stmt.getGeneratedKeys()) {
                assertFalse(keys.next());
            }

            // DDL with RETURN_GENERATED_KEYS must not fail and must yield empty keys
            stmt.execute("CREATE TABLE non_dml (id INT)", Statement.RETURN_GENERATED_KEYS);
            try (ResultSet keys = stmt.getGeneratedKeys()) {
                assertFalse(keys.next());
            }
        }
    }

    public static void test_generated_keys_schema_qualified() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE seq_schema START WITH 7");
            stmt.execute("CREATE TABLE \"QualifiedTable\" (\"Id\" BIGINT DEFAULT nextval('seq_schema'), name VARCHAR)");

            String sql = "INSERT INTO main.\"QualifiedTable\" (name) VALUES ('duck')";

            // auto-detect on a schema-qualified, quoted identifier
            try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                assertEquals(ps.executeUpdate(), 1);
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 7L);
                    assertFalse(keys.next());
                }
            }

            // quoted column name selector
            try (PreparedStatement ps = conn.prepareStatement(sql, new String[] {"id"})) {
                assertEquals(ps.executeUpdate(), 1);
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getLong(1), 8L);
                    assertFalse(keys.next());
                }
            }
        }
    }

    public static void test_generated_keys_int_array_multi() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE multi_idx (a INT, b INT, c INT)");

            String sql = "INSERT INTO multi_idx VALUES (1, 2, 3)";

            // two columns, plus an out-of-range index that should be skipped
            try (PreparedStatement ps = conn.prepareStatement(sql, new int[] {2, 99, 1})) {
                assertEquals(ps.executeUpdate(), 1);
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getInt(1), 2);
                    assertEquals(keys.getInt(2), 1);
                    assertFalse(keys.next());
                }
            }
        }
    }

    public static void test_generated_keys_string_array_missing() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE missing_str (id INT, name VARCHAR)");

            String sql = "INSERT INTO missing_str VALUES (1, 'x')";

            // requesting a non-existent column from RETURNING is rejected by the engine during prepare
            assertThrows(
                () -> conn.prepareStatement(sql, new String[] {"does_not_exist"}), SQLException.class);
        }
    }

    public static void test_generated_keys_before_execute() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE before_exec (id INT)");

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO before_exec VALUES (1)")) {
                // getGeneratedKeys before any execution returns an empty result set
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertFalse(keys.next());
                }
            }
        }
    }

    public static void test_generated_keys_typed_values() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE typed_keys (id BIGINT, ratio DOUBLE, label VARCHAR)");

            String sql = "INSERT INTO typed_keys VALUES (1, 3.5, 'duck')";

            try (PreparedStatement ps = conn.prepareStatement(sql, new String[] {"ratio", "label"})) {
                assertEquals(ps.executeUpdate(), 1);
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(keys.getDouble(1), 3.5, 0.0001);
                    assertEquals(keys.getString(2), "duck");
                    assertFalse(keys.next());
                }
            }
        }
    }

    public static void test_generated_keys_invalid_flag_value() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            assertThrows(
                () -> conn.prepareStatement("INSERT INTO t VALUES (1)", 42), SQLFeatureNotSupportedException.class);

            try (Statement s = conn.createStatement()) {
                assertThrows(
                        () -> s.execute("INSERT INTO t VALUES (1)", 42), SQLFeatureNotSupportedException.class);

                assertThrows(
                        () -> s.executeUpdate("INSERT INTO t VALUES (1)", 42),
                        SQLFeatureNotSupportedException.class);
            }
        }
    }

    public static void test_generated_keys_invalid_flag_value_message() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement s = conn.createStatement()) {
            String msg = assertThrows(
                    () -> conn.prepareStatement("INSERT INTO t VALUES (1)", 42), SQLFeatureNotSupportedException.class);
            assertTrue(msg.contains("autoGeneratedKeys="), "message should report the value but was: " + msg);
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

    public static void test_prepared_statement_array_parameter() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement ps = conn.prepareStatement("SELECT ?::INT[]")) {
            Array arrParam = conn.createArrayOf("INT", new Object[] {41, 42});
            ps.setObject(1, arrParam, Types.ARRAY);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Array arrWrapper = rs.getArray(1);
                Object[] arr = (Object[]) arrWrapper.getArray();
                assertEquals(arr.length, 2);
                assertEquals(arr[0], 41);
                assertEquals(arr[1], 42);
                assertFalse(rs.next());
            }
        }
    }
}
