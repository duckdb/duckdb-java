package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.sql.*;
import java.util.Properties;

public class TestBatch {

    public static void test_batch_prepared_statement() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE test (x INT, y INT, z INT)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test (x, y, z) VALUES (?, ?, ?);")) {
                ps.setObject(1, 1);
                ps.setObject(2, 2);
                ps.setObject(3, 3);
                ps.addBatch();

                ps.setObject(1, 4);
                ps.setObject(2, 5);
                ps.setObject(3, 6);
                ps.addBatch();

                ps.executeBatch();
            }
            try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM test ORDER BY x")) {
                rs.next();
                assertEquals(rs.getInt(1), rs.getObject(1, Integer.class));
                assertEquals(rs.getObject(1, Integer.class), 1);

                assertEquals(rs.getInt(2), rs.getObject(2, Integer.class));
                assertEquals(rs.getObject(2, Integer.class), 2);

                assertEquals(rs.getInt(3), rs.getObject(3, Integer.class));
                assertEquals(rs.getObject(3, Integer.class), 3);

                rs.next();
                assertEquals(rs.getInt(1), rs.getObject(1, Integer.class));
                assertEquals(rs.getObject(1, Integer.class), 4);

                assertEquals(rs.getInt(2), rs.getObject(2, Integer.class));
                assertEquals(rs.getObject(2, Integer.class), 5);

                assertEquals(rs.getInt(3), rs.getObject(3, Integer.class));
                assertEquals(rs.getObject(3, Integer.class), 6);
            }
        }
    }

    public static void test_batch_statement() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE test (x INT, y INT, z INT)");

                s.addBatch("INSERT INTO test (x, y, z) VALUES (1, 2, 3);");
                s.addBatch("INSERT INTO test (x, y, z) VALUES (4, 5, 6);");

                s.executeBatch();
            }
            try (Statement s2 = conn.createStatement();
                 ResultSet rs = s2.executeQuery("SELECT * FROM test ORDER BY x")) {
                rs.next();
                assertEquals(rs.getInt(1), rs.getObject(1, Integer.class));
                assertEquals(rs.getObject(1, Integer.class), 1);

                assertEquals(rs.getInt(2), rs.getObject(2, Integer.class));
                assertEquals(rs.getObject(2, Integer.class), 2);

                assertEquals(rs.getInt(3), rs.getObject(3, Integer.class));
                assertEquals(rs.getObject(3, Integer.class), 3);

                rs.next();
                assertEquals(rs.getInt(1), rs.getObject(1, Integer.class));
                assertEquals(rs.getObject(1, Integer.class), 4);

                assertEquals(rs.getInt(2), rs.getObject(2, Integer.class));
                assertEquals(rs.getObject(2, Integer.class), 5);

                assertEquals(rs.getInt(3), rs.getObject(3, Integer.class));
                assertEquals(rs.getObject(3, Integer.class), 6);
            }
        }
    }

    public static void test_execute_while_batch() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE test (id INT)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test (id) VALUES (?)")) {
                ps.setObject(1, 1);
                ps.addBatch();

                String msg =
                    assertThrows(() -> { ps.execute("INSERT INTO test (id) VALUES (1);"); }, SQLException.class);
                assertTrue(msg.contains("Batched queries must be executed with executeBatch."));

                String msg2 =
                    assertThrows(() -> { ps.executeUpdate("INSERT INTO test (id) VALUES (1);"); }, SQLException.class);
                assertTrue(msg2.contains("Batched queries must be executed with executeBatch."));

                String msg3 = assertThrows(() -> { ps.executeQuery("SELECT * FROM test"); }, SQLException.class);
                assertTrue(msg3.contains("Batched queries must be executed with executeBatch."));
            }
        }
    }

    public static void test_prepared_statement_batch_exception() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE test (id INT)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test (id) VALUES (?)")) {
                String msg = assertThrows(() -> { ps.addBatch("DUMMY SQL"); }, SQLException.class);
                assertTrue(msg.contains("Cannot add batched SQL statement to PreparedStatement"));
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test (id) VALUES (?)")) {
                ps.setString(1, "foo");
                ps.addBatch();
                String msg = assertThrows(ps::executeBatch, SQLException.class);
                assertTrue(msg.contains("Conversion Error: Could not convert string 'foo' to INT32"));
            }
        }
    }

    public static void test_prepared_statement_batch_autocommit() throws Exception {
        long count = 1 << 10;
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            assertTrue(conn.getAutoCommit());
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tab1 (col1 BIGINT, col2 VARCHAR)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tab1 VALUES(?, ?)")) {
                for (long i = 0; i < count; i++) {
                    ps.setLong(1, i);
                    ps.setString(2, i + "foo");
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), count);
            }
        }
    }

    public static void test_statement_batch_autocommit() throws Exception {
        long count = 1 << 10;
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            assertTrue(conn.getAutoCommit());
            stmt.execute("CREATE TABLE tab1 (col1 BIGINT, col2 VARCHAR)");
            for (long i = 0; i < count; i++) {
                stmt.addBatch("INSERT INTO tab1 VALUES(" + i + ", '" + i + "foo')");
            }
            stmt.executeBatch();
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), count);
            }
        }
    }

    public static void test_prepared_statement_batch_rollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tab1 (col1 BIGINT, col2 VARCHAR)");
            }
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO tab1 VALUES(-1, 'bar')");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tab1 VALUES(?, ?)")) {
                for (long i = 0; i < 1 << 10; i++) {
                    ps.setLong(1, i);
                    ps.setString(2, i + "foo");
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            conn.rollback();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), 0L);
            }
        }
    }

    public static void test_statement_batch_rollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1 (col1 BIGINT, col2 VARCHAR)");
            conn.setAutoCommit(false);
            stmt.execute("INSERT INTO tab1 VALUES(-1, 'bar')");
            for (long i = 0; i < 1 << 10; i++) {
                stmt.addBatch("INSERT INTO tab1 VALUES(" + i + ", '" + i + "foo')");
            }
            stmt.executeBatch();
            conn.rollback();
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), 0L);
            }
        }
    }

    public static void test_statement_batch_autocommit_constraint_violation() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            assertTrue(conn.getAutoCommit());
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tab1 (col1 VARCHAR NOT NULL)");
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.addBatch("INSERT INTO tab1 VALUES('foo')");
                stmt.addBatch("INSERT INTO tab1 VALUES(NULL)");
                assertThrows(stmt::executeBatch, SQLException.class);
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), 0L);
            }
        }
    }

    public static void test_prepared_statement_batch_autocommit_constraint_violation() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            assertTrue(conn.getAutoCommit());
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tab1 (col1 VARCHAR NOT NULL)");
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tab1 VALUES(?)")) {
                ps.setString(1, "foo");
                ps.addBatch();
                ps.setString(1, null);
                ps.addBatch();
                assertThrows(ps::executeBatch, SQLException.class);
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), 0L);
            }
        }
    }

    public static void test_statement_batch_constraint_violation() throws Exception {
        Properties config = new Properties();
        config.put(DuckDBDriver.JDBC_AUTO_COMMIT, false);
        try (Connection conn = DriverManager.getConnection(JDBC_URL, config)) {
            assertFalse(conn.getAutoCommit());
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tab1 (col1 VARCHAR NOT NULL)");
                conn.commit();
            }
            boolean thrown = false;
            try (Statement stmt = conn.createStatement()) {
                stmt.addBatch("INSERT INTO tab1 VALUES('foo')");
                stmt.addBatch("INSERT INTO tab1 VALUES(NULL)");
                stmt.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                thrown = true;
                conn.rollback();
            }
            assertTrue(thrown);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), 0L);
            }
        }
    }

    public static void test_prepared_statement_batch_constraint_violation() throws Exception {
        Properties config = new Properties();
        config.put(DuckDBDriver.JDBC_AUTO_COMMIT, false);
        try (Connection conn = DriverManager.getConnection(JDBC_URL, config)) {
            assertFalse(conn.getAutoCommit());
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tab1 (col1 VARCHAR NOT NULL)");
                conn.commit();
            }
            boolean thrown = false;
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tab1 VALUES(?)")) {
                ps.setString(1, "foo");
                ps.addBatch();
                ps.setString(1, null);
                ps.addBatch();
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                thrown = true;
                conn.rollback();
            }
            assertTrue(thrown);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), 0L);
            }
        }
    }
}
