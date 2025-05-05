package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.sql.*;

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
}
