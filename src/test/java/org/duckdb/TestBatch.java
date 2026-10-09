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

    private interface BatchExecutor {
        void execute() throws Exception;
    }

    private static BatchUpdateException executeBatchExpectingFailure(BatchExecutor executor) throws Exception {
        try {
            executor.execute();
        } catch (BatchUpdateException e) {
            return e;
        }
        fail("Expected the batch execution to throw BatchUpdateException");
        return null;
    }

    private static void assertPartialFailureException(BatchUpdateException e, SQLException reference,
                                                      long[] expectedCounts, boolean large) throws Exception {
        // JDBC requires the SQLState, message and cause of the failing command to be preserved
        assertEquals(e.getSQLState(), reference.getSQLState());
        assertEquals(e.getMessage(), reference.getMessage());
        assertNotNull(e.getCause());
        assertTrue(e.getCause() instanceof SQLException, "cause should be the underlying SQLException");
        assertEquals(((SQLException) e.getCause()).getSQLState(), reference.getSQLState());
        assertEquals(e.getCause().getMessage(), reference.getMessage());

        // only the commands executed successfully before the failure are reported
        long[] counts = e.getLargeUpdateCounts();
        assertEquals(counts.length, expectedCounts.length);
        for (int i = 0; i < expectedCounts.length; i++) {
            assertEquals(counts[i], expectedCounts[i]);
        }
        if (!large) {
            int[] intCounts = e.getUpdateCounts();
            assertEquals(intCounts.length, expectedCounts.length);
            for (int i = 0; i < expectedCounts.length; i++) {
                assertEquals(intCounts[i], (int) expectedCounts[i]);
            }
        }
    }

    private static SQLException newNullViolationFailure(boolean prepared) throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tab1 (col1 VARCHAR NOT NULL)");
            }
            try {
                if (prepared) {
                    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tab1 VALUES(?)")) {
                        ps.setString(1, null);
                        ps.executeUpdate();
                    }
                } else {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("INSERT INTO tab1 VALUES(NULL)");
                    }
                }
            } catch (SQLException e) {
                return e;
            }
            fail("expected the NOT NULL violation to fail");
            return null;
        }
    }

    private static SQLException newSqlFailure(String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                return e;
            }
            fail("expected the invalid statement to fail");
            return null;
        }
    }

    private static void checkPartialFailureBatch(boolean prepared, boolean large, boolean autoCommit) throws Exception {
        SQLException reference = newNullViolationFailure(prepared);
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            conn.setAutoCommit(autoCommit);
            assertEquals(conn.getAutoCommit(), autoCommit);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tab1 (col1 VARCHAR NOT NULL)");
            }
            if (!autoCommit) {
                conn.commit();
            }

            BatchUpdateException bue;
            if (prepared) {
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO tab1 VALUES(?)")) {
                    ps.setString(1, "a");
                    ps.addBatch();
                    ps.setString(1, "b");
                    ps.addBatch();
                    ps.setString(1, "c");
                    ps.addBatch();
                    ps.setString(1, null);
                    ps.addBatch();
                    ps.setString(1, "d");
                    ps.addBatch();
                    bue = executeBatchExpectingFailure(() -> {
                        if (large) {
                            ps.executeLargeBatch();
                        } else {
                            ps.executeBatch();
                        }
                    });
                }
            } else {
                try (Statement stmt = conn.createStatement()) {
                    stmt.addBatch("INSERT INTO tab1 VALUES('a')");
                    stmt.addBatch("INSERT INTO tab1 VALUES('b')");
                    stmt.addBatch("INSERT INTO tab1 VALUES('c')");
                    stmt.addBatch("INSERT INTO tab1 VALUES(NULL)");
                    stmt.addBatch("INSERT INTO tab1 VALUES('d')");
                    bue = executeBatchExpectingFailure(() -> {
                        if (large) {
                            stmt.executeLargeBatch();
                        } else {
                            stmt.executeBatch();
                        }
                    });
                }
            }
            assertPartialFailureException(bue, reference, new long[] {1L, 1L, 1L}, large);

            // rollback/autocommit semantics must be unchanged: no row of the failed batch survives
            if (!autoCommit) {
                conn.rollback();
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), 0L);
            }

            // the connection stays usable after the failure
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO tab1 VALUES('after')");
                if (!autoCommit) {
                    conn.commit();
                }
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), 1L);
            }
        }
    }

    public static void test_prepared_statement_batch_partial_failure() throws Exception {
        checkPartialFailureBatch(true, false, true);
        checkPartialFailureBatch(true, false, false);
    }

    public static void test_prepared_statement_large_batch_partial_failure() throws Exception {
        checkPartialFailureBatch(true, true, true);
        checkPartialFailureBatch(true, true, false);
    }

    public static void test_statement_batch_partial_failure() throws Exception {
        checkPartialFailureBatch(false, false, true);
        checkPartialFailureBatch(false, false, false);
    }

    public static void test_statement_large_batch_partial_failure() throws Exception {
        checkPartialFailureBatch(false, true, true);
        checkPartialFailureBatch(false, true, false);
    }

    public static void test_statement_batch_partial_failure_invalid_sql() throws Exception {
        SQLException reference = newSqlFailure("SELCT 1");
        assertNotNull(reference);

        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tab1 (col1 VARCHAR NOT NULL)");
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.addBatch("INSERT INTO tab1 VALUES('a')");
                stmt.addBatch("INSERT INTO tab1 VALUES('b')");
                stmt.addBatch("SELCT 1");
                stmt.addBatch("INSERT INTO tab1 VALUES('c')");
                BatchUpdateException e = executeBatchExpectingFailure(stmt::executeBatch);
                // the two statements before the invalid one ran, the trailing one was not attempted
                assertPartialFailureException(e, reference, new long[] {1L, 1L}, false);
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                assertEquals(rs.getLong(1), 0L);
            }
        }
    }
}
