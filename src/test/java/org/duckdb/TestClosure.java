package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.io.File;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestClosure {

    // https://github.com/duckdb/duckdb-java/issues/101
    public static void test_unclosed_statement_does_not_hang() throws Exception {
        String dbName = "test_issue_101.db";
        String url = JDBC_URL + dbName;
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("select 42");
        // statement not closed explicitly
        conn.close();
        assertTrue(stmt.isClosed());
        Connection connOther = DriverManager.getConnection(url);
        connOther.close();
        assertTrue(new File(dbName).delete());
    }

    public static void test_result_set_auto_closed() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            Statement stmt = conn.createStatement();
            ResultSet rs1 = stmt.executeQuery("select 42");
            ResultSet rs2 = stmt.executeQuery("select 43");
            assertTrue(rs1.isClosed());
            stmt.close();
            assertTrue(rs2.isClosed());
        }
    }

    public static void test_statements_auto_closed_on_conn_close() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt1 = conn.createStatement();
        stmt1.execute("select 42");
        PreparedStatement stmt2 = conn.prepareStatement("select 43");
        stmt2.execute();
        Statement stmt3 = conn.createStatement();
        stmt3.execute("select 44");
        stmt3.close();
        conn.close();
        assertTrue(stmt1.isClosed());
        assertTrue(stmt2.isClosed());
    }

    public static void test_appender_auto_closed_on_conn_close() throws Exception {
        DuckDBConnection conn = DriverManager.getConnection(JDBC_URL).unwrap(DuckDBConnection.class);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tab1(col1 INT, col2 VARCHAR)");
        }
        DuckDBAppender appender = conn.createAppender("tab1");
        appender.beginRow().append(42).append("foo").endRow().flush();
        conn.close();
        assertTrue(appender.isClosed());
    }

    public static void test_results_auto_closed_on_conn_close() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select 42");
        rs.next();
        conn.close();
        assertTrue(rs.isClosed());
        assertTrue(stmt.isClosed());
    }

    public static void test_statement_auto_closed_on_completion() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            Statement stmt = conn.createStatement();
            stmt.closeOnCompletion();
            assertTrue(stmt.isCloseOnCompletion());
            try (ResultSet rs = stmt.executeQuery("select 42")) {
                rs.next();
            }
            assertTrue(stmt.isClosed());
        }
    }

    public static void test_long_query_conn_close() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE test_fib1(i bigint, p double, f double)");
        stmt.execute("INSERT INTO test_fib1 values(1, 0, 1)");
        long start = System.currentTimeMillis();
        Thread th = new Thread(() -> {
            try {
                Thread.sleep(1000);
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        th.start();
        assertThrows(
            ()
                -> stmt.executeQuery(
                    "WITH RECURSIVE cte AS ("
                    +
                    "SELECT * from test_fib1 UNION ALL SELECT cte.i + 1, cte.f, cte.p + cte.f from cte WHERE cte.i < 150000) "
                    + "SELECT avg(f) FROM cte"),
            SQLException.class);
        th.join();
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 2000);
        assertTrue(stmt.isClosed());
        assertTrue(conn.isClosed());
    }

    public static void test_long_query_stmt_close() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE test_fib1(i bigint, p double, f double)");
            stmt.execute("INSERT INTO test_fib1 values(1, 0, 1)");
            long start = System.currentTimeMillis();
            Thread th = new Thread(() -> {
                try {
                    Thread.sleep(1000);
                    stmt.cancel();
                    stmt.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            th.start();
            assertThrows(
                ()
                    -> stmt.executeQuery(
                        "WITH RECURSIVE cte AS ("
                        +
                        "SELECT * from test_fib1 UNION ALL SELECT cte.i + 1, cte.f, cte.p + cte.f from cte WHERE cte.i < 150000) "
                        + "SELECT avg(f) FROM cte"),
                SQLException.class);
            th.join();
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed < 2000);
            assertTrue(stmt.isClosed());
            assertFalse(conn.isClosed());
        }
    }

    public static void test_conn_close_no_crash() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        for (int i = 0; i < 1 << 7; i++) {
            Connection conn = DriverManager.getConnection(JDBC_URL);
            Statement stmt = conn.createStatement();
            Future<?> future = executor.submit(() -> {
                try {
                    conn.close();
                } catch (SQLException e) {
                    fail();
                }
            });
            try {
                stmt.executeQuery("select 42");
            } catch (SQLException e) {
            }
            future.get();
        }
    }

    public static void test_stmt_close_no_crash() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            for (int i = 0; i < 1 << 10; i++) {
                Statement stmt = conn.createStatement();
                Future<?> future = executor.submit(() -> {
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                        fail();
                    }
                });
                try {
                    stmt.executeQuery("select 42");
                } catch (SQLException e) {
                }
                future.get();
            }
        }
    }

    public static void test_results_close_no_crash() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            for (int i = 0; i < 1 << 12; i++) {
                ResultSet rs = stmt.executeQuery("select 42");
                Future<?> future = executor.submit(() -> {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        fail();
                    }
                });
                try {
                    rs.next();
                } catch (SQLException e) {
                }
                future.get();
            }
        }
    }

    public static void test_results_close_prepared_stmt_no_crash() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             PreparedStatement stmt = conn.prepareStatement("select 42")) {
            for (int i = 0; i < 1 << 12; i++) {
                ResultSet rs = stmt.executeQuery();
                Future<?> future = executor.submit(() -> {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        fail();
                    }
                });
                try {
                    rs.next();
                } catch (SQLException e) {
                }
                future.get();
            }
        }
    }

    @SuppressWarnings("try")
    public static void test_results_fetch_no_hang() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Properties config = new Properties();
        config.put(DuckDBDriver.JDBC_STREAM_RESULTS, true);
        long rowsCount = 1 << 24;
        int iterations = 1;
        for (int i = 0; i < iterations; i++) {
            try (Connection conn = DriverManager.getConnection(JDBC_URL, config);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT i, i::VARCHAR FROM range(0, " + rowsCount + ") AS t(i)")) {
                executor.submit(() -> {
                    try {
                        Thread.sleep(100);
                        conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                long[] resultsCount = new long[1];
                assertThrows(() -> {
                    while (rs.next()) {
                        resultsCount[0]++;
                    }
                }, SQLException.class);
                assertTrue(resultsCount[0] > 0);
                assertTrue(resultsCount[0] < rowsCount);
            }
        }
    }

    public static void test_stmt_can_only_cancel_self() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt1 = conn.createStatement();
             Statement stmt2 = conn.createStatement()) {
            stmt1.execute("DROP TABLE IF EXISTS test_fib1");
            stmt1.execute("CREATE TABLE test_fib1(i bigint, p double, f double)");
            stmt1.execute("INSERT INTO test_fib1 values(1, 0, 1)");
            long start = System.currentTimeMillis();
            Thread th = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    stmt1.cancel();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            th.start();
            try (
                ResultSet rs = stmt2.executeQuery(
                    "WITH RECURSIVE cte AS ("
                    +
                    "SELECT * from test_fib1 UNION ALL SELECT cte.i + 1, cte.f, cte.p + cte.f from cte WHERE cte.i < 50000) "
                    + "SELECT avg(f) FROM cte")) {
                rs.next();
                assertTrue(rs.getDouble(1) > 0);
            }
            th.join();
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed > 1000);
            assertFalse(conn.isClosed());
            assertFalse(stmt1.isClosed());
            assertFalse(stmt2.isClosed());
        }
    }

    public static void test_stmt_query_timeout() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(1);
            stmt.execute("CREATE TABLE test_fib1(i bigint, p double, f double)");
            stmt.execute("INSERT INTO test_fib1 values(1, 0, 1)");
            long start = System.currentTimeMillis();
            assertThrows(
                ()
                    -> stmt.executeQuery(
                        "WITH RECURSIVE cte AS ("
                        +
                        "SELECT * from test_fib1 UNION ALL SELECT cte.i + 1, cte.f, cte.p + cte.f from cte WHERE cte.i < 150000) "
                        + "SELECT avg(f) FROM cte"),
                SQLTimeoutException.class);
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed < 1500);
            assertFalse(conn.isClosed());
            assertTrue(stmt.isClosed());
            assertEquals(DuckDBDriver.scheduler.getQueue().size(), 0);
        }
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(1);
            assertThrows(() -> { stmt.execute("FAIL"); }, SQLException.class);
            assertEquals(DuckDBDriver.scheduler.getQueue().size(), 0);
        }
    }

    public static void manual_test_set_query_timeout_wo_scheduler() throws Exception {
        assertTrue(DuckDBDriver.shutdownQueryCancelScheduler());
        assertFalse(DuckDBDriver.shutdownQueryCancelScheduler());
        try (Connection conn = DriverManager.getConnection(JDBC_URL); Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(1);
            stmt.execute("CREATE TABLE test_fib1(i bigint, p double, f double)");
            stmt.execute("INSERT INTO test_fib1 values(1, 0, 1)");
            long start = System.currentTimeMillis();
            stmt.executeQuery(
                "WITH RECURSIVE cte AS ("
                +
                "SELECT * from test_fib1 UNION ALL SELECT cte.i + 1, cte.f, cte.p + cte.f from cte WHERE cte.i < 50000) "
                + "SELECT avg(f) FROM cte");
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed > 1500);
        }
    }
}
