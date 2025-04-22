package org.duckdb;

import static org.duckdb.TestDuckDBJDBC.JDBC_URL;
import static org.duckdb.test.Assertions.*;

import java.io.File;
import java.sql.*;
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
        stmt.execute("DROP TABLE IF EXISTS test_fib1");
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
            stmt.execute("DROP TABLE IF EXISTS test_fib1");
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
}
