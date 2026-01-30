package org.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class TestOptimizerCrash {
    public static void test_optimizer_access() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            // preparing a statement triggers the optimizer
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 42")) {
                stmt.execute();
            }
        }
    }

    public static void test_optimizer_crash_on_exception() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try {
                // This should fail during prepare/plan
                conn.prepareStatement("SELECT * FROM non_existent_table");
            } catch (SQLException e) {
                // Expected
            }
            
            // This should succeed if active_query was reset correctly
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 42")) {
                stmt.execute();
            }
        }
    }

    public static void test_optimizer_simple_statement() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 42");
            }
        }
    }

    public static void test_optimizer_crash_on_prepare_fail_repeated() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            for (int i = 0; i < 10; i++) {
                try {
                    conn.prepareStatement("SELECT * FROM non_existent_table");
                } catch (SQLException e) {
                    // Expected
                }
            }
            try (PreparedStatement stmt = conn.prepareStatement("SELECT 42")) {
                stmt.execute();
            }
        }
    }
}
