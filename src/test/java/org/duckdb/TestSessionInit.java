package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.JdbcUtils.bytesToHex;
import static org.duckdb.test.Assertions.*;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.sql.*;
import java.util.Properties;
import org.duckdb.test.TempDirectory;

public class TestSessionInit {

    public static void test_session_init_db_only() throws Exception {
        try (TempDirectory td = new TempDirectory()) {
            Path initSqlFile = td.path().resolve("init.sql");
            Files.write(initSqlFile, "CREATE TABLE tab1(col1 int);".getBytes());
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:;session_init_sql_file=" + initSqlFile);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE tab1");
            }
        }
    }

    public static void test_session_init_db_and_connection() throws Exception {
        try (TempDirectory td = new TempDirectory()) {
            Path initSqlFile = td.path().resolve("init.sql");
            Files.write(initSqlFile, ("CREATE TABLE tab1(col1 int);\n"
                                      + " /* DUCKDB_CONNECTION_INIT_BELOW_MARKER   */ \n"
                                      + "INSERT INTO tab1 VALUES(42);")
                                         .getBytes());
            String url = "jdbc:duckdb:memory:test1;session_init_sql_file=" + initSqlFile;
            try (Connection conn1 = DriverManager.getConnection(url);
                 Connection conn2 = DriverManager.getConnection(url); Statement stmt = conn2.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM tab1")) {
                rs.next();
                assertEquals(rs.getInt(1), 42);
                rs.next();
                assertEquals(rs.getInt(1), 42);
            }
        }
    }

    public static void test_session_init_connection_only() throws Exception {
        try (TempDirectory td = new TempDirectory()) {
            Path initSqlFile = td.path().resolve("init.sql");
            Files.write(initSqlFile, (" /* DUCKDB_CONNECTION_INIT_BELOW_MARKER   */ \n"
                                      + "CREATE TABLE tab1(col1 int)")
                                         .getBytes());
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:;session_init_sql_file=" + initSqlFile);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE tab1");
            }
        }
    }

    public static void test_session_init_sha256() throws Exception {
        try (TempDirectory td = new TempDirectory()) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            Path initSqlFile = td.path().resolve("init.sql");

            final DigestOutputStream dos;
            try (OutputStream os = Files.newOutputStream(initSqlFile)) {
                dos = new DigestOutputStream(os, md);
                dos.write("CREATE TABLE tab1(col1 int)".getBytes(UTF_8));
                dos.flush();
            }

            String sha256 = bytesToHex(md.digest());
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:;"
                                                               + "session_init_sql_file=" + initSqlFile + ";"
                                                               + "session_init_sql_file_sha256=" + sha256);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE tab1");
            }

            assertThrows(() -> {
                DriverManager.getConnection("jdbc:duckdb:;"
                                            + "session_init_sql_file=" + initSqlFile + ";"
                                            + "session_init_sql_file_sha25=fail");
            }, SQLException.class);

            assertThrows(() -> {
                DriverManager.getConnection("jdbc:duckdb:"
                                            + "session_init_sql_file=" + initSqlFile + ";"
                                            + "threads=1;"
                                            + "session_init_sql_file_sha256=" + sha256);
            }, SQLException.class);

            assertThrows(() -> {
                DriverManager.getConnection("jdbc:duckdb:;"
                                            + "session_init_sql_file=" + initSqlFile + ";"
                                            + "session_init_sql_file_sha256=" + sha256 + ";"
                                            + "session_init_sql_file_sha256=" + sha256);
            }, SQLException.class);

            assertThrows(() -> {
                Properties config = new Properties();
                config.put("session_init_sql_file_sha256", sha256);
                DriverManager.getConnection("jdbc:duckdb:;"
                                                + "session_init_sql_file=" + initSqlFile,
                                            config);
            }, SQLException.class);
        }
    }

    public static void test_session_init_tracing() throws Exception {
        String sql = " CREATE TABLE tab1(col1 int)\n\n";
        try (TempDirectory td = new TempDirectory()) {
            Path initSqlFile = td.path().resolve("init.sql");
            Files.write(initSqlFile, sql.getBytes());
            try (DuckDBConnection conn =
                     DriverManager.getConnection("jdbc:duckdb:;session_init_sql_file=" + initSqlFile)
                         .unwrap(DuckDBConnection.class)) {
                assertEquals(conn.getSessionInitSQL(), sql);
            }
        }
    }

    public static void test_session_init_invalid_params() throws Exception {
        try (TempDirectory td = new TempDirectory()) {
            Path initSqlFile = td.path().resolve("init.sql");
            Files.write(initSqlFile, "CREATE TABLE tab1(col1 int);".getBytes());
            DriverManager.getConnection("jdbc:duckdb:;session_init_sql_file=" + initSqlFile).close();
            assertThrows(() -> {
                DriverManager.getConnection("jdbc:duckdb:;"
                                            + "threads=1;"
                                            + "session_init_sql_file=" + initSqlFile);
            }, SQLException.class);
            assertThrows(() -> {
                DriverManager.getConnection("jdbc:duckdb:;"
                                            + "session_init_sql_file=" + initSqlFile + ";"
                                            + "session_init_sql_file=" + initSqlFile);
            }, SQLException.class);
            assertThrows(() -> {
                Properties config = new Properties();
                config.put("session_init_sql_file", "initSqlFile");
                DriverManager.getConnection("jdbc:duckdb:;", config);
            }, SQLException.class);
        }
    }

    public static void test_session_init_invalid_file() throws Exception {
        try (TempDirectory td = new TempDirectory()) {
            Path initSqlFile = td.path().resolve("init.sql");
            Files.write(initSqlFile, ("CREATE TABLE tab1(col1 int);\n"
                                      + " /* DUCKDB_CONNECTION_INIT_BELOW_MARKER   */ \n"
                                      + "INSERT INTO tab1 VALUES(42);\n"
                                      + " /* DUCKDB_CONNECTION_INIT_BELOW_MARKER   */ \n"
                                      + "INSERT INTO tab1 VALUES(43);\n")
                                         .getBytes());
            assertThrows(() -> {
                DriverManager.getConnection("jdbc:duckdb:;session_init_sql_file=" + initSqlFile);
            }, SQLException.class);
        }
    }
}
