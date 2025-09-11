import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

class SetupDuckLake {

    static final String POSTGRES_HOST = fromEnv("POSTGRES_HOST", "127.0.0.1");
    static final String POSTGRES_PORT = fromEnv("POSTGRES_PORT", "5432");
    static final String POSTGRES_MAINTENANCE_DB = fromEnv("POSTGRES_MAINTENANCE_DB", "postgres");
    static final String POSTGRES_USERNAME = fromEnv("POSTGRES_USERNAME", "postgres");
    static final String POSTGRES_PASSWORD = fromEnv("POSTGRES_PASSWORD", "postgres");
    static final String POSTGRES_URL = fromEnv("POSTGRES_URL", "jdbc:postgresql://" + POSTGRES_HOST + ":" +
                                                                   POSTGRES_PORT + "/" + POSTGRES_MAINTENANCE_DB);
    static final String DUCKLAKE_CATALOG_DB = fromEnv("DUCKLAKE_CATALOG_DB_NAME", "lake_test");
    static final String DUCKLAKE_URL =
        fromEnv("DUCKLAKE_URL", "ducklake:postgres:postgresql://" + POSTGRES_USERNAME + ":" + POSTGRES_PASSWORD + "@" +
                                    POSTGRES_HOST + ":" + POSTGRES_PORT + "/" + DUCKLAKE_CATALOG_DB);
    static final String PARQUET_FILE_URL =
        fromEnv("DUCKLAKE_DATA_PATH", "https://blobs.duckdb.org/data/taxi_2019_04.parquet");
    static final String SESSION_INIT_SQL_FILE =
        fromEnv("SESSION_INIT_SQL_FILE", "./src/test/external/spark-session-init.sql");

    public static void main(String[] args) throws Exception {
        setupPostgres();
        setupDuckLake();
        System.out.println("Success");
    }

    static void setupPostgres() throws Exception {
        System.out.println("Creating Postgres database ...");
        try (Connection conn = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USERNAME, POSTGRES_PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + DUCKLAKE_CATALOG_DB);
            stmt.execute("CREATE DATABASE " + DUCKLAKE_CATALOG_DB);
        }
    }

    static void setupDuckLake() throws Exception {
        System.out.println("Creating DuckLake instance ...");
        try (Connection conn =
                 DriverManager.getConnection("jdbc:duckdb:;session_init_sql_file=" + SESSION_INIT_SQL_FILE + ";");
             Statement stmt = conn.createStatement()) {
            stmt.execute("ATTACH '" + DUCKLAKE_URL + "' AS lake (DATA_PATH 's3://bucket1')");
            stmt.execute("USE lake");
            System.out.println("Loading data from URL: '" + PARQUET_FILE_URL + "' ...");
            stmt.execute("CREATE TABLE tab1 AS FROM '" + PARQUET_FILE_URL + "'");
        }
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + DUCKLAKE_URL +
                                                           ";session_init_sql_file=" + SESSION_INIT_SQL_FILE + ";");
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM tab1")) {
                rs.next();
                System.out.println("Records loaded: " + rs.getLong(1));
            }
        }
    }

    static String fromEnv(String envVarName, String defaultValue) {
        String env = System.getenv(envVarName);
        if (null != env) {
            return env;
        }
        return defaultValue;
    }
}
