import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RunSpark {

    static final String DUCKDB_JDBC_JAR = fromEnv("DUCKDB_JDBC_JAR", "./build/release/duckdb_jdbc.jar");
    static final String SPARK_SQL_EXE = fromEnv("SPARK_SQL_EXE", "../spark/spark-3.5.5-bin-hadoop3/bin/spark-sql");

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new RuntimeException("Path to Spark SQL script must be specified as a first and only argument");
        }
        Process ps = new ProcessBuilder(SPARK_SQL_EXE, "--driver-class-path", DUCKDB_JDBC_JAR, "-f", args[0])
                         .redirectInput(INHERIT)
                         .redirectError(INHERIT)
                         .start();
        String output = new String(ps.getInputStream().readAllBytes(), UTF_8);
        System.out.print(output);
        int status = ps.waitFor();
        String[] lines = output.split("\n");
        if (lines.length < 2 || !"7433139".equals(lines[0]) || !"1.429378704487457E8".equals(lines[1])) {
            throw new RuntimeException("Spark SQL test output check failed");
        }
        if (status == 0) {
            System.out.println("Success");
        }
        System.exit(status);
    }

    static String fromEnv(String envVarName, String defaultValue) {
        String env = System.getenv(envVarName);
        if (null != env) {
            return env;
        }
        return defaultValue;
    }
}
