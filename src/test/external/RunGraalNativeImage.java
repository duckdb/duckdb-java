import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class RunGraalNativeImage {

    static final String DUCKDB_JDBC_JAR = fromEnv("DUCKDB_JDBC_JAR", "./build/release/duckdb_jdbc.jar");

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new RuntimeException("Path to the Graal sample directory must be specified as the only argument");
        }
        Path srcDir = Paths.get(args[0]);
        Path jar = Paths.get(DUCKDB_JDBC_JAR).toAbsolutePath();

        Path workDir = Paths.get("graal-hello").toAbsolutePath();
        Path configDir = workDir.resolve("config");
        Files.createDirectories(configDir);
        Files.copy(srcDir.resolve("Hello.java"), workDir.resolve("Hello.java"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(srcDir.resolve("resource-config.json"), configDir.resolve("resource-config.json"),
                   StandardCopyOption.REPLACE_EXISTING);

        run(workDir, "javac", "-cp", jar.toString(), "Hello.java");
        run(workDir, "native-image", "--no-fallback", "--enable-native-access=ALL-UNNAMED", "-cp",
            jar + File.pathSeparator + ".", "-H:ConfigurationFileDirectories=config", "-o", "hello", "Hello");

        Process ps = new ProcessBuilder(workDir.resolve("hello").toString())
                         .directory(workDir.toFile())
                         .redirectError(INHERIT)
                         .start();
        String output = new String(ps.getInputStream().readAllBytes(), UTF_8).trim();
        int status = ps.waitFor();
        System.out.println(output);
        if (status != 0) {
            throw new RuntimeException("Native image run failed, status: " + status);
        }
        if (!"42".equals(output)) {
            throw new RuntimeException("Native image output check failed, expected: 42, actual: " + output);
        }
        System.out.println("Success");
    }

    static void run(Path workDir, String... command) throws Exception {
        int status = new ProcessBuilder(command).directory(workDir.toFile()).inheritIO().start().waitFor();
        if (status != 0) {
            throw new RuntimeException("Command failed, status: " + status + ", command: " + String.join(" ", command));
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
