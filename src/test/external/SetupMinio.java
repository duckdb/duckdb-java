import static java.lang.Integer.parseInt;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readString;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class SetupMinio {

    static final String MINIO_EXE_PATH = fromEnv("MINIO_EXE", "../minio/minio");
    static final String PID_PATH = fromEnv("MINIO_PID", "../minio/minio.pid");
    static final String MC_EXE_PATH = fromEnv("MC_EXE", "../minio/mc");
    static final String DATA_PATH = fromEnv("MINIO_DATA", "./build/data1");
    static final String MINIO_HOST = fromEnv("MINIO_HOST", "127.0.0.1");
    static final String MINIO_PORT = fromEnv("MINIO_PORT", "9000");

    public static void main(String[] args) throws Exception {
        killMinioServer();
        deleteMinioData();
        setupMinio();
    }

    static void killMinioServer() throws Exception {
        Path pidPath = Paths.get(PID_PATH);
        if (!Files.exists(pidPath)) {
            return;
        }
        long pid = Long.parseLong(readString(pidPath, UTF_8));
        System.out.println("Killing Minio server process, pid: " + pid + " ...");
        new ProcessBuilder("/usr/bin/kill", String.valueOf(pid)).inheritIO().start().waitFor();
        Files.delete(pidPath);
    }

    static void deleteMinioData() throws Exception {
        Path minioDataPath = Paths.get(DATA_PATH);
        if (!Files.exists(minioDataPath)) {
            return;
        }
        System.out.println("Deleting Minio data: " + minioDataPath + " ...");
        Files.walk(minioDataPath).sorted(Comparator.reverseOrder()).forEach(p -> {
            try {
                Files.delete(p);
            } catch (IOException e) {
                throw new RuntimeException("Failed to delete " + p, e);
            }
        });
    }

    static void setupMinio() throws Exception {
        System.out.println("Starting Minio server ...");
        Process minioServerProcess =
            new ProcessBuilder(MINIO_EXE_PATH, "server", "--address", MINIO_HOST + ":" + MINIO_PORT, DATA_PATH)
                .inheritIO()
                .start();
        Files.write(Paths.get(PID_PATH), String.valueOf(minioServerProcess.pid()).getBytes(UTF_8));
        boolean minioServerStarted = false;
        for (int i = 0; i < 16; i++) {
            try (Socket sock = new Socket(MINIO_HOST, parseInt(MINIO_PORT))) {
                minioServerStarted = true;
                break;
            } catch (IOException e) {
                Thread.sleep(1000);
            }
        }
        if (!minioServerStarted) {
            throw new RuntimeException("Cannot start Minio");
        }
        Thread.sleep(2000); // improve log output
        System.out.println("Minio server started, pid: " + minioServerProcess.pid() + ", creating bucket ...");
        int mcAliasStatus = new ProcessBuilder(MC_EXE_PATH, "alias", "set", "local",
                                               "http://" + MINIO_HOST + ":" + MINIO_PORT, "minioadmin", "minioadmin")
                                .inheritIO()
                                .start()
                                .waitFor();
        if (mcAliasStatus != 0) {
            killMinioServer();
            throw new RuntimeException("Minio mc alias set error, status: " + mcAliasStatus);
        }
        int mcMbStatus = new ProcessBuilder(MC_EXE_PATH, "mb", "local/bucket1").inheritIO().start().waitFor();
        if (mcMbStatus != 0) {
            killMinioServer();
            throw new RuntimeException("Minio mc mb error, status: " + mcAliasStatus);
        }
        System.out.println("Minio server set up successfully");
    }

    static String fromEnv(String envVarName, String defaultValue) {
        String env = System.getenv(envVarName);
        if (null != env) {
            return env;
        }
        return defaultValue;
    }
}
