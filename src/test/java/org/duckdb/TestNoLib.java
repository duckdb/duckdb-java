package org.duckdb;

import static java.util.Arrays.asList;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import org.duckdb.test.TempDirectory;

public class TestNoLib {

    private static Path javaExe() {
        String javaHomeProp = System.getProperty("java.home");
        Path javaHome = Paths.get(javaHomeProp);
        boolean isWindows = "windows".equals(DuckDBNative.osName());
        return isWindows ? javaHome.resolve("bin/java.exe") : javaHome.resolve("bin/java");
    }

    private static void runQuickTest(Path currentJarDir) throws Exception {
        String dir = currentJarDir.toAbsolutePath().toString();
        ProcessBuilder pb = new ProcessBuilder(javaExe().toAbsolutePath().toString(),
                                               "-Djava.library.path=" + currentJarDir.toAbsolutePath(), "-cp",
                                               dir + File.separator + "duckdb_jdbc_tests.jar" + File.pathSeparator +
                                                   dir + File.separator + "duckdb_jdbc_nolib.jar",
                                               "org.duckdb.TestDuckDBJDBC", "test_spatial_POINT_2D")
                                .inheritIO();
        int code = pb.start().waitFor();
        if (0 != code) {
            throw new RuntimeException("Spawned test failure, code: " + code);
        }
    }

    private static String platformLibName() throws Exception {
        String os = DuckDBNative.osName();
        switch (os) {
        case "windows":
            return "duckdb_java.dll";
        case "osx":
            return "libduckdb_java.dylib";
        case "linux":
            return "libduckdb_java.so";
        default:
            throw new SQLException("Unsupported OS: " + os);
        }
    }
    private static Path nativeLibPathInBuildTree(Path buildDir) throws SQLException {
        String libName = DuckDBNative.nativeLibName();
        Path libPath = buildDir.resolve(libName);
        if (Files.exists(libPath)) {
            return libPath;
        }
        for (String subdirName : asList("Release", "Debug", "RelWithDebInfo")) {
            Path dir = buildDir.resolve(subdirName);
            Path libPathSubdir = dir.resolve(libName);
            if (Files.exists(libPathSubdir)) {
                return libPathSubdir;
            }
        }
        throw new SQLException("Native lib not found in build tree, name: '" + libName + "'");
    }

    public static void test_nolib_next_to_jar() throws Exception {
        try (TempDirectory td = new TempDirectory()) {
            Path dir = DuckDBNative.currentJarDir();
            Path nativeLib = nativeLibPathInBuildTree(dir);
            Files.copy(dir.resolve("duckdb_jdbc_nolib.jar"), td.path().resolve("duckdb_jdbc_nolib.jar"));
            Files.copy(dir.resolve("duckdb_jdbc_tests.jar"), td.path().resolve("duckdb_jdbc_tests.jar"));
            Files.copy(nativeLib, td.path().resolve(nativeLib.getFileName()));
            System.out.println();
            System.out.println("----");
            runQuickTest(td.path());
            System.out.println("----");
        }
    }

    public static void test_nolib_by_name() throws Exception {
        try (TempDirectory td = new TempDirectory()) {
            Path dir = DuckDBNative.currentJarDir();
            Path nativeLib = nativeLibPathInBuildTree(dir);
            Files.copy(dir.resolve("duckdb_jdbc_nolib.jar"), td.path().resolve("duckdb_jdbc_nolib.jar"));
            Files.copy(dir.resolve("duckdb_jdbc_tests.jar"), td.path().resolve("duckdb_jdbc_tests.jar"));
            Files.copy(nativeLib, td.path().resolve(platformLibName()));
            System.out.println();
            System.out.println("----");
            runQuickTest(td.path());
            System.out.println("----");
        }
    }
}
