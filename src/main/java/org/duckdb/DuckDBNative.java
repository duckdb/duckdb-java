package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.sql.SQLException;
import java.util.Properties;

final class DuckDBNative {

    private static final String ARCH_X86_64 = "amd64";
    private static final String ARCH_AARCH64 = "arm64";

    private static final String OS_WINDOWS = "windows";
    private static final String OS_MACOS = "macos";
    private static final String OS_LINUX = "linux";

    static {
        try {
            loadNativeLibrary();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void loadNativeLibrary() throws Exception {
        String libName = nativeLibName();
        URL libRes = DuckDBNative.class.getResource("/" + libName);

        // The current JAR has a native library bundled, in this case we unpack and load it.
        // There is no fallback if the unpacking or loading fails. We expect that only
        // the '-nolib' JAR can be used with an external native lib
        if (null != libRes) {
            unpackAndLoad(libRes);
            return;
        }

        // There is no native library inside the JAR file, so we try to load it by name
        try {
            System.loadLibrary("duckdb_java");
        } catch (UnsatisfiedLinkError e) {
            // Native library cannot be loaded by name using ordinary JVM mechanisms, we try to load it directly
            // from FS - from the same directory where the current JAR resides
            try {
                loadFromCurrentJarDir(libName);
            } catch (Throwable t) {
                e.printStackTrace();
                throw new IllegalStateException(t);
            }
        }
    }

    private static String cpuArch() {
        String prop = System.getProperty("os.arch").toLowerCase().trim();
        switch (prop) {
        case "x86_64":
        case "amd64":
            return ARCH_X86_64;
        case "aarch64":
        case "arm64":
            return ARCH_AARCH64;
        default:
            return prop.replaceAll("[^a-z0-9_\\-.]", "");
        }
    }

    static String osName() {
        String prop = System.getProperty("os.name").toLowerCase().trim();
        if (prop.startsWith("windows")) {
            return OS_WINDOWS;
        } else if (prop.startsWith("mac")) {
            return OS_MACOS;
        } else if (prop.startsWith("linux")) {
            return OS_LINUX;
        } else {
            return prop.replaceAll("[^a-z0-9_\\-.]", "");
        }
    }

    static String nativeLibName() {
        return "libduckdb_java.so_" + osName() + "_" + cpuArch();
    }

    static Path currentJarDir() throws Exception {
        ProtectionDomain pd = DuckDBNative.class.getProtectionDomain();
        CodeSource cs = pd.getCodeSource();
        URL loc = cs.getLocation();
        URI uri = loc.toURI();
        Path jarPath = Paths.get(uri);
        Path dirPath = jarPath.getParent();
        return dirPath.toRealPath();
    }

    private static void unpackAndLoad(URL nativeLibRes) throws IOException {
        Path tmpFile = Files.createTempFile("libduckdb_java", ".so");
        try (InputStream is = nativeLibRes.openStream()) {
            Files.copy(is, tmpFile, REPLACE_EXISTING);
        }
        tmpFile.toFile().deleteOnExit();
        // Harmless on the officially supported platforms but required on some other.
        tmpFile.toFile().setExecutable(true);
        System.load(tmpFile.toAbsolutePath().toString());
    }

    private static void loadFromCurrentJarDir(String libName) throws Exception {
        Path dir = currentJarDir();
        Path libPath = dir.resolve(libName);
        if (Files.exists(libPath)) {
            System.load(libPath.toAbsolutePath().toString());
        } else {
            throw new FileNotFoundException("DuckDB JNI library not found, path: '" + libPath.toAbsolutePath() + "'");
        }
    }

    // We use zero-length ByteBuffer-s as a hacky but cheap way to pass C++ pointers
    // back and forth

    /*
     * NB: if you change anything below, run `javah` on this class to re-generate
     * the C header. CMake does this as well
     */

    // The statements, queries and results are implemented in Java on top of the C API v2 entry points of
    // DuckDBBindings, see DuckDBCore

    // returns stmt_ref result reference object
    static DuckDBCore.StatementRef duckdb_jdbc_prepare(DuckDBConnection conn, byte[] query) throws SQLException {
        return DuckDBCore.prepare(conn, query);
    }

    static void duckdb_jdbc_release(DuckDBCore.StatementRef stmt_ref) {
        try {
            DuckDBCore.release(stmt_ref);
        } catch (SQLException e) {
            // the statement is released, nothing else can be done
        }
    }

    static DuckDBResultSetMetaData duckdb_jdbc_query_result_meta(DuckDBCore.ResultRef result_ref) throws SQLException {
        return DuckDBCore.queryResultMeta(result_ref);
    }

    static DuckDBResultSetMetaData duckdb_jdbc_prepared_statement_meta(DuckDBCore.StatementRef stmt_ref)
        throws SQLException {
        return DuckDBCore.preparedStatementMeta(stmt_ref);
    }

    // returns res_ref result reference object
    static DuckDBCore.ResultRef duckdb_jdbc_execute(DuckDBCore.StatementRef stmt_ref, Object[] params)
        throws SQLException {
        return DuckDBCore.execute(stmt_ref, params);
    }

    static ByteBuffer duckdb_jdbc_execute_capi(DuckDBCore.StatementRef stmt_ref, Object[] params) throws SQLException {
        return DuckDBCore.executeChunked(stmt_ref, params);
    }

    static DuckDBCore.PendingRef duckdb_jdbc_pending_query(DuckDBConnection conn, byte[] query) throws SQLException {
        return DuckDBCore.pendingQuery(conn, query);
    }

    static DuckDBCore.ResultRef duckdb_jdbc_execute_pending(DuckDBCore.PendingRef pending_ref) throws SQLException {
        return DuckDBCore.executePending(pending_ref);
    }

    static void duckdb_jdbc_release_pending(DuckDBCore.PendingRef pending_ref) throws SQLException {
        DuckDBCore.releasePending(pending_ref);
    }

    static void duckdb_jdbc_free_result(DuckDBCore.ResultRef res_ref) {
        try {
            DuckDBCore.freeResult(res_ref);
        } catch (SQLException e) {
            // the result is freed, nothing else can be done
        }
    }

    static DuckDBVector[] duckdb_jdbc_fetch(DuckDBCore.ResultRef res_ref, ByteBuffer conn_ref) throws SQLException {
        return DuckDBCore.fetch(res_ref);
    }

    static String[] duckdb_jdbc_cast_result_to_strings(DuckDBCore.ResultRef res_ref, ByteBuffer conn_ref, long col_idx)
        throws SQLException {
        return DuckDBCore.castResultToStrings(res_ref, col_idx);
    }

    static long duckdb_jdbc_arrow_stream(DuckDBCore.ResultRef res_ref, DuckDBConnection conn, long batch_size)
        throws SQLException {
        return DuckDBCore.exportArrow(res_ref, conn, batch_size);
    }

    static void duckdb_jdbc_arrow_register(DuckDBConnection conn, long arrow_array_stream_pointer, String name)
        throws SQLException {
        DuckDBCore.registerArrowStream(conn, arrow_array_stream_pointer, name);
    }

    static void duckdb_jdbc_arrow_close_streams(ByteBuffer conn_ref) {
        DuckDBBindings.duckdb_v2_arrow_close_streams(conn_ref);
    }

    static int duckdb_jdbc_fetch_size() {
        return (int) DuckDBBindings.duckdb_vector_size();
    }

    static void duckdb_jdbc_interrupt(ByteBuffer conn_ref) throws SQLException {
        DuckDBBindings.duckdb_v2_connection_interrupt(conn_ref);
    }

    static QueryProgress duckdb_jdbc_query_progress(ByteBuffer conn_ref) throws SQLException {
        return DuckDBBindings.duckdb_v2_connection_progress_get(conn_ref);
    }

    protected static String duckdb_jdbc_get_profiling_information(ByteBuffer conn_ref, ProfilerPrintFormat format)
        throws SQLException {
        String name = "default".equals(format.getName()) ? "" : format.getName();
        return new String(DuckDBBindings.duckdb_v2_connection_get_profiling_information(conn_ref, name.getBytes(UTF_8)),
                          UTF_8);
    }
}
