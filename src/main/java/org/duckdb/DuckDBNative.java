package org.duckdb;

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
    private static final String ARCH_UNIVERSAL = "universal";

    private static final String OS_WINDOWS = "windows";
    private static final String OS_MACOS = "osx";
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
            throw new IllegalStateException("Unsupported system architecture: '" + prop + "'");
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
            throw new IllegalStateException("Unsupported OS: '" + prop + "'");
        }
    }

    static String nativeLibName() {
        String os = osName();
        final String arch;
        if (OS_MACOS.equals(os)) {
            arch = ARCH_UNIVERSAL;
        } else {
            arch = cpuArch();
        }
        return "libduckdb_java.so_" + os + "_" + arch;
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

    // results ConnectionHolder reference object
    static native ByteBuffer duckdb_jdbc_startup(byte[] path, boolean read_only, Properties props) throws SQLException;

    // returns conn_ref connection reference object
    static native ByteBuffer duckdb_jdbc_connect(ByteBuffer conn_ref) throws SQLException;

    static native ByteBuffer duckdb_jdbc_create_db_ref(ByteBuffer conn_ref) throws SQLException;

    static native void duckdb_jdbc_destroy_db_ref(ByteBuffer db_ref) throws SQLException;

    static native void duckdb_jdbc_set_auto_commit(ByteBuffer conn_ref, boolean auto_commit) throws SQLException;

    static native boolean duckdb_jdbc_get_auto_commit(ByteBuffer conn_ref) throws SQLException;

    static native void duckdb_jdbc_disconnect(ByteBuffer conn_ref);

    static native void duckdb_jdbc_set_schema(ByteBuffer conn_ref, String schema);

    static native void duckdb_jdbc_set_catalog(ByteBuffer conn_ref, String catalog);

    static native String duckdb_jdbc_get_schema(ByteBuffer conn_ref);

    static native String duckdb_jdbc_get_catalog(ByteBuffer conn_ref);

    // returns stmt_ref result reference object
    static native ByteBuffer duckdb_jdbc_prepare(ByteBuffer conn_ref, byte[] query) throws SQLException;

    static native void duckdb_jdbc_release(ByteBuffer stmt_ref);

    static native DuckDBResultSetMetaData duckdb_jdbc_query_result_meta(ByteBuffer result_ref) throws SQLException;

    static native DuckDBResultSetMetaData duckdb_jdbc_prepared_statement_meta(ByteBuffer stmt_ref) throws SQLException;

    // returns res_ref result reference object
    static native ByteBuffer duckdb_jdbc_execute(ByteBuffer stmt_ref, Object[] params) throws SQLException;

    static native void duckdb_jdbc_free_result(ByteBuffer res_ref);

    static native DuckDBVector[] duckdb_jdbc_fetch(ByteBuffer res_ref, ByteBuffer conn_ref) throws SQLException;

    static native String[] duckdb_jdbc_cast_result_to_strings(ByteBuffer res_ref, ByteBuffer conn_ref, long col_idx)
        throws SQLException;

    static native int duckdb_jdbc_fetch_size();

    static native long duckdb_jdbc_arrow_stream(ByteBuffer res_ref, long batch_size);

    static native void duckdb_jdbc_arrow_register(ByteBuffer conn_ref, long arrow_array_stream_pointer, byte[] name);

    static native ByteBuffer duckdb_jdbc_create_appender(ByteBuffer conn_ref, byte[] schema_name, byte[] table_name)
        throws SQLException;

    static native void duckdb_jdbc_appender_begin_row(ByteBuffer appender_ref) throws SQLException;

    static native void duckdb_jdbc_appender_end_row(ByteBuffer appender_ref) throws SQLException;

    static native void duckdb_jdbc_appender_flush(ByteBuffer appender_ref) throws SQLException;

    static native void duckdb_jdbc_interrupt(ByteBuffer conn_ref);

    static native QueryProgress duckdb_jdbc_query_progress(ByteBuffer conn_ref);

    static native void duckdb_jdbc_appender_close(ByteBuffer appender_ref) throws SQLException;

    static native void duckdb_jdbc_appender_append_boolean(ByteBuffer appender_ref, boolean value) throws SQLException;

    static native void duckdb_jdbc_appender_append_byte(ByteBuffer appender_ref, byte value) throws SQLException;

    static native void duckdb_jdbc_appender_append_short(ByteBuffer appender_ref, short value) throws SQLException;

    static native void duckdb_jdbc_appender_append_int(ByteBuffer appender_ref, int value) throws SQLException;

    static native void duckdb_jdbc_appender_append_long(ByteBuffer appender_ref, long value) throws SQLException;

    static native void duckdb_jdbc_appender_append_float(ByteBuffer appender_ref, float value) throws SQLException;

    static native void duckdb_jdbc_appender_append_double(ByteBuffer appender_ref, double value) throws SQLException;

    static native void duckdb_jdbc_appender_append_string(ByteBuffer appender_ref, byte[] value) throws SQLException;

    static native void duckdb_jdbc_appender_append_bytes(ByteBuffer appender_ref, byte[] value) throws SQLException;

    static native void duckdb_jdbc_appender_append_timestamp(ByteBuffer appender_ref, long value) throws SQLException;

    static native void duckdb_jdbc_appender_append_decimal(ByteBuffer appender_ref, BigDecimal value)
        throws SQLException;

    static native void duckdb_jdbc_appender_append_null(ByteBuffer appender_ref) throws SQLException;

    static native void duckdb_jdbc_create_extension_type(ByteBuffer conn_ref) throws SQLException;

    protected static native String duckdb_jdbc_get_profiling_information(ByteBuffer conn_ref,
                                                                         ProfilerPrintFormat format)
        throws SQLException;

    public static void duckdb_jdbc_create_extension_type(DuckDBConnection conn) throws SQLException {
        duckdb_jdbc_create_extension_type(conn.connRef);
    }
}
