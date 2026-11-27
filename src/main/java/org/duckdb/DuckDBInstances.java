package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Opens the database instances and the connections to them, on top of the C API v2 entry points of
 * {@link DuckDBBindings}.
 */
final class DuckDBInstances {
    private DuckDBInstances() {
    }

    static final String JDBC_STREAM_RESULTS_OPTION = "jdbc_stream_results";

    // The options that need a running instance, they cannot be given at startup
    private static final String[] RUNTIME_OPTIONS = {"allowed_directories", "allowed_paths",     "disabled_filesystems",
                                                     "enable_logging",      "logging_level",     "logging_mode",
                                                     "logging_storage",     "enabled_log_types", "disabled_log_types"};

    private static final String MEMORY_PREFIX = ":memory:";

    // The addresses of the instances that are not shared are even, of the shared ones are odd
    private static final AtomicLong NEXT_ADDRESS = new AtomicLong(1);

    private static ByteBuffer defaultEnvironment = null;

    /**
     * A database instance, that is shared by the connections to it (and by the pinned references to it). The
     * instance is destroyed with the last reference.
     */
    static final class InstanceRef {
        private int refs = 1;
        private final ByteBuffer environment;
        private ByteBuffer instance;
        final long address;

        private InstanceRef(ByteBuffer environment, ByteBuffer instance, long address) {
            this.environment = environment;
            this.instance = instance;
            this.address = address;
        }

        synchronized InstanceRef retain() {
            if (instance == null) {
                throw new IllegalStateException("The instance is destroyed");
            }
            refs++;
            return this;
        }

        ByteBuffer instance() {
            return instance;
        }

        void release() {
            ByteBuffer toDestroy = null;
            synchronized (this) {
                if (instance == null) {
                    return;
                }
                if (--refs == 0) {
                    toDestroy = instance;
                    instance = null;
                }
            }
            if (toDestroy != null) {
                try {
                    duckdb_v2_instance_destroy(toDestroy);
                    if (environment != null) {
                        duckdb_v2_environment_destroy(environment);
                    }
                } catch (SQLException e) {
                    // nothing else can be done
                }
            }
        }
    }

    /** A connection with the instance that it works on. */
    static final class Connected {
        final InstanceRef instanceRef;
        final ByteBuffer conn;

        Connected(InstanceRef instanceRef, ByteBuffer conn) {
            this.instanceRef = instanceRef;
            this.conn = conn;
        }
    }

    private static synchronized ByteBuffer defaultEnvironment() throws SQLException {
        if (defaultEnvironment == null) {
            defaultEnvironment = duckdb_v2_environment_create();
        }
        return defaultEnvironment;
    }

    private static boolean isRuntimeOption(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        for (String option : RUNTIME_OPTIONS) {
            if (option.equals(lower)) {
                return true;
            }
        }
        return false;
    }

    private static String quoteListEntry(String entry) {
        return "'" + entry.replace("'", "''") + "'";
    }

    // The text of a Java configuration value in the form that is accepted by SET, null for nulls
    private static String valueToText(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Float || value instanceof Double) {
            return Double.toString(((Number) value).doubleValue());
        } else if (value instanceof Iterable) {
            StringBuilder res = new StringBuilder("[");
            boolean first = true;
            // all list entries are coalesced to string
            for (Object entry : (Iterable<?>) value) {
                if (!first) {
                    res.append(", ");
                }
                first = false;
                res.append(quoteListEntry(String.valueOf(entry)));
            }
            return res.append("]").toString();
        }
        return value.toString();
    }

    // The message of the error without the name of its type, as in "Invalid Input Error: message"
    static String rawMessage(String message) {
        if (message == null) {
            return "";
        }
        int pos = message.indexOf(" Error: ");
        if (pos < 0 || pos > 40) {
            return message;
        }
        return message.substring(pos + " Error: ".length());
    }

    private static byte[][] utf8(List<String> strings) {
        byte[][] res = new byte[strings.size()][];
        for (int i = 0; i < res.length; i++) {
            res[i] = strings.get(i).getBytes(UTF_8);
        }
        return res;
    }

    private static void registerStreamResultsOption(ByteBuffer instance) throws SQLException {
        ByteBuffer conn = duckdb_v2_connection_create(instance);
        try {
            ByteBuffer factory = duckdb_v2_connection_get_factory(conn);
            ByteBuffer option = duckdb_v2_custom_option_create(factory);
            try {
                ByteBuffer type = duckdb_create_logical_type(CAPIType.DUCKDB_TYPE_BOOLEAN.typeId);
                ByteBuffer defaultValue = duckdb_v2_value_create_bool(factory, false);
                try {
                    duckdb_v2_custom_option_set_name(option, JDBC_STREAM_RESULTS_OPTION.getBytes(UTF_8));
                    duckdb_v2_custom_option_set_type(option, type);
                    duckdb_v2_custom_option_set_default_value(option, defaultValue);
                    duckdb_v2_custom_option_set_description(
                        option,
                        "Whether to stream results. Only one ResultSet on a connection can be open at once when true"
                            .getBytes(UTF_8));
                    duckdb_v2_custom_option_set_default_scope(option, SCOPE_SESSION);
                    duckdb_v2_custom_option_set_supports_scope(option, SCOPE_GLOBAL, true);
                    duckdb_v2_custom_option_set_supports_scope(option, SCOPE_SESSION, true);
                    duckdb_v2_connection_register_custom_option(conn, option);
                    duckdb_v2_arrow_register_scan_function(conn);
                } finally {
                    duckdb_v2_value_destroy(defaultValue);
                    duckdb_destroy_logical_type(type);
                }
            } finally {
                duckdb_v2_custom_option_destroy(option);
            }
        } finally {
            duckdb_v2_connection_destroy(conn);
        }
    }

    // The settings that cannot be given at startup are written when the instance is created
    private static void initInstance(ByteBuffer instance, List<String[]> runtimeOptions) throws SQLException {
        registerStreamResultsOption(instance);
        ByteBuffer config = duckdb_v2_instance_get_config(instance);
        for (String[] option : runtimeOptions) {
            try {
                duckdb_v2_config_set_option_text(config, option[0].getBytes(UTF_8), option[1].getBytes(UTF_8),
                                                 SCOPE_GLOBAL);
            } catch (SQLException e) {
                throw new SQLException("Failed to set configuration option \"" + option[0] +
                                       "\", error: " + rawMessage(e.getMessage()));
            }
        }
    }

    /** Opens the database and connects to it. */
    static Connected startup(String database, boolean readOnly, Properties props, boolean cacheInstance)
        throws SQLException {
        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        List<String[]> runtimeOptions = new ArrayList<>();
        names.add("duckdb_api");
        values.add("java");
        if (readOnly) {
            names.add("access_mode");
            values.add("READ_ONLY");
        }
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = valueToText(entry.getValue());
            if (value == null) {
                continue;
            }
            if (JDBC_STREAM_RESULTS_OPTION.equals(key) || isRuntimeOption(key)) {
                runtimeOptions.add(new String[] {key, value});
            } else {
                names.add(key);
                values.add(value);
            }
        }

        boolean shouldCache = cacheInstance && !database.equals(MEMORY_PREFIX) && !database.isEmpty();
        InstanceRef ref;
        if (shouldCache) {
            ByteBuffer instance;
            try {
                instance = duckdb_v2_instance_open_shared(defaultEnvironment(), database.getBytes(UTF_8), utf8(names),
                                                          utf8(values), inst -> initInstance(inst, runtimeOptions));
            } catch (SQLException e) {
                throw new SQLException("Connection Error: " + rawMessage(e.getMessage()), e.getSQLState(),
                                       e.getErrorCode(), e);
            }
            ref = new InstanceRef(null, instance, hashOfPath(database) | 1L);
        } else {
            ByteBuffer environment = duckdb_v2_environment_create();
            ByteBuffer instance = null;
            try {
                instance = duckdb_v2_instance_create_with_options(environment, utf8(names), utf8(values));
                if (database.length() > MEMORY_PREFIX.length() && database.startsWith(MEMORY_PREFIX)) {
                    // a named in-memory database
                    duckdb_v2_instance_attach(instance, database.substring(MEMORY_PREFIX.length()).getBytes(UTF_8),
                                              MEMORY_PREFIX.getBytes(UTF_8), true);
                } else {
                    duckdb_v2_instance_attach(instance, null,
                                              (database.isEmpty() ? MEMORY_PREFIX : database).getBytes(UTF_8), true);
                }
                initInstance(instance, runtimeOptions);
            } catch (SQLException | RuntimeException e) {
                if (instance != null) {
                    duckdb_v2_instance_destroy(instance);
                }
                duckdb_v2_environment_destroy(environment);
                throw e;
            }
            ref = new InstanceRef(environment, instance, NEXT_ADDRESS.incrementAndGet() << 1);
        }
        try {
            return new Connected(ref, duckdb_v2_connection_create(ref.instance()));
        } catch (SQLException | RuntimeException e) {
            ref.release();
            throw e;
        }
    }

    private static long hashOfPath(String database) {
        String key;
        try {
            key = Paths.get(database).toAbsolutePath().normalize().toString();
        } catch (RuntimeException e) {
            key = database;
        }
        return key.hashCode() & 0xffffffffL;
    }

    /** Connects to the instance of the connection, the session settings of the connection are copied. */
    static Connected connect(DuckDBConnection source) throws SQLException {
        InstanceRef ref = source.instanceRef.retain();
        ByteBuffer conn = null;
        try {
            conn = duckdb_v2_connection_create(ref.instance());
            copySessionSettings(source.connRef, conn);
            return new Connected(ref, conn);
        } catch (SQLException | RuntimeException e) {
            if (conn != null) {
                duckdb_v2_connection_destroy(conn);
            }
            ref.release();
            throw e;
        }
    }

    /** Copies the session settings of the connection to the other connection. */
    static void copySessionSettings(ByteBuffer from, ByteBuffer to) throws SQLException {
        ByteBuffer fromConfig = duckdb_v2_connection_get_config(from);
        ByteBuffer toConfig = duckdb_v2_connection_get_config(to);
        long count = duckdb_v2_config_get_option_count(fromConfig);
        int[] scope = new int[1];
        for (long i = 0; i < count; i++) {
            ByteBuffer option = duckdb_v2_config_get_option_by_index(fromConfig, i);
            try {
                if (!duckdb_v2_option_supports_scope(option, SCOPE_SESSION)) {
                    continue;
                }
                byte[] name = duckdb_v2_option_get_name(option);
                ByteBuffer value = duckdb_v2_config_get_option_value_scoped(fromConfig, name, scope);
                try {
                    if (scope[0] == SCOPE_SESSION) {
                        duckdb_v2_config_set_option_value(toConfig, name, value, SCOPE_SESSION);
                    }
                } finally {
                    duckdb_v2_value_destroy(value);
                }
            } catch (SQLException e) {
                // the setting cannot be copied
            } finally {
                duckdb_v2_option_destroy(option);
            }
        }
    }

    /** Closes the connection and releases its reference to the instance. */
    static void disconnect(ByteBuffer conn, InstanceRef instanceRef) {
        try {
            duckdb_v2_connection_destroy(conn);
        } catch (SQLException e) {
            // nothing else can be done
        } finally {
            instanceRef.release();
        }
    }
}
