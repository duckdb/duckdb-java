package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;

/**
 * Appends the data chunks to a table, using the C API v2 that has no appender: the chunks are collected into a
 * column data collection, that is read by a prepared {@code INSERT INTO table SELECT * FROM <collection>}.
 */
final class DuckDBV2Appender {
    private static final byte[] BUFFER_NAME = "__duckdb_appender_buffer".getBytes(UTF_8);

    private final DuckDBConnection conn;
    private final String catalog;
    private final String schema;
    private final String table;
    private final ByteBuffer[] types;
    private ByteBuffer statement;
    private ByteBuffer collection;
    private ByteBuffer appendState;
    // the SQL texts of the default values of the columns, they are loaded when the first default is appended
    private List<String> defaults;
    private String error = "";

    private DuckDBV2Appender(DuckDBConnection conn, String catalog, String schema, String table, ByteBuffer[] types) {
        this.conn = conn;
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.types = types;
    }

    static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    static String quoteLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static ByteBuffer parseSingle(ByteBuffer connHandle, String sql) throws SQLException {
        return DuckDBCore.parseSingleStatement(connHandle, sql);
    }

    /** Creates an appender to the table, the catalog and the schema can be null. */
    static DuckDBV2Appender create(DuckDBConnection conn, String catalog, String schema, String table)
        throws SQLException {
        StringBuilder qualified = new StringBuilder();
        if (catalog != null) {
            qualified.append(quoteIdentifier(catalog)).append('.');
        }
        if (schema != null) {
            qualified.append(quoteIdentifier(schema)).append('.');
        }
        qualified.append(quoteIdentifier(table));

        ByteBuffer connHandle = duckdb_v2_connection_from_ref(conn.connRef);
        ByteBuffer factory = duckdb_v2_connection_get_factory(connHandle);

        // the columns of the table: binding reports them without reading a row
        ByteBuffer probe = parseSingle(connHandle, "SELECT * FROM " + qualified);
        ByteBuffer[] schemas = null;
        ByteBuffer[] types;
        String[] names;
        try {
            schemas = duckdb_v2_statement_bind(connHandle, probe);
            int count = (int) duckdb_v2_schema_get_count(schemas[0]);
            if (count == 0) {
                throw new SQLException("Invalid Input Error: table " + qualified + " has no columns to append to");
            }
            types = new ByteBuffer[count];
            names = new String[count];
            try {
                for (int i = 0; i < count; i++) {
                    types[i] = duckdb_v2_logical_type_copy(duckdb_v2_schema_get_field_type(schemas[0], i));
                    names[i] = new String(duckdb_v2_schema_get_field_name(schemas[0], i), UTF_8);
                }
            } catch (SQLException e) {
                destroyTypes(types);
                throw e;
            }
        } finally {
            if (schemas != null) {
                duckdb_v2_schema_destroy(schemas[0]);
                if (schemas[1] != null) {
                    duckdb_v2_schema_destroy(schemas[1]);
                }
            }
            duckdb_v2_sql_statement_destroy(probe);
        }

        StringBuilder columns = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                columns.append(", ");
            }
            columns.append(quoteIdentifier(names[i]));
        }
        String insert =
            "INSERT INTO " + qualified + " (" + columns + ") SELECT * FROM " + new String(BUFFER_NAME, UTF_8);

        DuckDBV2Appender appender = new DuckDBV2Appender(conn, catalog, schema, table, types);
        boolean ok = false;
        try {
            appender.statement = parseSingle(connHandle, insert);
            appender.collection = duckdb_v2_column_data_collection_create(factory, types);
            appender.appendState = duckdb_v2_column_data_collection_append_state_create(appender.collection);
            byte[][] nameBytes = new byte[names.length][];
            for (int i = 0; i < names.length; i++) {
                nameBytes[i] = names[i].getBytes(UTF_8);
            }
            duckdb_v2_statement_add_collection(appender.statement, BUFFER_NAME, appender.collection, nameBytes);
            ok = true;
            return appender;
        } finally {
            if (!ok) {
                appender.destroy();
            }
        }
    }

    private static void destroyTypes(ByteBuffer[] types) {
        for (ByteBuffer type : types) {
            if (type != null) {
                duckdb_destroy_logical_type(type);
            }
        }
    }

    long columnCount() {
        return types.length;
    }

    /** Returns a copy of the type of the column, that is owned by the caller, or null for an invalid column. */
    ByteBuffer columnType(long idx) throws SQLException {
        if (idx < 0 || idx >= types.length) {
            return null;
        }
        return duckdb_v2_logical_type_copy(types[(int) idx]);
    }

    /** The message of the last failure. */
    byte[] error() {
        return error.getBytes(UTF_8);
    }

    private int fail(SQLException e) {
        error = e.getMessage() == null ? e.toString() : e.getMessage();
        return 1;
    }

    /** Appends the chunk (that is borrowed for the call) to the buffer, returns 0 on success. */
    int appendChunk(ByteBuffer chunk) {
        try {
            duckdb_v2_column_data_collection_append(collection, appendState, chunk);
            return 0;
        } catch (SQLException e) {
            return fail(e);
        }
    }

    private void resetBuffer() throws SQLException {
        // the append state references the segments that are released by the clear
        duckdb_v2_column_data_collection_append_state_destroy(appendState);
        appendState = null;
        duckdb_v2_column_data_collection_clear(collection);
        appendState = duckdb_v2_column_data_collection_append_state_create(collection);
    }

    /** Appends the buffered rows to the table, returns 0 on success. */
    int flush() {
        try {
            if (duckdb_v2_column_data_collection_row_count(collection) == 0) {
                return 0;
            }
            conn.queryGate.acquireUninterruptibly();
            try {
                ByteBuffer connHandle = duckdb_v2_connection_from_ref(conn.connRef);
                ByteBuffer result = duckdb_v2_statement_execute(connHandle, statement, null);
                try {
                    duckdb_v2_result_drain(result);
                } finally {
                    duckdb_v2_result_destroy(result);
                }
            } finally {
                conn.queryGate.release();
            }
            resetBuffer();
            return 0;
        } catch (SQLException e) {
            int state = fail(e);
            try {
                // a failed flush drops the rows, a retry must not run the same failing statement over them
                resetBuffer();
            } catch (SQLException ignored) {
                // nothing else can be done
            }
            return state;
        }
    }

    int close() {
        return flush();
    }

    /** Appends the default value of the column to the row of the chunk, returns 0 on success. */
    int appendDefaultToChunk(ByteBuffer chunk, long col, long row) {
        try {
            if (defaults == null) {
                // the default values of all columns of the table, in the order of the columns
                String sql = "SELECT column_default FROM duckdb_columns() WHERE table_name = " + quoteLiteral(table);
                if (schema != null) {
                    sql += " AND schema_name = " + quoteLiteral(schema);
                }
                if (catalog != null) {
                    sql += " AND database_name = " + quoteLiteral(catalog);
                }
                sql += " ORDER BY column_index";
                defaults = DuckDBCore.queryStrings(conn, sql);
            }
            ByteBuffer vec = duckdb_data_chunk_get_vector(chunk, col);
            // the size of the vector is set by the caller when the chunk is complete
            if (duckdb_v2_vector_get_size(vec) <= row) {
                duckdb_v2_vector_set_size(vec, row + 1);
            }
            String defaultSql = col < defaults.size() ? defaults.get((int) col) : null;
            ByteBuffer value;
            if (defaultSql != null) {
                // the default value is an expression, that is evaluated every time as it can be volatile
                value = DuckDBCore.queryValue(conn, "SELECT " + defaultSql);
            } else {
                // a column without a default value gets the NULL default
                value =
                    DuckDBCore.nullValue(duckdb_v2_connection_get_factory(duckdb_v2_connection_from_ref(conn.connRef)));
            }
            try {
                duckdb_v2_vector_set_value(vec, row, value);
            } finally {
                duckdb_v2_value_destroy(value);
            }
            return 0;
        } catch (SQLException e) {
            return fail(e);
        }
    }

    /** Releases the resources of the appender, the buffered rows are not appended. */
    void destroy() {
        try {
            if (appendState != null) {
                duckdb_v2_column_data_collection_append_state_destroy(appendState);
                appendState = null;
            }
            if (statement != null) {
                duckdb_v2_sql_statement_destroy(statement);
                statement = null;
            }
            if (collection != null) {
                duckdb_v2_column_data_collection_destroy(collection);
                collection = null;
            }
        } catch (SQLException e) {
            // nothing else can be done
        }
        destroyTypes(types);
        for (int i = 0; i < types.length; i++) {
            types[i] = null;
        }
    }
}
