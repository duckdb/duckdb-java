package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.duckdb.user.DuckDBMap;

/**
 * The logic of the driver that works on top of the C API v2 entry points of {@link DuckDBBindings}: preparing,
 * executing, reading the results and converting the parameters.
 */
final class DuckDBCore {
    private DuckDBCore() {
    }

    private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    // type IDs of the C API
    private static final int TYPE_BOOLEAN = 1;
    private static final int TYPE_TINYINT = 2;
    private static final int TYPE_SMALLINT = 3;
    private static final int TYPE_INTEGER = 4;
    private static final int TYPE_BIGINT = 5;
    private static final int TYPE_UTINYINT = 6;
    private static final int TYPE_USMALLINT = 7;
    private static final int TYPE_UINTEGER = 8;
    private static final int TYPE_UBIGINT = 9;
    private static final int TYPE_FLOAT = 10;
    private static final int TYPE_DOUBLE = 11;
    private static final int TYPE_TIMESTAMP = 12;
    private static final int TYPE_DATE = 13;
    private static final int TYPE_TIME = 14;
    private static final int TYPE_INTERVAL = 15;
    private static final int TYPE_HUGEINT = 16;
    private static final int TYPE_VARCHAR = 17;
    private static final int TYPE_BLOB = 18;
    private static final int TYPE_DECIMAL = 19;
    private static final int TYPE_TIMESTAMP_S = 20;
    private static final int TYPE_TIMESTAMP_MS = 21;
    private static final int TYPE_TIMESTAMP_NS = 22;
    private static final int TYPE_ENUM = 23;
    private static final int TYPE_LIST = 24;
    private static final int TYPE_STRUCT = 25;
    private static final int TYPE_MAP = 26;
    private static final int TYPE_UUID = 27;
    private static final int TYPE_UNION = 28;
    private static final int TYPE_BIT = 29;
    private static final int TYPE_TIME_TZ = 30;
    private static final int TYPE_TIMESTAMP_TZ = 31;
    private static final int TYPE_UHUGEINT = 32;
    private static final int TYPE_ARRAY = 33;
    private static final int TYPE_TIME_NS = 39;
    private static final int TYPE_GEOMETRY = 40;
    private static final int TYPE_VARIANT = 41;

    // DUCKDB_V2_STATEMENT_TYPE
    private static final int STATEMENT_SELECT = 1;
    private static final int STATEMENT_INSERT = 2;
    private static final int STATEMENT_UPDATE = 3;
    private static final int STATEMENT_DELETE = 5;
    private static final int STATEMENT_COPY = 10;
    private static final int STATEMENT_MERGE_INTO = 30;

    private static final byte[] STREAM_RESULTS_OPTION = "jdbc_stream_results".getBytes(UTF_8);

    /** A prepared statement. */
    static final class StatementRef {
        final DuckDBConnection conn;
        final int statementType;
        final long parameterCount;
        ByteBuffer prepared;
        ByteBuffer outputSchema;
        ByteBuffer parametersSchema;

        StatementRef(DuckDBConnection conn, int statementType, ByteBuffer prepared, ByteBuffer outputSchema,
                     ByteBuffer parametersSchema, long parameterCount) {
            this.conn = conn;
            this.statementType = statementType;
            this.prepared = prepared;
            this.outputSchema = outputSchema;
            this.parametersSchema = parametersSchema;
            this.parameterCount = parameterCount;
        }
    }

    /** A query that is submitted but is not completed yet. */
    static final class PendingRef {
        final DuckDBConnection conn;
        final int statementType;
        final boolean streaming;
        ByteBuffer result;

        PendingRef(DuckDBConnection conn, int statementType, boolean streaming, ByteBuffer result) {
            this.conn = conn;
            this.statementType = statementType;
            this.streaming = streaming;
            this.result = result;
        }
    }

    /**
     * A query result, that is either streamed from the query or is read into the memory when the query is
     * completed.
     */
    static final class ResultRef {
        final ByteBuffer connRef;
        final int resultType;
        final String[] names;
        // the result that is streamed, null if the result is in the memory
        ByteBuffer streamResult;
        // the chunks that are read but are not consumed yet
        final ArrayDeque<ByteBuffer> chunks = new ArrayDeque<>();
        // the chunk that is being consumed, it keeps the memory of the vectors of the fetched DuckDBVector-s
        ByteBuffer currentChunk;
        ByteBuffer schema;

        ResultRef(ByteBuffer connRef, ByteBuffer streamResult, ByteBuffer schema, int resultType, String[] names) {
            this.connRef = connRef;
            this.streamResult = streamResult;
            this.schema = schema;
            this.resultType = resultType;
            this.names = names;
        }
    }

    private static SQLException invalid(String message) {
        return new SQLException("Invalid Input Error: " + message);
    }

    private static ByteBuffer conn(ByteBuffer connRef) throws SQLException {
        if (connRef == null) {
            throw invalid("Invalid connection");
        }
        return duckdb_v2_connection_from_ref(connRef);
    }

    private static long fieldCount(ByteBuffer schema) throws SQLException {
        return schema == null ? 0 : duckdb_v2_schema_get_count(schema);
    }

    private static boolean streamResultsOption(ByteBuffer conn) {
        try {
            ByteBuffer config = duckdb_v2_connection_get_config(conn);
            ByteBuffer value = duckdb_v2_config_get_option_value(config, STREAM_RESULTS_OPTION);
            try {
                return duckdb_get_bool(value);
            } finally {
                duckdb_v2_value_destroy(value);
            }
        } catch (SQLException e) {
            return false;
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Statements
    // ---------------------------------------------------------------------------------------------------------

    private static void destroyStatements(List<ByteBuffer> statements) {
        for (ByteBuffer statement : statements) {
            try {
                duckdb_v2_sql_statement_destroy(statement);
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private static List<ByteBuffer> parse(ByteBuffer conn, byte[] query) throws SQLException {
        ByteBuffer iterator = duckdb_v2_parse_sql(conn, query);
        List<ByteBuffer> statements = new ArrayList<>();
        try {
            while (true) {
                ByteBuffer statement = duckdb_v2_statement_iterator_next(iterator);
                if (statement == null) {
                    break;
                }
                statements.add(statement);
            }
        } catch (SQLException e) {
            destroyStatements(statements);
            throw e;
        } finally {
            duckdb_v2_statement_iterator_destroy(iterator);
        }
        return statements;
    }

    /**
     * Parses the query and returns its last statement, the other statements are destroyed. The statements before
     * the last one are executed unless the query is prepared: preparing must not have side effects, so a query with
     * multiple statements is rejected then.
     */
    private static ByteBuffer extractLastStatement(ByteBuffer conn, byte[] query, boolean isPrepare)
        throws SQLException {
        List<ByteBuffer> statements = parse(conn, query);
        try {
            if (statements.isEmpty()) {
                throw invalid("No statements to execute.");
            }
            if (isPrepare && statements.size() > 1) {
                throw invalid("Cannot prepare multiple statements at once, statements count: " + statements.size());
            }
            // if there are multiple statements, we directly execute the statements besides the last one,
            // we only return the result of the last statement to the user, unless one of the previous statements fails
            for (int i = 0; i + 1 < statements.size(); i++) {
                ByteBuffer result = duckdb_v2_statement_execute(conn, statements.get(i), null);
                try {
                    duckdb_v2_result_drain(result);
                } finally {
                    duckdb_v2_result_destroy(result);
                }
            }
            return statements.remove(statements.size() - 1);
        } finally {
            destroyStatements(statements);
        }
    }

    static StatementRef prepare(DuckDBConnection connection, byte[] query) throws SQLException {
        connection.queryGate.acquireUninterruptibly();
        try {
            return prepareLocked(connection, query);
        } finally {
            connection.queryGate.release();
        }
    }

    private static StatementRef prepareLocked(DuckDBConnection connection, byte[] query) throws SQLException {
        ByteBuffer conn = conn(connection.connRef);
        ByteBuffer statement = extractLastStatement(conn, query, true);
        ByteBuffer prepared = null;
        ByteBuffer[] schemas = null;
        try {
            int statementType = duckdb_v2_sql_statement_get_type(statement);
            prepared = duckdb_v2_prepared_statement_create(conn, statement, false);
            schemas = duckdb_v2_statement_bind(conn, statement);
            long parameterCount = fieldCount(schemas[1]);
            StatementRef ref =
                new StatementRef(connection, statementType, prepared, schemas[0], schemas[1], parameterCount);
            prepared = null;
            schemas = null;
            return ref;
        } finally {
            if (prepared != null) {
                duckdb_v2_prepared_statement_destroy(prepared);
            }
            if (schemas != null) {
                duckdb_v2_schema_destroy(schemas[0]);
                if (schemas[1] != null) {
                    duckdb_v2_schema_destroy(schemas[1]);
                }
            }
            duckdb_v2_sql_statement_destroy(statement);
        }
    }

    static void release(StatementRef ref) throws SQLException {
        if (ref == null || ref.prepared == null) {
            return;
        }
        ByteBuffer prepared = ref.prepared;
        ref.prepared = null;
        try {
            duckdb_v2_prepared_statement_destroy(prepared);
        } finally {
            if (ref.outputSchema != null) {
                duckdb_v2_schema_destroy(ref.outputSchema);
                ref.outputSchema = null;
            }
            if (ref.parametersSchema != null) {
                duckdb_v2_schema_destroy(ref.parametersSchema);
                ref.parametersSchema = null;
            }
        }
    }

    // The query gate of the connection is held until the pending query is executed or released
    static PendingRef pendingQuery(DuckDBConnection connection, byte[] query) throws SQLException {
        connection.queryGate.acquireUninterruptibly();
        boolean ok = false;
        try {
            ByteBuffer conn = conn(connection.connRef);
            ByteBuffer statement = extractLastStatement(conn, query, false);
            try {
                int statementType = duckdb_v2_sql_statement_get_type(statement);
                boolean streaming = streamResultsOption(conn);
                ByteBuffer result = duckdb_v2_statement_execute(conn, statement, null);
                PendingRef ref = new PendingRef(connection, statementType, streaming, result);
                ok = true;
                return ref;
            } finally {
                duckdb_v2_sql_statement_destroy(statement);
            }
        } finally {
            if (!ok) {
                connection.queryGate.release();
            }
        }
    }

    static void releasePending(PendingRef ref) throws SQLException {
        if (ref == null || ref.result == null) {
            return;
        }
        ByteBuffer result = ref.result;
        ref.result = null;
        try {
            duckdb_v2_result_destroy(result);
        } finally {
            ref.conn.queryGate.release();
        }
    }

    static ResultRef executePending(PendingRef ref) throws SQLException {
        if (ref == null || ref.result == null) {
            throw invalid("Invalid pending query");
        }
        ByteBuffer result = ref.result;
        ref.result = null;
        try {
            // the pending result was submitted from the SQL statement as written, so its type is the written type
            return completeResult(ref.conn.connRef, result, ref.statementType, ref.streaming);
        } finally {
            ref.conn.queryGate.release();
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Execution
    // ---------------------------------------------------------------------------------------------------------

    private static StatementRef checkStatement(StatementRef ref) throws SQLException {
        if (ref == null || ref.prepared == null) {
            throw invalid("Invalid statement");
        }
        return ref;
    }

    private static ByteBuffer executePrepared(StatementRef ref, Object[] params) throws SQLException {
        checkStatement(ref);
        ByteBuffer conn = conn(ref.conn.connRef);
        int paramLen = params == null ? 0 : params.length;
        if (paramLen != ref.parameterCount) {
            throw invalid("Parameter count mismatch");
        }
        ByteBuffer factory = duckdb_v2_connection_get_factory(conn);
        ByteBuffer[] values = new ByteBuffer[paramLen];
        try {
            for (int i = 0; i < paramLen; i++) {
                values[i] = toValue(factory, params[i]);
            }
            return duckdb_v2_prepared_statement_execute(ref.prepared, values);
        } finally {
            for (ByteBuffer value : values) {
                if (value != null) {
                    duckdb_v2_value_destroy(value);
                }
            }
        }
    }

    static ResultRef execute(StatementRef ref, Object[] params) throws SQLException {
        checkStatement(ref);
        ref.conn.queryGate.acquireUninterruptibly();
        try {
            boolean streamResults = streamResultsOption(conn(ref.conn.connRef));
            ByteBuffer result = executePrepared(ref, params);
            return completeResult(ref.conn.connRef, result, ref.statementType, streamResults);
        } finally {
            ref.conn.queryGate.release();
        }
    }

    static ByteBuffer executeChunked(StatementRef ref, Object[] params) throws SQLException {
        checkStatement(ref);
        ref.conn.queryGate.acquireUninterruptibly();
        try {
            ByteBuffer result = executePrepared(ref, params);
            return duckdb_v2_chunked_result_create(result);
        } finally {
            ref.conn.queryGate.release();
        }
    }

    // Streaming is only used for the SELECT statements
    private static boolean streamsByDefault(int writtenType, int resultType) {
        return writtenType == STATEMENT_SELECT && resultType == RESULT_TYPE_QUERY_RESULT;
    }

    /**
     * Completes the execution of the result: the rows are read into the memory, or the result is kept to be streamed.
     * The result is owned by the returned reference (or is destroyed on failure).
     */
    private static ResultRef completeResult(ByteBuffer connRef, ByteBuffer result, int writtenType,
                                            boolean streamResultsUser) throws SQLException {
        ByteBuffer firstChunk = null;
        ByteBuffer schema = null;
        ResultRef ref = null;
        try {
            // the first chunk is read to find out if the query is running without errors
            firstChunk = duckdb_v2_result_fetch_chunk(result);
            schema = duckdb_v2_result_get_schema(result);
            int resultType = duckdb_v2_result_get_result_type(result);
            long count = fieldCount(schema);
            String[] names = new String[(int) count];
            for (int i = 0; i < count; i++) {
                names[i] = new String(duckdb_v2_schema_get_field_name(schema, i), UTF_8);
            }
            boolean stream = streamResultsUser && streamsByDefault(writtenType, resultType);
            ref = new ResultRef(connRef, stream ? result : null, schema, resultType, names);
            // the reference owns the schema and the streamed result now
            schema = null;
            ByteBuffer ownedResult = result;
            if (stream) {
                result = null;
            }
            if (firstChunk != null) {
                ref.chunks.add(firstChunk);
                firstChunk = null;
            }
            if (!stream) {
                ByteBuffer chunk;
                while ((chunk = duckdb_v2_result_fetch_chunk(ownedResult)) != null) {
                    ref.chunks.add(chunk);
                }
            }
            ResultRef res = ref;
            ref = null;
            return res;
        } finally {
            if (ref != null) {
                freeResult(ref);
            }
            if (firstChunk != null) {
                duckdb_destroy_data_chunk(firstChunk);
            }
            if (schema != null) {
                duckdb_v2_schema_destroy(schema);
            }
            if (result != null) {
                duckdb_v2_result_destroy(result);
            }
        }
    }

    static void freeResult(ResultRef ref) throws SQLException {
        if (ref == null) {
            return;
        }
        if (ref.currentChunk != null) {
            duckdb_destroy_data_chunk(ref.currentChunk);
            ref.currentChunk = null;
        }
        ByteBuffer chunk;
        while ((chunk = ref.chunks.poll()) != null) {
            duckdb_destroy_data_chunk(chunk);
        }
        if (ref.streamResult != null) {
            ByteBuffer result = ref.streamResult;
            ref.streamResult = null;
            duckdb_v2_result_destroy(result);
        }
        if (ref.schema != null) {
            ByteBuffer schema = ref.schema;
            ref.schema = null;
            duckdb_v2_schema_destroy(schema);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Metadata
    // ---------------------------------------------------------------------------------------------------------

    private static String returnTypeName(int resultType) {
        switch (resultType) {
        case RESULT_TYPE_QUERY_RESULT:
            return "QUERY_RESULT";
        case RESULT_TYPE_CHANGED_ROWS:
            return "CHANGED_ROWS";
        default:
            return "NOTHING";
        }
    }

    // The return type of a prepared statement is not known before it is executed, it is derived from its columns
    private static String preparedReturnType(StatementRef ref) throws SQLException {
        long count = fieldCount(ref.outputSchema);
        if (count == 0) {
            return "NOTHING";
        }
        if (count == 1 && "Count".equals(new String(duckdb_v2_schema_get_field_name(ref.outputSchema, 0), UTF_8)) &&
            duckdb_get_type_id(duckdb_v2_schema_get_field_type(ref.outputSchema, 0)) == TYPE_BIGINT) {
            switch (ref.statementType) {
            case STATEMENT_INSERT:
            case STATEMENT_UPDATE:
            case STATEMENT_DELETE:
            case STATEMENT_COPY:
            case STATEMENT_MERGE_INTO:
                return "CHANGED_ROWS";
            default:
                break;
            }
        }
        return "QUERY_RESULT";
    }

    // The text of the type, the same as the one the SQL shows
    private static String typeText(ByteBuffer factory, ByteBuffer type) throws SQLException {
        if (duckdb_get_type_id(type) == TYPE_ENUM) {
            return "ENUM";
        }
        ByteBuffer value = duckdb_v2_value_create_type(factory, type);
        try {
            return new String(duckdb_v2_value_to_string(value), UTF_8);
        } finally {
            duckdb_v2_value_destroy(value);
        }
    }

    // The name of the type for the Java side, DECIMAL types also have the width and the scale
    private static String typeDetail(ByteBuffer factory, ByteBuffer type) throws SQLException {
        int id = duckdb_get_type_id(type);
        switch (id) {
        case TYPE_DECIMAL: {
            String physical;
            switch (duckdb_decimal_internal_type(type)) {
            case TYPE_SMALLINT:
                physical = "DECIMAL16";
                break;
            case TYPE_INTEGER:
                physical = "DECIMAL32";
                break;
            case TYPE_BIGINT:
                physical = "DECIMAL64";
                break;
            default:
                physical = "DECIMAL128";
                break;
            }
            return physical + ";" + duckdb_decimal_width(type) + ";" + duckdb_decimal_scale(type);
        }
        case TYPE_BOOLEAN:
            return "BOOLEAN";
        case TYPE_TINYINT:
            return "TINYINT";
        case TYPE_SMALLINT:
            return "SMALLINT";
        case TYPE_INTEGER:
            return "INTEGER";
        case TYPE_BIGINT:
            return "BIGINT";
        case TYPE_UTINYINT:
            return "UTINYINT";
        case TYPE_USMALLINT:
            return "USMALLINT";
        case TYPE_UINTEGER:
            return "UINTEGER";
        case TYPE_UBIGINT:
            return "UBIGINT";
        case TYPE_HUGEINT:
            return "HUGEINT";
        case TYPE_UHUGEINT:
            return "UHUGEINT";
        case TYPE_FLOAT:
            return "FLOAT";
        case TYPE_DOUBLE:
            return "DOUBLE";
        case TYPE_TIMESTAMP:
            return "TIMESTAMP";
        case TYPE_TIMESTAMP_S:
            return "TIMESTAMP_S";
        case TYPE_TIMESTAMP_MS:
            return "TIMESTAMP_MS";
        case TYPE_TIMESTAMP_NS:
            return "TIMESTAMP_NS";
        case TYPE_TIMESTAMP_TZ:
            return "TIMESTAMP WITH TIME ZONE";
        case TYPE_DATE:
            return "DATE";
        case TYPE_TIME:
            return "TIME";
        case TYPE_TIME_NS:
            return "TIME_NS";
        case TYPE_TIME_TZ:
            return "TIME WITH TIME ZONE";
        case TYPE_VARCHAR:
            // JSON requires special handling because it is mapped to JsonNode class
            return "JSON".equals(typeText(factory, type)) ? "JSON" : "VARCHAR";
        case TYPE_BLOB:
            return "BLOB";
        case TYPE_UUID:
            return "UUID";
        case TYPE_ENUM:
            return "ENUM";
        case TYPE_LIST:
            return "LIST";
        case TYPE_STRUCT:
            return "STRUCT";
        case TYPE_MAP:
            return "MAP";
        case TYPE_UNION:
            return "UNION";
        case TYPE_ARRAY:
            return "ARRAY";
        case TYPE_GEOMETRY:
            return "GEOMETRY";
        case TYPE_VARIANT:
            return "VARIANT";
        case TYPE_INTERVAL:
            return "INTERVAL";
        case TYPE_BIT:
            return "BIT";
        default:
            return "UNKNOWN";
        }
    }

    private static DuckDBResultSetMetaData buildMeta(ByteBuffer factory, String[] names, ByteBuffer schema,
                                                     ByteBuffer parametersSchema, String returnType)
        throws SQLException {
        int columnCount = names.length;
        String[] typeNames = new String[columnCount];
        String[] typeDetails = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ByteBuffer type = duckdb_v2_schema_get_field_type(schema, i);
            typeNames[i] = typeText(factory, type);
            typeDetails[i] = typeDetail(factory, type);
        }
        int paramCount = (int) fieldCount(parametersSchema);
        String[] paramTypeNames = new String[paramCount];
        String[] paramTypeDetails = new String[paramCount];
        for (int i = 0; i < paramCount; i++) {
            ByteBuffer type = duckdb_v2_schema_get_field_type(parametersSchema, i);
            paramTypeNames[i] = typeText(factory, type);
            paramTypeDetails[i] = typeDetail(factory, type);
        }
        return new DuckDBResultSetMetaData(paramCount, columnCount, names, typeNames, typeDetails, returnType,
                                           paramTypeNames, paramTypeDetails);
    }

    static DuckDBResultSetMetaData queryResultMeta(ResultRef ref) throws SQLException {
        if (ref == null || ref.schema == null) {
            throw invalid("Invalid result set");
        }
        ByteBuffer factory = duckdb_v2_connection_get_factory(conn(ref.connRef));
        return buildMeta(factory, ref.names, ref.schema, null, returnTypeName(ref.resultType));
    }

    static DuckDBResultSetMetaData preparedStatementMeta(StatementRef ref) throws SQLException {
        checkStatement(ref);
        ByteBuffer factory = duckdb_v2_connection_get_factory(conn(ref.conn.connRef));
        int count = (int) fieldCount(ref.outputSchema);
        String[] names = new String[count];
        for (int i = 0; i < count; i++) {
            names[i] = new String(duckdb_v2_schema_get_field_name(ref.outputSchema, i), UTF_8);
        }
        return buildMeta(factory, names, ref.outputSchema, ref.parametersSchema, preparedReturnType(ref));
    }

    // ---------------------------------------------------------------------------------------------------------
    // Fetching
    // ---------------------------------------------------------------------------------------------------------

    static DuckDBVector[] fetch(ResultRef ref) throws SQLException {
        if (ref == null || ref.schema == null) {
            throw invalid("Invalid result set");
        }
        ByteBuffer connHandle = conn(ref.connRef);
        if (ref.currentChunk != null) {
            duckdb_destroy_data_chunk(ref.currentChunk);
            ref.currentChunk = null;
        }
        ByteBuffer chunk = ref.chunks.poll();
        if (chunk == null && ref.streamResult != null) {
            chunk = duckdb_v2_result_fetch_chunk(ref.streamResult);
        }
        if (chunk == null) {
            return new DuckDBVector[0];
        }
        ref.currentChunk = chunk;
        int rowCount = (int) duckdb_data_chunk_get_size(chunk);
        int columnCount = (int) duckdb_data_chunk_get_column_count(chunk);
        ByteBuffer factory = duckdb_v2_connection_get_factory(connHandle);
        DuckDBVector[] vectors = new DuckDBVector[columnCount];
        for (int col = 0; col < columnCount; col++) {
            vectors[col] = processVector(factory, duckdb_data_chunk_get_vector(chunk, col), rowCount);
        }
        return vectors;
    }

    static String[] castResultToStrings(ResultRef ref, long colIdx) throws SQLException {
        if (ref == null || ref.schema == null) {
            throw invalid("Invalid result set");
        }
        if (ref.currentChunk == null) {
            return null;
        }
        int rowCount = (int) duckdb_data_chunk_get_size(ref.currentChunk);
        ByteBuffer vec = duckdb_data_chunk_get_vector(ref.currentChunk, colIdx);
        String[] res = new String[rowCount];
        for (int row = 0; row < rowCount; row++) {
            res[row] = valueToString(vec, row);
        }
        return res;
    }

    // Returns null for a NULL value
    private static String valueToString(ByteBuffer vec, int row) throws SQLException {
        ByteBuffer value = duckdb_v2_vector_get_value(vec, row);
        try {
            if (duckdb_is_null_value(value)) {
                return null;
            }
            return new String(duckdb_v2_value_to_string(value), UTF_8);
        } finally {
            duckdb_v2_value_destroy(value);
        }
    }

    private static boolean[] nullMask(ByteBuffer vec, int rowCount) {
        boolean[] nulls = new boolean[rowCount];
        ByteBuffer validity = duckdb_vector_get_validity(vec, rowCount);
        if (validity != null) {
            validity.order(NATIVE_ORDER);
            for (int row = 0; row < rowCount; row++) {
                long mask = validity.getLong((row >> 6) << 3);
                nulls[row] = (mask & (1L << (row & 63))) == 0;
            }
        }
        return nulls;
    }

    private static int fixedWidth(int typeId, ByteBuffer type) throws SQLException {
        switch (typeId) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
        case TYPE_UTINYINT:
            return 1;
        case TYPE_SMALLINT:
        case TYPE_USMALLINT:
            return 2;
        case TYPE_INTEGER:
        case TYPE_UINTEGER:
        case TYPE_FLOAT:
        case TYPE_DATE:
            return 4;
        case TYPE_BIGINT:
        case TYPE_UBIGINT:
        case TYPE_DOUBLE:
        case TYPE_TIME:
        case TYPE_TIME_NS:
        case TYPE_TIME_TZ:
        case TYPE_TIMESTAMP:
        case TYPE_TIMESTAMP_S:
        case TYPE_TIMESTAMP_MS:
        case TYPE_TIMESTAMP_NS:
        case TYPE_TIMESTAMP_TZ:
            return 8;
        case TYPE_HUGEINT:
        case TYPE_UHUGEINT:
        case TYPE_UUID:
            return 16;
        case TYPE_DECIMAL:
            switch (duckdb_decimal_internal_type(type)) {
            case TYPE_SMALLINT:
                return 2;
            case TYPE_INTEGER:
                return 4;
            case TYPE_BIGINT:
                return 8;
            case TYPE_HUGEINT:
                return 16;
            default:
                throw new SQLException("Unimplemented physical type for decimal");
            }
        default:
            return 0;
        }
    }

    static DuckDBVector processVector(ByteBuffer factory, ByteBuffer vec, int rowCount) throws SQLException {
        duckdb_v2_vector_flatten(vec);
        ByteBuffer type = duckdb_vector_get_column_type(vec);
        try {
            int typeId = duckdb_get_type_id(type);
            boolean[] nulls = nullMask(vec, rowCount);
            DuckDBVector jvec = new DuckDBVector(typeDetail(factory, type), rowCount, nulls);

            int width = fixedWidth(typeId, type);
            if (width > 0) {
                jvec.constlen_data = duckdb_vector_get_data(vec, (long) rowCount * width);
                return jvec;
            }

            switch (typeId) {
            case TYPE_STRUCT:
            case TYPE_UNION:
                processStruct(factory, vec, type, jvec, rowCount);
                return jvec;
            case TYPE_BLOB:
            case TYPE_GEOMETRY: {
                byte[][] blobs = new byte[rowCount][];
                ByteBuffer data = duckdb_vector_get_data(vec, (long) rowCount * 16);
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        blobs[row] = duckdb_vector_get_string(data, row);
                    }
                }
                jvec.varlen_data = blobs;
                return jvec;
            }
            case TYPE_ARRAY: {
                int size = (int) duckdb_array_type_array_size(type);
                DuckDBVector child =
                    processVector(factory, duckdb_array_vector_get_child(vec), Math.multiplyExact(rowCount, size));
                DuckDBArray[] arrays = new DuckDBArray[rowCount];
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        arrays[row] = new DuckDBArray(child, row * size, size);
                    }
                }
                jvec.varlen_data = arrays;
                return jvec;
            }
            case TYPE_MAP:
            case TYPE_LIST: {
                ByteBuffer entries = duckdb_vector_get_data(vec, (long) rowCount * 16).order(NATIVE_ORDER);
                int listSize = (int) duckdb_list_vector_get_size(vec);
                DuckDBVector child = processVector(factory, duckdb_list_vector_get_child(vec), listSize);
                DuckDBArray[] arrays = new DuckDBArray[rowCount];
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        int offset = (int) entries.getLong(row * 16);
                        int length = (int) entries.getLong(row * 16 + 8);
                        arrays[row] = new DuckDBArray(child, offset, length);
                    }
                }
                jvec.varlen_data = arrays;
                return jvec;
            }
            case TYPE_VARIANT: {
                Object[] variants = new Object[rowCount];
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        variants[row] = variantToVector(factory, vec, row);
                    }
                }
                jvec.varlen_data = variants;
                return jvec;
            }
            case TYPE_VARCHAR: {
                String[] strings = new String[rowCount];
                ByteBuffer data = duckdb_vector_get_data(vec, (long) rowCount * 16);
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        strings[row] = new String(duckdb_vector_get_string(data, row), UTF_8);
                    }
                }
                jvec.varlen_data = strings;
                return jvec;
            }
            default: {
                // ENUM and the types that have no special representation are read as strings
                String[] strings = new String[rowCount];
                for (int row = 0; row < rowCount; row++) {
                    if (!nulls[row]) {
                        strings[row] = valueToString(vec, row);
                    }
                }
                jvec.varlen_data = strings;
                return jvec;
            }
            }
        } finally {
            duckdb_destroy_logical_type(type);
        }
    }

    private static void processStruct(ByteBuffer factory, ByteBuffer vec, ByteBuffer type, DuckDBVector jvec,
                                      int rowCount) throws SQLException {
        int childCount = (int) duckdb_struct_type_child_count(type);
        DuckDBVector[] columns = new DuckDBVector[childCount];
        String[] names = new String[childCount];
        for (int i = 0; i < childCount; i++) {
            columns[i] = processVector(factory, duckdb_struct_vector_get_child(vec, i), rowCount);
            byte[] name = duckdb_struct_type_child_name(type, i);
            // the members of a TUPLE are unnamed, use the positional names: "element1", "element2", ...
            names[i] = name == null || name.length == 0 ? "element" + (i + 1) : new String(name, UTF_8);
        }
        String typeName = typeText(factory, type);
        DuckDBStruct[] structs = new DuckDBStruct[rowCount];
        for (int row = 0; row < rowCount; row++) {
            structs[row] = new DuckDBStruct(names, columns, row, typeName);
        }
        jvec.varlen_data = structs;
    }

    // The value of a VARIANT is read as a vector of one row of the type of the value
    private static DuckDBVector variantToVector(ByteBuffer factory, ByteBuffer vec, int row) throws SQLException {
        ByteBuffer variant = duckdb_v2_vector_get_value(vec, row);
        ByteBuffer value = null;
        ByteBuffer type = null;
        ByteBuffer single = null;
        try {
            value = duckdb_v2_value_variant_to_value(variant);
            if (duckdb_is_null_value(value)) {
                return null;
            }
            type = duckdb_v2_value_get_logical_type(value);
            single = duckdb_create_vector(type);
            duckdb_v2_vector_set_size(single, 1);
            duckdb_v2_vector_set_value(single, 0, value);
            DuckDBVector res = processVector(factory, single, 1);
            res.retainConstlenData();
            return res;
        } finally {
            if (single != null) {
                duckdb_destroy_vector(single);
            }
            if (type != null) {
                duckdb_destroy_logical_type(type);
            }
            if (value != null) {
                duckdb_v2_value_destroy(value);
            }
            duckdb_v2_value_destroy(variant);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Parameters
    // ---------------------------------------------------------------------------------------------------------

    private static ByteBuffer parseType(ByteBuffer factory, String text) throws SQLException {
        return duckdb_v2_factory_create_type_from_text(factory, text.getBytes(UTF_8));
    }

    private static ByteBuffer nullValue(ByteBuffer factory) throws SQLException {
        ByteBuffer type = parseType(factory, "NULL");
        try {
            return duckdb_v2_value_create_null(factory, type);
        } finally {
            duckdb_destroy_logical_type(type);
        }
    }

    private static void destroyValues(List<ByteBuffer> values) {
        for (ByteBuffer value : values) {
            try {
                duckdb_v2_value_destroy(value);
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private static ByteBuffer decimalValue(ByteBuffer factory, BigDecimal decimal) throws SQLException {
        int precision = decimal.precision();
        int scale = decimal.scale();
        // Java BigDecimal type can have scale that exceeds the precision
        // Which our DECIMAL type does not support (assert(width >= scale))
        if (scale > precision) {
            precision = scale;
        }
        // DECIMAL scale is unsigned, so negative values are not supported
        if (scale < 0) {
            throw invalid("Converting from a BigDecimal with negative scale is not supported");
        }
        if (precision > 38) {
            return nullValue(factory);
        }
        BigInteger unscaled = decimal.unscaledValue();
        return duckdb_v2_value_create_decimal(factory, unscaled.longValue(), unscaled.shiftRight(64).longValue(),
                                              precision, scale);
    }

    static ByteBuffer toValue(ByteBuffer factory, Object param) throws SQLException {
        param = DuckDBTimestamp.valueOf(param);
        if (param == null) {
            return nullValue(factory);
        } else if (param instanceof Boolean) {
            return duckdb_v2_value_create_bool(factory, (Boolean) param);
        } else if (param instanceof Byte) {
            return duckdb_v2_value_create_tinyint(factory, (Byte) param);
        } else if (param instanceof Short) {
            return duckdb_v2_value_create_smallint(factory, (Short) param);
        } else if (param instanceof Integer) {
            return duckdb_v2_value_create_int(factory, (Integer) param);
        } else if (param instanceof Long) {
            return duckdb_v2_value_create_bigint(factory, (Long) param);
        } else if (param instanceof DuckDBHugeInt) {
            DuckDBHugeInt hugeInt = (DuckDBHugeInt) param;
            return duckdb_v2_value_create_hugeint(factory, hugeInt.lower(), hugeInt.upper());
        } else if (param instanceof DuckDBTimestampTZ) { // Check for subclass before superclass!
            return duckdb_v2_value_create_timestamp_tz(factory, ((DuckDBTimestampTZ) param).getMicrosEpoch());
        } else if (param instanceof DuckDBDate) {
            return duckdb_v2_value_create_date(factory, (int) ((DuckDBDate) param).getDaysSinceEpoch());
        } else if (param instanceof DuckDBTime) {
            return duckdb_v2_value_create_time(factory, ((DuckDBTime) param).getMicrosEpoch());
        } else if (param instanceof DuckDBTimestampNS) { // Check for subclass before superclass!
            return duckdb_v2_value_create_timestamp_ns(factory, ((DuckDBTimestampNS) param).getNanosEpoch());
        } else if (param instanceof DuckDBTimestamp) {
            return duckdb_v2_value_create_timestamp(factory, ((DuckDBTimestamp) param).getMicrosEpoch());
        } else if (param instanceof Float) {
            return duckdb_v2_value_create_float(factory, (Float) param);
        } else if (param instanceof Double) {
            return duckdb_v2_value_create_double(factory, (Double) param);
        } else if (param instanceof BigDecimal) {
            return decimalValue(factory, (BigDecimal) param);
        } else if (param instanceof String) {
            return duckdb_v2_value_create_varchar(factory, ((String) param).getBytes(UTF_8));
        } else if (param instanceof byte[]) {
            return duckdb_v2_value_create_blob(factory, (byte[]) param);
        } else if (param instanceof UUID) {
            UUID uuid = (UUID) param;
            // Account for the following logic in UUID::FromString:
            // Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
            long upper = uuid.getMostSignificantBits() ^ Long.MIN_VALUE;
            return duckdb_v2_value_create_uuid(factory, uuid.getLeastSignificantBits(), upper);
        } else if (param instanceof DuckDBMap) {
            return mapValue(factory, (DuckDBMap<?, ?>) param);
        } else if (param instanceof Struct) {
            return structValue(factory, (Struct) param);
        } else if (param instanceof java.sql.Array) {
            return arrayValue(factory, (java.sql.Array) param);
        }
        throw invalid("Unsupported parameter type");
    }

    private static ByteBuffer typeParam(ByteBuffer type, long index) throws SQLException {
        ByteBuffer value = duckdb_v2_logical_type_get_param_value(type, index);
        try {
            return duckdb_v2_value_get_type(value);
        } finally {
            duckdb_v2_value_destroy(value);
        }
    }

    private static ByteBuffer mapValue(ByteBuffer factory, DuckDBMap<?, ?> map) throws SQLException {
        ByteBuffer mapType = parseType(factory, map.getSQLTypeName());
        ByteBuffer keyType = null;
        ByteBuffer valueType = null;
        List<ByteBuffer> keys = new ArrayList<>();
        List<ByteBuffer> values = new ArrayList<>();
        try {
            if (duckdb_v2_logical_type_get_param_count(mapType) >= 2) {
                keyType = typeParam(mapType, 0);
                valueType = typeParam(mapType, 1);
            } else {
                // the map type has one parameter, the type of the entries
                ByteBuffer entryType = typeParam(mapType, 0);
                try {
                    keyType = typeParam(entryType, 0);
                    valueType = typeParam(entryType, 1);
                } finally {
                    duckdb_destroy_logical_type(entryType);
                }
            }
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                keys.add(toValue(factory, entry.getKey()));
                values.add(toValue(factory, entry.getValue()));
            }
            return duckdb_v2_value_create_map(factory, keyType, valueType, keys.toArray(new ByteBuffer[0]),
                                              values.toArray(new ByteBuffer[0]));
        } finally {
            destroyValues(keys);
            destroyValues(values);
            if (keyType != null) {
                duckdb_destroy_logical_type(keyType);
            }
            if (valueType != null) {
                duckdb_destroy_logical_type(valueType);
            }
            duckdb_destroy_logical_type(mapType);
        }
    }

    private static ByteBuffer structValue(ByteBuffer factory, Struct struct) throws SQLException {
        ByteBuffer type = parseType(factory, struct.getSQLTypeName());
        List<ByteBuffer> children = new ArrayList<>();
        try {
            Object[] attributes = struct.getAttributes();
            byte[][] names = new byte[attributes.length][];
            for (int i = 0; i < attributes.length; i++) {
                names[i] = duckdb_struct_type_child_name(type, i);
                children.add(toValue(factory, attributes[i]));
            }
            return duckdb_v2_value_create_struct(factory, names, children.toArray(new ByteBuffer[0]));
        } finally {
            destroyValues(children);
            duckdb_destroy_logical_type(type);
        }
    }

    private static ByteBuffer arrayValue(ByteBuffer factory, java.sql.Array array) throws SQLException {
        ByteBuffer type = parseType(factory, array.getBaseTypeName());
        List<ByteBuffer> children = new ArrayList<>();
        try {
            Object[] elements = (Object[]) array.getArray();
            for (Object element : elements) {
                children.add(toValue(factory, element));
            }
            return duckdb_v2_value_create_list(factory, type, children.toArray(new ByteBuffer[0]));
        } finally {
            destroyValues(children);
            duckdb_destroy_logical_type(type);
        }
    }
}
