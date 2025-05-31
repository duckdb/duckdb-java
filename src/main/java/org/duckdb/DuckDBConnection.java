package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBDriver.JDBC_AUTO_COMMIT;
import static org.duckdb.JdbcUtils.*;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.duckdb.user.DuckDBMap;
import org.duckdb.user.DuckDBUserArray;
import org.duckdb.user.DuckDBUserStruct;

public final class DuckDBConnection implements java.sql.Connection {

    /** Name of the DuckDB default schema. */
    public static final String DEFAULT_SCHEMA = "main";

    ByteBuffer connRef;
    final Lock connRefLock = new ReentrantLock();
    final LinkedHashSet<DuckDBPreparedStatement> preparedStatements = new LinkedHashSet<>();
    volatile boolean closing;

    volatile boolean autoCommit;
    volatile boolean transactionRunning;
    final String url;
    private final boolean readOnly;

    public static DuckDBConnection newConnection(String url, boolean readOnly, Properties properties)
        throws SQLException {
        if (null == properties) {
            properties = new Properties();
        }
        String dbName = dbNameFromUrl(url);
        String autoCommitStr = removeOption(properties, JDBC_AUTO_COMMIT);
        boolean autoCommit = isStringTruish(autoCommitStr, true);
        ByteBuffer nativeReference = DuckDBNative.duckdb_jdbc_startup(dbName.getBytes(UTF_8), readOnly, properties);
        return new DuckDBConnection(nativeReference, url, readOnly, autoCommit);
    }

    private DuckDBConnection(ByteBuffer connectionReference, String url, boolean readOnly, boolean autoCommit)
        throws SQLException {
        this.connRef = connectionReference;
        this.url = url;
        this.readOnly = readOnly;
        this.autoCommit = autoCommit;
        // Hardcoded 'true' here is intentional, autocommit is handled in stmt#execute()
        DuckDBNative.duckdb_jdbc_set_auto_commit(connectionReference, true);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        checkOpen();
        if (resultSetConcurrency == ResultSet.CONCUR_READ_ONLY && resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
            return new DuckDBPreparedStatement(this);
        }
        throw new SQLFeatureNotSupportedException("createStatement");
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        checkOpen();
        if (resultSetConcurrency == ResultSet.CONCUR_READ_ONLY && resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
            return new DuckDBPreparedStatement(this, sql);
        }
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public Connection duplicate() throws SQLException {
        checkOpen();
        connRefLock.lock();
        try {
            checkOpen();
            return new DuckDBConnection(DuckDBNative.duckdb_jdbc_connect(connRef), url, readOnly, autoCommit);
        } finally {
            connRefLock.unlock();
        }
    }

    public void commit() throws SQLException {
        try (Statement s = createStatement()) {
            s.execute("COMMIT");
            transactionRunning = false;
        }
    }

    public void rollback() throws SQLException {
        try (Statement s = createStatement()) {
            s.execute("ROLLBACK");
            transactionRunning = false;
        }
    }

    protected void finalize() throws Throwable {
        close();
    }

    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }
        connRefLock.lock();
        try {
            if (isClosed()) {
                return;
            }

            // Mark this instance as 'closing' to skip untrack call in
            // prepared statements, that requires connection lock and can
            // cause a deadlock when the statement closure is caused by the
            // connection interrupt called by us.
            this.closing = true;

            // Interrupt running query if any
            try {
                interrupt();
            } catch (SQLException e) {
                // suppress
            }

            // Last statement created is first deleted
            List<DuckDBPreparedStatement> psList = new ArrayList<>(preparedStatements);
            Collections.reverse(psList);
            for (DuckDBPreparedStatement ps : psList) {
                ps.close();
            }
            preparedStatements.clear();

            DuckDBNative.duckdb_jdbc_disconnect(connRef);
            connRef = null;
        } finally {
            connRefLock.unlock();
        }
    }

    public boolean isClosed() throws SQLException {
        return connRef == null;
    }

    public boolean isValid(int timeout) throws SQLException {
        if (isClosed()) {
            return false;
        }
        // run a query just to be sure
        try (Statement s = createStatement(); ResultSet rs = s.executeQuery("SELECT 42")) {
            return rs.next() && rs.getInt(1) == 42;
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {
    }

    public void setTransactionIsolation(int level) throws SQLException {
        if (level > TRANSACTION_REPEATABLE_READ) {
            throw new SQLFeatureNotSupportedException("setTransactionIsolation");
        }
    }

    public int getTransactionIsolation() throws SQLException {
        return TRANSACTION_REPEATABLE_READ;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly != this.readOnly) {
            throw new SQLFeatureNotSupportedException("Can't change read-only status on connection level.");
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection was closed");
        }

        if (this.autoCommit != autoCommit) {
            this.autoCommit = autoCommit;

            // A running transaction is committed if switched to auto-commit
            if (transactionRunning && autoCommit) {
                this.commit();
            }
        }
        return;

        // Native method is not working as one would expect ... uncomment maybe later
        // DuckDBNative.duckdb_jdbc_set_auto_commit(conn_ref, autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection was closed");
        }
        return this.autoCommit;

        // Native method is not working as one would expect ... uncomment maybe later
        // return DuckDBNative.duckdb_jdbc_get_auto_commit(conn_ref);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 0);
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return new DuckDBDatabaseMetaData(this);
    }

    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        connRefLock.lock();
        try {
            checkOpen();
            DuckDBNative.duckdb_jdbc_set_catalog(connRef, catalog);
        } finally {
            connRefLock.unlock();
        }
    }

    public String getCatalog() throws SQLException {
        checkOpen();
        connRefLock.lock();
        try {
            checkOpen();
            return DuckDBNative.duckdb_jdbc_get_catalog(connRef);
        } finally {
            connRefLock.unlock();
        }
    }

    public void setSchema(String schema) throws SQLException {
        checkOpen();
        connRefLock.lock();
        try {
            checkOpen();
            DuckDBNative.duckdb_jdbc_set_schema(connRef, schema);
        } finally {
            connRefLock.unlock();
        }
    }

    public String getSchema() throws SQLException {
        checkOpen();
        connRefLock.lock();
        try {
            checkOpen();
            return DuckDBNative.duckdb_jdbc_get_schema(connRef);
        } finally {
            connRefLock.unlock();
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("abort");
    }

    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createClob");
    }

    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob");
    }

    // less likely to implement this stuff

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("nativeSQL");
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, 0);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, 0);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection was closed");
        }
        return new HashMap<>();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection was closed");
        }
        if (map != null && (map instanceof java.util.HashMap)) {
            // we return an empty Hash map if the user gives this back make sure we accept it.
            if (map.isEmpty()) {
                return;
            }
        }
        throw new SQLFeatureNotSupportedException("setTypeMap");
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("setHoldability");
    }

    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("getHoldability");
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("releaseSavepoint");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML"); // hell no
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    public String getClientInfo(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfo");
    }

    public Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("getClientInfo");
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return new DuckDBUserArray(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return new DuckDBUserStruct(typeName, attributes);
    }

    public <K, V> Map<K, V> createMap(String typeName, Map<K, V> map) {
        return new DuckDBMap<>(typeName, map);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNetworkTimeout");
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("getNetworkTimeout");
    }

    public DuckDBAppender createAppender(String schemaName, String tableName) throws SQLException {
        return new DuckDBAppender(this, schemaName, tableName);
    }

    private static long getArrowStreamAddress(Object arrow_array_stream) {
        try {
            Class<?> arrow_array_stream_class = Class.forName("org.apache.arrow.c.ArrowArrayStream");
            if (!arrow_array_stream_class.isInstance(arrow_array_stream)) {
                throw new RuntimeException("Need to pass an ArrowArrayStream");
            }
            return (Long) arrow_array_stream_class.getMethod("memoryAddress").invoke(arrow_array_stream);

        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | SecurityException |
                 ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerArrowStream(String name, Object arrow_array_stream) {
        try {
            checkOpen();
            long array_stream_address = getArrowStreamAddress(arrow_array_stream);
            connRefLock.lock();
            try {
                checkOpen();
                DuckDBNative.duckdb_jdbc_arrow_register(connRef, array_stream_address, name.getBytes(UTF_8));
            } finally {
                connRefLock.unlock();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getProfilingInformation(ProfilerPrintFormat format) throws SQLException {
        checkOpen();
        connRefLock.lock();
        try {
            checkOpen();
            return DuckDBNative.duckdb_jdbc_get_profiling_information(connRef, format);
        } finally {
            connRefLock.unlock();
        }
    }

    public DuckDBHugeInt createHugeInt(long lower, long upper) throws SQLException {
        return new DuckDBHugeInt(lower, upper);
    }

    void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection was closed");
        }
    }

    /**
     * This function calls the underlying C++ interrupt function which aborts the query running on this connection.
     */
    void interrupt() throws SQLException {
        checkOpen();
        connRefLock.lock();
        try {
            checkOpen();
            DuckDBNative.duckdb_jdbc_interrupt(connRef);
        } finally {
            connRefLock.unlock();
        }
    }

    QueryProgress queryProgress() throws SQLException {
        checkOpen();
        connRefLock.lock();
        try {
            checkOpen();
            return DuckDBNative.duckdb_jdbc_query_progress(connRef);
        } finally {
            connRefLock.unlock();
        }
    }
}
