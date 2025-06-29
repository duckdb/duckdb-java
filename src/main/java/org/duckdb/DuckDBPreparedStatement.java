package org.duckdb;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.duckdb.StatementReturnType.*;
import static org.duckdb.io.IOUtils.*;

import java.io.*;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DuckDBPreparedStatement implements PreparedStatement {
    private DuckDBConnection conn;

    private ByteBuffer stmtRef = null;
    final ReentrantLock stmtRefLock = new ReentrantLock();
    volatile boolean closeOnCompletion = false;

    private DuckDBResultSet selectResult = null;
    private long updateResult = 0;

    private boolean returnsChangedRows = false;
    private boolean returnsNothing = false;
    private boolean returnsResultSet = false;
    private Object[] params = new Object[0];
    private DuckDBResultSetMetaData meta = null;
    private final List<Object[]> batchedParams = new ArrayList<>();
    private final List<String> batchedStatements = new ArrayList<>();
    private Boolean isBatch = false;
    private Boolean isPreparedStatement = false;
    private int queryTimeoutSeconds = 0;
    private ScheduledFuture<?> cancelQueryFuture = null;

    public DuckDBPreparedStatement(DuckDBConnection conn) throws SQLException {
        if (conn == null) {
            throw new SQLException("connection parameter cannot be null");
        }
        this.conn = conn;
    }

    public DuckDBPreparedStatement(DuckDBConnection conn, String sql) throws SQLException {
        if (conn == null) {
            throw new SQLException("connection parameter cannot be null");
        }
        if (sql == null) {
            throw new SQLException("sql query parameter cannot be null");
        }
        this.conn = conn;
        this.isPreparedStatement = true;
        prepare(sql);
    }

    private boolean isConnAutoCommit() throws SQLException {
        checkOpen();
        try {
            return this.conn.autoCommit;
        } catch (NullPointerException e) {
            throw new SQLException(e);
        }
    }

    private boolean startTransaction() throws SQLException {
        checkOpen();
        try {
            if (this.conn.transactionRunning) {
                return false;
            }
            this.conn.transactionRunning = true;
            // Start transaction via Statement
            try (Statement s = conn.createStatement()) {
                s.execute("BEGIN TRANSACTION;");
                return true;
            }
        } catch (NullPointerException e) {
            throw new SQLException(e);
        }
    }

    private void prepare(String sql) throws SQLException {
        checkOpen();
        if (sql == null) {
            throw new SQLException("sql query parameter cannot be null");
        }

        stmtRefLock.lock();
        try {
            checkOpen();

            // In case the statement is reused, release old one first
            if (stmtRef != null) {
                DuckDBNative.duckdb_jdbc_release(stmtRef);
                stmtRef = null;
            }

            meta = null;
            params = new Object[0];

            if (selectResult != null) {
                selectResult.close();
            }
            selectResult = null;
            updateResult = 0;

            // Lock connection while still holding statement lock
            conn.connRefLock.lock();
            try {
                conn.checkOpen();
                stmtRef = DuckDBNative.duckdb_jdbc_prepare(conn.connRef, sql.getBytes(UTF_8));
                // Track prepared statement inside the parent connection
                conn.preparedStatements.add(this);
            } finally {
                conn.connRefLock.unlock();
            }

            meta = DuckDBNative.duckdb_jdbc_prepared_statement_meta(stmtRef);
        } catch (SQLException e) {
            close();
            throw e;
        } finally {
            stmtRefLock.unlock();
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(true);
    }

    private boolean execute(boolean startTransaction) throws SQLException {
        checkOpen();
        checkPrepared();

        // Wait with dispatching a new query if connection is locked by cancel() call
        Lock connLock = getConnRefLock();
        connLock.lock();
        connLock.unlock();

        ByteBuffer resultRef = null;

        stmtRefLock.lock();
        try {
            checkOpen();
            checkPrepared();

            if (selectResult != null) {
                selectResult.close();
            }
            selectResult = null;

            if (startTransaction && !isConnAutoCommit()) {
                startTransaction();
            }

            if (queryTimeoutSeconds > 0) {
                cleanupCancelQueryTask();
                cancelQueryFuture =
                    DuckDBDriver.scheduler.schedule(new CancelQueryTask(), queryTimeoutSeconds, SECONDS);
            }

            resultRef = DuckDBNative.duckdb_jdbc_execute(stmtRef, params);
            cleanupCancelQueryTask();
            DuckDBResultSetMetaData resultMeta = DuckDBNative.duckdb_jdbc_query_result_meta(resultRef);
            selectResult = new DuckDBResultSet(conn, this, resultMeta, resultRef);
            returnsResultSet = resultMeta.return_type.equals(QUERY_RESULT);
            returnsChangedRows = resultMeta.return_type.equals(CHANGED_ROWS);
            returnsNothing = resultMeta.return_type.equals(NOTHING);

        } catch (SQLException e) {
            // Delete result set that might have been allocated
            if (selectResult != null) {
                selectResult.close();
            } else if (resultRef != null) {
                DuckDBNative.duckdb_jdbc_free_result(resultRef);
                resultRef = null;
            }
            close();
            throw e;

        } finally {
            stmtRefLock.unlock();
        }

        if (returnsChangedRows) {
            if (selectResult.next()) {
                updateResult = selectResult.getLong(1);
            }
            selectResult.close();
        }

        return returnsResultSet;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        requireNonBatch();
        execute();
        if (!returnsResultSet) {
            throw new SQLException("executeQuery() can only be used with queries that return a ResultSet");
        }
        return selectResult;
    }

    @Override
    public int executeUpdate() throws SQLException {
        long res = executeLargeUpdate();
        return intFromLong(res);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        requireNonBatch();
        execute();
        if (!(returnsChangedRows || returnsNothing)) {
            throw new SQLException(
                "executeUpdate() can only be used with queries that return nothing (eg, a DDL statement), or update rows");
        }
        return getUpdateCountInternal();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        requireNonBatch();
        prepare(sql);
        return execute();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        prepare(sql);
        return executeQuery();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        long res = executeLargeUpdate(sql);
        return intFromLong(res);
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        prepare(sql);
        return executeLargeUpdate();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        checkPrepared();
        return meta;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkOpen();
        checkPrepared();
        return meta.param_meta;
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkOpen();
        int paramsCount = getParameterMetaData().getParameterCount();
        if (parameterIndex < 1 || parameterIndex > paramsCount) {
            throw new SQLException("Parameter index out of bounds");
        }
        if (params.length == 0) {
            params = new Object[paramsCount];
        }
        // we are doing lower/upper extraction from BigInteger on Java side
        if (x instanceof BigInteger) {
            x = new DuckDBHugeInt((BigInteger) x);
        }
        params[parameterIndex - 1] = x;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setObject(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void clearParameters() throws SQLException {
        checkOpen();
        params = new Object[0];
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }
        stmtRefLock.lock();
        try {
            if (isClosed()) {
                return;
            }
            cleanupCancelQueryTask();
            if (selectResult != null) {
                selectResult.close();
                selectResult = null;
            }
            if (stmtRef != null) {
                // Delete prepared statement
                DuckDBNative.duckdb_jdbc_release(stmtRef);

                // Untrack prepared statement from parent connection,
                // if 'closing' flag is set it means that the parent connection itself
                // is being closed and we don't need to untrack this instance from the statement.
                if (!conn.closing) {
                    conn.connRefLock.lock();
                    try {
                        conn.preparedStatements.remove(this);
                    } finally {
                        conn.connRefLock.unlock();
                    }
                }

                stmtRef = null;
            }
            conn = null; // we use this as a check for closed-ness
        } finally {
            stmtRefLock.unlock();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        // Cannot check stmtRef here because it is created only
        // when prepare() is called.
        return conn == null || conn.connRef == null;
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void finalize() throws Throwable {
        close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkOpen();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkOpen();
    }

    @Override
    public int getMaxRows() throws SQLException {
        return (int) getLargeMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        setLargeMaxRows(max);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        checkOpen();
        return 0;
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        checkOpen();
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkOpen();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkOpen();
        return queryTimeoutSeconds;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkOpen();
        if (seconds < 0) {
            throw new SQLException("Invalid negative timeout value: " + seconds);
        }
        this.queryTimeoutSeconds = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        checkOpen();
        // Only proceed to interrupt call after ensuring that the query on
        // this statement is still running.
        if (!stmtRefLock.isLocked()) {
            return;
        }
        // Cancel is intended to be called concurrently with execute,
        // thus we cannot take the statement lock that is held while
        // query is running. NPE may be thrown if connection is closed
        // concurrently.
        try {
            // Taking connection lock will prevent new queries to be executed
            Lock connLock = getConnRefLock();
            connLock.lock();
            try {
                if (!stmtRefLock.isLocked()) {
                    return;
                }
                conn.interrupt();
            } finally {
                connLock.unlock();
            }
        } catch (NullPointerException e) {
            throw new SQLException(e);
        }
    }

    public QueryProgress getQueryProgress() throws SQLException {
        checkOpen();
        try {
            // getQueryProgress is intended to be called concurrently with execute,
            // thus we cannot take the statement lock that is held while
            // query is running. NPE may be thrown if connection is closed
            // concurrently.
            return conn.queryProgress();
        } catch (NullPointerException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        checkOpen();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was closed");
        }
        if (stmtRef == null) {
            throw new SQLException("Prepare something first");
        }

        if (!returnsResultSet) {
            return null;
        }

        // getResultSet can only be called once per result
        ResultSet to_return = selectResult;
        this.selectResult = null;
        return to_return;
    }

    private long getUpdateCountInternal() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was closed");
        }
        if (stmtRef == null) {
            // It is not required by JDBC spec to return anything in this case,
            // but clients can call this method before preparing/executing the query
            return -1;
        }

        if (returnsResultSet || returnsNothing || selectResult.isFinished()) {
            return -1;
        }
        return updateResult;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        // getUpdateCount can only be called once per result
        long res = getUpdateCountInternal();
        updateResult = -1;
        return res;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        long res = getLargeUpdateCount();
        return intFromLong(res);
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        if (direction == ResultSet.FETCH_FORWARD) {
            return;
        }
        throw new SQLFeatureNotSupportedException("setFetchDirection");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkOpen();
        return DuckDBNative.duckdb_jdbc_fetch_size();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkOpen();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkOpen();
        requireNonPreparedStatement();
        this.batchedStatements.add(sql);
        this.isBatch = true;
    }

    @Override
    public void clearBatch() throws SQLException {
        checkOpen();
        this.batchedParams.clear();
        this.batchedStatements.clear();
        this.isBatch = false;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        long[] res = executeLargeBatch();
        return intArrayFromLong(res);
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkOpen();
        if (this.isPreparedStatement) {
            return executeBatchedPreparedStatement();
        } else {
            return executeBatchedStatements();
        }
    }

    private long[] executeBatchedPreparedStatement() throws SQLException {
        stmtRefLock.lock();
        boolean tranStarted = false;
        DuckDBConnection conn = this.conn;
        try {
            checkOpen();
            checkPrepared();

            tranStarted = startTransaction();

            long[] updateCounts = new long[this.batchedParams.size()];
            for (int i = 0; i < this.batchedParams.size(); i++) {
                params = this.batchedParams.get(i);
                execute(false);
                updateCounts[i] = getUpdateCountInternal();
            }
            clearBatch();

            if (tranStarted && isConnAutoCommit()) {
                this.conn.commit();
            }

            return updateCounts;

        } catch (SQLException e) {
            if (tranStarted && conn.getAutoCommit()) {
                conn.rollback();
            }
            throw e;
        } finally {
            stmtRefLock.unlock();
        }
    }

    private long[] executeBatchedStatements() throws SQLException {
        stmtRefLock.lock();
        boolean tranStarted = false;
        DuckDBConnection conn = this.conn;
        try {
            checkOpen();

            tranStarted = startTransaction();

            long[] updateCounts = new long[this.batchedStatements.size()];
            for (int i = 0; i < this.batchedStatements.size(); i++) {
                prepare(this.batchedStatements.get(i));
                execute(false);
                updateCounts[i] = getUpdateCountInternal();
            }
            clearBatch();

            if (tranStarted && isConnAutoCommit()) {
                this.conn.commit();
            }

            return updateCounts;

        } catch (SQLException e) {
            if (tranStarted && conn.getAutoCommit()) {
                conn.rollback();
            }
            throw e;
        } finally {
            stmtRefLock.unlock();
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkOpen();
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("getGeneratedKeys");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        long res = executeLargeUpdate(sql, autoGeneratedKeys);
        return intFromLong(res);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (NO_GENERATED_KEYS == autoGeneratedKeys) {
            return executeLargeUpdate(sql);
        }
        throw new SQLFeatureNotSupportedException("executeUpdate(String sql, int autoGeneratedKeys)");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        long res = executeLargeUpdate(sql, columnIndexes);
        return intFromLong(res);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        if (columnIndexes == null || columnIndexes.length == 0) {
            return executeLargeUpdate(sql);
        }
        throw new SQLFeatureNotSupportedException("executeUpdate(String sql, int[] columnIndexes)");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        long res = executeLargeUpdate(sql, columnNames);
        return intFromLong(res);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        if (columnNames == null || columnNames.length == 0) {
            return executeUpdate(sql);
        }
        throw new SQLFeatureNotSupportedException("executeUpdate(String sql, String[] columnNames)");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (NO_GENERATED_KEYS == autoGeneratedKeys) {
            return execute(sql);
        }
        throw new SQLFeatureNotSupportedException("execute(String sql, int autoGeneratedKeys)");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (columnIndexes == null || columnIndexes.length == 0) {
            return execute(sql);
        }
        throw new SQLFeatureNotSupportedException("execute(String sql, int[] columnIndexes)");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (columnNames == null || columnNames.length == 0) {
            return execute(sql);
        }
        throw new SQLFeatureNotSupportedException("execute(String sql, String[] columnNames)");
    }

    @Override
    @SuppressWarnings("deprecation")
    public int getResultSetHoldability() throws SQLException {
        checkOpen();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkOpen();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkOpen();
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkOpen();
        return closeOnCompletion;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setDate(parameterIndex, x, null);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setTime(parameterIndex, x, null);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setTimestamp(parameterIndex, x, null);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, (long) length);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setCharacterStreamInternal(parameterIndex, x, length, UTF_8);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, (long) length);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        checkOpen();

        if (x == null) {
            setNull(parameterIndex, targetSqlType);
            return;
        }
        switch (targetSqlType) {
        case Types.BOOLEAN:
        case Types.BIT:
            if (x instanceof Boolean) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).byteValue() == 1);
            } else if (x instanceof String) {
                setObject(parameterIndex, Boolean.parseBoolean((String) x));
            } else {
                throw new SQLException("Can't convert value to boolean " + x.getClass().toString());
            }
            break;
        case Types.TINYINT:
            if (x instanceof Byte) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).byteValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Byte.parseByte((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (byte) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to byte " + x.getClass().toString());
            }
            break;
        case Types.SMALLINT:
            if (x instanceof Short) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).shortValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Short.parseShort((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (short) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to short " + x.getClass().toString());
            }
            break;
        case Types.INTEGER:
            if (x instanceof Integer) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).intValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Integer.parseInt((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, ((Boolean) x) ? 1 : 0);
            } else {
                throw new SQLException("Can't convert value to int " + x.getClass().toString());
            }
            break;
        case Types.BIGINT:
            if (x instanceof Long) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).longValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Long.parseLong((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (long) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to long " + x.getClass().toString());
            }
            break;
        case Types.REAL:
        case Types.FLOAT:
            if (x instanceof Float) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).floatValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Float.parseFloat((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (float) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to float " + x.getClass().toString());
            }
            break;
        case Types.DECIMAL:
            if (x instanceof BigDecimal) {
                setObject(parameterIndex, x);
            } else if (x instanceof Double) {
                setObject(parameterIndex, new BigDecimal((Double) x));
            } else if (x instanceof String) {
                setObject(parameterIndex, new BigDecimal((String) x));
            } else {
                throw new SQLException("Can't convert value to double " + x.getClass().toString());
            }
            break;
        case Types.NUMERIC:
        case Types.DOUBLE:
            if (x instanceof Double) {
                setObject(parameterIndex, x);
            } else if (x instanceof Number) {
                setObject(parameterIndex, ((Number) x).doubleValue());
            } else if (x instanceof String) {
                setObject(parameterIndex, Double.parseDouble((String) x));
            } else if (x instanceof Boolean) {
                setObject(parameterIndex, (double) (((Boolean) x) ? 1 : 0));
            } else {
                throw new SQLException("Can't convert value to double " + x.getClass().toString());
            }
            break;
        case Types.CHAR:
        case Types.LONGVARCHAR:
        case Types.VARCHAR:
            if (x instanceof String) {
                setObject(parameterIndex, (String) x);
            } else {
                setObject(parameterIndex, x.toString());
            }
            break;
        case Types.TIMESTAMP:
        case Types.TIMESTAMP_WITH_TIMEZONE:
            if (x instanceof Timestamp) {
                setObject(parameterIndex, x);
            } else if (x instanceof LocalDateTime) {
                setObject(parameterIndex, x);
            } else if (x instanceof OffsetDateTime) {
                setObject(parameterIndex, x);
            } else {
                throw new SQLException("Can't convert value to timestamp " + x.getClass().toString());
            }
            break;
        default:
            throw new SQLException("Unknown target type " + targetSqlType);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        checkOpen();
        batchedParams.add(params);
        clearParameters();
        this.isBatch = true;
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (long) length);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setRef");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (null == x || null == cal) {
            setObject(parameterIndex, x);
            return;
        }
        Instant instant = Instant.ofEpochMilli(x.getTime());
        ZoneId zoneId = cal.getTimeZone().toZoneId();
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);
        LocalDate ld = zdt.toLocalDate();
        setObject(parameterIndex, ld);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (null == x || null == cal) {
            setObject(parameterIndex, x);
            return;
        }
        Instant instant = Instant.ofEpochMilli(x.getTime());
        ZoneId zoneId = cal.getTimeZone().toZoneId();
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);
        LocalTime lt = zdt.toLocalTime();
        setObject(parameterIndex, lt);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (null == x || null == cal) {
            setObject(parameterIndex, x);
            return;
        }
        Instant instant = Instant.ofEpochMilli(x.getTime());
        ZoneId zoneId = cal.getTimeZone().toZoneId();
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, zoneId);
        LocalDateTime ldt = zdt.toLocalDateTime();
        setObject(parameterIndex, ldt);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setURL");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setRowId");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setSQLXML");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setCharacterStreamInternal(parameterIndex, x, length, US_ASCII);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        checkOpen();
        InputStream stream = wrapStreamWithMaxBytes(x, length);
        byte[] bytes = readAllBytes(stream);
        setObject(parameterIndex, bytes);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterReaderInternal(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setBinaryStream(parameterIndex, x, -1);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader, -1);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setNCharacterStream(parameterIndex, value, -1);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setBlob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    public void setHugeInt(int parameterIndex, DuckDBHugeInt hi) throws SQLException {
        setObject(parameterIndex, hi);
    }

    public void setBigInteger(int parameterIndex, BigInteger bi) throws SQLException {
        setObject(parameterIndex, bi);
    }

    private void requireNonBatch() throws SQLException {
        if (this.isBatch) {
            throw new SQLException("Batched queries must be executed with executeBatch.");
        }
    }

    private void requireNonPreparedStatement() throws SQLException {
        if (this.isPreparedStatement) {
            throw new SQLException("Cannot add batched SQL statement to PreparedStatement");
        }
    }

    private void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement was closed");
        }
    }

    private void checkPrepared() throws SQLException {
        if (stmtRef == null) {
            throw new SQLException("Prepare something first");
        }
    }

    private void setCharacterStreamInternal(int parameterIndex, InputStream x, long length, Charset charset)
        throws SQLException {
        checkOpen();
        InputStream stream = wrapStreamWithMaxBytes(x, length);
        Reader reader = new InputStreamReader(stream, charset);
        String str = readToString(reader);
        setObject(parameterIndex, str);
    }

    private void setCharacterReaderInternal(int parameterIndex, Reader reader, long lenght) throws SQLException {
        checkOpen();
        Reader wrappedReader = wrapReaderWithMaxChars(reader, lenght);
        String str = readToString(wrappedReader);
        setObject(parameterIndex, str);
    }

    private int intFromLong(long val) {
        if (val <= Integer.MAX_VALUE) {
            return (int) val;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    private int[] intArrayFromLong(long[] arr) {
        int[] res = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            res[i] = intFromLong(arr[i]);
        }
        return res;
    }

    private Lock getConnRefLock() throws SQLException {
        // NPE can be thrown if statement is closed concurrently.
        try {
            return conn.connRefLock;
        } catch (NullPointerException e) {
            throw new SQLException(e);
        }
    }

    private void cleanupCancelQueryTask() {
        if (cancelQueryFuture != null) {
            cancelQueryFuture.cancel(false);
            cancelQueryFuture = null;
        }
    }

    private class CancelQueryTask implements Runnable {
        @Override
        public void run() {
            try {
                if (DuckDBPreparedStatement.this.isClosed()) {
                    return;
                }
                DuckDBPreparedStatement.this.cancel();
            } catch (SQLException e) {
                // suppress
            }
        }
    }
}
