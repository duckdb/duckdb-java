package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;

public class DuckDBTableFunctionBuilder implements AutoCloseable {
    private ByteBuffer tableFunctionRef;
    boolean finalized;
    String functionName;
    DuckDBTableFunction<?, ?, ?> function;

    DuckDBTableFunctionBuilder() throws SQLException {
        this.tableFunctionRef = duckdb_create_table_function();
        if (tableFunctionRef == null) {
            throw JdbcUtils.createSQLException("Failed to create table function", ErrorCode.TABLE_FUNCTION_CREATE);
        }
    }

    public DuckDBTableFunctionBuilder withName(String name) throws SQLException {
        ensureNotFinalized();
        if (name == null || name.trim().isEmpty()) {
            throw JdbcUtils.createSQLException("Function name cannot be null or empty",
                                               ErrorCode.TABLE_FUNCTION_NAME_EMPTY);
        }
        this.functionName = name;
        duckdb_table_function_set_name(tableFunctionRef, name.getBytes(UTF_8));
        return this;
    }

    public DuckDBTableFunctionBuilder withParameter(Class<?> parameterType) throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw JdbcUtils.createSQLException("Parameter type cannot be null", ErrorCode.TABLE_FUNCTION_PARAM_NULL);
        }
        DuckDBColumnType mappedType = DuckDBScalarFunctionAdapter.mapJavaClassToDuckDBType(parameterType);
        return withParameter(mappedType);
    }

    public DuckDBTableFunctionBuilder withParameter(DuckDBColumnType parameterType) throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw JdbcUtils.createSQLException("Parameter type cannot be null", ErrorCode.TABLE_FUNCTION_PARAM_NULL);
        }
        try (DuckDBLogicalType logicalType = DuckDBLogicalType.of(parameterType)) {
            return withParameter(logicalType);
        }
    }

    public DuckDBTableFunctionBuilder withParameter(DuckDBLogicalType parameterType) throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw JdbcUtils.createSQLException("Parameter type cannot be null", ErrorCode.TABLE_FUNCTION_PARAM_NULL);
        }
        duckdb_table_function_add_parameter(tableFunctionRef, parameterType.logicalTypeRef());
        return this;
    }

    public DuckDBTableFunctionBuilder withParameters(Class<?>... parameterTypes) throws SQLException {
        ensureNotFinalized();
        if (parameterTypes == null) {
            throw JdbcUtils.createSQLException("Parameter types cannot be null", ErrorCode.TABLE_FUNCTION_PARAMS_NULL);
        }
        for (Class<?> parameterType : parameterTypes) {
            withParameter(parameterType);
        }
        return this;
    }

    public DuckDBTableFunctionBuilder withParameters(DuckDBColumnType... parameterTypes) throws SQLException {
        ensureNotFinalized();
        if (parameterTypes == null) {
            throw JdbcUtils.createSQLException("Parameter types cannot be null", ErrorCode.TABLE_FUNCTION_PARAMS_NULL);
        }
        for (DuckDBColumnType parameterType : parameterTypes) {
            withParameter(parameterType);
        }
        return this;
    }

    public DuckDBTableFunctionBuilder withParameters(DuckDBLogicalType... parameterTypes) throws SQLException {
        ensureNotFinalized();
        if (parameterTypes == null) {
            throw JdbcUtils.createSQLException("Parameter types cannot be null", ErrorCode.TABLE_FUNCTION_PARAMS_NULL);
        }
        for (DuckDBLogicalType parameterType : parameterTypes) {
            withParameter(parameterType);
        }
        return this;
    }

    public DuckDBTableFunctionBuilder withNamedParameter(String name, Class<?> parameterType) throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw JdbcUtils.createSQLException("Parameter type cannot be null", ErrorCode.TABLE_FUNCTION_PARAM_NULL);
        }
        DuckDBColumnType mappedType = DuckDBScalarFunctionAdapter.mapJavaClassToDuckDBType(parameterType);
        return withNamedParameter(name, mappedType);
    }

    public DuckDBTableFunctionBuilder withNamedParameter(String name, DuckDBColumnType parameterType)
        throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw JdbcUtils.createSQLException("Parameter type cannot be null", ErrorCode.TABLE_FUNCTION_PARAM_NULL);
        }
        try (DuckDBLogicalType logicalType = DuckDBLogicalType.of(parameterType)) {
            return withNamedParameter(name, logicalType);
        }
    }

    public DuckDBTableFunctionBuilder withNamedParameter(String name, DuckDBLogicalType parameterType)
        throws SQLException {
        ensureNotFinalized();
        if (name == null || name.trim().isEmpty()) {
            throw JdbcUtils.createSQLException("Parameter name cannot be empty",
                                               ErrorCode.TABLE_FUNCTION_PARAM_NAME_EMPTY);
        }
        if (parameterType == null) {
            throw JdbcUtils.createSQLException("Parameter type cannot be null", ErrorCode.TABLE_FUNCTION_PARAM_NULL);
        }
        byte[] nameBytes = name.getBytes(UTF_8);
        duckdb_table_function_add_named_parameter(tableFunctionRef, nameBytes, parameterType.logicalTypeRef());
        return this;
    }

    public DuckDBTableFunctionBuilder withProjectionPushdown() throws SQLException {
        ensureNotFinalized();
        duckdb_table_function_supports_projection_pushdown(tableFunctionRef, true);
        return this;
    }

    public DuckDBTableFunctionBuilder withFunction(DuckDBTableFunction<?, ?, ?> function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw JdbcUtils.createSQLException("Table function object cannot be null",
                                               ErrorCode.TABLE_FUNCTION_OBJECT_NULL);
        }
        this.function = function;
        return this;
    }

    public DuckDBFunctions.RegisteredFunction register(Connection connection) throws SQLException {
        ensureNotFinalized();
        if (connection == null) {
            throw JdbcUtils.createSQLException("Connection cannot be null", ErrorCode.TABLE_FUNCTION_CONNECTION_NULL);
        }
        if (functionName == null) {
            throw JdbcUtils.createSQLException("Function name must be defined", ErrorCode.TABLE_FUNCTION_NO_NAME);
        }
        if (function == null) {
            throw JdbcUtils.createSQLException("Table function callback must be defined",
                                               ErrorCode.TABLE_FUNCTION_NO_CALLBACK);
        }

        DuckDBTableFunctionWrapper wrapper = new DuckDBTableFunctionWrapper(function);
        duckdb_table_function_set_extra_info(tableFunctionRef, wrapper);
        duckdb_table_function_set_bind(tableFunctionRef);
        duckdb_table_function_set_init(tableFunctionRef);
        duckdb_table_function_set_local_init(tableFunctionRef);
        duckdb_table_function_set_function(tableFunctionRef);

        DuckDBConnection duckConnection = connection.unwrap(DuckDBConnection.class);
        Lock connectionLock = duckConnection.connRefLock;
        connectionLock.lock();
        try {
            duckConnection.checkOpen();
            int status = duckdb_register_table_function(duckConnection.connRef, tableFunctionRef);
            if (status != 0) {
                throw JdbcUtils.createSQLException("Failed to register table function '" + functionName + "'",
                                                   ErrorCode.TABLE_FUNCTION_REGISTER_NATIVE);
            }
            return DuckDBDriver.registerFunction(functionName, DuckDBFunctions.Kind.TABLE);
        } finally {
            connectionLock.unlock();
            close();
        }
    }

    @Override
    public void close() {
        if (finalized) {
            return;
        }
        if (tableFunctionRef != null) {
            duckdb_destroy_table_function(tableFunctionRef);
            tableFunctionRef = null;
        }
        finalized = true;
    }

    private void ensureNotFinalized() throws SQLException {
        if (finalized || tableFunctionRef == null) {
            throw JdbcUtils.createSQLException("Table function builder is already finalized",
                                               ErrorCode.TABLE_FUNCTION_FINALIZED);
        }
    }
}
