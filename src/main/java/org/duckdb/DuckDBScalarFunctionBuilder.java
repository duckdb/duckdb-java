package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.JdbcUtils.createSQLException;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntUnaryOperator;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import org.duckdb.DuckDBFunctions.RegisteredFunction;

public final class DuckDBScalarFunctionBuilder implements AutoCloseable {
    private ByteBuffer scalarFunctionRef;
    private String functionName;
    private DuckDBLogicalType returnType;
    private DuckDBColumnType returnColumnType;
    private Class<?> returnJavaType;
    private DuckDBScalarFunction callback;
    private DuckDBLogicalType varArgType;
    private final List<DuckDBLogicalType> parameterTypes = new ArrayList<>();
    private final List<DuckDBColumnType> parameterColumnTypes = new ArrayList<>();
    private final List<Class<?>> parameterJavaTypes = new ArrayList<>();
    private boolean nullInNullOutFlag;
    private boolean finalized;

    DuckDBScalarFunctionBuilder() throws SQLException {
        this.scalarFunctionRef = duckdb_create_scalar_function();
        if (scalarFunctionRef == null) {
            throw createSQLException("Failed to create scalar function", ErrorCode.FUNCTION_CREATE);
        }
    }

    public DuckDBScalarFunctionBuilder withName(String name) throws SQLException {
        ensureNotFinalized();
        if (name == null || name.trim().isEmpty()) {
            throw createSQLException("Function name cannot be null or empty", ErrorCode.FUNCTION_NAME_EMPTY);
        }
        this.functionName = name;
        duckdb_scalar_function_set_name(scalarFunctionRef, name.getBytes(UTF_8));
        return this;
    }

    public DuckDBScalarFunctionBuilder withParameter(Class<?> parameterType) throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw createSQLException("Parameter type cannot be null", ErrorCode.FUNCTION_PARAM_NULL);
        }
        DuckDBColumnType mappedType = DuckDBScalarFunctionAdapter.mapJavaClassToDuckDBType(parameterType);
        return addMappedParameterType(mappedType, parameterType);
    }

    public DuckDBScalarFunctionBuilder withParameter(DuckDBColumnType parameterType) throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw createSQLException("Parameter type cannot be null", ErrorCode.FUNCTION_PARAM_NULL);
        }
        return addMappedParameterType(parameterType, null);
    }

    public DuckDBScalarFunctionBuilder withParameter(DuckDBLogicalType parameterType) throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw createSQLException("Parameter type cannot be null", ErrorCode.FUNCTION_PARAM_NULL);
        }
        parameterTypes.add(parameterType);
        parameterColumnTypes.add(null);
        parameterJavaTypes.add(null);
        duckdb_scalar_function_add_parameter(scalarFunctionRef, parameterType.logicalTypeRef());
        return this;
    }

    public DuckDBScalarFunctionBuilder withParameters(Class<?>... parameterTypes) throws SQLException {
        ensureNotFinalized();
        if (parameterTypes == null) {
            throw createSQLException("Parameter types cannot be null", ErrorCode.FUNCTION_PARAMS_NULL);
        }
        for (Class<?> parameterType : parameterTypes) {
            withParameter(parameterType);
        }
        return this;
    }

    public DuckDBScalarFunctionBuilder withParameters(DuckDBColumnType... parameterTypes) throws SQLException {
        ensureNotFinalized();
        if (parameterTypes == null) {
            throw createSQLException("Parameter types cannot be null", ErrorCode.FUNCTION_PARAMS_NULL);
        }
        for (DuckDBColumnType parameterType : parameterTypes) {
            withParameter(parameterType);
        }
        return this;
    }

    public DuckDBScalarFunctionBuilder withParameters(DuckDBLogicalType... parameterTypes) throws SQLException {
        ensureNotFinalized();
        if (parameterTypes == null) {
            throw createSQLException("Parameter types cannot be null", ErrorCode.FUNCTION_PARAMS_NULL);
        }
        for (DuckDBLogicalType parameterType : parameterTypes) {
            withParameter(parameterType);
        }
        return this;
    }

    public DuckDBScalarFunctionBuilder withReturnType(Class<?> returnType) throws SQLException {
        ensureNotFinalized();
        if (returnType == null) {
            throw createSQLException("Return type cannot be null", ErrorCode.FUNCTION_RETURN_NULL);
        }
        DuckDBColumnType mappedType = DuckDBScalarFunctionAdapter.mapJavaClassToDuckDBType(returnType);
        return setMappedReturnType(mappedType, returnType);
    }

    public DuckDBScalarFunctionBuilder withReturnType(DuckDBColumnType returnType) throws SQLException {
        ensureNotFinalized();
        if (returnType == null) {
            throw createSQLException("Return type cannot be null", ErrorCode.FUNCTION_RETURN_NULL);
        }
        return setMappedReturnType(returnType, null);
    }

    public DuckDBScalarFunctionBuilder withReturnType(DuckDBLogicalType returnType) throws SQLException {
        ensureNotFinalized();
        if (returnType == null) {
            throw createSQLException("Return type cannot be null", ErrorCode.FUNCTION_RETURN_NULL);
        }
        this.returnType = returnType;
        this.returnColumnType = null;
        this.returnJavaType = null;
        duckdb_scalar_function_set_return_type(scalarFunctionRef, returnType.logicalTypeRef());
        return this;
    }

    public DuckDBScalarFunctionBuilder withVectorizedFunction(DuckDBScalarFunction function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        return setCallback(function, false);
    }

    public DuckDBScalarFunctionBuilder withIntFunction(IntUnaryOperator function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        enablePrimitiveNullPropagation();
        ensurePrimitiveCallbackCompatible("withIntFunction");
        ensureUnaryPrimitiveSignature(DuckDBColumnType.INTEGER, "withIntFunction");
        return setCallback(DuckDBScalarFunctionAdapter.intUnary(function), true);
    }

    public DuckDBScalarFunctionBuilder withIntFunction(IntBinaryOperator function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        enablePrimitiveNullPropagation();
        ensurePrimitiveCallbackCompatible("withIntFunction");
        ensureBinaryPrimitiveSignature(DuckDBColumnType.INTEGER, "withIntFunction");
        return setCallback(DuckDBScalarFunctionAdapter.intBinary(function), true);
    }

    public DuckDBScalarFunctionBuilder withDoubleFunction(DoubleUnaryOperator function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        enablePrimitiveNullPropagation();
        ensurePrimitiveCallbackCompatible("withDoubleFunction");
        ensureUnaryPrimitiveSignature(DuckDBColumnType.DOUBLE, "withDoubleFunction");
        return setCallback(DuckDBScalarFunctionAdapter.doubleUnary(function), true);
    }

    public DuckDBScalarFunctionBuilder withDoubleFunction(DoubleBinaryOperator function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        enablePrimitiveNullPropagation();
        ensurePrimitiveCallbackCompatible("withDoubleFunction");
        ensureBinaryPrimitiveSignature(DuckDBColumnType.DOUBLE, "withDoubleFunction");
        return setCallback(DuckDBScalarFunctionAdapter.doubleBinary(function), true);
    }

    public DuckDBScalarFunctionBuilder withLongFunction(LongUnaryOperator function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        enablePrimitiveNullPropagation();
        ensurePrimitiveCallbackCompatible("withLongFunction");
        ensureUnaryPrimitiveSignature(DuckDBColumnType.BIGINT, "withLongFunction");
        return setCallback(DuckDBScalarFunctionAdapter.longUnary(function), true);
    }

    public DuckDBScalarFunctionBuilder withLongFunction(LongBinaryOperator function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        enablePrimitiveNullPropagation();
        ensurePrimitiveCallbackCompatible("withLongFunction");
        ensureBinaryPrimitiveSignature(DuckDBColumnType.BIGINT, "withLongFunction");
        return setCallback(DuckDBScalarFunctionAdapter.longBinary(function), true);
    }

    public <INPUT, OUTPUT> DuckDBScalarFunctionBuilder withFunction(Function<INPUT, OUTPUT> function)
        throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        if (varArgType != null) {
            throw createSQLException("Function callback does not support varargs; use withVarArgsFunction instead",
                                     ErrorCode.FUNCTION_VARARGS_MISUSE);
        }
        if (parameterTypes.size() != 1) {
            throw createSQLException("Function callback requires exactly 1 declared parameter",
                                     ErrorCode.FUNCTION_PARAM_COUNT);
        }
        DuckDBColumnType parameterType = effectiveParameterType(0);
        Class<?> parameterJavaType = effectiveParameterJavaType(0);
        DuckDBColumnType resolvedReturnType = effectiveReturnType();
        Class<?> resolvedReturnJavaType = effectiveReturnJavaType();
        return withVectorizedFunction(DuckDBScalarFunctionAdapter.unary(function, parameterType, parameterJavaType,
                                                                        resolvedReturnType, resolvedReturnJavaType));
    }

    public <LEFT, RIGHT, OUTPUT> DuckDBScalarFunctionBuilder withFunction(BiFunction<LEFT, RIGHT, OUTPUT> function)
        throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        if (varArgType != null) {
            throw createSQLException("BiFunction callback does not support varargs; use withVarArgsFunction instead",
                                     ErrorCode.FUNCTION_VARARGS_MISUSE);
        }
        if (parameterTypes.size() != 2) {
            throw createSQLException("BiFunction callback requires exactly 2 declared parameters",
                                     ErrorCode.FUNCTION_PARAM_COUNT);
        }
        DuckDBColumnType leftType = effectiveParameterType(0);
        Class<?> leftJavaType = effectiveParameterJavaType(0);
        DuckDBColumnType rightType = effectiveParameterType(1);
        Class<?> rightJavaType = effectiveParameterJavaType(1);
        DuckDBColumnType resolvedReturnType = effectiveReturnType();
        Class<?> resolvedReturnJavaType = effectiveReturnJavaType();
        return withVectorizedFunction(DuckDBScalarFunctionAdapter.binary(
            function, leftType, leftJavaType, rightType, rightJavaType, resolvedReturnType, resolvedReturnJavaType));
    }

    public <OUTPUT> DuckDBScalarFunctionBuilder withFunction(Supplier<OUTPUT> function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        if (!parameterTypes.isEmpty()) {
            throw createSQLException("Supplier callback requires zero declared parameters",
                                     ErrorCode.FUNCTION_PARAM_COUNT);
        }
        if (varArgType != null) {
            throw createSQLException("Supplier callback does not support varargs", ErrorCode.FUNCTION_VARARGS_MISUSE);
        }
        DuckDBColumnType resolvedReturnType = effectiveReturnType();
        Class<?> resolvedReturnJavaType = effectiveReturnJavaType();
        return withVectorizedFunction(
            DuckDBScalarFunctionAdapter.nullary(function, resolvedReturnType, resolvedReturnJavaType));
    }

    public DuckDBScalarFunctionBuilder withVarArgsFunction(Function<Object[], ?> function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw createSQLException("Scalar function callback cannot be null", ErrorCode.FUNCTION_CALLBACK_NULL);
        }
        if (varArgType == null) {
            throw createSQLException("Varargs functional callback requires withVarArgs(...) declaration",
                                     ErrorCode.FUNCTION_VARARGS_MISUSE);
        }
        DuckDBColumnType[] fixedTypes = effectiveFixedParameterTypes();
        Class<?>[] fixedJavaTypes = effectiveFixedParameterJavaTypes();
        DuckDBColumnType varArgColumnType = DuckDBScalarFunctionAdapter.mapLogicalTypeToDuckDBType(varArgType);
        DuckDBColumnType resolvedReturnType = effectiveReturnType();
        Class<?> resolvedReturnJavaType = effectiveReturnJavaType();
        return withVectorizedFunction(DuckDBScalarFunctionAdapter.variadic(
            function, fixedTypes, fixedJavaTypes, varArgColumnType, null, resolvedReturnType, resolvedReturnJavaType));
    }

    public DuckDBScalarFunctionBuilder withVarArgs(DuckDBLogicalType varArgType) throws SQLException {
        ensureNotFinalized();
        if (varArgType == null) {
            throw createSQLException("Varargs type cannot be null", ErrorCode.FUNCTION_VARARGS_TYPE_NULL);
        }
        this.varArgType = varArgType;
        duckdb_scalar_function_set_varargs(scalarFunctionRef, varArgType.logicalTypeRef());
        return this;
    }

    public DuckDBScalarFunctionBuilder withVolatile() throws SQLException {
        ensureNotFinalized();
        duckdb_scalar_function_set_volatile(scalarFunctionRef);
        return this;
    }

    public DuckDBScalarFunctionBuilder withNullInNullOut() throws SQLException {
        ensureNotFinalized();
        this.nullInNullOutFlag = true;
        return this;
    }

    public RegisteredFunction register(Connection connection) throws SQLException {
        ensureNotFinalized();
        if (connection == null) {
            throw createSQLException("Connection cannot be null", ErrorCode.FUNCTION_CONNECTION_NULL);
        }
        if (functionName == null) {
            throw createSQLException("Function name must be defined", ErrorCode.FUNCTION_NO_NAME);
        }
        if (returnType == null && returnColumnType == null) {
            throw createSQLException("Return type must be defined", ErrorCode.FUNCTION_NO_RETURN);
        }
        if (callback == null) {
            throw createSQLException("Scalar function callback must be defined", ErrorCode.FUNCTION_NO_CALLBACK);
        }
        if (!nullInNullOutFlag) {
            duckdb_scalar_function_set_special_handling(scalarFunctionRef);
        }
        DuckDBConnection duckConnection = unwrapConnection(connection);
        Lock connectionLock = duckConnection.connRefLock;
        connectionLock.lock();
        try {
            duckConnection.checkOpen();
            int status = duckdb_register_scalar_function(duckConnection.connRef, scalarFunctionRef);
            if (status != 0) {
                throw createSQLException("Failed to register scalar function '" + functionName + "'",
                                         ErrorCode.FUNCTION_REGISTER_NATIVE);
            }
            return DuckDBDriver.registerFunction(functionName, DuckDBFunctions.Kind.SCALAR);
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
        if (scalarFunctionRef != null) {
            duckdb_destroy_scalar_function(scalarFunctionRef);
            scalarFunctionRef = null;
        }
        if (varArgType != null) {
            varArgType.close();
            varArgType = null;
        }
        for (DuckDBLogicalType lt : parameterTypes) {
            if (null != lt) {
                lt.close();
            }
        }
        parameterTypes.clear();
        finalized = true;
    }

    private void ensureNotFinalized() throws SQLException {
        if (finalized || scalarFunctionRef == null) {
            throw createSQLException("Scalar function builder is already finalized", ErrorCode.FUNCTION_FINALIZED);
        }
    }

    private DuckDBColumnType effectiveParameterType(int index) throws SQLException {
        DuckDBColumnType parameterColumnType = parameterColumnTypes.get(index);
        if (parameterColumnType != null) {
            return parameterColumnType;
        }
        DuckDBLogicalType parameterLogicalType = parameterTypes.get(index);
        return DuckDBScalarFunctionAdapter.mapLogicalTypeToDuckDBType(parameterLogicalType);
    }

    private DuckDBColumnType effectiveReturnType() throws SQLException {
        if (returnColumnType != null) {
            return returnColumnType;
        }
        if (returnType != null) {
            return DuckDBScalarFunctionAdapter.mapLogicalTypeToDuckDBType(returnType);
        }
        throw createSQLException("Return type must be defined before functional callback",
                                 ErrorCode.FUNCTION_NO_RETURN);
    }

    private Class<?> effectiveParameterJavaType(int index) {
        return parameterJavaTypes.get(index);
    }

    private Class<?> effectiveReturnJavaType() {
        return returnJavaType;
    }

    private DuckDBColumnType[] effectiveFixedParameterTypes() throws SQLException {
        DuckDBColumnType[] fixedTypes = new DuckDBColumnType[parameterTypes.size()];
        for (int i = 0; i < fixedTypes.length; i++) {
            fixedTypes[i] = effectiveParameterType(i);
        }
        return fixedTypes;
    }

    private Class<?>[] effectiveFixedParameterJavaTypes() {
        return parameterJavaTypes.toArray(new Class<?>[0]);
    }

    private DuckDBScalarFunctionBuilder setMappedReturnType(DuckDBColumnType mappedType, Class<?> javaType)
        throws SQLException {
        this.returnType = null;
        this.returnColumnType = mappedType;
        this.returnJavaType = javaType;
        try (DuckDBLogicalType logicalType = DuckDBLogicalType.of(mappedType)) {
            duckdb_scalar_function_set_return_type(scalarFunctionRef, logicalType.logicalTypeRef());
        }
        return this;
    }

    private DuckDBScalarFunctionBuilder addMappedParameterType(DuckDBColumnType mappedType, Class<?> javaType)
        throws SQLException {
        parameterTypes.add(null);
        parameterColumnTypes.add(mappedType);
        parameterJavaTypes.add(javaType);
        try (DuckDBLogicalType logicalType = DuckDBLogicalType.of(mappedType)) {
            duckdb_scalar_function_add_parameter(scalarFunctionRef, logicalType.logicalTypeRef());
        }
        return this;
    }

    private DuckDBScalarFunctionBuilder setCallback(DuckDBScalarFunction function, boolean requiresNullPropagation)
        throws SQLException {
        this.callback = function;
        DuckDBScalarFunctionWrapper wrapper = new DuckDBScalarFunctionWrapper(function);
        duckdb_scalar_function_set_extra_info(scalarFunctionRef, wrapper);
        duckdb_scalar_function_set_function(scalarFunctionRef);
        return this;
    }

    private void ensurePrimitiveCallbackCompatible(String callbackMethodName) throws SQLException {
        if (varArgType != null) {
            throw createSQLException(callbackMethodName +
                                         " does not support varargs; use withVarArgsFunction instead",
                                     ErrorCode.FUNCTION_VARARGS_MISUSE);
        }
    }

    private void enablePrimitiveNullPropagation() {
        nullInNullOutFlag = false;
    }

    private void ensureUnaryPrimitiveSignature(DuckDBColumnType expectedType, String callbackMethodName)
        throws SQLException {
        if (parameterTypes.size() != 1) {
            throw createSQLException(callbackMethodName + " requires exactly 1 declared parameter",
                                     ErrorCode.FUNCTION_PARAM_COUNT);
        }
        ensurePrimitiveParameterType(0, expectedType, callbackMethodName);
        ensurePrimitiveReturnType(expectedType, callbackMethodName);
    }

    private void ensureBinaryPrimitiveSignature(DuckDBColumnType expectedType, String callbackMethodName)
        throws SQLException {
        if (parameterTypes.size() != 2) {
            throw createSQLException(callbackMethodName + " requires exactly 2 declared parameters",
                                     ErrorCode.FUNCTION_PARAM_COUNT);
        }
        ensurePrimitiveParameterType(0, expectedType, callbackMethodName);
        ensurePrimitiveParameterType(1, expectedType, callbackMethodName);
        ensurePrimitiveReturnType(expectedType, callbackMethodName);
    }

    private void ensurePrimitiveParameterType(int index, DuckDBColumnType expectedType, String callbackMethodName)
        throws SQLException {
        DuckDBColumnType actualType = effectiveParameterType(index);
        if (actualType != expectedType) {
            throw createSQLException(callbackMethodName + " requires parameter " + index + " to be " + expectedType +
                                         ", got " + actualType,
                                     ErrorCode.FUNCTION_TYPE_MISMATCH);
        }
    }

    private void ensurePrimitiveReturnType(DuckDBColumnType expectedType, String callbackMethodName)
        throws SQLException {
        DuckDBColumnType actualType = effectiveReturnType();
        if (actualType != expectedType) {
            throw createSQLException(callbackMethodName + " requires return type " + expectedType + ", got " +
                                         actualType,
                                     ErrorCode.FUNCTION_TYPE_MISMATCH);
        }
    }

    private static DuckDBConnection unwrapConnection(Connection connection) throws SQLException {
        try {
            return connection.unwrap(DuckDBConnection.class);
        } catch (SQLException exception) {
            throw createSQLException("Scalar function registration requires a DuckDB JDBC connection",
                                     ErrorCode.FUNCTION_REGISTER_CONNECTION, exception);
        }
    }
}
