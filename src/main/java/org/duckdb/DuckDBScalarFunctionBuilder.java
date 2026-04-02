package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public final class DuckDBScalarFunctionBuilder implements AutoCloseable {
    private ByteBuffer scalarFunctionRef;
    private String functionName;
    private DuckDBLogicalType returnType;
    private DuckDBColumnType returnColumnType;
    private Class<?> returnJavaType;
    private DuckDBScalarVectorFunction callback;
    private DuckDBLogicalType varArgType;
    private final List<DuckDBLogicalType> parameterTypes = new ArrayList<>();
    private final List<DuckDBColumnType> parameterColumnTypes = new ArrayList<>();
    private final List<Class<?>> parameterJavaTypes = new ArrayList<>();
    private boolean volatileFlag;
    private boolean specialHandlingFlag;
    private boolean finalized;

    DuckDBScalarFunctionBuilder() throws SQLException {
        this.scalarFunctionRef = duckdb_create_scalar_function();
        if (scalarFunctionRef == null) {
            throw new SQLException("Failed to create scalar function");
        }
    }

    public DuckDBScalarFunctionBuilder withName(String name) throws SQLException {
        ensureNotFinalized();
        if (name == null || name.trim().isEmpty()) {
            throw new SQLException("Function name cannot be null or empty");
        }
        this.functionName = name;
        duckdb_scalar_function_set_name(scalarFunctionRef, name.getBytes(UTF_8));
        return this;
    }

    public DuckDBScalarFunctionBuilder withReturnType(DuckDBLogicalType returnType) throws SQLException {
        ensureNotFinalized();
        if (returnType == null) {
            throw new SQLException("Return type cannot be null");
        }
        this.returnType = returnType;
        this.returnColumnType = null;
        this.returnJavaType = null;
        duckdb_scalar_function_set_return_type(scalarFunctionRef, returnType.logicalTypeRef());
        return this;
    }

    public DuckDBScalarFunctionBuilder withParameter(DuckDBLogicalType parameterType) throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw new SQLException("Parameter type cannot be null");
        }
        parameterTypes.add(parameterType);
        parameterColumnTypes.add(null);
        parameterJavaTypes.add(null);
        duckdb_scalar_function_add_parameter(scalarFunctionRef, parameterType.logicalTypeRef());
        return this;
    }

    public DuckDBScalarFunctionBuilder withReturnType(Class<?> returnType) throws SQLException {
        ensureNotFinalized();
        if (returnType == null) {
            throw new SQLException("Return type cannot be null");
        }
        DuckDBColumnType mappedType = DuckDBScalarFunctionAdapter.mapJavaClassToDuckDBType(returnType);
        return setMappedReturnType(mappedType, returnType);
    }

    public DuckDBScalarFunctionBuilder withParameter(Class<?> parameterType) throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw new SQLException("Parameter type cannot be null");
        }
        DuckDBColumnType mappedType = DuckDBScalarFunctionAdapter.mapJavaClassToDuckDBType(parameterType);
        return addMappedParameterType(mappedType, parameterType);
    }

    public DuckDBScalarFunctionBuilder withReturnType(DuckDBColumnType returnType) throws SQLException {
        ensureNotFinalized();
        if (returnType == null) {
            throw new SQLException("Return type cannot be null");
        }
        return setMappedReturnType(returnType, null);
    }

    public DuckDBScalarFunctionBuilder withParameter(DuckDBColumnType parameterType) throws SQLException {
        ensureNotFinalized();
        if (parameterType == null) {
            throw new SQLException("Parameter type cannot be null");
        }
        return addMappedParameterType(parameterType, null);
    }

    public DuckDBScalarFunctionBuilder withVectorFunction(DuckDBScalarVectorFunction function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw new SQLException("Scalar function callback cannot be null");
        }
        this.callback = function;
        duckdb_scalar_function_set_function(scalarFunctionRef, new DuckDBScalarFunctionWrapper(function));
        return this;
    }

    public DuckDBScalarFunctionBuilder withFunction(Function<?, ?> function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw new SQLException("Scalar function callback cannot be null");
        }
        if (varArgType != null) {
            throw new SQLException("Function callback does not support varargs; use withVarArgsFunction instead");
        }
        if (parameterTypes.size() != 1) {
            throw new SQLException("Function callback requires exactly 1 declared parameter");
        }
        DuckDBColumnType parameterType = effectiveParameterType(0);
        Class<?> parameterJavaType = effectiveParameterJavaType(0);
        DuckDBColumnType resolvedReturnType = effectiveReturnType();
        Class<?> resolvedReturnJavaType = effectiveReturnJavaType();
        return withVectorFunction(DuckDBScalarFunctionAdapter.unary(function, parameterType, parameterJavaType,
                                                                    resolvedReturnType, resolvedReturnJavaType));
    }

    public DuckDBScalarFunctionBuilder withFunction(BiFunction<?, ?, ?> function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw new SQLException("Scalar function callback cannot be null");
        }
        if (varArgType != null) {
            throw new SQLException("BiFunction callback does not support varargs; use withVarArgsFunction instead");
        }
        if (parameterTypes.size() != 2) {
            throw new SQLException("BiFunction callback requires exactly 2 declared parameters");
        }
        DuckDBColumnType leftType = effectiveParameterType(0);
        Class<?> leftJavaType = effectiveParameterJavaType(0);
        DuckDBColumnType rightType = effectiveParameterType(1);
        Class<?> rightJavaType = effectiveParameterJavaType(1);
        DuckDBColumnType resolvedReturnType = effectiveReturnType();
        Class<?> resolvedReturnJavaType = effectiveReturnJavaType();
        return withVectorFunction(DuckDBScalarFunctionAdapter.binary(
            function, leftType, leftJavaType, rightType, rightJavaType, resolvedReturnType, resolvedReturnJavaType));
    }

    public DuckDBScalarFunctionBuilder withFunction(Supplier<?> function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw new SQLException("Scalar function callback cannot be null");
        }
        if (!parameterTypes.isEmpty()) {
            throw new SQLException("Supplier callback requires zero declared parameters");
        }
        if (varArgType != null) {
            throw new SQLException("Supplier callback does not support varargs");
        }
        DuckDBColumnType resolvedReturnType = effectiveReturnType();
        Class<?> resolvedReturnJavaType = effectiveReturnJavaType();
        return withVectorFunction(
            DuckDBScalarFunctionAdapter.nullary(function, resolvedReturnType, resolvedReturnJavaType));
    }

    public DuckDBScalarFunctionBuilder withVarArgsFunction(Function<Object[], ?> function) throws SQLException {
        ensureNotFinalized();
        if (function == null) {
            throw new SQLException("Scalar function callback cannot be null");
        }
        if (varArgType == null) {
            throw new SQLException("Varargs functional callback requires withVarArgs(...) declaration");
        }
        DuckDBColumnType[] fixedTypes = effectiveFixedParameterTypes();
        Class<?>[] fixedJavaTypes = effectiveFixedParameterJavaTypes();
        DuckDBColumnType varArgColumnType = DuckDBScalarFunctionAdapter.mapLogicalTypeToDuckDBType(varArgType);
        DuckDBColumnType resolvedReturnType = effectiveReturnType();
        Class<?> resolvedReturnJavaType = effectiveReturnJavaType();
        return withVectorFunction(DuckDBScalarFunctionAdapter.variadic(
            function, fixedTypes, fixedJavaTypes, varArgColumnType, null, resolvedReturnType, resolvedReturnJavaType));
    }

    public DuckDBScalarFunctionBuilder withVarArgs(DuckDBLogicalType varArgType) throws SQLException {
        ensureNotFinalized();
        if (varArgType == null) {
            throw new SQLException("Varargs type cannot be null");
        }
        this.varArgType = varArgType;
        duckdb_scalar_function_set_varargs(scalarFunctionRef, varArgType.logicalTypeRef());
        return this;
    }

    public DuckDBScalarFunctionBuilder withVolatile() throws SQLException {
        ensureNotFinalized();
        this.volatileFlag = true;
        duckdb_scalar_function_set_volatile(scalarFunctionRef);
        return this;
    }

    public DuckDBScalarFunctionBuilder withSpecialHandling() throws SQLException {
        ensureNotFinalized();
        this.specialHandlingFlag = true;
        duckdb_scalar_function_set_special_handling(scalarFunctionRef);
        return this;
    }

    public DuckDBScalarFunction register(DuckDBConnection connection) throws SQLException {
        ensureNotFinalized();
        if (connection == null) {
            throw new SQLException("Connection cannot be null");
        }
        if (functionName == null) {
            throw new SQLException("Function name must be defined");
        }
        if (returnType == null && returnColumnType == null) {
            throw new SQLException("Return type must be defined");
        }
        if (callback == null) {
            throw new SQLException("Scalar function callback must be defined");
        }
        boolean lockAcquired = false;
        try {
            connection.connRefLock.lock();
            lockAcquired = true;
            connection.checkOpen();
            int status = duckdb_register_scalar_function(connection.connRef, scalarFunctionRef);
            if (status != 0) {
                throw new SQLException("Failed to register scalar function '" + functionName + "'");
            }
            return DuckDBScalarFunction.of(functionName, parameterTypes, parameterColumnTypes, returnType,
                                           returnColumnType, callback, varArgType, volatileFlag, specialHandlingFlag);
        } finally {
            close();
            if (lockAcquired) {
                connection.connRefLock.unlock();
            }
        }
    }

    @Override
    public void close() {
        if (scalarFunctionRef != null) {
            duckdb_destroy_scalar_function(scalarFunctionRef);
            scalarFunctionRef = null;
        }
        finalized = true;
    }

    private void ensureNotFinalized() throws SQLException {
        if (finalized || scalarFunctionRef == null) {
            throw new SQLException("Scalar function builder is already finalized");
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
        throw new SQLException("Return type must be defined before functional callback");
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
        return parameterJavaTypes.toArray(new Class<?>[ 0 ]);
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
}
