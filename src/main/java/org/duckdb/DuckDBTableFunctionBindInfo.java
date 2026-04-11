package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.duckdb.DuckDBFunctions.FunctionException;

public final class DuckDBTableFunctionBindInfo {
    private final ByteBuffer bindInfoRef;
    private final ConcurrentLinkedQueue<DuckDBValue> accessedParameters = new ConcurrentLinkedQueue<>();

    public DuckDBTableFunctionBindInfo(ByteBuffer bindInfoRef) {
        this.bindInfoRef = bindInfoRef;
    }

    public DuckDBTableFunctionBindInfo addResultColumn(String name, Class<?> columnType) {
        if (null == columnType) {
            throw new FunctionException("Specified column type must be not null");
        }
        DuckDBColumnType mappedType = null;
        try {
            mappedType = DuckDBScalarFunctionAdapter.mapJavaClassToDuckDBType(columnType);
        } catch (SQLException e) {
            throw new FunctionException(e);
        }
        return addResultColumn(name, mappedType);
    }

    public DuckDBTableFunctionBindInfo addResultColumn(String name, DuckDBColumnType columnType) {
        if (null == columnType) {
            throw new FunctionException("Specified column type must be not null");
        }
        try (DuckDBLogicalType logicalType = DuckDBLogicalType.of(columnType)) {
            return addResultColumn(name, logicalType);
        } catch (SQLException e) {
            throw new FunctionException(e);
        }
    }

    public DuckDBTableFunctionBindInfo addResultColumn(String name, DuckDBLogicalType columnType) {
        if (null == name || name.isEmpty()) {
            throw new FunctionException("Specified column name must be not empty");
        }
        try {
            byte[] name_bytes = name.getBytes(UTF_8);
            duckdb_bind_add_result_column(bindInfoRef, name_bytes, columnType.logicalTypeRef());
            return this;
        } catch (SQLException e) {
            throw new FunctionException(e);
        }
    }

    public DuckDBValue getParameter(long index) {
        if (index < 0 || index >= parametersCount()) {
            throw new IndexOutOfBoundsException("Parameter index out of bounds: " + index);
        }
        ByteBuffer valueRef = duckdb_bind_get_parameter(bindInfoRef, index);
        if (valueRef == null) {
            throw new FunctionException("Parameter at index " + index + " not found");
        }
        DuckDBValue par = new DuckDBValue(valueRef);
        accessedParameters.add(par);
        return par;
    }

    public DuckDBValue getNamedParameter(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new FunctionException("Parameter name cannot be empty");
        }
        byte[] name_bytes = name.getBytes(UTF_8);
        ByteBuffer valueRef = duckdb_bind_get_named_parameter(bindInfoRef, name_bytes);
        if (valueRef == null) {
            throw new FunctionException("Named parameter '" + name + "' not found");
        }
        DuckDBValue par = new DuckDBValue(valueRef);
        accessedParameters.add(par);
        return par;
    }

    void clearAccessedParameters() {
        for (DuckDBValue par : accessedParameters) {
            par.close();
        }
        accessedParameters.clear();
    }

    public long parametersCount() {
        return duckdb_bind_get_parameter_count(bindInfoRef);
    }

    public void setCardinality(long cardinality, boolean isExact) {
        duckdb_bind_set_cardinality(bindInfoRef, cardinality, isExact);
    }
}
