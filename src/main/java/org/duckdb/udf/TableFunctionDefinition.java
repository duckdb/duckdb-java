package org.duckdb.udf;

import java.util.Objects;
import org.duckdb.DuckDBColumnType;

public final class TableFunctionDefinition {
    private final boolean projectionPushdown;
    private final UdfLogicalType[] parameterLogicalTypes;

    public TableFunctionDefinition() {
        this(false, new UdfLogicalType[] {UdfLogicalType.of(DuckDBColumnType.BIGINT)});
    }

    private TableFunctionDefinition(boolean projectionPushdown, UdfLogicalType[] parameterLogicalTypes) {
        this.projectionPushdown = projectionPushdown;
        this.parameterLogicalTypes = Objects.requireNonNull(parameterLogicalTypes, "parameterLogicalTypes").clone();
        for (UdfLogicalType parameterType : this.parameterLogicalTypes) {
            Objects.requireNonNull(parameterType, "parameterLogicalTypes cannot contain null values");
        }
    }

    public TableFunctionDefinition withProjectionPushdown(boolean enabled) {
        return new TableFunctionDefinition(enabled, parameterLogicalTypes);
    }

    public TableFunctionDefinition withParameterTypes(DuckDBColumnType[] parameterTypes) {
        Objects.requireNonNull(parameterTypes, "parameterTypes");
        UdfLogicalType[] logicalTypes = new UdfLogicalType[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            logicalTypes[i] = UdfLogicalType.of(
                Objects.requireNonNull(parameterTypes[i], "parameterTypes cannot contain null values"));
        }
        return new TableFunctionDefinition(projectionPushdown, logicalTypes);
    }

    public TableFunctionDefinition withParameterTypes(UdfLogicalType[] parameterLogicalTypes) {
        return new TableFunctionDefinition(projectionPushdown, parameterLogicalTypes);
    }

    public boolean isProjectionPushdownEnabled() {
        return projectionPushdown;
    }

    public DuckDBColumnType[] getParameterTypes() {
        DuckDBColumnType[] types = new DuckDBColumnType[parameterLogicalTypes.length];
        for (int i = 0; i < parameterLogicalTypes.length; i++) {
            types[i] = parameterLogicalTypes[i].getType();
        }
        return types;
    }

    public UdfLogicalType[] getParameterLogicalTypes() {
        return parameterLogicalTypes.clone();
    }
}
