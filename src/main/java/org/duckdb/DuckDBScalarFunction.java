package org.duckdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class DuckDBScalarFunction {
    private final String name;
    private final List<DuckDBLogicalType> parameterTypes;
    private final List<DuckDBColumnType> parameterColumnTypes;
    private final DuckDBLogicalType returnType;
    private final DuckDBColumnType returnColumnType;
    private final DuckDBScalarVectorFunction function;
    private final DuckDBLogicalType varArgType;
    private final boolean volatileFlag;
    private final boolean specialHandlingFlag;

    private DuckDBScalarFunction(String name, List<DuckDBLogicalType> parameterTypes,
                                 List<DuckDBColumnType> parameterColumnTypes, DuckDBLogicalType returnType,
                                 DuckDBColumnType returnColumnType, DuckDBScalarVectorFunction function,
                                 DuckDBLogicalType varArgType, boolean volatileFlag, boolean specialHandlingFlag) {
        this.name = name;
        this.parameterTypes = parameterTypes;
        this.parameterColumnTypes = parameterColumnTypes;
        this.returnType = returnType;
        this.returnColumnType = returnColumnType;
        this.function = function;
        this.varArgType = varArgType;
        this.volatileFlag = volatileFlag;
        this.specialHandlingFlag = specialHandlingFlag;
    }

    public String name() {
        return name;
    }

    public List<DuckDBLogicalType> parameterTypes() {
        return parameterTypes;
    }

    public List<DuckDBColumnType> parameterColumnTypes() {
        return parameterColumnTypes;
    }

    public DuckDBLogicalType returnType() {
        return returnType;
    }

    public DuckDBColumnType returnColumnType() {
        return returnColumnType;
    }

    public DuckDBScalarVectorFunction function() {
        return function;
    }

    public DuckDBLogicalType varArgType() {
        return varArgType;
    }

    public boolean isVolatile() {
        return volatileFlag;
    }

    public boolean hasSpecialHandling() {
        return specialHandlingFlag;
    }

    static DuckDBScalarFunction of(String name, List<DuckDBLogicalType> parameterTypes,
                                   List<DuckDBColumnType> parameterColumnTypes, DuckDBLogicalType returnType,
                                   DuckDBColumnType returnColumnType, DuckDBScalarVectorFunction function,
                                   DuckDBLogicalType varArgType, boolean volatileFlag, boolean specialHandlingFlag) {
        return new DuckDBScalarFunction(name, Collections.unmodifiableList(new ArrayList<>(parameterTypes)),
                                        Collections.unmodifiableList(new ArrayList<>(parameterColumnTypes)), returnType,
                                        returnColumnType, function, varArgType, volatileFlag, specialHandlingFlag);
    }
}
