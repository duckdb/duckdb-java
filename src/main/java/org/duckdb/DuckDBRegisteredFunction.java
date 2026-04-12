package org.duckdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class DuckDBRegisteredFunction {
    private final String name;
    private final DuckDBFunctions.DuckDBFunctionKind functionKind;
    private final List<DuckDBLogicalType> parameterTypes;
    private final List<DuckDBColumnType> parameterColumnTypes;
    private final DuckDBLogicalType returnType;
    private final DuckDBColumnType returnColumnType;
    private final DuckDBScalarFunction function;
    private final DuckDBLogicalType varArgType;
    private final boolean volatileFlag;
    private final boolean specialHandlingFlag;
    private final boolean propagateNullsFlag;

    private DuckDBRegisteredFunction(String name, DuckDBFunctions.DuckDBFunctionKind functionKind,
                                     List<DuckDBLogicalType> parameterTypes,
                                     List<DuckDBColumnType> parameterColumnTypes, DuckDBLogicalType returnType,
                                     DuckDBColumnType returnColumnType, DuckDBScalarFunction function,
                                     DuckDBLogicalType varArgType, boolean volatileFlag, boolean specialHandlingFlag,
                                     boolean propagateNullsFlag) {
        this.name = name;
        this.functionKind = functionKind;
        this.parameterTypes = parameterTypes;
        this.parameterColumnTypes = parameterColumnTypes;
        this.returnType = returnType;
        this.returnColumnType = returnColumnType;
        this.function = function;
        this.varArgType = varArgType;
        this.volatileFlag = volatileFlag;
        this.specialHandlingFlag = specialHandlingFlag;
        this.propagateNullsFlag = propagateNullsFlag;
    }

    public String name() {
        return name;
    }

    public DuckDBFunctions.DuckDBFunctionKind functionKind() {
        return functionKind;
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

    public DuckDBScalarFunction function() {
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

    public boolean propagateNulls() {
        return propagateNullsFlag;
    }

    public boolean isScalar() {
        return functionKind == DuckDBFunctions.DuckDBFunctionKind.SCALAR;
    }

    static DuckDBRegisteredFunction of(String name, List<DuckDBLogicalType> parameterTypes,
                                       List<DuckDBColumnType> parameterColumnTypes, DuckDBLogicalType returnType,
                                       DuckDBColumnType returnColumnType, DuckDBScalarFunction function,
                                       DuckDBLogicalType varArgType, boolean volatileFlag, boolean specialHandlingFlag,
                                       boolean propagateNullsFlag) {
        return new DuckDBRegisteredFunction(name, DuckDBFunctions.DuckDBFunctionKind.SCALAR,
                                            Collections.unmodifiableList(new ArrayList<>(parameterTypes)),
                                            Collections.unmodifiableList(new ArrayList<>(parameterColumnTypes)),
                                            returnType, returnColumnType, function, varArgType, volatileFlag,
                                            specialHandlingFlag, propagateNullsFlag);
    }
}
