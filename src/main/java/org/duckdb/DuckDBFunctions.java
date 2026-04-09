package org.duckdb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class DuckDBFunctions {
    public enum Kind { SCALAR }

    private DuckDBFunctions() {
    }

    public static DuckDBScalarFunctionBuilder scalarFunction() throws SQLException {
        return new DuckDBScalarFunctionBuilder();
    }

    static RegisteredFunction createRegisteredFunction(String name, List<DuckDBLogicalType> parameterTypes,
                                                       List<DuckDBColumnType> parameterColumnTypes,
                                                       DuckDBLogicalType returnType, DuckDBColumnType returnColumnType,
                                                       DuckDBScalarFunction function, DuckDBLogicalType varArgType,
                                                       boolean volatileFlag, boolean specialHandlingFlag,
                                                       boolean propagateNullsFlag) {
        return new RegisteredFunction(name, Kind.SCALAR, Collections.unmodifiableList(new ArrayList<>(parameterTypes)),
                                      Collections.unmodifiableList(new ArrayList<>(parameterColumnTypes)), returnType,
                                      returnColumnType, function, varArgType, volatileFlag, specialHandlingFlag,
                                      propagateNullsFlag);
    }

    public static class FunctionException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public FunctionException(String message) {
            super(message);
        }

        public FunctionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class RegisteredFunction {
        private final String name;
        private final Kind functionKind;
        private final List<DuckDBLogicalType> parameterTypes;
        private final List<DuckDBColumnType> parameterColumnTypes;
        private final DuckDBLogicalType returnType;
        private final DuckDBColumnType returnColumnType;
        private final DuckDBScalarFunction function;
        private final DuckDBLogicalType varArgType;
        private final boolean volatileFlag;
        private final boolean nullInNullOutFlag;
        private final boolean propagateNullsFlag;

        private RegisteredFunction(String name, Kind functionKind, List<DuckDBLogicalType> parameterTypes,
                                   List<DuckDBColumnType> parameterColumnTypes, DuckDBLogicalType returnType,
                                   DuckDBColumnType returnColumnType, DuckDBScalarFunction function,
                                   DuckDBLogicalType varArgType, boolean volatileFlag, boolean nullInNullOutFlag,
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
            this.nullInNullOutFlag = nullInNullOutFlag;
            this.propagateNullsFlag = propagateNullsFlag;
        }

        public String name() {
            return name;
        }

        public Kind functionKind() {
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

        public boolean isNullInNullOut() {
            return nullInNullOutFlag;
        }

        public boolean propagateNulls() {
            return propagateNullsFlag;
        }

        public boolean isScalar() {
            return functionKind == Kind.SCALAR;
        }
    }
}
