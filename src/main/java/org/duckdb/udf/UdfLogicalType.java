package org.duckdb.udf;

import java.util.Arrays;
import java.util.Objects;
import org.duckdb.DuckDBColumnType;

public final class UdfLogicalType {
    private static final int DECIMAL_WIDTH_MIN = 1;
    private static final int DECIMAL_WIDTH_MAX = 38;
    private static final int DECIMAL_SCALE_MIN = 0;
    private static final int DEFAULT_DECIMAL_WIDTH = 18;
    private static final int DEFAULT_DECIMAL_SCALE = 3;

    private final DuckDBColumnType type;
    private final UdfLogicalType childType;
    private final long arraySize;
    private final UdfLogicalType keyType;
    private final UdfLogicalType valueType;
    private final String[] fieldNames;
    private final UdfLogicalType[] fieldTypes;
    private final String[] enumValues;
    private final int decimalWidth;
    private final int decimalScale;

    private UdfLogicalType(DuckDBColumnType type, UdfLogicalType childType, long arraySize, UdfLogicalType keyType,
                           UdfLogicalType valueType, String[] fieldNames, UdfLogicalType[] fieldTypes,
                           String[] enumValues, int decimalWidth, int decimalScale) {
        this.type = Objects.requireNonNull(type, "type");
        this.childType = childType;
        this.arraySize = arraySize;
        this.keyType = keyType;
        this.valueType = valueType;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.enumValues = enumValues;
        this.decimalWidth = decimalWidth;
        this.decimalScale = decimalScale;
    }

    public static UdfLogicalType of(DuckDBColumnType type) {
        Objects.requireNonNull(type, "type");
        switch (type) {
        case LIST:
        case ARRAY:
        case MAP:
        case STRUCT:
        case UNION:
        case ENUM:
            throw new IllegalArgumentException("Use container/enum-specific factory for type " + type);
        case DECIMAL:
            return decimal(DEFAULT_DECIMAL_WIDTH, DEFAULT_DECIMAL_SCALE);
        default:
            return new UdfLogicalType(type, null, 0, null, null, null, null, null, 0, 0);
        }
    }

    public static UdfLogicalType list(UdfLogicalType childType) {
        return new UdfLogicalType(DuckDBColumnType.LIST, Objects.requireNonNull(childType, "childType"), 0, null, null,
                                  null, null, null, 0, 0);
    }

    public static UdfLogicalType array(UdfLogicalType childType, long arraySize) {
        if (arraySize <= 0) {
            throw new IllegalArgumentException("arraySize must be > 0");
        }
        return new UdfLogicalType(DuckDBColumnType.ARRAY, Objects.requireNonNull(childType, "childType"), arraySize,
                                  null, null, null, null, null, 0, 0);
    }

    public static UdfLogicalType map(UdfLogicalType keyType, UdfLogicalType valueType) {
        return new UdfLogicalType(DuckDBColumnType.MAP, null, 0, Objects.requireNonNull(keyType, "keyType"),
                                  Objects.requireNonNull(valueType, "valueType"), null, null, null, 0, 0);
    }

    public static UdfLogicalType struct(String[] fieldNames, UdfLogicalType[] fieldTypes) {
        validateFields(fieldNames, fieldTypes, "struct");
        return new UdfLogicalType(DuckDBColumnType.STRUCT, null, 0, null, null, fieldNames.clone(), fieldTypes.clone(),
                                  null, 0, 0);
    }

    public static UdfLogicalType unionType(String[] fieldNames, UdfLogicalType[] fieldTypes) {
        validateFields(fieldNames, fieldTypes, "union");
        return new UdfLogicalType(DuckDBColumnType.UNION, null, 0, null, null, fieldNames.clone(), fieldTypes.clone(),
                                  null, 0, 0);
    }

    public static UdfLogicalType enumeration(String... enumValues) {
        Objects.requireNonNull(enumValues, "enumValues");
        if (enumValues.length == 0) {
            throw new IllegalArgumentException("enumValues must not be empty");
        }
        String[] values = enumValues.clone();
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null || values[i].isEmpty()) {
                throw new IllegalArgumentException("enumValues[" + i + "] must not be null/empty");
            }
        }
        return new UdfLogicalType(DuckDBColumnType.ENUM, null, 0, null, null, null, null, values, 0, 0);
    }

    public static UdfLogicalType decimal(int width, int scale) {
        if (width < DECIMAL_WIDTH_MIN || width > DECIMAL_WIDTH_MAX) {
            throw new IllegalArgumentException("decimal width must be between " + DECIMAL_WIDTH_MIN + " and " +
                                               DECIMAL_WIDTH_MAX);
        }
        if (scale < DECIMAL_SCALE_MIN || scale > width) {
            throw new IllegalArgumentException("decimal scale must be between " + DECIMAL_SCALE_MIN + " and width");
        }
        return new UdfLogicalType(DuckDBColumnType.DECIMAL, null, 0, null, null, null, null, null, width, scale);
    }

    public DuckDBColumnType getType() {
        return type;
    }

    public UdfLogicalType getChildType() {
        return childType;
    }

    public long getArraySize() {
        return arraySize;
    }

    public UdfLogicalType getKeyType() {
        return keyType;
    }

    public UdfLogicalType getValueType() {
        return valueType;
    }

    public String[] getFieldNames() {
        return fieldNames == null ? null : fieldNames.clone();
    }

    public UdfLogicalType[] getFieldTypes() {
        return fieldTypes == null ? null : fieldTypes.clone();
    }

    public String[] getEnumValues() {
        return enumValues == null ? null : enumValues.clone();
    }

    public int getDecimalWidth() {
        return decimalWidth;
    }

    public int getDecimalScale() {
        return decimalScale;
    }

    @Override
    public String toString() {
        return "UdfLogicalType{"
            + "type=" + type + ", childType=" + childType + ", arraySize=" + arraySize + ", keyType=" + keyType +
            ", valueType=" + valueType + ", fieldNames=" + Arrays.toString(fieldNames) +
            ", fieldTypes=" + Arrays.toString(fieldTypes) + ", enumValues=" + Arrays.toString(enumValues) +
            ", decimalWidth=" + decimalWidth + ", decimalScale=" + decimalScale + "}";
    }

    private static void validateFields(String[] fieldNames, UdfLogicalType[] fieldTypes, String kind) {
        Objects.requireNonNull(fieldNames, "fieldNames");
        Objects.requireNonNull(fieldTypes, "fieldTypes");
        if (fieldNames.length == 0) {
            throw new IllegalArgumentException(kind + " fieldNames must not be empty");
        }
        if (fieldNames.length != fieldTypes.length) {
            throw new IllegalArgumentException(kind + " fieldNames/fieldTypes length mismatch");
        }
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i] == null || fieldNames[i].isEmpty()) {
                throw new IllegalArgumentException(kind + " fieldNames[" + i + "] must not be null/empty");
            }
            if (fieldTypes[i] == null) {
                throw new IllegalArgumentException(kind + " fieldTypes[" + i + "] must not be null");
            }
        }
    }
}
