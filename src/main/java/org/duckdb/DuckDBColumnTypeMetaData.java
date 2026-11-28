package org.duckdb;

public class DuckDBColumnTypeMetaData {
    /**
     * Value of {@link #type_size} for a DECIMAL wider than 38 digits, such values have no fixed size.
     */
    public static final int VARIABLE_TYPE_SIZE = 0;

    public final int type_size;
    public final int width;
    public final int scale;

    public DuckDBColumnTypeMetaData(int type_size, int width, int scale) {
        this.type_size = type_size;
        this.width = width;
        this.scale = scale;
    }

    public static DuckDBColumnTypeMetaData parseColumnTypeMetadata(String columnTypeDetail) {
        String[] split_details = columnTypeDetail.split(";");
        String storage = split_details[0].replace("DECIMAL", "");
        int type_size = "BIGNUM".equals(storage) ? VARIABLE_TYPE_SIZE : Integer.parseInt(storage);
        return new DuckDBColumnTypeMetaData(type_size, Integer.parseInt(split_details[1]),
                                            Integer.parseInt(split_details[2]));
    }
}
