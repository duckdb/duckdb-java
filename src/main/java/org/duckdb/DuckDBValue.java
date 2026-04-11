package org.duckdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.duckdb.DuckDBBindings.*;
import static org.duckdb.DuckDBBindings.CAPIType.*;
import static org.duckdb.DuckDBReadableVector.unsignedLongToBigInteger;
import static org.duckdb.DuckDBTimestamp.localDateTimeFromTimestamp;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import org.duckdb.DuckDBFunctions.FunctionException;

public final class DuckDBValue implements AutoCloseable {
    private ByteBuffer valueRef;

    DuckDBValue(ByteBuffer valueRef) {
        this.valueRef = valueRef;
    }

    public boolean isNull() {
        checkOpen();
        return duckdb_is_null_value(valueRef);
    }

    public boolean getBoolean() {
        checkType(DUCKDB_TYPE_BOOLEAN);
        if (isNull()) {
            throw new FunctionException("Parameter value is NULL");
        }
        return duckdb_get_bool(valueRef);
    }

    public boolean getBoolean(boolean defaultValue) {
        checkType(DUCKDB_TYPE_BOOLEAN);
        if (isNull()) {
            return defaultValue;
        }
        return duckdb_get_bool(valueRef);
    }

    public byte getByte() {
        checkType(DUCKDB_TYPE_TINYINT);
        if (isNull()) {
            throw new FunctionException("Parameter value is NULL");
        }
        return duckdb_get_int8(valueRef);
    }

    public byte getByte(byte defaultValue) {
        checkType(DUCKDB_TYPE_TINYINT);
        if (isNull()) {
            return defaultValue;
        }
        return duckdb_get_int8(valueRef);
    }

    public short getUint8() {
        checkType(DUCKDB_TYPE_UTINYINT);
        if (isNull()) {
            throw new FunctionException("Parameter value is NULL");
        }
        return duckdb_get_uint8(valueRef);
    }

    public short getUint8(short defaultValue) {
        checkType(DUCKDB_TYPE_UTINYINT);
        if (isNull()) {
            return defaultValue;
        }
        return duckdb_get_uint8(valueRef);
    }

    public short getShort() {
        checkType(DUCKDB_TYPE_SMALLINT);
        if (isNull()) {
            throw new FunctionException("Parameter value is NULL");
        }
        return duckdb_get_int16(valueRef);
    }

    public short getShort(short defaultValue) {
        checkType(DUCKDB_TYPE_SMALLINT);
        if (isNull()) {
            return defaultValue;
        }
        return duckdb_get_int16(valueRef);
    }

    public int getUint16() {
        checkType(DUCKDB_TYPE_USMALLINT);
        if (isNull()) {
            throw new FunctionException("Parameter value is NULL");
        }
        return duckdb_get_uint16(valueRef);
    }

    public int getUint16(int defaultValue) {
        checkType(DUCKDB_TYPE_USMALLINT);
        if (isNull()) {
            return defaultValue;
        }
        return duckdb_get_uint16(valueRef);
    }

    public int getInt() {
        checkType(DUCKDB_TYPE_INTEGER);
        if (isNull()) {
            throw new FunctionException("Parameter value is NULL");
        }
        return duckdb_get_int32(valueRef);
    }

    public int getInt(int defaultValue) {
        checkType(DUCKDB_TYPE_INTEGER);
        if (isNull()) {
            return defaultValue;
        }
        return duckdb_get_int32(valueRef);
    }

    public long getUint32() {
        checkType(DUCKDB_TYPE_UINTEGER);
        if (isNull()) {
            throw new FunctionException("Parameter value is NULL");
        }
        return duckdb_get_uint32(valueRef);
    }

    public long getUint32(long defaultValue) {
        checkType(DUCKDB_TYPE_UINTEGER);
        if (isNull()) {
            return defaultValue;
        }
        return duckdb_get_uint32(valueRef);
    }

    public long getLong() {
        checkType(DUCKDB_TYPE_BIGINT);
        if (isNull()) {
            throw new FunctionException("Parameter value is NULL");
        }
        return duckdb_get_int64(valueRef);
    }

    public long getLong(long defaultValue) {
        checkType(DUCKDB_TYPE_BIGINT);
        if (isNull()) {
            return defaultValue;
        }
        return duckdb_get_int64(valueRef);
    }

    public BigInteger getUint64() {
        checkType(DUCKDB_TYPE_UBIGINT);
        if (isNull()) {
            return null;
        }
        long unsigned = duckdb_get_uint64(valueRef);
        return unsignedLongToBigInteger(unsigned);
    }

    public BigInteger getHugeInt() {
        checkType(DUCKDB_TYPE_HUGEINT);
        if (isNull()) {
            return null;
        }
        return duckdb_get_hugeint(valueRef);
    }

    public BigInteger getUHugeInt() {
        checkType(DUCKDB_TYPE_UHUGEINT);
        if (isNull()) {
            return null;
        }
        return duckdb_get_uhugeint(valueRef);
    }

    public float getFloat() {
        checkType(DUCKDB_TYPE_FLOAT);
        if (isNull()) {
            throw new FunctionException("Parameter value is NULL");
        }
        return duckdb_get_float(valueRef);
    }

    public float getFloat(float defaultValue) {
        checkType(DUCKDB_TYPE_FLOAT);
        if (isNull()) {
            return defaultValue;
        }
        return duckdb_get_float(valueRef);
    }

    public double getDouble() {
        checkType(DUCKDB_TYPE_DOUBLE);
        if (isNull()) {
            throw new FunctionException("Parameter value is NULL");
        }
        return duckdb_get_double(valueRef);
    }

    public double getDouble(double defaultValue) {
        checkType(DUCKDB_TYPE_DOUBLE);
        if (isNull()) {
            return defaultValue;
        }
        return duckdb_get_double(valueRef);
    }

    public BigDecimal getBigDecimal() {
        checkType(DUCKDB_TYPE_DECIMAL);
        if (isNull()) {
            return null;
        }
        return duckdb_get_decimal(valueRef);
    }

    public LocalDate getLocalDate() {
        checkType(DUCKDB_TYPE_DATE);
        if (isNull()) {
            return null;
        }
        int days = duckdb_get_date(valueRef);
        return LocalDate.ofEpochDay(days);
    }

    public LocalDateTime getLocalDateTime() {
        CAPIType ctype = checkTypes(DUCKDB_TYPE_TIMESTAMP, DUCKDB_TYPE_TIMESTAMP_NS, DUCKDB_TYPE_TIMESTAMP_MS,
                                    DUCKDB_TYPE_TIMESTAMP_S);

        try {
            switch (ctype) {
            case DUCKDB_TYPE_TIMESTAMP_S:
                long seconds = duckdb_get_timestamp_s(valueRef);
                return localDateTimeFromTimestamp(seconds, ChronoUnit.SECONDS);
            case DUCKDB_TYPE_TIMESTAMP_MS:
                long millis = duckdb_get_timestamp_ms(valueRef);
                return localDateTimeFromTimestamp(millis, ChronoUnit.MILLIS);
            case DUCKDB_TYPE_TIMESTAMP:
                long micros = duckdb_get_timestamp(valueRef);
                return localDateTimeFromTimestamp(micros, ChronoUnit.MICROS);
            case DUCKDB_TYPE_TIMESTAMP_NS:
                long nanos = duckdb_get_timestamp_ns(valueRef);
                return localDateTimeFromTimestamp(nanos, ChronoUnit.NANOS);
            default:
                throw new FunctionException("Invalid type: " + ctype);
            }
        } catch (SQLException e) {
            throw new FunctionException(e);
        }
    }

    public String getString() {
        checkType(DUCKDB_TYPE_VARCHAR);
        byte[] bytes = duckdb_get_varchar(valueRef);
        if (bytes == null) {
            return null;
        }
        return new String(bytes, UTF_8);
    }

    @Override
    public void close() {
        if (null != valueRef) {
            duckdb_destroy_value(valueRef);
            valueRef = null;
        }
    }

    private void checkOpen() {
        if (null == valueRef) {
            throw new FunctionException(
                "Parameter native value was closed, it is only available during the 'bind()' invocation");
        }
    }

    private void checkType(CAPIType expected) {
        checkOpen();
        try {
            int typeId = duckdb_get_value_type(valueRef);
            CAPIType ctype = CAPIType.capiTypeFromTypeId(typeId);
            if (ctype != expected) {
                throw new FunctionException("Invalid value type, expected: " + expected + ", actual: " + ctype);
            }
        } catch (SQLException e) {
            throw new FunctionException(e);
        }
    }

    private CAPIType checkTypes(CAPIType... expected) {
        checkOpen();
        try {
            int typeId = duckdb_get_value_type(valueRef);
            CAPIType ctype = CAPIType.capiTypeFromTypeId(typeId);
            for (CAPIType ex : expected) {
                if (ctype == ex) {
                    return ctype;
                }
            }
            throw new FunctionException("Invalid value type, expected one of: " + Arrays.toString(expected) +
                                        ", actual: " + ctype);
        } catch (SQLException e) {
            throw new FunctionException(e);
        }
    }
}
