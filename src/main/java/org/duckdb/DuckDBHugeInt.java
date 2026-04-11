package org.duckdb;

import static org.duckdb.DuckDBVector.ULONG_MULTIPLIER;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;

class DuckDBHugeInt {
    static final BigInteger HUGE_INT_MIN = BigInteger.ONE.shiftLeft(127).negate();
    static final BigInteger HUGE_INT_MAX = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);
    static final BigInteger UHUGE_INT_MAX = BigInteger.ONE.shiftLeft(128).subtract(BigInteger.ONE);

    private final long lower;
    private final long upper;

    DuckDBHugeInt(long lower, long upper) {
        this.lower = lower;
        this.upper = upper;
    }

    DuckDBHugeInt(BigInteger bi) throws SQLException {
        if (null == bi) {
            throw new SQLException("Specified BigInteger instance is null");
        }
        if (bi.compareTo(HUGE_INT_MIN) < 0 || bi.compareTo(HUGE_INT_MAX) > 0) {
            throw new SQLException("Specified BigInteger value is out of range for HUGEINT field");
        }
        this.lower = bi.longValue();
        this.upper = bi.shiftRight(64).longValue();
    }

    static BigInteger toBigInteger(long lower, long upper) {
        byte[] bytes = new byte[Long.BYTES * 2];
        ByteBuffer.wrap(bytes).putLong(upper).putLong(lower);
        return new BigInteger(bytes);
    }

    static BigInteger toUnsignedBigInteger(long lower, long upper) {
        byte[] bytes = new byte[Long.BYTES * 2];
        ByteBuffer.wrap(bytes).putLong(upper).putLong(lower);
        return new BigInteger(1, bytes);
    }

    static BigDecimal toBigDecimal(long lower, long upper, int scale) {
        return new BigDecimal(upper)
            .multiply(ULONG_MULTIPLIER)
            .add(new BigDecimal(Long.toUnsignedString(lower)))
            .scaleByPowerOfTen(scale * -1);
    }

    long lower() {
        return lower;
    }

    long upper() {
        return upper;
    }
}
