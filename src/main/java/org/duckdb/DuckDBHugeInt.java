package org.duckdb;

import java.math.BigInteger;
import java.sql.SQLException;

class DuckDBHugeInt {
    private static final BigInteger HUGE_INT_MIN = BigInteger.ONE.shiftLeft(127).negate();
    private static final BigInteger HUGE_INT_MAX = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);

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
}
