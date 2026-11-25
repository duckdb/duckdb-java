package org.duckdb;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Conversions between the BIGNUM storage blob and {@link BigInteger}. A DECIMAL wider than 38 digits stores its
 * unscaled value in this form.
 *
 * <p>The blob is a 3-byte header followed by the magnitude in big-endian byte order without leading zero bytes
 * (the magnitude of zero is a single zero byte). The header holds the number of magnitude bytes with the most
 * significant bit set. For a negative value every byte of the blob, header included, is complemented.
 */
final class DuckDBBignum {
    static final int HEADER_SIZE = 3;
    static final int MAX_DATA_SIZE = 8388607;

    private DuckDBBignum() {
    }

    static BigInteger toBigInteger(byte[] blob) {
        if (blob == null || blob.length <= HEADER_SIZE) {
            throw new IllegalArgumentException("Invalid BIGNUM blob size: " + (blob == null ? 0 : blob.length));
        }
        boolean negative = (blob[0] & 0x80) == 0;
        byte[] magnitude = new byte[blob.length - HEADER_SIZE];
        for (int i = 0; i < magnitude.length; i++) {
            byte b = blob[HEADER_SIZE + i];
            magnitude[i] = negative ? (byte) ~b : b;
        }
        return new BigInteger(negative ? -1 : 1, magnitude);
    }

    static BigDecimal toBigDecimal(byte[] blob, int scale) {
        return new BigDecimal(toBigInteger(blob), scale);
    }

    static byte[] fromBigInteger(BigInteger value) {
        boolean negative = value.signum() < 0;
        // big-endian two's complement of the absolute value: at most one leading zero byte
        byte[] magnitude = value.abs().toByteArray();
        int skip = magnitude.length > 1 && magnitude[0] == 0 ? 1 : 0;
        int size = magnitude.length - skip;
        if (size > MAX_DATA_SIZE) {
            throw new IllegalArgumentException("Value is too large for BIGNUM storage, bytes: " + size);
        }
        int header = size | 0x00800000;
        if (negative) {
            header = ~header;
        }
        byte[] blob = new byte[HEADER_SIZE + size];
        blob[0] = (byte) (header >> 16);
        blob[1] = (byte) (header >> 8);
        blob[2] = (byte) header;
        for (int i = 0; i < size; i++) {
            byte b = magnitude[skip + i];
            blob[HEADER_SIZE + i] = negative ? (byte) ~b : b;
        }
        return blob;
    }
}
