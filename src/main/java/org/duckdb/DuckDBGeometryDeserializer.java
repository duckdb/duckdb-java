package org.duckdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

/**
 * Standalone Java class for deserializing GEOMETRY BLOBs into Well-Known Text (WKT) format.
 * Supports common geometry types: POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING,
 * MULTIPOLYGON.
 *
 * Handles both Well-Known Binary (WKB) and database-specific binary formats.
 */
public class DuckDBGeometryDeserializer {

    // WKB Geometry Types
    private static final int WKB_POINT = 1;
    private static final int WKB_LINESTRING = 2;
    private static final int WKB_POLYGON = 3;
    private static final int WKB_MULTIPOINT = 4;
    private static final int WKB_MULTILINESTRING = 5;
    private static final int WKB_MULTIPOLYGON = 6;
    private static final int WKB_GEOMETRYCOLLECTION = 7;

    // Byte order constants
    private static final byte WKB_XDR = 0; // Big Endian
    private static final byte WKB_NDR = 1; // Little Endian

    /**
     * Main method to deserialize a GEOMETRY BLOB to WKT string
     *
     * @param blob The GEOMETRY BLOB from JDBC
     * @return WKT representation of the geometry
     * @throws SQLException if blob cannot be read
     * @throws IllegalArgumentException if geometry format is not supported
     */
    public static String deserializeToWKT(Blob blob) throws SQLException {
        if (blob == null) {
            return null;
        }

        byte[] binaryData = blob.getBytes(1, (int) blob.length());
        return deserializeToWKT(binaryData);
    }

    /**
     * Deserialize byte array to WKT string
     *
     * @param binaryData The binary geometry data
     * @return WKT representation of the geometry
     * @throws IllegalArgumentException if geometry format is not supported
     */
    public static String deserializeToWKT(byte[] binaryData) {
        if (binaryData == null || binaryData.length == 0) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(binaryData);

        // Check if this is DuckDB SPATIAL format first
        if (isDuckDBSpatialFormat(binaryData)) {
            return parseDuckDBSpatial(buffer);
        }

        // Check if this is WKB format (starts with byte order marker)
        if (binaryData[0] == WKB_XDR || binaryData[0] == WKB_NDR) {
            return parseWKB(buffer);
        }

        // Try to parse as database-specific format
        return parseProprietaryFormat(buffer);
    }

    /**
     * Check if binary data uses DuckDB SPATIAL format
     */
    private static boolean isDuckDBSpatialFormat(byte[] data) {
        if (data.length < 8)
            return false;

        // DuckDB SPATIAL format starts with:
        // [geometry_type:1 byte][flags:1 byte][unused:2 bytes][padding:4 bytes]

        // First byte should be geometry type (0-6)
        int geometryType = data[0] & 0xFF;
        if (geometryType <= 6) {
            return true;
        }

        // Fallback: if no clear WKB markers, assume DuckDB
        boolean hasWKBMarker = false;
        for (int i = 0; i < Math.min(data.length, 20); i++) {
            if (data[i] == WKB_XDR || data[i] == WKB_NDR) {
                hasWKBMarker = true;
                break;
            }
        }

        return !hasWKBMarker;
    }

    /**
     * Parse DuckDB SPATIAL format Based on DBeaver's DuckDBGeometryConverter implementation
     */
    private static String parseDuckDBSpatial(ByteBuffer buffer) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.position(0);

        // DuckDB SPATIAL format header:
        // [geometry_type:1 byte][flags:1 byte][unused:2 bytes][padding:4 bytes]

        int geometryType = buffer.get() & 0xFF; // unsigned byte
        int flags = buffer.get() & 0xFF;        // unsigned byte
        buffer.getShort();                      // unused 2 bytes
        buffer.getInt();                        // padding 4 bytes

        boolean hasZ = (flags & 0x01) != 0;
        boolean hasM = (flags & 0x02) != 0;
        boolean hasBBox = (flags & 0x04) != 0;
        int dimensions = 2 + (hasZ ? 1 : 0) + (hasM ? 1 : 0);

        // Skip bounding box if present (dimensions * float size * 2 for min/max)
        if (hasBBox) {
            buffer.position(buffer.position() + dimensions * Float.BYTES * 2);
        }

        return deserializeRecursive(buffer, hasZ, hasM);
    }

    /**
     * Deserialize geometry recursively - handles both compact and individual point formats for
     * MULTIPOINT
     */
    private static String deserializeRecursive(ByteBuffer buffer, boolean hasZ, boolean hasM) {
        int type = buffer.getInt();
        int count = buffer.getInt();

        switch (type) {
        case 0: // POINT
            return readPoint(buffer, count, hasZ, hasM);

        case 1: // LINESTRING
            return readLineString(buffer, count, hasZ, hasM);

        case 2: // POLYGON
            return readPolygon(buffer, count, hasZ, hasM);

        case 3: // MULTI_POINT
            return readMultiPoint(buffer, count, hasZ, hasM);

        case 4: // MULTI_LINESTRING
            StringBuilder mlsSb = new StringBuilder("MULTILINESTRING (");
            for (int i = 0; i < count; i++) {
                if (i > 0)
                    mlsSb.append(", ");
                String lineString = deserializeRecursive(buffer, hasZ, hasM);
                // Extract coordinates from "LINESTRING (coords)" format
                String coords = lineString.substring(lineString.indexOf('(') + 1, lineString.indexOf(')'));
                mlsSb.append("(").append(coords).append(")");
            }
            mlsSb.append(")");
            return mlsSb.toString();

        case 5: // MULTI_POLYGON
            StringBuilder mpolySb = new StringBuilder("MULTIPOLYGON (");
            for (int i = 0; i < count; i++) {
                if (i > 0)
                    mpolySb.append(", ");
                String polygon = deserializeRecursive(buffer, hasZ, hasM);
                // Extract coordinates from "POLYGON (coords)" format
                String coords = polygon.substring(polygon.indexOf('(') + 1, polygon.lastIndexOf(')'));
                mpolySb.append("(").append(coords).append(")");
            }
            mpolySb.append(")");
            return mpolySb.toString();

        case 6: // MULTI_GEOMETRY / GEOMETRYCOLLECTION
            StringBuilder gcSb = new StringBuilder("GEOMETRYCOLLECTION (");
            for (int i = 0; i < count; i++) {
                if (i > 0)
                    gcSb.append(", ");
                gcSb.append(deserializeRecursive(buffer, hasZ, hasM));
            }
            gcSb.append(")");
            return gcSb.toString();

        default:
            throw new IllegalArgumentException("Unknown DuckDB geometry type: " + type);
        }
    }

    private static String readPoint(ByteBuffer buffer, int count, boolean hasZ, boolean hasM) {
        // Point should have exactly 1 coordinate
        if (count != 1) {
            throw new IllegalArgumentException("Point should have exactly 1 coordinate, got: " + count);
        }

        double x = buffer.getDouble();
        double y = buffer.getDouble();
        if (hasZ)
            buffer.getDouble(); // skip Z
        if (hasM)
            buffer.getDouble(); // skip M

        return String.format("POINT (%s %s)", formatCoordinate(x), formatCoordinate(y));
    }

    private static String readLineString(ByteBuffer buffer, int count, boolean hasZ, boolean hasM) {
        StringBuilder sb = new StringBuilder("LINESTRING (");

        for (int i = 0; i < count; i++) {
            if (i > 0)
                sb.append(", ");

            double x = buffer.getDouble();
            double y = buffer.getDouble();
            if (hasZ)
                buffer.getDouble(); // skip Z
            if (hasM)
                buffer.getDouble(); // skip M

            sb.append(formatCoordinate(x)).append(" ").append(formatCoordinate(y));
        }

        sb.append(")");
        return sb.toString();
    }

    private static String readMultiPoint(ByteBuffer buffer, int count, boolean hasZ, boolean hasM) {
        StringBuilder sb = new StringBuilder("MULTIPOINT (");

        // Try to determine format by checking if first 8 bytes look like coordinates or headers
        int savePos = buffer.position();

        // Read the first 8 bytes and see what they look like
        long firstLong = buffer.getLong();
        buffer.position(savePos); // Reset

        // Convert to double and see if it looks reasonable
        double possibleCoordinate = Double.longBitsToDouble(firstLong);

        // If it looks like a reasonable coordinate (not NaN, not infinity, reasonable range)
        // then assume direct coordinate storage, otherwise assume individual point headers
        boolean looksLikeCoordinate = !Double.isNaN(possibleCoordinate) && !Double.isInfinite(possibleCoordinate) &&
                                      Math.abs(possibleCoordinate) < 1e6 && Math.abs(possibleCoordinate) > 1e-10;

        if (!looksLikeCoordinate) {
            // Format: MULTIPOINT ((x y), (z w)) - each point has its own type/count header
            for (int i = 0; i < count; i++) {
                if (i > 0)
                    sb.append(", ");
                String point = deserializeRecursive(buffer, hasZ, hasM);
                // Extract coordinates from "POINT (x y)" format
                String coords = point.substring(point.indexOf('(') + 1, point.indexOf(')'));
                sb.append("(").append(coords).append(")");
            }
        } else {
            // Format: MULTIPOINT (x y, z w) - direct coordinate storage
            for (int i = 0; i < count; i++) {
                if (i > 0)
                    sb.append(", ");

                double x = buffer.getDouble();
                double y = buffer.getDouble();
                if (hasZ)
                    buffer.getDouble(); // skip Z
                if (hasM)
                    buffer.getDouble(); // skip M

                sb.append(formatCoordinate(x)).append(" ").append(formatCoordinate(y));
            }
        }

        sb.append(")");
        return sb.toString();
    }

    private static String readPolygon(ByteBuffer buffer, int ringCount, boolean hasZ, boolean hasM) {
        StringBuilder sb = new StringBuilder("POLYGON (");

        // Read ring count with padding (like DBeaver implementation)
        int paddedRingCount = ringCount + (ringCount % 2 == 1 ? 1 : 0);

        // Read ring sizes
        int[] ringSizes = new int[paddedRingCount];
        for (int i = 0; i < paddedRingCount; i++) {
            ringSizes[i] = buffer.getInt();
        }

        // Read rings
        for (int ring = 0; ring < ringCount; ring++) {
            if (ring > 0)
                sb.append(", ");
            sb.append("(");

            int ringSize = ringSizes[ring];
            for (int i = 0; i < ringSize; i++) {
                if (i > 0)
                    sb.append(", ");

                double x = buffer.getDouble();
                double y = buffer.getDouble();
                if (hasZ)
                    buffer.getDouble(); // skip Z
                if (hasM)
                    buffer.getDouble(); // skip M

                sb.append(formatCoordinate(x)).append(" ").append(formatCoordinate(y));
            }

            sb.append(")");
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * Parse Well-Known Binary (WKB) format - fallback for standard WKB
     */
    private static String parseWKB(ByteBuffer buffer) {
        // Read byte order
        byte byteOrder = buffer.get();
        if (byteOrder == WKB_XDR) {
            buffer.order(ByteOrder.BIG_ENDIAN);
        } else if (byteOrder == WKB_NDR) {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
        } else {
            throw new IllegalArgumentException("Invalid WKB byte order: " + byteOrder);
        }

        // Read geometry type
        int geometryType = buffer.getInt();

        // Handle extended geometry types (with Z, M, or ZM dimensions)
        int baseType = geometryType & 0xFF; // Get lower 8 bits
        if (baseType == 0) {
            baseType = (geometryType >> 8) & 0xFF;
        }
        if (baseType == 0) {
            baseType = geometryType & 0x1F; // Lower 5 bits
        }

        switch (baseType) {
        case 0:
            return "GEOMETRYCOLLECTION EMPTY";
        case WKB_POINT:
            return parseWKBPoint(buffer, geometryType);
        case WKB_LINESTRING:
            return parseWKBLineString(buffer);
        case WKB_POLYGON:
            return parseWKBPolygon(buffer);
        case WKB_MULTIPOINT:
            return parseWKBMultiPoint(buffer);
        case WKB_MULTILINESTRING:
            return parseWKBMultiLineString(buffer);
        case WKB_MULTIPOLYGON:
            return parseWKBMultiPolygon(buffer);
        default:
            throw new IllegalArgumentException("Unsupported geometry type: " + geometryType +
                                               " (base type: " + baseType + ")");
        }
    }

    private static String parseWKBPoint(ByteBuffer buffer, int geometryType) {
        // Check if this is an extended geometry type (3D, measured, etc.)
        boolean hasZ = (geometryType & 0x80000000) != 0 || (geometryType >= 1000 && geometryType < 2000);
        boolean hasM = (geometryType & 0x40000000) != 0 || (geometryType >= 2000 && geometryType < 3000);

        double x = buffer.getDouble();
        double y = buffer.getDouble();

        // Skip Z coordinate if present
        if (hasZ) {
            buffer.getDouble(); // Z coordinate - skip for now
        }

        // Skip M coordinate if present
        if (hasM) {
            buffer.getDouble(); // M coordinate - skip for now
        }

        return String.format("POINT (%s %s)", formatCoordinate(x), formatCoordinate(y));
    }

    private static String parseWKBLineString(ByteBuffer buffer) {
        int numPoints = buffer.getInt();
        StringBuilder sb = new StringBuilder("LINESTRING (");

        for (int i = 0; i < numPoints; i++) {
            if (i > 0)
                sb.append(", ");
            double x = buffer.getDouble();
            double y = buffer.getDouble();
            sb.append(formatCoordinate(x)).append(" ").append(formatCoordinate(y));
        }

        sb.append(")");
        return sb.toString();
    }

    private static String parseWKBPolygon(ByteBuffer buffer) {
        int numRings = buffer.getInt();
        StringBuilder sb = new StringBuilder("POLYGON (");

        for (int ring = 0; ring < numRings; ring++) {
            if (ring > 0)
                sb.append(", ");
            sb.append("(");

            int numPoints = buffer.getInt();
            for (int i = 0; i < numPoints; i++) {
                if (i > 0)
                    sb.append(", ");
                double x = buffer.getDouble();
                double y = buffer.getDouble();
                sb.append(formatCoordinate(x)).append(" ").append(formatCoordinate(y));
            }

            sb.append(")");
        }

        sb.append(")");
        return sb.toString();
    }

    private static String parseWKBMultiPoint(ByteBuffer buffer) {
        int numPoints = buffer.getInt();
        StringBuilder sb = new StringBuilder("MULTIPOINT (");

        for (int i = 0; i < numPoints; i++) {
            if (i > 0)
                sb.append(", ");

            // Skip byte order and geometry type for each point
            buffer.get();    // byte order
            buffer.getInt(); // geometry type

            double x = buffer.getDouble();
            double y = buffer.getDouble();
            sb.append("(").append(formatCoordinate(x)).append(" ").append(formatCoordinate(y)).append(")");
        }

        sb.append(")");
        return sb.toString();
    }

    private static String parseWKBMultiLineString(ByteBuffer buffer) {
        int numLineStrings = buffer.getInt();
        StringBuilder sb = new StringBuilder("MULTILINESTRING (");

        for (int i = 0; i < numLineStrings; i++) {
            if (i > 0)
                sb.append(", ");

            // Skip byte order and geometry type for each linestring
            buffer.get();    // byte order
            buffer.getInt(); // geometry type

            sb.append("(");
            int numPoints = buffer.getInt();
            for (int j = 0; j < numPoints; j++) {
                if (j > 0)
                    sb.append(", ");
                double x = buffer.getDouble();
                double y = buffer.getDouble();
                sb.append(formatCoordinate(x)).append(" ").append(formatCoordinate(y));
            }
            sb.append(")");
        }

        sb.append(")");
        return sb.toString();
    }

    private static String parseWKBMultiPolygon(ByteBuffer buffer) {
        int numPolygons = buffer.getInt();
        StringBuilder sb = new StringBuilder("MULTIPOLYGON (");

        for (int i = 0; i < numPolygons; i++) {
            if (i > 0)
                sb.append(", ");

            // Skip byte order and geometry type for each polygon
            buffer.get();    // byte order
            buffer.getInt(); // geometry type

            sb.append("(");
            int numRings = buffer.getInt();
            for (int ring = 0; ring < numRings; ring++) {
                if (ring > 0)
                    sb.append(", ");
                sb.append("(");

                int numPoints = buffer.getInt();
                for (int j = 0; j < numPoints; j++) {
                    if (j > 0)
                        sb.append(", ");
                    double x = buffer.getDouble();
                    double y = buffer.getDouble();
                    sb.append(formatCoordinate(x)).append(" ").append(formatCoordinate(y));
                }

                sb.append(")");
            }
            sb.append(")");
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * Parse database-specific proprietary format
     */
    private static String parseProprietaryFormat(ByteBuffer buffer) {
        // Try Oracle SDO_GEOMETRY format
        String result = tryOracleFormat(buffer);
        if (result != null)
            return result;

        // Try PostGIS extended format
        result = tryPostGISFormat(buffer);
        if (result != null)
            return result;

        // Try SQL Server format
        result = trySQLServerFormat(buffer);
        if (result != null)
            return result;

        // Generic approach: scan for WKB markers
        return scanForWKBData(buffer);
    }

    private static String tryOracleFormat(ByteBuffer buffer) {
        buffer.position(0);
        if (buffer.remaining() >= 32) {
            buffer.position(32);
            if (buffer.remaining() >= 5) {
                byte b = buffer.get(buffer.position());
                if (b == WKB_XDR || b == WKB_NDR) {
                    try {
                        return parseWKB(buffer);
                    } catch (Exception e) {
                        // Continue with other attempts
                    }
                }
            }
        }
        return null;
    }

    private static String tryPostGISFormat(ByteBuffer buffer) {
        buffer.position(0);
        if (buffer.remaining() >= 9) {
            buffer.position(4);
            byte b = buffer.get(buffer.position());
            if (b == WKB_XDR || b == WKB_NDR) {
                try {
                    return parseWKB(buffer);
                } catch (Exception e) {
                    // Continue
                }
            }
        }
        return null;
    }

    private static String trySQLServerFormat(ByteBuffer buffer) {
        buffer.position(0);
        int[] offsets = {0, 6, 8, 16, 20, 24};

        for (int offset : offsets) {
            if (buffer.remaining() > offset + 5) {
                buffer.position(offset);
                byte b = buffer.get(buffer.position());
                if (b == WKB_XDR || b == WKB_NDR) {
                    try {
                        return parseWKB(buffer);
                    } catch (Exception e) {
                        // Continue
                    }
                }
            }
        }
        return null;
    }

    private static String scanForWKBData(ByteBuffer buffer) {
        buffer.position(0);

        for (int i = 0; i < Math.min(buffer.remaining() - 5, 50); i++) {
            buffer.position(i);
            if (buffer.remaining() < 5)
                break;

            byte b = buffer.get();
            if (b == WKB_XDR || b == WKB_NDR) {
                buffer.position(i);
                try {
                    return parseWKB(buffer);
                } catch (Exception e) {
                    // Continue searching
                }
            }
        }

        throw new IllegalArgumentException("Unable to parse geometry format. Data length: " + buffer.capacity() +
                                           " bytes. First 20 bytes: " + bytesToHex(buffer.array(), 20));
    }

    /**
     * Check if a coordinate value seems reasonable
     */
    private static boolean isReasonableCoordinate(double coord) {
        return !Double.isNaN(coord) && !Double.isInfinite(coord) && Math.abs(coord) < 1e6 &&
            Math.abs(coord) > 1e-50; // Tighter bounds
    }

    /**
     * Format coordinate value, removing unnecessary decimal places and handling floating-point
     * precision
     */
    private static String formatCoordinate(double coord) {
        // Handle floating-point precision issues by rounding to reasonable precision
        double rounded = Math.round(coord * 1e10) / 1e10; // Round to 10 decimal places

        // Check if it's very close to an integer
        if (Math.abs(rounded - Math.round(rounded)) < 1e-10) {
            return String.valueOf(Math.round(rounded));
        }

        // Format with minimal decimal places, removing trailing zeros
        String formatted = String.format("%.10f", rounded);

        // Remove trailing zeros
        if (formatted.contains(".")) {
            formatted = formatted.replaceAll("0+$", "");
            formatted = formatted.replaceAll("\\.$", "");
        }

        return formatted;
    }

    /**
     * Helper method to convert bytes to hex string for debugging
     */
    private static String bytesToHex(byte[] bytes, int maxLength) {
        StringBuilder sb = new StringBuilder();
        int length = Math.min(bytes.length, maxLength);
        for (int i = 0; i < length; i++) {
            sb.append(String.format("%02X ", bytes[i]));
        }
        return sb.toString();
    }

    private static String getDuckDBGeometryTypeName(int type) {
        switch (type) {
        case 0:
            return "POINT";
        case 1:
            return "LINESTRING";
        case 2:
            return "POLYGON";
        case 3:
            return "MULTI_POINT";
        case 4:
            return "MULTI_LINESTRING";
        case 5:
            return "MULTI_POLYGON";
        case 6:
            return "MULTI_GEOMETRY";
        default:
            return "UNKNOWN";
        }
    }
}
