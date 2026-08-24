package org.duckdb.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;
import org.duckdb.ErrorCode;
import org.duckdb.SQLState;

public class IOUtils {

    public static byte[] readAllBytes(InputStream x) throws SQLException {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int length;
            while ((length = x.read(buffer)) != -1) {
                out.write(buffer, 0, length);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new SQLException(e.getMessage(), ErrorCode.IO_ERROR.getSQLState().getCode(),
                                   ErrorCode.IO_ERROR.getCode(), e);
        }
    }

    public static String readToString(Reader reader) throws SQLException {
        try {
            StringBuilder sb = new StringBuilder();
            char[] buffer = new char[4096];
            int length;
            while ((length = reader.read(buffer)) != -1) {
                sb.append(buffer, 0, length);
            }
            return sb.toString();
        } catch (IOException e) {
            throw new SQLException(e.getMessage(), ErrorCode.IO_STREAM_ERROR.getSQLState().getCode(),
                                   ErrorCode.IO_STREAM_ERROR.getCode(), e);
        }
    }

    public static InputStream wrapStreamWithMaxBytes(InputStream is, long maxBytes) {
        if (maxBytes < 0) {
            return is;
        }
        return new LimitedInputStream(is, maxBytes);
    }

    public static Reader wrapReaderWithMaxChars(Reader reader, long maxChars) {
        if (maxChars < 0) {
            return reader;
        }
        return new LimitedReader(reader, maxChars);
    }
}
