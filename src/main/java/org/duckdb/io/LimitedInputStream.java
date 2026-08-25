package org.duckdb.io;

import java.io.IOException;
import java.io.InputStream;

public class LimitedInputStream extends InputStream {
    private final InputStream source;
    private final long maxBytesToRead;
    private long bytesRead;

    public LimitedInputStream(InputStream source, long maxBytesToRead) {
        if (source == null) {
            throw new IllegalArgumentException("Source input stream cannot be null");
        }
        if (maxBytesToRead < 0) {
            throw new IllegalArgumentException("maxBytesToRead must be non-negative");
        }
        this.source = source;
        this.maxBytesToRead = maxBytesToRead;
        this.bytesRead = 0;
    }

    @Override
    public int read() throws IOException {
        if (bytesRead >= maxBytesToRead) {
            return -1; // EOF
        }
        int result = source.read();
        if (result != -1) {
            bytesRead++;
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (bytesRead >= maxBytesToRead) {
            return -1; // EOF
        }
        // Calculate the maximum number of bytes we can read
        long bytesRemaining = maxBytesToRead - bytesRead;
        int bytesToRead = (int) Math.min(len, bytesRemaining);
        int result = source.read(b, off, bytesToRead);
        if (result != -1) {
            bytesRead += result;
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        source.close();
    }
}
