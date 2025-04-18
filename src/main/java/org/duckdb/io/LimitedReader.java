package org.duckdb.io;

import java.io.IOException;
import java.io.Reader;

public class LimitedReader extends Reader {
    private final Reader source;
    private final long maxCharsToRead;
    private long charsRead;

    public LimitedReader(Reader source, long maxCharsToRead) {
        if (source == null) {
            throw new IllegalArgumentException("Source Reader cannot be null");
        }
        if (maxCharsToRead < 0) {
            throw new IllegalArgumentException("maxCharsToRead must be non-negative");
        }
        this.source = source;
        this.maxCharsToRead = maxCharsToRead;
        this.charsRead = 0;
    }

    @Override
    public int read() throws IOException {
        if (charsRead >= maxCharsToRead) {
            return -1; // EOF
        }
        int result = source.read();
        if (result != -1) {
            charsRead++;
        }
        return result;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (charsRead >= maxCharsToRead) {
            return -1; // EOF
        }
        long charsRemaining = maxCharsToRead - charsRead;
        int charsToRead = (int) Math.min(len, charsRemaining);
        int result = source.read(cbuf, off, charsToRead);
        if (result != -1) {
            charsRead += result;
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        source.close();
    }
}
