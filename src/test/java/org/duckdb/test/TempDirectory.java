package org.duckdb.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class TempDirectory implements AutoCloseable {
    private final Path tempDir;

    public TempDirectory() throws IOException {
        this.tempDir = Files.createTempDirectory("duckdb_tempdir_");
    }

    public Path path() {
        return tempDir;
    }

    @Override
    public void close() throws IOException {
        // Recursively delete the directory and its contents
        if (Files.exists(tempDir)) {
            Files.walk(tempDir).sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to delete " + p, e);
                }
            });
        }
    }
}
