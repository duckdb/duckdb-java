package org.duckdb.udf;

public final class UdfOptions {
    public boolean deterministic = true;
    public boolean nullSpecialHandling = false;
    public boolean returnNullOnException = false;
    public boolean varArgs = false;

    public UdfOptions() {
    }

    public UdfOptions deterministic(boolean value) {
        this.deterministic = value;
        return this;
    }

    public UdfOptions nullSpecialHandling(boolean value) {
        this.nullSpecialHandling = value;
        return this;
    }

    public UdfOptions returnNullOnException(boolean value) {
        this.returnNullOnException = value;
        return this;
    }

    public UdfOptions varArgs(boolean value) {
        this.varArgs = value;
        return this;
    }
}
