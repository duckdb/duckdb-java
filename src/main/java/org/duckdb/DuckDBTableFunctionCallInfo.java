package org.duckdb;

public final class DuckDBTableFunctionCallInfo {
    private final Object bindData;
    private final Object globalData;
    private final Object localData;

    public DuckDBTableFunctionCallInfo(Object bindData, Object globalData, Object localData) {
        this.bindData = bindData;
        this.globalData = globalData;
        this.localData = localData;
    }

    @SuppressWarnings("unchecked")
    public <T> T getBindData() {
        return (T) bindData;
    }

    @SuppressWarnings("unchecked")
    public <T> T getInitData() {
        return (T) globalData;
    }

    @SuppressWarnings("unchecked")
    public <T> T getLocalInitData() {
        return (T) localData;
    }
}
