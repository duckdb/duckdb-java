package org.duckdb;

public enum ProfilerPrintFormat {
    DEFAULT("default"),
    TEXT("text"),
    QUERY_TREE("query_tree"),
    QUERY_TREE_OPTIMIZER("query_tree_optimizer"),
    NO_OUTPUT("no_output"),
    JSON("json"),
    HTML("html"),
    GRAPHVIZ("graphviz"),
    YAML("yaml"),
    MERMAID("mermaid");

    private final String name;

    ProfilerPrintFormat(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
