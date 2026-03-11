package org.duckdb.udf;

import org.duckdb.UdfOutputAppender;

public interface TableFunction {
    TableBindResult bind(BindContext ctx, Object[] parameters) throws Exception;

    TableState init(InitContext ctx, TableBindResult bind) throws Exception;

    int produce(TableState state, UdfOutputAppender out) throws Exception;
}
