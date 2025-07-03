#pragma once

extern "C" {
#include "duckdb.h"
}

#include "org_duckdb_DuckDBBindings.h"

duckdb_logical_type logical_type_buf_to_logical_type(JNIEnv *env, jobject logical_type_buf);

duckdb_data_chunk chunk_buf_to_chunk(JNIEnv *env, jobject chunk_buf);
