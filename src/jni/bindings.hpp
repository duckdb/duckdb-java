#pragma once

#include "capi_v2.hpp"
#include "org_duckdb_DuckDBBindings.h"
#include "util.hpp"

/**
 * Logical types are passed to Java as owned C API v2 handles (or as the placeholder INVALID type).
 */
duckdb_v2_logical_type_handle logical_type_buf_to_logical_type(JNIEnv *env, jobject logical_type_buf);

/**
 * Passes the owned logical type to Java, the type is destroyed if it cannot be passed.
 */
jobject make_logical_type_buf(JNIEnv *env, duckdb_v2_logical_type_handle logical_type);

/**
 * Data chunks are passed to Java as C API v2 handles, owned (created or fetched chunks) or borrowed
 * (chunks passed to function callbacks).
 */
duckdb_v2_data_chunk_handle chunk_buf_to_chunk(JNIEnv *env, jobject chunk_buf);

/**
 * Passes the owned data chunk to Java, the chunk is destroyed if it cannot be passed.
 */
jobject make_data_chunk_buf(JNIEnv *env, duckdb_v2_data_chunk_handle chunk);

/**
 * The C API v2 derives the row count of a data chunk from its vectors, so the row count of the input
 * chunk of a scalar function without arguments (that has no vectors) cannot be set. The row count of
 * the input chunk is kept here while the scalar function callback is running.
 */
struct ScalarFunctionInputRowCount {
	duckdb_v2_data_chunk_handle chunk = nullptr;
	idx_t row_count = 0;
};

extern thread_local ScalarFunctionInputRowCount scalar_function_input_row_count;

/**
 * Converts an error reported by the C API v2 into a Java SQLException, unless a Java exception is already pending.
 */
void throw_sql_exception(JNIEnv *env, const v2_error &error);
