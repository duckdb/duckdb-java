#pragma once

extern "C" {
#include "duckdb.h"
}

#include <jni.h>
#include <vector>

struct JavaScalarUdfCallbackData {
	JavaVM *vm;
	jobject callback_ref;
	bool return_null_on_exception;
	bool var_args;
	std::vector<duckdb_type> argument_types;
	duckdb_type var_args_type;
	duckdb_type return_type;
};

struct JavaTableFunctionCallbackData {
	JavaVM *vm;
	jobject callback_ref;
	bool thread_safe;
	idx_t max_threads;
	std::vector<duckdb_logical_type> parameter_logical_types;
};

struct JavaTableFunctionBindData {
	JavaVM *vm;
	jobject bind_result_ref;
};

struct JavaTableFunctionInitData {
	JavaVM *vm;
	jobject state_ref;
};

void destroy_java_scalar_udf_callback_data(void *ptr);

void destroy_java_table_function_callback_data(void *ptr);

void destroy_java_table_function_bind_data(void *ptr);

void destroy_java_table_function_init_data(void *ptr);

void java_scalar_udf_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);

void java_table_function_bind_callback(duckdb_bind_info info);

void java_table_function_init_callback(duckdb_init_info info);

void java_table_function_main_callback(duckdb_function_info info, duckdb_data_chunk output);
