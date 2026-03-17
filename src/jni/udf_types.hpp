#pragma once

extern "C" {
#include "duckdb.h"
}

#include <cstdint>
#include <jni.h>
#include <string>

enum UdfVectorAccessMask : uint32_t {
	ACCESS_GET_INT = 1 << 0,
	ACCESS_GET_LONG = 1 << 1,
	ACCESS_GET_FLOAT = 1 << 2,
	ACCESS_GET_DOUBLE = 1 << 3,
	ACCESS_GET_BOOLEAN = 1 << 4,
	ACCESS_GET_STRING = 1 << 5,
	ACCESS_GET_BYTES = 1 << 6,
	ACCESS_SET_INT = 1 << 7,
	ACCESS_SET_LONG = 1 << 8,
	ACCESS_SET_FLOAT = 1 << 9,
	ACCESS_SET_DOUBLE = 1 << 10,
	ACCESS_SET_BOOLEAN = 1 << 11,
	ACCESS_SET_STRING = 1 << 12,
	ACCESS_SET_BYTES = 1 << 13,
	ACCESS_GET_DECIMAL = 1 << 14,
	ACCESS_SET_DECIMAL = 1 << 15,
};

struct UdfTypeSpec {
	duckdb_type type;
	const char *duckdb_column_type_name;
	bool udf_vector_supported;
	bool scalar_udf_implemented;
	bool table_bind_schema_supported;
	bool requires_vector_ref;
	uint8_t fixed_width_bytes;
	uint32_t access_mask;
};

extern const char *UNSUPPORTED_SCALAR_UDF_TYPE_ERROR;
extern const char *UNSUPPORTED_TABLE_FUNCTION_PARAMETER_TYPE_ERROR;

const UdfTypeSpec *find_udf_type_spec(duckdb_type type);

bool capi_type_id_to_duckdb_type(jint type_id, duckdb_type &out_type);

bool is_supported_scalar_udf_type(duckdb_type type);

bool table_column_type_from_java(JNIEnv *env, jobject duckdb_column_type_obj, duckdb_type &out_type);

bool is_supported_table_bind_parameter_logical_type(duckdb_logical_type logical_type, std::string &error);

duckdb_logical_type create_udf_logical_type(duckdb_type type);

duckdb_logical_type create_table_logical_type_from_java(JNIEnv *env, jobject logical_type_obj, std::string &error);
