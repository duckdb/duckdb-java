extern "C" {
#include "duckdb.h"
}
#include "config.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "functions.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "types.hpp"
#include "udf_registration_internal.hpp"
#include "util.hpp"

#include <cstdint>
#include <cstring>
#include <limits>

using namespace duckdb;
using namespace std;

static jint JNI_VERSION = JNI_VERSION_1_6;
static JavaVM *GLOBAL_JVM = nullptr;

void ThrowJNI(JNIEnv *env, const char *message) {
	D_ASSERT(J_SQLException);
	env->ThrowNew(J_SQLException, message);
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
	GLOBAL_JVM = vm;
	JNIEnv *env;
	if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
		return JNI_ERR;
	}

	try {
		create_refs(env);
	} catch (const std::exception &e) {
		if (!env->ExceptionCheck()) {
			auto re_class = env->FindClass("java/lang/RuntimeException");
			if (nullptr != re_class) {
				env->ThrowNew(re_class, e.what());
			}
		}
		return JNI_ERR;
	}

	return JNI_VERSION;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *vm, void *reserved) {
	JNIEnv *env;
	if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
		return;
	}
	delete_global_refs(env);
}

//! The database instance cache, used so that multiple connections to the same file point to the same database object
duckdb::DBInstanceCache instance_cache;

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

static const UdfTypeSpec UDF_TYPE_SPECS[] = {
    {DUCKDB_TYPE_BOOLEAN, "BOOLEAN", true, true, true, false, 1, ACCESS_GET_BOOLEAN | ACCESS_SET_BOOLEAN},
    {DUCKDB_TYPE_TINYINT, "TINYINT", true, true, true, false, 1, ACCESS_GET_INT | ACCESS_SET_INT},
    {DUCKDB_TYPE_SMALLINT, "SMALLINT", true, true, true, false, 2, ACCESS_GET_INT | ACCESS_SET_INT},
    {DUCKDB_TYPE_INTEGER, "INTEGER", true, true, true, false, 4, ACCESS_GET_INT | ACCESS_SET_INT},
    {DUCKDB_TYPE_BIGINT, "BIGINT", true, true, true, false, 8, ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_UTINYINT, "UTINYINT", true, true, true, false, 1, ACCESS_GET_INT | ACCESS_SET_INT},
    {DUCKDB_TYPE_USMALLINT, "USMALLINT", true, true, true, false, 2, ACCESS_GET_INT | ACCESS_SET_INT},
    {DUCKDB_TYPE_UINTEGER, "UINTEGER", true, true, true, false, 4, ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_UBIGINT, "UBIGINT", true, true, true, false, 8, ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_HUGEINT, "HUGEINT", true, true, true, false, 16, ACCESS_GET_BYTES | ACCESS_SET_BYTES},
    {DUCKDB_TYPE_UHUGEINT, "UHUGEINT", true, true, true, false, 16, ACCESS_GET_BYTES | ACCESS_SET_BYTES},
    {DUCKDB_TYPE_FLOAT, "FLOAT", true, true, true, false, 4, ACCESS_GET_FLOAT | ACCESS_SET_FLOAT},
    {DUCKDB_TYPE_DOUBLE, "DOUBLE", true, true, true, false, 8, ACCESS_GET_DOUBLE | ACCESS_SET_DOUBLE},
    {DUCKDB_TYPE_VARCHAR, "VARCHAR", true, true, true, true, 0, ACCESS_GET_STRING | ACCESS_SET_STRING},
    {DUCKDB_TYPE_BLOB, "BLOB", true, true, true, true, 0, ACCESS_GET_BYTES | ACCESS_SET_BYTES},
    {DUCKDB_TYPE_DECIMAL, "DECIMAL", true, true, true, true, 0, ACCESS_GET_DECIMAL | ACCESS_SET_DECIMAL},
    {DUCKDB_TYPE_DATE, "DATE", true, true, true, false, 4, ACCESS_GET_INT | ACCESS_SET_INT},
    {DUCKDB_TYPE_TIME, "TIME", true, true, true, false, 8, ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_TIME_NS, "TIME_NS", true, true, true, false, 8, ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_TIME_TZ, "TIME_WITH_TIME_ZONE", true, true, true, false, 8, ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_TIMESTAMP, "TIMESTAMP", true, true, true, false, 8, ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_TIMESTAMP_S, "TIMESTAMP_S", true, true, true, false, 8, ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_TIMESTAMP_MS, "TIMESTAMP_MS", true, true, true, false, 8, ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_TIMESTAMP_NS, "TIMESTAMP_NS", true, true, true, false, 8, ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_TIMESTAMP_TZ, "TIMESTAMP_WITH_TIME_ZONE", true, true, true, false, 8,
     ACCESS_GET_LONG | ACCESS_SET_LONG},
    {DUCKDB_TYPE_UUID, "UUID", true, true, true, false, 16, ACCESS_GET_BYTES | ACCESS_SET_BYTES},
};

static constexpr uint8_t DEFAULT_DECIMAL_WIDTH = 18;
static constexpr uint8_t DEFAULT_DECIMAL_SCALE = 3;

static duckdb_logical_type create_udf_logical_type(duckdb_type type) {
	if (type == DUCKDB_TYPE_DECIMAL) {
		return duckdb_create_decimal_type(DEFAULT_DECIMAL_WIDTH, DEFAULT_DECIMAL_SCALE);
	}
	return duckdb_create_logical_type(type);
}

static const UdfTypeSpec *find_udf_type_spec(duckdb_type type) {
	for (const auto &spec : UDF_TYPE_SPECS) {
		if (spec.type == type) {
			return &spec;
		}
	}
	return nullptr;
}

static const UdfTypeSpec *find_udf_type_spec_by_name(const std::string &name) {
	for (const auto &spec : UDF_TYPE_SPECS) {
		if (name == spec.duckdb_column_type_name) {
			return &spec;
		}
	}
	return nullptr;
}

static bool capi_type_id_to_duckdb_type(jint type_id, duckdb_type &out_type) {
	auto requested_type = static_cast<duckdb_type>(type_id);
	auto spec = find_udf_type_spec(requested_type);
	if (!spec) {
		return false;
	}
	out_type = spec->type;
	return true;
}

static bool is_supported_scalar_udf_type(duckdb_type type) {
	auto spec = find_udf_type_spec(type);
	return spec != nullptr && spec->scalar_udf_implemented;
}

static bool is_supported_table_function_parameter_type(duckdb_type type) {
	auto spec = find_udf_type_spec(type);
	return spec != nullptr && spec->udf_vector_supported;
}

static const char *UNSUPPORTED_SCALAR_UDF_TYPE_ERROR =
    "Supported scalar UDF types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR, DECIMAL, "
    "BLOB, DATE, TIME, TIME_NS, TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, UTINYINT, USMALLINT, "
    "UINTEGER, UBIGINT, HUGEINT, UHUGEINT, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, UUID";

static const char *UNSUPPORTED_TABLE_FUNCTION_PARAMETER_TYPE_ERROR =
    "Supported table function parameter types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, "
    "VARCHAR, DECIMAL, BLOB, DATE, TIME, TIME_NS, TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, "
    "UTINYINT, USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, "
    "UUID";

static bool duckdb_type_from_java_column_type_name(const std::string &name, duckdb_type &out_type) {
	if (name == "LIST") {
		out_type = DUCKDB_TYPE_LIST;
		return true;
	}
	if (name == "ARRAY") {
		out_type = DUCKDB_TYPE_ARRAY;
		return true;
	}
	if (name == "MAP") {
		out_type = DUCKDB_TYPE_MAP;
		return true;
	}
	if (name == "STRUCT") {
		out_type = DUCKDB_TYPE_STRUCT;
		return true;
	}
	if (name == "UNION") {
		out_type = DUCKDB_TYPE_UNION;
		return true;
	}
	if (name == "ENUM") {
		out_type = DUCKDB_TYPE_ENUM;
		return true;
	}
	auto spec = find_udf_type_spec_by_name(name);
	if (spec) {
		out_type = spec->type;
		return true;
	}
	return false;
}

static bool table_column_type_from_java(JNIEnv *env, jobject duckdb_column_type_obj, duckdb_type &out_type) {
	auto name_j = reinterpret_cast<jstring>(env->CallObjectMethod(duckdb_column_type_obj, J_Enum_name));
	if (env->ExceptionCheck() || !name_j) {
		return false;
	}
	auto name = jstring_to_string(env, name_j);
	env->DeleteLocalRef(name_j);
	duckdb_type resolved_type = DUCKDB_TYPE_INVALID;
	if (!duckdb_type_from_java_column_type_name(name, resolved_type)) {
		return false;
	}
	auto spec = find_udf_type_spec(resolved_type);
	if (spec && spec->table_bind_schema_supported) {
		out_type = resolved_type;
		return true;
	}
	return false;
}

static bool table_any_column_type_from_java(JNIEnv *env, jobject duckdb_column_type_obj, duckdb_type &out_type) {
	auto name_j = reinterpret_cast<jstring>(env->CallObjectMethod(duckdb_column_type_obj, J_Enum_name));
	if (env->ExceptionCheck() || !name_j) {
		return false;
	}
	auto name = jstring_to_string(env, name_j);
	env->DeleteLocalRef(name_j);
	return duckdb_type_from_java_column_type_name(name, out_type);
}

static bool is_supported_table_bind_parameter_logical_type(duckdb_logical_type logical_type, std::string &error) {
	if (!logical_type) {
		error = "Invalid null logical type for table function parameter";
		return false;
	}

	auto type = static_cast<duckdb_type>(duckdb_get_type_id(logical_type));
	if (is_supported_table_function_parameter_type(type)) {
		return true;
	}

	switch (type) {
	case DUCKDB_TYPE_ENUM:
		return true;
	case DUCKDB_TYPE_LIST:
	case DUCKDB_TYPE_ARRAY: {
		auto child_type = type == DUCKDB_TYPE_LIST ? duckdb_list_type_child_type(logical_type)
		                                           : duckdb_array_type_child_type(logical_type);
		if (!child_type) {
			error = "Failed to inspect child type for LIST/ARRAY parameter";
			return false;
		}
		std::string child_error;
		auto ok = is_supported_table_bind_parameter_logical_type(child_type, child_error);
		duckdb_destroy_logical_type(&child_type);
		if (!ok) {
			error = "Unsupported LIST/ARRAY child type: " + child_error;
		}
		return ok;
	}
	case DUCKDB_TYPE_MAP: {
		auto key_type = duckdb_map_type_key_type(logical_type);
		auto value_type = duckdb_map_type_value_type(logical_type);
		if (!key_type || !value_type) {
			if (key_type) {
				duckdb_destroy_logical_type(&key_type);
			}
			if (value_type) {
				duckdb_destroy_logical_type(&value_type);
			}
			error = "Failed to inspect key/value types for MAP parameter";
			return false;
		}
		std::string key_error;
		std::string value_error;
		auto key_ok = is_supported_table_bind_parameter_logical_type(key_type, key_error);
		auto value_ok = is_supported_table_bind_parameter_logical_type(value_type, value_error);
		duckdb_destroy_logical_type(&key_type);
		duckdb_destroy_logical_type(&value_type);
		if (!key_ok || !value_ok) {
			error = "Unsupported MAP parameter type: key(" + key_error + "), value(" + value_error + ")";
			return false;
		}
		return true;
	}
	case DUCKDB_TYPE_STRUCT:
	case DUCKDB_TYPE_UNION: {
		auto child_count = type == DUCKDB_TYPE_STRUCT ? duckdb_struct_type_child_count(logical_type)
		                                              : duckdb_union_type_member_count(logical_type);
		for (idx_t i = 0; i < child_count; i++) {
			auto child_type = type == DUCKDB_TYPE_STRUCT ? duckdb_struct_type_child_type(logical_type, i)
			                                             : duckdb_union_type_member_type(logical_type, i);
			if (!child_type) {
				error = "Failed to inspect child type for STRUCT/UNION parameter";
				return false;
			}
			std::string child_error;
			auto child_ok = is_supported_table_bind_parameter_logical_type(child_type, child_error);
			duckdb_destroy_logical_type(&child_type);
			if (!child_ok) {
				error = "Unsupported STRUCT/UNION child type: " + child_error;
				return false;
			}
		}
		return true;
	}
	default:
		error = "Unsupported table function parameter logical type id: " + std::to_string(static_cast<int>(type));
		return false;
	}
}

static duckdb_logical_type create_table_logical_type_from_java(JNIEnv *env, jobject logical_type_obj,
                                                               std::string &error);

static duckdb_logical_type create_struct_or_union_type_from_java(JNIEnv *env, jobject logical_type_obj,
                                                                 duckdb_type type, std::string &error) {
	auto field_names =
	    reinterpret_cast<jobjectArray>(env->CallObjectMethod(logical_type_obj, J_UdfLogicalType_getFieldNames));
	auto field_types =
	    reinterpret_cast<jobjectArray>(env->CallObjectMethod(logical_type_obj, J_UdfLogicalType_getFieldTypes));
	if (env->ExceptionCheck() || !field_names || !field_types) {
		error = "Invalid Java logical type for struct/union";
		if (field_names) {
			env->DeleteLocalRef(field_names);
		}
		if (field_types) {
			env->DeleteLocalRef(field_types);
		}
		return nullptr;
	}
	auto field_count = env->GetArrayLength(field_names);
	if (field_count <= 0 || field_count != env->GetArrayLength(field_types)) {
		error = "Struct/union logical type requires matching non-empty field names/types";
		env->DeleteLocalRef(field_names);
		env->DeleteLocalRef(field_types);
		return nullptr;
	}

	std::vector<std::string> field_name_storage;
	std::vector<const char *> field_name_ptrs;
	std::vector<duckdb_logical_type> member_types;
	field_name_storage.reserve(field_count);
	field_name_ptrs.reserve(field_count);
	member_types.reserve(field_count);

	for (jsize i = 0; i < field_count; i++) {
		auto field_name_j = reinterpret_cast<jstring>(env->GetObjectArrayElement(field_names, i));
		auto field_type_obj = env->GetObjectArrayElement(field_types, i);
		if (env->ExceptionCheck() || !field_name_j || !field_type_obj) {
			error = "Invalid struct/union field descriptor in Java logical type";
			if (field_name_j) {
				env->DeleteLocalRef(field_name_j);
			}
			if (field_type_obj) {
				env->DeleteLocalRef(field_type_obj);
			}
			break;
		}
		auto field_name = jstring_to_string(env, field_name_j);
		env->DeleteLocalRef(field_name_j);
		if (field_name.empty()) {
			error = "Struct/union field names must be non-empty";
			env->DeleteLocalRef(field_type_obj);
			break;
		}
		std::string member_error;
		auto member_type = create_table_logical_type_from_java(env, field_type_obj, member_error);
		env->DeleteLocalRef(field_type_obj);
		if (env->ExceptionCheck() || !member_type) {
			error = member_error.empty() ? "Invalid struct/union field type in Java logical type" : member_error;
			break;
		}
		field_name_storage.push_back(field_name);
		field_name_ptrs.push_back(field_name_storage.back().c_str());
		member_types.push_back(member_type);
	}

	duckdb_logical_type result = nullptr;
	if (error.empty()) {
		if (type == DUCKDB_TYPE_STRUCT) {
			result = duckdb_create_struct_type(member_types.data(), field_name_ptrs.data(), member_types.size());
		} else {
			result = duckdb_create_union_type(member_types.data(), field_name_ptrs.data(), member_types.size());
		}
		if (!result) {
			error = "Failed to create struct/union logical type";
		}
	}

	for (auto &member_type : member_types) {
		duckdb_destroy_logical_type(&member_type);
	}
	env->DeleteLocalRef(field_names);
	env->DeleteLocalRef(field_types);
	return result;
}

static duckdb_logical_type create_enum_type_from_java(JNIEnv *env, jobject logical_type_obj, std::string &error) {
	auto enum_values =
	    reinterpret_cast<jobjectArray>(env->CallObjectMethod(logical_type_obj, J_UdfLogicalType_getEnumValues));
	if (env->ExceptionCheck() || !enum_values) {
		error = "Invalid Java logical type for enum";
		if (enum_values) {
			env->DeleteLocalRef(enum_values);
		}
		return nullptr;
	}

	auto value_count = env->GetArrayLength(enum_values);
	if (value_count <= 0) {
		error = "Enum logical type requires at least one value";
		env->DeleteLocalRef(enum_values);
		return nullptr;
	}

	std::vector<std::string> value_storage;
	std::vector<const char *> value_ptrs;
	value_storage.reserve(value_count);
	value_ptrs.reserve(value_count);
	for (jsize i = 0; i < value_count; i++) {
		auto enum_value_j = reinterpret_cast<jstring>(env->GetObjectArrayElement(enum_values, i));
		if (env->ExceptionCheck() || !enum_value_j) {
			error = "Enum values must be non-null strings";
			if (enum_value_j) {
				env->DeleteLocalRef(enum_value_j);
			}
			break;
		}
		auto enum_value = jstring_to_string(env, enum_value_j);
		env->DeleteLocalRef(enum_value_j);
		if (enum_value.empty()) {
			error = "Enum values must be non-empty strings";
			break;
		}
		value_storage.push_back(enum_value);
		value_ptrs.push_back(value_storage.back().c_str());
	}
	env->DeleteLocalRef(enum_values);

	if (!error.empty()) {
		return nullptr;
	}
	auto enum_type = duckdb_create_enum_type(value_ptrs.data(), value_ptrs.size());
	if (!enum_type) {
		error = "Failed to create enum logical type";
	}
	return enum_type;
}

static duckdb_logical_type create_table_logical_type_from_java(JNIEnv *env, jobject logical_type_obj,
                                                               std::string &error) {
	if (!logical_type_obj) {
		error = "Java logical type is null";
		return nullptr;
	}

	auto column_type_obj = env->CallObjectMethod(logical_type_obj, J_UdfLogicalType_getType);
	if (env->ExceptionCheck() || !column_type_obj) {
		error = "Java logical type has invalid DuckDBColumnType";
		if (column_type_obj) {
			env->DeleteLocalRef(column_type_obj);
		}
		return nullptr;
	}

	duckdb_type type = DUCKDB_TYPE_INVALID;
	auto type_ok = table_any_column_type_from_java(env, column_type_obj, type);
	env->DeleteLocalRef(column_type_obj);
	if (!type_ok) {
		error = "Unsupported DuckDBColumnType in Java logical type";
		return nullptr;
	}

	switch (type) {
	case DUCKDB_TYPE_LIST: {
		auto child_type_obj = env->CallObjectMethod(logical_type_obj, J_UdfLogicalType_getChildType);
		if (env->ExceptionCheck() || !child_type_obj) {
			error = "List logical type requires child type";
			if (child_type_obj) {
				env->DeleteLocalRef(child_type_obj);
			}
			return nullptr;
		}
		std::string child_error;
		auto child_type = create_table_logical_type_from_java(env, child_type_obj, child_error);
		env->DeleteLocalRef(child_type_obj);
		if (!child_type || env->ExceptionCheck()) {
			error = child_error.empty() ? "Failed to create list child logical type" : child_error;
			return nullptr;
		}
		auto list_type = duckdb_create_list_type(child_type);
		duckdb_destroy_logical_type(&child_type);
		if (!list_type) {
			error = "Failed to create list logical type";
		}
		return list_type;
	}
	case DUCKDB_TYPE_ARRAY: {
		auto child_type_obj = env->CallObjectMethod(logical_type_obj, J_UdfLogicalType_getChildType);
		auto array_size = env->CallLongMethod(logical_type_obj, J_UdfLogicalType_getArraySize);
		if (env->ExceptionCheck() || !child_type_obj || array_size <= 0) {
			error = "Array logical type requires child type and positive array size";
			if (child_type_obj) {
				env->DeleteLocalRef(child_type_obj);
			}
			return nullptr;
		}
		std::string child_error;
		auto child_type = create_table_logical_type_from_java(env, child_type_obj, child_error);
		env->DeleteLocalRef(child_type_obj);
		if (!child_type || env->ExceptionCheck()) {
			error = child_error.empty() ? "Failed to create array child logical type" : child_error;
			return nullptr;
		}
		auto array_type = duckdb_create_array_type(child_type, array_size);
		duckdb_destroy_logical_type(&child_type);
		if (!array_type) {
			error = "Failed to create array logical type";
		}
		return array_type;
	}
	case DUCKDB_TYPE_MAP: {
		auto key_type_obj = env->CallObjectMethod(logical_type_obj, J_UdfLogicalType_getKeyType);
		auto value_type_obj = env->CallObjectMethod(logical_type_obj, J_UdfLogicalType_getValueType);
		if (env->ExceptionCheck() || !key_type_obj || !value_type_obj) {
			error = "Map logical type requires key and value types";
			if (key_type_obj) {
				env->DeleteLocalRef(key_type_obj);
			}
			if (value_type_obj) {
				env->DeleteLocalRef(value_type_obj);
			}
			return nullptr;
		}
		std::string key_error;
		std::string value_error;
		auto key_type = create_table_logical_type_from_java(env, key_type_obj, key_error);
		auto value_type = create_table_logical_type_from_java(env, value_type_obj, value_error);
		env->DeleteLocalRef(key_type_obj);
		env->DeleteLocalRef(value_type_obj);
		if (!key_type || !value_type || env->ExceptionCheck()) {
			error = !key_error.empty() ? key_error : value_error;
			if (error.empty()) {
				error = "Failed to create map key/value logical types";
			}
			if (key_type) {
				duckdb_destroy_logical_type(&key_type);
			}
			if (value_type) {
				duckdb_destroy_logical_type(&value_type);
			}
			return nullptr;
		}
		auto map_type = duckdb_create_map_type(key_type, value_type);
		duckdb_destroy_logical_type(&key_type);
		duckdb_destroy_logical_type(&value_type);
		if (!map_type) {
			error = "Failed to create map logical type";
		}
		return map_type;
	}
	case DUCKDB_TYPE_STRUCT:
	case DUCKDB_TYPE_UNION:
		return create_struct_or_union_type_from_java(env, logical_type_obj, type, error);
	case DUCKDB_TYPE_ENUM:
		return create_enum_type_from_java(env, logical_type_obj, error);
	case DUCKDB_TYPE_DECIMAL: {
		static constexpr jint DECIMAL_WIDTH_MIN = 1;
		static constexpr jint DECIMAL_WIDTH_MAX = 38;
		static constexpr jint DECIMAL_SCALE_MIN = 0;

		auto width = env->CallIntMethod(logical_type_obj, J_UdfLogicalType_getDecimalWidth);
		auto scale = env->CallIntMethod(logical_type_obj, J_UdfLogicalType_getDecimalScale);
		if (env->ExceptionCheck()) {
			error = "Decimal logical type has invalid width/scale";
			return nullptr;
		}
		if (width < DECIMAL_WIDTH_MIN || width > DECIMAL_WIDTH_MAX) {
			error = "Decimal logical type width must be between 1 and 38";
			return nullptr;
		}
		if (scale < DECIMAL_SCALE_MIN || scale > width) {
			error = "Decimal logical type scale must be between 0 and width";
			return nullptr;
		}
		auto decimal_type = duckdb_create_decimal_type(static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		if (!decimal_type) {
			error = "Failed to create decimal logical type";
		}
		return decimal_type;
	}
	default:
		return create_udf_logical_type(type);
	}
}

static jobject table_bind_parameter_to_java_internal(JNIEnv *env, duckdb_value val, duckdb_logical_type logical_type,
                                                     std::string &error);
static jobject decimal_to_bigdecimal(JNIEnv *env, duckdb_decimal decimal, std::string &error);

static int64_t floor_divide_i64(int64_t value, int64_t divisor) {
	D_ASSERT(divisor > 0);
	auto quotient = value / divisor;
	auto remainder = value % divisor;
	if (remainder < 0) {
		quotient -= 1;
	}
	return quotient;
}

static int64_t floor_modulo_i64(int64_t value, int64_t divisor) {
	D_ASSERT(divisor > 0);
	auto remainder = value % divisor;
	if (remainder < 0) {
		remainder += divisor;
	}
	return remainder;
}

static jobject date_to_local_date(JNIEnv *env, int64_t epoch_days, std::string &error) {
	auto local_date = env->CallStaticObjectMethod(J_LocalDate, J_LocalDate_ofEpochDay, static_cast<jlong>(epoch_days));
	if (env->ExceptionCheck() || !local_date) {
		error = "Failed to materialize DATE table function parameter as LocalDate";
		return nullptr;
	}
	return local_date;
}

static jobject nanos_to_local_time(JNIEnv *env, int64_t nanos_of_day, std::string &error) {
	auto local_time =
	    env->CallStaticObjectMethod(J_LocalTime, J_LocalTime_ofNanoOfDay, static_cast<jlong>(nanos_of_day));
	if (env->ExceptionCheck() || !local_time) {
		error = "Failed to materialize TIME table function parameter as LocalTime";
		return nullptr;
	}
	return local_time;
}

static jobject timestamp_to_local_date_time(JNIEnv *env, int64_t epoch_value, int64_t units_per_second,
                                            int64_t nanos_per_unit, std::string &error) {
	auto seconds = floor_divide_i64(epoch_value, units_per_second);
	auto remainder_units = floor_modulo_i64(epoch_value, units_per_second);
	auto nanos = remainder_units * nanos_per_unit;
	auto local_date_time =
	    env->CallStaticObjectMethod(J_LocalDateTime, J_LocalDateTime_ofEpochSecond, static_cast<jlong>(seconds),
	                                static_cast<jint>(nanos), J_ZoneOffset_UTC);
	if (env->ExceptionCheck() || !local_date_time) {
		error = "Failed to materialize TIMESTAMP table function parameter as LocalDateTime";
		return nullptr;
	}
	return local_date_time;
}

static jobject timestamp_to_offset_date_time(JNIEnv *env, int64_t micros_since_epoch, std::string &error) {
	auto local_date_time = timestamp_to_local_date_time(env, micros_since_epoch, 1000000, 1000, error);
	if (!local_date_time) {
		return nullptr;
	}
	auto offset_date_time = env->CallObjectMethod(local_date_time, J_LocalDateTime_atOffset, J_ZoneOffset_UTC);
	env->DeleteLocalRef(local_date_time);
	if (env->ExceptionCheck() || !offset_date_time) {
		error = "Failed to materialize TIMESTAMP WITH TIME ZONE table function parameter as OffsetDateTime";
		return nullptr;
	}
	return offset_date_time;
}

static jobject timetz_to_offset_time(JNIEnv *env, uint64_t time_tz_bits, std::string &error) {
	static constexpr int64_t MAX_TZ_SECONDS = 16 * 60 * 60 - 1;
	int64_t signed_bits = static_cast<int64_t>(time_tz_bits);
	int64_t micros = signed_bits >> 24;
	int64_t inverted_biased_offset = signed_bits & 0x0FFFFFF;
	int64_t offset_seconds = MAX_TZ_SECONDS - inverted_biased_offset;

	auto local_time = nanos_to_local_time(env, micros * 1000, error);
	if (!local_time) {
		return nullptr;
	}
	auto zone_offset =
	    env->CallStaticObjectMethod(J_ZoneOffset, J_ZoneOffset_ofTotalSeconds, static_cast<jint>(offset_seconds));
	if (env->ExceptionCheck() || !zone_offset) {
		env->DeleteLocalRef(local_time);
		error = "Failed to materialize TIME WITH TIME ZONE offset for table function parameter";
		return nullptr;
	}
	auto offset_time = env->CallStaticObjectMethod(J_OffsetTime, J_OffsetTime_of, local_time, zone_offset);
	env->DeleteLocalRef(local_time);
	env->DeleteLocalRef(zone_offset);
	if (env->ExceptionCheck() || !offset_time) {
		error = "Failed to materialize TIME WITH TIME ZONE table function parameter as OffsetTime";
		return nullptr;
	}
	return offset_time;
}

static jobject uuid_to_java_uuid(JNIEnv *env, duckdb_uhugeint uuid, std::string &error) {
	auto most_significant_bits = static_cast<jlong>(uuid.upper);
	auto least_significant_bits = static_cast<jlong>(uuid.lower);
	auto uuid_obj = env->NewObject(J_UUID, J_UUID_init, most_significant_bits, least_significant_bits);
	if (env->ExceptionCheck() || !uuid_obj) {
		error = "Failed to materialize UUID table function parameter as java.util.UUID";
		return nullptr;
	}
	return uuid_obj;
}

static jobject table_bind_parameter_scalar_to_java(JNIEnv *env, duckdb_value val, duckdb_type type,
                                                   duckdb_logical_type logical_type, std::string &error) {
	switch (type) {
	case DUCKDB_TYPE_BOOLEAN:
		return env->NewObject(J_Bool, J_Bool_init, static_cast<jboolean>(duckdb_get_bool(val)));
	case DUCKDB_TYPE_TINYINT:
		return env->NewObject(J_Byte, J_Byte_init, static_cast<jbyte>(duckdb_get_int64(val)));
	case DUCKDB_TYPE_SMALLINT:
		return env->NewObject(J_Short, J_Short_init, static_cast<jshort>(duckdb_get_int64(val)));
	case DUCKDB_TYPE_INTEGER:
		return env->NewObject(J_Int, J_Int_init, static_cast<jint>(duckdb_get_int64(val)));
	case DUCKDB_TYPE_BIGINT:
		return env->NewObject(J_Long, J_Long_init, static_cast<jlong>(duckdb_get_int64(val)));
	case DUCKDB_TYPE_UTINYINT:
		return env->NewObject(J_Short, J_Short_init, static_cast<jshort>(duckdb_get_uint8(val)));
	case DUCKDB_TYPE_USMALLINT:
		return env->NewObject(J_Int, J_Int_init, static_cast<jint>(duckdb_get_uint16(val)));
	case DUCKDB_TYPE_UINTEGER:
		return env->NewObject(J_Long, J_Long_init, static_cast<jlong>(duckdb_get_uint32(val)));
	case DUCKDB_TYPE_UBIGINT:
		return env->NewObject(J_Long, J_Long_init, static_cast<jlong>(duckdb_get_uint64(val)));
	case DUCKDB_TYPE_HUGEINT: {
		auto huge = duckdb_get_hugeint(val);
		auto bytes = env->NewByteArray(static_cast<jsize>(sizeof(huge)));
		if (!bytes) {
			error = "Failed to allocate byte array for HUGEINT table function parameter";
			return nullptr;
		}
		env->SetByteArrayRegion(bytes, 0, static_cast<jsize>(sizeof(huge)), reinterpret_cast<const jbyte *>(&huge));
		return bytes;
	}
	case DUCKDB_TYPE_UHUGEINT: {
		auto uhuge = duckdb_get_uhugeint(val);
		auto bytes = env->NewByteArray(static_cast<jsize>(sizeof(uhuge)));
		if (!bytes) {
			error = "Failed to allocate byte array for UHUGEINT table function parameter";
			return nullptr;
		}
		env->SetByteArrayRegion(bytes, 0, static_cast<jsize>(sizeof(uhuge)), reinterpret_cast<const jbyte *>(&uhuge));
		return bytes;
	}
	case DUCKDB_TYPE_FLOAT:
		return env->NewObject(J_Float, J_Float_init, static_cast<jfloat>(duckdb_get_double(val)));
	case DUCKDB_TYPE_DOUBLE:
		return env->NewObject(J_Double, J_Double_init, static_cast<jdouble>(duckdb_get_double(val)));
	case DUCKDB_TYPE_DECIMAL: {
		auto decimal = duckdb_get_decimal(val);
		auto decimal_obj = decimal_to_bigdecimal(env, decimal, error);
		if (!decimal_obj) {
			if (error.empty()) {
				error = "Failed to materialize DECIMAL table function parameter";
			}
			return nullptr;
		}
		return decimal_obj;
	}
	case DUCKDB_TYPE_VARCHAR: {
		varchar_ptr str_ptr(duckdb_get_varchar(val), varchar_deleter);
		if (!str_ptr) {
			error = "Failed to materialize VARCHAR table function parameter";
			return nullptr;
		}
		auto str_len = static_cast<idx_t>(std::strlen(str_ptr.get()));
		auto jstr = decode_charbuffer_to_jstring(env, str_ptr.get(), str_len);
		if (env->ExceptionCheck()) {
			error = "Failed to decode VARCHAR table function parameter";
			return nullptr;
		}
		return jstr;
	}
	case DUCKDB_TYPE_BLOB: {
		auto blob = duckdb_get_blob(val);
		if (blob.size > 0 && blob.data == nullptr) {
			error = "Failed to materialize BLOB table function parameter";
			return nullptr;
		}
		auto bytes = env->NewByteArray(static_cast<jsize>(blob.size));
		if (blob.size > 0 && bytes) {
			env->SetByteArrayRegion(bytes, 0, static_cast<jsize>(blob.size),
			                        reinterpret_cast<const jbyte *>(blob.data));
		}
		if (blob.data) {
			duckdb_free(blob.data);
		}
		if (!bytes) {
			error = "Failed to allocate byte array for BLOB table function parameter";
			return nullptr;
		}
		return bytes;
	}
	case DUCKDB_TYPE_DATE: {
		auto date = duckdb_get_date(val);
		return date_to_local_date(env, date.days, error);
	}
	case DUCKDB_TYPE_TIME: {
		auto time = duckdb_get_time(val);
		return nanos_to_local_time(env, time.micros * 1000, error);
	}
	case DUCKDB_TYPE_TIME_NS: {
		auto time_ns = duckdb_get_time_ns(val);
		return nanos_to_local_time(env, time_ns.nanos, error);
	}
	case DUCKDB_TYPE_TIME_TZ: {
		auto time_tz = duckdb_get_time_tz(val);
		return timetz_to_offset_time(env, time_tz.bits, error);
	}
	case DUCKDB_TYPE_TIMESTAMP: {
		auto ts = duckdb_get_timestamp(val);
		return timestamp_to_local_date_time(env, ts.micros, 1000000, 1000, error);
	}
	case DUCKDB_TYPE_TIMESTAMP_S: {
		auto ts_s = duckdb_get_timestamp_s(val);
		return timestamp_to_local_date_time(env, ts_s.seconds, 1, 1, error);
	}
	case DUCKDB_TYPE_TIMESTAMP_MS: {
		auto ts_ms = duckdb_get_timestamp_ms(val);
		return timestamp_to_local_date_time(env, ts_ms.millis, 1000, 1000000, error);
	}
	case DUCKDB_TYPE_TIMESTAMP_NS: {
		auto ts_ns = duckdb_get_timestamp_ns(val);
		return timestamp_to_local_date_time(env, ts_ns.nanos, 1000000000, 1, error);
	}
	case DUCKDB_TYPE_TIMESTAMP_TZ: {
		auto ts_tz = duckdb_get_timestamp_tz(val);
		return timestamp_to_offset_date_time(env, ts_tz.micros, error);
	}
	case DUCKDB_TYPE_UUID: {
		auto uuid = duckdb_get_uuid(val);
		return uuid_to_java_uuid(env, uuid, error);
	}
	case DUCKDB_TYPE_ENUM: {
		auto enum_idx = duckdb_get_enum_value(val);
		auto dictionary_size = duckdb_enum_dictionary_size(logical_type);
		if (enum_idx >= dictionary_size) {
			error = "Invalid enum value index in table function parameter";
			return nullptr;
		}
		varchar_ptr enum_value_ptr(duckdb_enum_dictionary_value(logical_type, enum_idx), varchar_deleter);
		if (!enum_value_ptr) {
			error = "Failed to materialize ENUM table function parameter";
			return nullptr;
		}
		auto str_len = static_cast<idx_t>(std::strlen(enum_value_ptr.get()));
		auto jstr = decode_charbuffer_to_jstring(env, enum_value_ptr.get(), str_len);
		if (env->ExceptionCheck()) {
			error = "Failed to decode ENUM table function parameter";
			return nullptr;
		}
		return jstr;
	}
	default:
		error = "Unsupported scalar parameter type in Java table function bind callback";
		return nullptr;
	}
}

static jobject table_bind_list_parameter_to_java(JNIEnv *env, duckdb_value val, duckdb_logical_type logical_type,
                                                 duckdb_type type, std::string &error) {
	auto child_type = type == DUCKDB_TYPE_LIST ? duckdb_list_type_child_type(logical_type)
	                                           : duckdb_array_type_child_type(logical_type);
	if (!child_type) {
		error = "Failed to inspect LIST/ARRAY child type in table function parameter";
		return nullptr;
	}

	auto list = env->NewObject(J_ArrayList, J_ArrayList_init);
	if (!list || env->ExceptionCheck()) {
		duckdb_destroy_logical_type(&child_type);
		error = "Failed to allocate Java list for table function parameter";
		return nullptr;
	}

	auto size = duckdb_get_list_size(val);
	for (idx_t i = 0; i < size; i++) {
		auto child_value = duckdb_get_list_child(val, i);
		std::string child_error;
		auto child_obj = table_bind_parameter_to_java_internal(env, child_value, child_type, child_error);
		duckdb_destroy_value(&child_value);
		if (!child_error.empty() || env->ExceptionCheck()) {
			if (child_obj) {
				env->DeleteLocalRef(child_obj);
			}
			env->DeleteLocalRef(list);
			duckdb_destroy_logical_type(&child_type);
			error =
			    child_error.empty() ? "Failed to convert LIST/ARRAY child in table function parameter" : child_error;
			return nullptr;
		}
		env->CallBooleanMethod(list, J_ArrayList_add, child_obj);
		if (child_obj) {
			env->DeleteLocalRef(child_obj);
		}
		if (env->ExceptionCheck()) {
			env->DeleteLocalRef(list);
			duckdb_destroy_logical_type(&child_type);
			error = "Failed to append LIST/ARRAY child in Java table function parameter";
			return nullptr;
		}
	}

	duckdb_destroy_logical_type(&child_type);
	return list;
}

static jobject table_bind_map_parameter_to_java(JNIEnv *env, duckdb_value val, duckdb_logical_type logical_type,
                                                std::string &error) {
	auto key_type = duckdb_map_type_key_type(logical_type);
	auto value_type = duckdb_map_type_value_type(logical_type);
	if (!key_type || !value_type) {
		if (key_type) {
			duckdb_destroy_logical_type(&key_type);
		}
		if (value_type) {
			duckdb_destroy_logical_type(&value_type);
		}
		error = "Failed to inspect MAP key/value types in table function parameter";
		return nullptr;
	}

	auto map = env->NewObject(J_LinkedHashMap, J_LinkedHashMap_init);
	if (!map || env->ExceptionCheck()) {
		duckdb_destroy_logical_type(&key_type);
		duckdb_destroy_logical_type(&value_type);
		error = "Failed to allocate Java map for table function parameter";
		return nullptr;
	}

	auto size = duckdb_get_map_size(val);
	for (idx_t i = 0; i < size; i++) {
		auto key_value = duckdb_get_map_key(val, i);
		auto mapped_value = duckdb_get_map_value(val, i);
		std::string key_error;
		std::string value_error;
		auto key_obj = table_bind_parameter_to_java_internal(env, key_value, key_type, key_error);
		auto value_obj = table_bind_parameter_to_java_internal(env, mapped_value, value_type, value_error);
		duckdb_destroy_value(&key_value);
		duckdb_destroy_value(&mapped_value);
		if (!key_error.empty() || !value_error.empty() || env->ExceptionCheck()) {
			if (key_obj) {
				env->DeleteLocalRef(key_obj);
			}
			if (value_obj) {
				env->DeleteLocalRef(value_obj);
			}
			env->DeleteLocalRef(map);
			duckdb_destroy_logical_type(&key_type);
			duckdb_destroy_logical_type(&value_type);
			error = !key_error.empty() ? key_error : value_error;
			if (error.empty()) {
				error = "Failed to convert MAP entry in table function parameter";
			}
			return nullptr;
		}
		auto old_value = env->CallObjectMethod(map, J_LinkedHashMap_put, key_obj, value_obj);
		if (old_value) {
			env->DeleteLocalRef(old_value);
		}
		if (key_obj) {
			env->DeleteLocalRef(key_obj);
		}
		if (value_obj) {
			env->DeleteLocalRef(value_obj);
		}
		if (env->ExceptionCheck()) {
			env->DeleteLocalRef(map);
			duckdb_destroy_logical_type(&key_type);
			duckdb_destroy_logical_type(&value_type);
			error = "Failed to append MAP entry in Java table function parameter";
			return nullptr;
		}
	}

	duckdb_destroy_logical_type(&key_type);
	duckdb_destroy_logical_type(&value_type);
	return map;
}

static jobject table_bind_struct_parameter_to_java(JNIEnv *env, duckdb_value val, duckdb_logical_type logical_type,
                                                   std::string &error) {
	auto map = env->NewObject(J_LinkedHashMap, J_LinkedHashMap_init);
	if (!map || env->ExceptionCheck()) {
		error = "Failed to allocate Java struct map for table function parameter";
		return nullptr;
	}

	auto child_count = duckdb_struct_type_child_count(logical_type);
	for (idx_t i = 0; i < child_count; i++) {
		varchar_ptr child_name_ptr(duckdb_struct_type_child_name(logical_type, i), varchar_deleter);
		auto child_type = duckdb_struct_type_child_type(logical_type, i);
		auto child_value = duckdb_get_struct_child(val, i);
		if (!child_name_ptr || !child_type) {
			if (child_type) {
				duckdb_destroy_logical_type(&child_type);
			}
			duckdb_destroy_value(&child_value);
			env->DeleteLocalRef(map);
			error = "Failed to inspect STRUCT child metadata in table function parameter";
			return nullptr;
		}

		auto child_name_len = static_cast<idx_t>(std::strlen(child_name_ptr.get()));
		auto child_name_j = decode_charbuffer_to_jstring(env, child_name_ptr.get(), child_name_len);
		std::string child_error;
		auto child_obj = table_bind_parameter_to_java_internal(env, child_value, child_type, child_error);
		duckdb_destroy_logical_type(&child_type);
		duckdb_destroy_value(&child_value);
		if (env->ExceptionCheck() || !child_error.empty() || !child_name_j) {
			if (child_name_j) {
				env->DeleteLocalRef(child_name_j);
			}
			if (child_obj) {
				env->DeleteLocalRef(child_obj);
			}
			env->DeleteLocalRef(map);
			error = child_error.empty() ? "Failed to convert STRUCT child in table function parameter" : child_error;
			return nullptr;
		}
		auto old_value = env->CallObjectMethod(map, J_LinkedHashMap_put, child_name_j, child_obj);
		if (old_value) {
			env->DeleteLocalRef(old_value);
		}
		env->DeleteLocalRef(child_name_j);
		if (child_obj) {
			env->DeleteLocalRef(child_obj);
		}
		if (env->ExceptionCheck()) {
			env->DeleteLocalRef(map);
			error = "Failed to append STRUCT child in Java table function parameter";
			return nullptr;
		}
	}

	return map;
}

static jobject table_bind_parameter_to_java_internal(JNIEnv *env, duckdb_value val, duckdb_logical_type logical_type,
                                                     std::string &error) {
	if (duckdb_is_null_value(val)) {
		return nullptr;
	}
	if (!logical_type) {
		error = "Invalid logical type for table function parameter";
		return nullptr;
	}

	auto type = static_cast<duckdb_type>(duckdb_get_type_id(logical_type));
	switch (type) {
	case DUCKDB_TYPE_LIST:
	case DUCKDB_TYPE_ARRAY:
		return table_bind_list_parameter_to_java(env, val, logical_type, type, error);
	case DUCKDB_TYPE_MAP:
		return table_bind_map_parameter_to_java(env, val, logical_type, error);
	case DUCKDB_TYPE_STRUCT:
		return table_bind_struct_parameter_to_java(env, val, logical_type, error);
	case DUCKDB_TYPE_UNION: {
		varchar_ptr repr_ptr(duckdb_value_to_string(val), varchar_deleter);
		if (!repr_ptr) {
			error = "Failed to materialize UNION table function parameter";
			return nullptr;
		}
		auto repr_len = static_cast<idx_t>(std::strlen(repr_ptr.get()));
		auto jstr = decode_charbuffer_to_jstring(env, repr_ptr.get(), repr_len);
		if (env->ExceptionCheck()) {
			error = "Failed to decode UNION table function parameter";
			return nullptr;
		}
		return jstr;
	}
	default:
		return table_bind_parameter_scalar_to_java(env, val, type, logical_type, error);
	}
}

static jobject table_bind_parameter_to_java(JNIEnv *env, duckdb_value val, duckdb_logical_type logical_type,
                                            std::vector<jobject> &local_refs, std::string &error) {
	auto param_obj = table_bind_parameter_to_java_internal(env, val, logical_type, error);
	if (param_obj != nullptr) {
		local_refs.push_back(param_obj);
	}
	return param_obj;
}

static duckdb_vector udf_vector_ref_buf_to_vector(JNIEnv *env, jobject vector_ref_buf) {
	if (vector_ref_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null vector reference");
		return nullptr;
	}
	auto vec = reinterpret_cast<duckdb_vector>(env->GetDirectBufferAddress(vector_ref_buf));
	if (!vec) {
		env->ThrowNew(J_SQLException, "Invalid vector reference");
		return nullptr;
	}
	return vec;
}

static jobject create_scalar_udf_input_reader(JNIEnv *env, duckdb_vector vector, duckdb_type type, idx_t row_count,
                                              jlong validity_size, std::vector<jobject> &local_refs) {
	auto spec = find_udf_type_spec(type);
	if (!spec || !spec->udf_vector_supported) {
		env->ThrowNew(J_SQLException, "Unsupported scalar UDF type");
		return nullptr;
	}

	auto validity_ptr = reinterpret_cast<uint64_t *>(duckdb_vector_get_validity(vector));
	auto validity_buf = validity_ptr ? env->NewDirectByteBuffer(validity_ptr, validity_size) : nullptr;
	if (validity_buf) {
		local_refs.push_back(validity_buf);
	}

	jobject data_buf = nullptr;
	jobject vector_ref_buf = nullptr;
	if (spec->requires_vector_ref) {
		vector_ref_buf = env->NewDirectByteBuffer(vector, 0);
		local_refs.push_back(vector_ref_buf);
	} else {
		auto data_ptr = duckdb_vector_get_data(vector);
		data_buf = env->NewDirectByteBuffer(
		    data_ptr, static_cast<jlong>(row_count * static_cast<idx_t>(spec->fixed_width_bytes)));
		local_refs.push_back(data_buf);
	}

	auto reader = env->NewObject(J_UdfNativeReader, J_UdfNativeReader_init, static_cast<jint>(type), data_buf,
	                             vector_ref_buf, validity_buf, static_cast<jint>(row_count));
	local_refs.push_back(reader);
	return reader;
}

static jobject create_scalar_udf_output_writer(JNIEnv *env, duckdb_vector vector, duckdb_type type, idx_t row_count,
                                               jlong validity_size, std::vector<jobject> &local_refs) {
	auto spec = find_udf_type_spec(type);
	if (!spec || !spec->udf_vector_supported) {
		env->ThrowNew(J_SQLException, "Unsupported scalar UDF output type");
		return nullptr;
	}

	auto validity_ptr = reinterpret_cast<uint64_t *>(duckdb_vector_get_validity(vector));
	auto validity_buf = validity_ptr ? env->NewDirectByteBuffer(validity_ptr, validity_size) : nullptr;
	if (validity_buf) {
		local_refs.push_back(validity_buf);
	}

	jobject data_buf = nullptr;
	jobject vector_ref_buf = nullptr;
	if (spec->requires_vector_ref) {
		vector_ref_buf = env->NewDirectByteBuffer(vector, 0);
		local_refs.push_back(vector_ref_buf);
	} else {
		auto data_ptr = duckdb_vector_get_data(vector);
		data_buf = env->NewDirectByteBuffer(
		    data_ptr, static_cast<jlong>(row_count * static_cast<idx_t>(spec->fixed_width_bytes)));
		local_refs.push_back(data_buf);
	}

	auto writer = env->NewObject(J_UdfScalarWriter, J_UdfScalarWriter_init, static_cast<jint>(type), data_buf,
	                             vector_ref_buf, validity_buf, static_cast<jint>(row_count));
	local_refs.push_back(writer);
	return writer;
}

static void destroy_java_scalar_udf_callback_data(void *ptr) {
	if (!ptr) {
		return;
	}
	auto data = reinterpret_cast<JavaScalarUdfCallbackData *>(ptr);
	CallbackEnvGuard env_guard(data->vm);
	auto env = env_guard.env();
	if (env && data->callback_ref) {
		delete_global_ref(env, data->callback_ref);
	}
	delete data;
}

static void destroy_java_table_function_callback_data(void *ptr) {
	if (!ptr) {
		return;
	}
	auto data = reinterpret_cast<JavaTableFunctionCallbackData *>(ptr);
	for (auto &logical_type : data->parameter_logical_types) {
		if (logical_type) {
			duckdb_destroy_logical_type(&logical_type);
		}
	}
	data->parameter_logical_types.clear();
	CallbackEnvGuard env_guard(data->vm);
	auto env = env_guard.env();
	if (env && data->callback_ref) {
		delete_global_ref(env, data->callback_ref);
	}
	delete data;
}

static void destroy_java_table_function_bind_data(void *ptr) {
	if (!ptr) {
		return;
	}
	auto data = reinterpret_cast<JavaTableFunctionBindData *>(ptr);
	CallbackEnvGuard env_guard(data->vm);
	auto env = env_guard.env();
	if (env && data->bind_result_ref) {
		delete_global_ref(env, data->bind_result_ref);
	}
	delete data;
}

static void destroy_java_table_function_init_data(void *ptr) {
	if (!ptr) {
		return;
	}
	auto data = reinterpret_cast<JavaTableFunctionInitData *>(ptr);
	CallbackEnvGuard env_guard(data->vm);
	auto env = env_guard.env();
	if (env && data->state_ref) {
		delete_global_ref(env, data->state_ref);
	}
	delete data;
}

static void java_scalar_udf_callback(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	auto data = reinterpret_cast<JavaScalarUdfCallbackData *>(duckdb_scalar_function_get_extra_info(info));
	if (!data) {
		duckdb_scalar_function_set_error(info, "Missing callback state for Java scalar UDF");
		return;
	}

	CallbackEnvGuard env_guard(data->vm);
	auto env = env_guard.env();
	if (!env) {
		duckdb_scalar_function_set_error(info, "Failed to acquire JNIEnv for Java scalar UDF callback");
		return;
	}

	auto row_count = duckdb_data_chunk_get_size(input);
	auto arg_count = duckdb_data_chunk_get_column_count(input);
	if ((!data->var_args && data->argument_types.size() != arg_count) ||
	    (data->var_args && data->argument_types.size() != 1)) {
		duckdb_scalar_function_set_error(info, "Scalar UDF argument mismatch");
		return;
	}
	duckdb_vector_ensure_validity_writable(output);
	auto output_validity_ptr = reinterpret_cast<uint64_t *>(duckdb_vector_get_validity(output));

	auto validity_size = static_cast<jlong>(((row_count + 63) / 64) * sizeof(uint64_t));
	auto args = env->NewObjectArray(arg_count, J_UdfReader, nullptr);
	std::vector<jobject> local_refs;
	local_refs.push_back(args);
	for (idx_t arg_idx = 0; arg_idx < arg_count; arg_idx++) {
		auto input_vector = duckdb_data_chunk_get_vector(input, arg_idx);
		auto argument_type = data->var_args ? data->var_args_type : data->argument_types[arg_idx];
		auto input_reader =
		    create_scalar_udf_input_reader(env, input_vector, argument_type, row_count, validity_size, local_refs);
		env->SetObjectArrayElement(args, static_cast<jsize>(arg_idx), input_reader);
	}
	if (env->ExceptionCheck()) {
		duckdb_scalar_function_set_error(info, "Failed to materialize scalar UDF input readers");
		delete_local_refs(env, local_refs);
		return;
	}
	auto output_writer =
	    create_scalar_udf_output_writer(env, output, data->return_type, row_count, validity_size, local_refs);
	if (env->ExceptionCheck()) {
		duckdb_scalar_function_set_error(info, "Failed to materialize scalar UDF output writer");
		delete_local_refs(env, local_refs);
		return;
	}

	env->CallVoidMethod(data->callback_ref, J_ScalarUdf_apply, nullptr, args, output_writer,
	                    static_cast<jint>(row_count));

	if (env->ExceptionCheck()) {
		auto exception = env->ExceptionOccurred();
		env->ExceptionClear();
		if (data->return_null_on_exception) {
			if (output_validity_ptr) {
				std::memset(output_validity_ptr, 0, static_cast<size_t>(validity_size));
			}
			if (exception) {
				delete_local_ref(env, exception);
			}
		} else {
			std::string error = "Exception in Java scalar UDF callback";
			if (exception) {
				auto message = reinterpret_cast<jstring>(env->CallObjectMethod(exception, J_Throwable_getMessage));
				if (message != nullptr) {
					error = jstring_to_string(env, message);
					delete_local_ref(env, message);
				}
				delete_local_ref(env, exception);
			}
			duckdb_scalar_function_set_error(info, error.c_str());
		}
	}

	delete_local_refs(env, local_refs);
}

jobject _duckdb_jdbc_startup(JNIEnv *env, jclass, jbyteArray database_j, jboolean read_only, jobject props) {
	auto database = jbyteArray_to_string(env, database_j);
	std::unique_ptr<DBConfig> config = create_db_config(env, read_only, props);
	bool cache_instance = database != ":memory:" && !database.empty();
	auto shared_db = instance_cache.GetOrCreateInstance(database, *config, cache_instance);
	auto conn_ref = new ConnectionHolder(shared_db);

	return env->NewDirectByteBuffer(conn_ref, 0);
}

static duckdb_scalar_function scalar_function_buf_to_scalar_function(JNIEnv *env, jobject scalar_function_buf) {
	if (scalar_function_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function buffer");
		return nullptr;
	}

	auto scalar_function = reinterpret_cast<duckdb_scalar_function>(env->GetDirectBufferAddress(scalar_function_buf));
	if (scalar_function == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function");
		return nullptr;
	}

	return scalar_function;
}

static duckdb_table_function table_function_buf_to_table_function(JNIEnv *env, jobject table_function_buf) {
	if (table_function_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function buffer");
		return nullptr;
	}

	auto table_function = reinterpret_cast<duckdb_table_function>(env->GetDirectBufferAddress(table_function_buf));
	if (table_function == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function");
		return nullptr;
	}

	return table_function;
}

static void java_table_function_bind_callback(duckdb_bind_info info);
static void java_table_function_init_callback(duckdb_init_info info);
static void java_table_function_main_callback(duckdb_function_info info, duckdb_data_chunk output);

static void register_scalar_udf_on_function(JNIEnv *env, duckdb_connection conn, duckdb_scalar_function scalar_function,
                                            jobject callback, jobjectArray argument_logical_types_j,
                                            jobject return_logical_type_j, jboolean return_null_on_exception,
                                            jboolean var_args) {
	if (argument_logical_types_j == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null argument types");
		return;
	}
	auto arg_count = env->GetArrayLength(argument_logical_types_j);
	if (arg_count < 0) {
		env->ThrowNew(J_SQLException, "Invalid scalar UDF argument count");
		return;
	}
	if (var_args && arg_count != 1) {
		env->ThrowNew(J_SQLException, "Scalar UDF varargs registration expects exactly one argument logical type");
		return;
	}

	std::vector<duckdb_logical_type> arg_types;
	arg_types.reserve(static_cast<size_t>(arg_count));
	std::vector<duckdb_type> arg_type_tags;
	arg_type_tags.reserve(static_cast<size_t>(arg_count));
	auto destroy_arg_types = [&arg_types]() {
		for (auto &arg_type : arg_types) {
			duckdb_destroy_logical_type(&arg_type);
		}
	};
	for (jsize i = 0; i < arg_count; i++) {
		auto argument_type_obj = env->GetObjectArrayElement(argument_logical_types_j, i);
		if (env->ExceptionCheck() || !argument_type_obj) {
			destroy_arg_types();
			env->ThrowNew(J_SQLException, "Invalid scalar UDF argument logical type descriptor");
			return;
		}

		std::string logical_type_error;
		auto arg_logical_type = create_table_logical_type_from_java(env, argument_type_obj, logical_type_error);
		delete_local_ref(env, argument_type_obj);
		if (env->ExceptionCheck() || !arg_logical_type) {
			destroy_arg_types();
			if (logical_type_error.empty()) {
				logical_type_error = "Unsupported scalar UDF argument logical type";
			}
			env->ThrowNew(J_SQLException, logical_type_error.c_str());
			return;
		}

		auto arg_type_id = duckdb_get_type_id(arg_logical_type);
		if (!is_supported_scalar_udf_type(arg_type_id)) {
			duckdb_destroy_logical_type(&arg_logical_type);
			destroy_arg_types();
			env->ThrowNew(J_SQLException, UNSUPPORTED_SCALAR_UDF_TYPE_ERROR);
			return;
		}
		arg_types.push_back(arg_logical_type);
		arg_type_tags.push_back(arg_type_id);
	}

	if (return_logical_type_j == nullptr) {
		destroy_arg_types();
		env->ThrowNew(J_SQLException, "Invalid null return type");
		return;
	}

	std::string return_type_error;
	auto return_type = create_table_logical_type_from_java(env, return_logical_type_j, return_type_error);
	if (env->ExceptionCheck() || !return_type) {
		destroy_arg_types();
		if (return_type_error.empty()) {
			return_type_error = "Unsupported scalar UDF return logical type";
		}
		env->ThrowNew(J_SQLException, return_type_error.c_str());
		return;
	}
	auto return_type_tag = duckdb_get_type_id(return_type);
	if (!is_supported_scalar_udf_type(return_type_tag)) {
		destroy_arg_types();
		duckdb_destroy_logical_type(&return_type);
		env->ThrowNew(J_SQLException, UNSUPPORTED_SCALAR_UDF_TYPE_ERROR);
		return;
	}

	auto callback_data = new JavaScalarUdfCallbackData();
	callback_data->vm = GLOBAL_JVM;
	callback_data->callback_ref = env->NewGlobalRef(callback);
	callback_data->return_null_on_exception = return_null_on_exception;
	callback_data->var_args = var_args;
	callback_data->argument_types = std::move(arg_type_tags);
	callback_data->var_args_type =
	    callback_data->argument_types.empty() ? DUCKDB_TYPE_INVALID : callback_data->argument_types[0];
	callback_data->return_type = return_type_tag;
	if (!callback_data->callback_ref) {
		delete callback_data;
		destroy_arg_types();
		duckdb_destroy_logical_type(&return_type);
		throw InvalidInputException("Failed to create global ref for Java scalar UDF callback");
	}

	if (var_args) {
		duckdb_scalar_function_set_varargs(scalar_function, arg_types[0]);
	} else {
		for (auto &arg_type : arg_types) {
			duckdb_scalar_function_add_parameter(scalar_function, arg_type);
		}
	}
	duckdb_scalar_function_set_return_type(scalar_function, return_type);
	duckdb_scalar_function_set_extra_info(scalar_function, callback_data, destroy_java_scalar_udf_callback_data);
	duckdb_scalar_function_set_function(scalar_function, java_scalar_udf_callback);

	auto register_state = duckdb_register_scalar_function(conn, scalar_function);

	destroy_arg_types();
	duckdb_destroy_logical_type(&return_type);

	if (register_state != DuckDBSuccess) {
		throw InvalidInputException("Failed to register Java scalar UDF");
	}
}

static void register_table_function_on_function(JNIEnv *env, duckdb_connection conn, duckdb_table_function table_fn,
                                                jobject callback, jobjectArray parameter_types_j, jint max_threads,
                                                jboolean thread_safe) {
	if (parameter_types_j == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null table function parameter types");
		return;
	}

	auto parameter_count = env->GetArrayLength(parameter_types_j);
	std::vector<duckdb_logical_type> parameter_logical_types;
	parameter_logical_types.reserve(static_cast<size_t>(parameter_count));
	for (jsize i = 0; i < parameter_count; i++) {
		auto parameter_type_obj = env->GetObjectArrayElement(parameter_types_j, i);
		if (env->ExceptionCheck() || !parameter_type_obj) {
			for (auto &parameter_logical_type : parameter_logical_types) {
				duckdb_destroy_logical_type(&parameter_logical_type);
			}
			env->ThrowNew(J_SQLException, "Invalid table function parameter logical type descriptor");
			return;
		}
		std::string logical_type_error;
		auto parameter_logical_type = create_table_logical_type_from_java(env, parameter_type_obj, logical_type_error);
		delete_local_ref(env, parameter_type_obj);
		if (env->ExceptionCheck() || !parameter_logical_type) {
			for (auto &existing_parameter_type : parameter_logical_types) {
				duckdb_destroy_logical_type(&existing_parameter_type);
			}
			if (logical_type_error.empty()) {
				logical_type_error = "Unsupported table function parameter logical type";
			}
			env->ThrowNew(J_SQLException, logical_type_error.c_str());
			return;
		}
		std::string support_error;
		if (!is_supported_table_bind_parameter_logical_type(parameter_logical_type, support_error)) {
			duckdb_destroy_logical_type(&parameter_logical_type);
			for (auto &existing_parameter_type : parameter_logical_types) {
				duckdb_destroy_logical_type(&existing_parameter_type);
			}
			if (support_error.empty()) {
				support_error = UNSUPPORTED_TABLE_FUNCTION_PARAMETER_TYPE_ERROR;
			}
			env->ThrowNew(J_SQLException, support_error.c_str());
			return;
		}
		duckdb_table_function_add_parameter(table_fn, parameter_logical_type);
		parameter_logical_types.push_back(parameter_logical_type);
	}

	auto callback_data = new JavaTableFunctionCallbackData();
	callback_data->vm = GLOBAL_JVM;
	callback_data->callback_ref = env->NewGlobalRef(callback);
	callback_data->thread_safe = thread_safe;
	callback_data->max_threads = static_cast<idx_t>(max_threads < 1 ? 1 : max_threads);
	callback_data->parameter_logical_types = std::move(parameter_logical_types);
	if (!callback_data->callback_ref) {
		for (auto &parameter_logical_type : callback_data->parameter_logical_types) {
			duckdb_destroy_logical_type(&parameter_logical_type);
		}
		delete callback_data;
		throw InvalidInputException("Failed to create global ref for Java table function callback");
	}
	duckdb_table_function_set_extra_info(table_fn, callback_data, destroy_java_table_function_callback_data);
	duckdb_table_function_set_bind(table_fn, java_table_function_bind_callback);
	duckdb_table_function_set_init(table_fn, java_table_function_init_callback);
	duckdb_table_function_set_function(table_fn, java_table_function_main_callback);

	auto register_state = duckdb_register_table_function(conn, table_fn);
	if (register_state != DuckDBSuccess) {
		throw InvalidInputException("Failed to register Java table function");
	}
}

void duckdb_jdbc_register_scalar_udf_impl(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray name_j,
                                          jobject callback, jobjectArray argument_logical_types_j,
                                          jobject return_logical_type_j, jboolean special_handling,
                                          jboolean return_null_on_exception, jboolean deterministic,
                                          jboolean var_args) {
	auto conn = conn_ref_buf_to_conn(env, conn_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null callback");
		return;
	}

	auto udf_name = jbyteArray_to_string(env, name_j);
	if (env->ExceptionCheck()) {
		return;
	}

	auto scalar_function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(scalar_function, udf_name.c_str());
	if (special_handling) {
		duckdb_scalar_function_set_special_handling(scalar_function);
	}
	if (!deterministic) {
		duckdb_scalar_function_set_volatile(scalar_function);
	}
	try {
		register_scalar_udf_on_function(env, conn, scalar_function, callback, argument_logical_types_j,
		                                return_logical_type_j, return_null_on_exception, var_args);
	} catch (...) {
		duckdb_destroy_scalar_function(&scalar_function);
		throw;
	}
	duckdb_destroy_scalar_function(&scalar_function);
}

void duckdb_jdbc_register_scalar_udf_on_function_impl(JNIEnv *env, jclass, jobject conn_ref_buf,
                                                      jobject scalar_function_buf, jobject callback,
                                                      jobjectArray argument_logical_types_j,
                                                      jobject return_logical_type_j, jboolean return_null_on_exception,
                                                      jboolean var_args) {
	auto conn = conn_ref_buf_to_conn(env, conn_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null callback");
		return;
	}
	auto scalar_function = scalar_function_buf_to_scalar_function(env, scalar_function_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	register_scalar_udf_on_function(env, conn, scalar_function, callback, argument_logical_types_j,
	                                return_logical_type_j, return_null_on_exception, var_args);
}

static void java_table_function_bind_callback(duckdb_bind_info info) {
	auto callback_data = reinterpret_cast<JavaTableFunctionCallbackData *>(duckdb_bind_get_extra_info(info));
	if (!callback_data) {
		duckdb_bind_set_error(info, "Missing callback state for Java table function");
		return;
	}
	CallbackEnvGuard env_guard(callback_data->vm);
	auto env = env_guard.env();
	if (!env) {
		duckdb_bind_set_error(info, "Failed to acquire JNIEnv for Java table bind callback");
		return;
	}

	auto parameter_count = duckdb_bind_get_parameter_count(info);
	if (callback_data->parameter_logical_types.size() != parameter_count) {
		duckdb_bind_set_error(info, "Table function parameter count mismatch");
		return;
	}
	auto parameters = env->NewObjectArray(static_cast<jsize>(parameter_count), J_Object, nullptr);
	std::vector<jobject> bind_local_refs;
	bind_local_refs.push_back(parameters);
	for (idx_t i = 0; i < parameter_count; i++) {
		auto val = duckdb_bind_get_parameter(info, i);
		std::string parameter_error;
		auto param_obj = table_bind_parameter_to_java(env, val, callback_data->parameter_logical_types[i],
		                                              bind_local_refs, parameter_error);
		duckdb_destroy_value(&val);
		if ((!parameter_error.empty() && param_obj == nullptr) || env->ExceptionCheck()) {
			if (parameter_error.empty()) {
				parameter_error = "Failed to materialize table function bind parameter";
			}
			duckdb_bind_set_error(info, parameter_error.c_str());
			delete_local_refs(env, bind_local_refs);
			return;
		}
		env->SetObjectArrayElement(parameters, static_cast<jsize>(i), param_obj);
		if (env->ExceptionCheck()) {
			duckdb_bind_set_error(info, "Failed to pass table function bind parameters to Java");
			delete_local_refs(env, bind_local_refs);
			return;
		}
	}

	auto bind_result = env->CallObjectMethod(callback_data->callback_ref, J_TableFunction_bind, nullptr, parameters);
	if (env->ExceptionCheck()) {
		auto exception = env->ExceptionOccurred();
		env->ExceptionClear();
		std::string error = "Exception in Java table function bind callback";
		if (exception) {
			auto message = reinterpret_cast<jstring>(env->CallObjectMethod(exception, J_Throwable_getMessage));
			if (message != nullptr) {
				error = jstring_to_string(env, message);
				delete_local_ref(env, message);
			}
			delete_local_ref(env, exception);
		}
		duckdb_bind_set_error(info, error.c_str());
		delete_local_refs(env, bind_local_refs);
		return;
	}
	if (bind_result == nullptr) {
		duckdb_bind_set_error(info, "Java table function bind returned null");
		delete_local_refs(env, bind_local_refs);
		return;
	}

	auto column_names =
	    reinterpret_cast<jobjectArray>(env->CallObjectMethod(bind_result, J_TableBindResult_getColumnNames));
	auto column_types =
	    reinterpret_cast<jobjectArray>(env->CallObjectMethod(bind_result, J_TableBindResult_getColumnTypes));
	auto column_logical_types =
	    reinterpret_cast<jobjectArray>(env->CallObjectMethod(bind_result, J_TableBindResult_getColumnLogicalTypes));
	if (env->ExceptionCheck() || column_names == nullptr || column_types == nullptr) {
		duckdb_bind_set_error(info, "Invalid Java table bind result");
		delete_local_ref(env, column_names);
		delete_local_ref(env, column_types);
		delete_local_ref(env, column_logical_types);
		delete_local_ref(env, bind_result);
		delete_local_refs(env, bind_local_refs);
		return;
	}
	auto name_count = env->GetArrayLength(column_names);
	auto type_count = env->GetArrayLength(column_types);
	if (name_count != type_count) {
		duckdb_bind_set_error(info, "Java table bind result has mismatched schema lengths");
		delete_local_ref(env, column_names);
		delete_local_ref(env, column_types);
		delete_local_ref(env, column_logical_types);
		delete_local_ref(env, bind_result);
		delete_local_refs(env, bind_local_refs);
		return;
	}
	if (column_logical_types != nullptr && env->GetArrayLength(column_logical_types) != name_count) {
		duckdb_bind_set_error(info, "Java table bind result has mismatched logical schema lengths");
		delete_local_ref(env, column_names);
		delete_local_ref(env, column_types);
		delete_local_ref(env, column_logical_types);
		delete_local_ref(env, bind_result);
		delete_local_refs(env, bind_local_refs);
		return;
	}
	for (jsize i = 0; i < name_count; i++) {
		auto name_j = reinterpret_cast<jstring>(env->GetObjectArrayElement(column_names, i));
		auto type_obj = env->GetObjectArrayElement(column_types, i);
		if (!name_j || !type_obj) {
			delete_local_ref(env, name_j);
			delete_local_ref(env, type_obj);
			duckdb_bind_set_error(info, "Unsupported column descriptor in Java table bind result");
			delete_local_ref(env, column_names);
			delete_local_ref(env, column_types);
			delete_local_ref(env, column_logical_types);
			delete_local_ref(env, bind_result);
			delete_local_refs(env, bind_local_refs);
			return;
		}
		auto name = jstring_to_string(env, name_j);
		duckdb_logical_type logical_type = nullptr;
		if (column_logical_types != nullptr) {
			auto logical_type_obj = env->GetObjectArrayElement(column_logical_types, i);
			std::string logical_error;
			logical_type = create_table_logical_type_from_java(env, logical_type_obj, logical_error);
			delete_local_ref(env, logical_type_obj);
			if (env->ExceptionCheck() || !logical_type) {
				if (logical_error.empty()) {
					logical_error = "Unsupported logical type in Java table bind result";
				}
				duckdb_bind_set_error(info, logical_error.c_str());
				delete_local_ref(env, name_j);
				delete_local_ref(env, type_obj);
				delete_local_ref(env, column_names);
				delete_local_ref(env, column_types);
				delete_local_ref(env, column_logical_types);
				delete_local_ref(env, bind_result);
				delete_local_refs(env, bind_local_refs);
				return;
			}
		} else {
			duckdb_type duck_type = DUCKDB_TYPE_INVALID;
			if (!table_column_type_from_java(env, type_obj, duck_type)) {
				duckdb_bind_set_error(info, "Unsupported column type in Java table bind result");
				delete_local_ref(env, name_j);
				delete_local_ref(env, type_obj);
				delete_local_ref(env, column_names);
				delete_local_ref(env, column_types);
				delete_local_ref(env, bind_result);
				delete_local_refs(env, bind_local_refs);
				return;
			}
			logical_type = create_udf_logical_type(duck_type);
		}
		duckdb_bind_add_result_column(info, name.c_str(), logical_type);
		duckdb_destroy_logical_type(&logical_type);
		delete_local_ref(env, name_j);
		delete_local_ref(env, type_obj);
	}

	auto bind_data = new JavaTableFunctionBindData();
	bind_data->vm = callback_data->vm;
	bind_data->bind_result_ref = env->NewGlobalRef(bind_result);
	if (!bind_data->bind_result_ref) {
		delete bind_data;
		duckdb_bind_set_error(info, "Failed to create global ref for Java table bind state");
		delete_local_ref(env, column_names);
		delete_local_ref(env, column_types);
		delete_local_ref(env, column_logical_types);
		delete_local_ref(env, bind_result);
		delete_local_refs(env, bind_local_refs);
		return;
	}
	duckdb_bind_set_bind_data(info, bind_data, destroy_java_table_function_bind_data);

	delete_local_ref(env, column_names);
	delete_local_ref(env, column_types);
	delete_local_ref(env, column_logical_types);
	delete_local_ref(env, bind_result);
	delete_local_refs(env, bind_local_refs);
}

static void java_table_function_init_callback(duckdb_init_info info) {
	auto callback_data = reinterpret_cast<JavaTableFunctionCallbackData *>(duckdb_init_get_extra_info(info));
	auto bind_data = reinterpret_cast<JavaTableFunctionBindData *>(duckdb_init_get_bind_data(info));
	if (!callback_data || !bind_data) {
		duckdb_init_set_error(info, "Missing callback/bind state for Java table function init");
		return;
	}
	CallbackEnvGuard env_guard(callback_data->vm);
	auto env = env_guard.env();
	if (!env) {
		duckdb_init_set_error(info, "Failed to acquire JNIEnv for Java table init callback");
		return;
	}

	auto projected_column_count = duckdb_init_get_column_count(info);
	auto projected_column_indexes = env->NewIntArray(static_cast<jsize>(projected_column_count));
	if (!projected_column_indexes) {
		duckdb_init_set_error(info, "Failed to allocate projected column index array");
		return;
	}
	std::vector<jint> projected_columns;
	projected_columns.reserve(projected_column_count);
	for (idx_t i = 0; i < projected_column_count; i++) {
		projected_columns.push_back(static_cast<jint>(duckdb_init_get_column_index(info, i)));
	}
	if (!projected_columns.empty()) {
		env->SetIntArrayRegion(projected_column_indexes, 0, static_cast<jsize>(projected_columns.size()),
		                       projected_columns.data());
	}
	auto init_ctx = env->NewObject(J_TableInitContext, J_TableInitContext_init, projected_column_indexes);
	delete_local_ref(env, projected_column_indexes);
	if (env->ExceptionCheck() || !init_ctx) {
		duckdb_init_set_error(info, "Failed to construct Java table init context");
		return;
	}

	auto state =
	    env->CallObjectMethod(callback_data->callback_ref, J_TableFunction_init, init_ctx, bind_data->bind_result_ref);
	delete_local_ref(env, init_ctx);
	if (env->ExceptionCheck()) {
		auto exception = env->ExceptionOccurred();
		env->ExceptionClear();
		std::string error = "Exception in Java table function init callback";
		if (exception) {
			auto message = reinterpret_cast<jstring>(env->CallObjectMethod(exception, J_Throwable_getMessage));
			if (message != nullptr) {
				error = jstring_to_string(env, message);
				delete_local_ref(env, message);
			}
			delete_local_ref(env, exception);
		}
		duckdb_init_set_error(info, error.c_str());
		return;
	}
	if (state == nullptr) {
		duckdb_init_set_error(info, "Java table function init returned null");
		return;
	}
	auto init_data = new JavaTableFunctionInitData();
	init_data->vm = callback_data->vm;
	init_data->state_ref = env->NewGlobalRef(state);
	if (!init_data->state_ref) {
		delete init_data;
		duckdb_init_set_error(info, "Failed to create global ref for Java table init state");
		delete_local_ref(env, state);
		return;
	}
	duckdb_init_set_init_data(info, init_data, destroy_java_table_function_init_data);
	if (callback_data->thread_safe) {
		duckdb_init_set_max_threads(info, callback_data->max_threads < 1 ? 1 : callback_data->max_threads);
	} else {
		duckdb_init_set_max_threads(info, 1);
	}

	delete_local_ref(env, state);
}

static void java_table_function_main_callback(duckdb_function_info info, duckdb_data_chunk output) {
	auto callback_data = reinterpret_cast<JavaTableFunctionCallbackData *>(duckdb_function_get_extra_info(info));
	auto init_data = reinterpret_cast<JavaTableFunctionInitData *>(duckdb_function_get_init_data(info));
	if (!callback_data || !init_data) {
		duckdb_function_set_error(info, "Missing callback/init state for Java table function");
		return;
	}
	CallbackEnvGuard env_guard(callback_data->vm);
	auto env = env_guard.env();
	if (!env) {
		duckdb_function_set_error(info, "Failed to acquire JNIEnv for Java table function callback");
		return;
	}

	auto row_capacity = duckdb_vector_size();
	auto output_ref = env->NewDirectByteBuffer(output, 0);
	if (!output_ref || env->ExceptionCheck()) {
		if (env->ExceptionCheck()) {
			env->ExceptionClear();
		}
		duckdb_function_set_error(info, "Failed to materialize Java table function output chunk");
		return;
	}

	auto out_appender = env->NewObject(J_UdfOutputAppender, J_UdfOutputAppender_init, output_ref);
	if (!out_appender || env->ExceptionCheck()) {
		if (env->ExceptionCheck()) {
			env->ExceptionClear();
		}
		delete_local_ref(env, output_ref);
		duckdb_function_set_error(info, "Failed to initialize Java table function output appender");
		return;
	}

	auto produced =
	    env->CallIntMethod(callback_data->callback_ref, J_TableFunction_produce, init_data->state_ref, out_appender);

	jthrowable callback_exception = nullptr;
	if (env->ExceptionCheck()) {
		callback_exception = env->ExceptionOccurred();
		env->ExceptionClear();
	}

	env->CallVoidMethod(out_appender, J_UdfOutputAppender_close);
	jthrowable close_exception = nullptr;
	if (env->ExceptionCheck()) {
		close_exception = env->ExceptionOccurred();
		env->ExceptionClear();
	}

	if (callback_exception || close_exception) {
		auto exception = callback_exception ? callback_exception : close_exception;
		std::string error = "Exception in Java table function callback";
		if (exception) {
			auto message = reinterpret_cast<jstring>(env->CallObjectMethod(exception, J_Throwable_getMessage));
			if (message != nullptr) {
				error = jstring_to_string(env, message);
				delete_local_ref(env, message);
			}
			delete_local_ref(env, exception);
			if (exception == close_exception) {
				close_exception = nullptr;
			}
		}
		duckdb_function_set_error(info, error.c_str());
	} else {
		if (produced < 0) {
			produced = 0;
		}
		if (produced > static_cast<jint>(row_capacity)) {
			produced = static_cast<jint>(row_capacity);
		}
		duckdb_data_chunk_set_size(output, static_cast<idx_t>(produced));
	}

	if (close_exception) {
		delete_local_ref(env, close_exception);
	}
	delete_local_ref(env, out_appender);
	delete_local_ref(env, output_ref);
}

void duckdb_jdbc_register_table_function_impl(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray name_j,
                                              jobject callback, jobjectArray parameter_types_j,
                                              jboolean supports_projection_pushdown, jint max_threads,
                                              jboolean thread_safe) {
	auto conn = conn_ref_buf_to_conn(env, conn_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null callback");
		return;
	}
	auto fn_name = jbyteArray_to_string(env, name_j);
	if (env->ExceptionCheck()) {
		return;
	}
	auto table_fn = duckdb_create_table_function();
	duckdb_table_function_set_name(table_fn, fn_name.c_str());
	if (supports_projection_pushdown) {
		duckdb_table_function_supports_projection_pushdown(table_fn, true);
	}
	try {
		register_table_function_on_function(env, conn, table_fn, callback, parameter_types_j, max_threads, thread_safe);
	} catch (...) {
		duckdb_destroy_table_function(&table_fn);
		throw;
	}
	duckdb_destroy_table_function(&table_fn);
	if (env->ExceptionCheck()) {
		return;
	}
}

void duckdb_jdbc_register_table_function_on_function_impl(JNIEnv *env, jclass, jobject conn_ref_buf,
                                                          jobject table_function_buf, jobject callback,
                                                          jobjectArray parameter_types_j, jint max_threads,
                                                          jboolean thread_safe) {
	auto conn = conn_ref_buf_to_conn(env, conn_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null callback");
		return;
	}
	auto table_fn = table_function_buf_to_table_function(env, table_function_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	register_table_function_on_function(env, conn, table_fn, callback, parameter_types_j, max_threads, thread_safe);
}

static jbyteArray udf_get_varlen_bytes(JNIEnv *env, duckdb_vector vector, jint row) {
	auto data = reinterpret_cast<duckdb_string_t *>(duckdb_vector_get_data(vector));
	auto &value = data[row];
	auto len = duckdb_string_t_length(value);
	auto ptr = duckdb_string_t_data(&value);
	auto bytes = env->NewByteArray(static_cast<jsize>(len));
	if (!bytes) {
		return nullptr;
	}
	if (len > 0) {
		env->SetByteArrayRegion(bytes, 0, static_cast<jsize>(len), reinterpret_cast<const jbyte *>(ptr));
	}
	return bytes;
}

static void udf_set_varlen_bytes(JNIEnv *env, duckdb_vector vector, jint row, jbyteArray value,
                                 const char *null_value_error) {
	if (value == nullptr) {
		env->ThrowNew(J_SQLException, null_value_error);
		return;
	}
	auto len = env->GetArrayLength(value);
	auto bytes = env->GetByteArrayElements(value, nullptr);
	if (!bytes) {
		env->ThrowNew(J_SQLException, "Failed to access varlen bytes");
		return;
	}
	duckdb_vector_assign_string_element_len(vector, row, reinterpret_cast<const char *>(bytes), len);
	env->ReleaseByteArrayElements(value, bytes, JNI_ABORT);
}

static duckdb_hugeint int64_to_hugeint(int64_t value) {
	duckdb_hugeint result;
	result.lower = static_cast<uint64_t>(value);
	result.upper = value < 0 ? -1 : 0;
	return result;
}

static bool decimal_vector_meta(duckdb_vector vector, uint8_t &width, uint8_t &scale, duckdb_type &internal_type,
                                std::string &error) {
	auto logical_type = duckdb_vector_get_column_type(vector);
	if (!logical_type) {
		error = "Failed to get DECIMAL logical type";
		return false;
	}
	auto type_id = duckdb_get_type_id(logical_type);
	if (type_id != DUCKDB_TYPE_DECIMAL) {
		duckdb_destroy_logical_type(&logical_type);
		error = "Native decimal accessor requires DECIMAL vector";
		return false;
	}
	width = duckdb_decimal_width(logical_type);
	scale = duckdb_decimal_scale(logical_type);
	internal_type = duckdb_decimal_internal_type(logical_type);
	duckdb_destroy_logical_type(&logical_type);
	return true;
}

static bool read_decimal_from_vector(duckdb_vector vector, jint row, duckdb_decimal &out_decimal, std::string &error) {
	uint8_t width = 0;
	uint8_t scale = 0;
	duckdb_type internal_type = DUCKDB_TYPE_INVALID;
	if (!decimal_vector_meta(vector, width, scale, internal_type, error)) {
		return false;
	}
	auto data = duckdb_vector_get_data(vector);
	if (!data) {
		error = "Failed to get DECIMAL vector data";
		return false;
	}

	out_decimal.width = width;
	out_decimal.scale = scale;
	switch (internal_type) {
	case DUCKDB_TYPE_SMALLINT:
		out_decimal.value = int64_to_hugeint(static_cast<int64_t>(reinterpret_cast<int16_t *>(data)[row]));
		return true;
	case DUCKDB_TYPE_INTEGER:
		out_decimal.value = int64_to_hugeint(static_cast<int64_t>(reinterpret_cast<int32_t *>(data)[row]));
		return true;
	case DUCKDB_TYPE_BIGINT:
		out_decimal.value = int64_to_hugeint(reinterpret_cast<int64_t *>(data)[row]);
		return true;
	case DUCKDB_TYPE_HUGEINT:
		out_decimal.value = reinterpret_cast<duckdb_hugeint *>(data)[row];
		return true;
	default:
		error = "Unsupported DECIMAL physical type for native accessor";
		return false;
	}
}

static jobject decimal_to_bigdecimal(JNIEnv *env, duckdb_decimal decimal, std::string &error) {
	auto decimal_value = duckdb_create_decimal(decimal);
	if (!decimal_value) {
		error = "Failed to materialize DECIMAL value";
		return nullptr;
	}
	varchar_ptr decimal_str_ptr(duckdb_value_to_string(decimal_value), varchar_deleter);
	duckdb_destroy_value(&decimal_value);
	if (!decimal_str_ptr) {
		error = "Failed to convert DECIMAL value to string";
		return nullptr;
	}

	auto decimal_str_len = static_cast<idx_t>(std::strlen(decimal_str_ptr.get()));
	auto decimal_str_j = decode_charbuffer_to_jstring(env, decimal_str_ptr.get(), decimal_str_len);
	if (env->ExceptionCheck() || !decimal_str_j) {
		if (decimal_str_j) {
			delete_local_ref(env, decimal_str_j);
		}
		error = "Failed to decode DECIMAL value as UTF-8";
		return nullptr;
	}

	auto big_decimal = env->NewObject(J_BigDecimal, J_BigDecimal_initString, decimal_str_j);
	delete_local_ref(env, decimal_str_j);
	if (env->ExceptionCheck() || !big_decimal) {
		error = "Failed to allocate BigDecimal for DECIMAL value";
		return nullptr;
	}
	return big_decimal;
}

static bool write_decimal_to_vector(JNIEnv *env, duckdb_vector vector, jint row, jobject value, std::string &error) {
	if (!value) {
		error = "Invalid null decimal value";
		return false;
	}
	if (!env->IsInstanceOf(value, J_BigDecimal)) {
		error = "Decimal accessor requires java.math.BigDecimal";
		return false;
	}

	uint8_t width = 0;
	uint8_t scale = 0;
	duckdb_type internal_type = DUCKDB_TYPE_INVALID;
	if (!decimal_vector_meta(vector, width, scale, internal_type, error)) {
		return false;
	}
	auto data = duckdb_vector_get_data(vector);
	if (!data) {
		error = "Failed to get DECIMAL vector data";
		return false;
	}

	try {
		auto decimal_value = create_value_from_bigdecimal(env, value);
		if (env->ExceptionCheck()) {
			error = "Failed to parse BigDecimal input";
			return false;
		}
		auto casted = decimal_value.DefaultCastAs(LogicalType::DECIMAL(width, scale));
		switch (internal_type) {
		case DUCKDB_TYPE_SMALLINT:
			reinterpret_cast<int16_t *>(data)[row] = casted.GetValueUnsafe<int16_t>();
			return true;
		case DUCKDB_TYPE_INTEGER:
			reinterpret_cast<int32_t *>(data)[row] = casted.GetValueUnsafe<int32_t>();
			return true;
		case DUCKDB_TYPE_BIGINT:
			reinterpret_cast<int64_t *>(data)[row] = casted.GetValueUnsafe<int64_t>();
			return true;
		case DUCKDB_TYPE_HUGEINT: {
			auto huge_value = casted.GetValueUnsafe<hugeint_t>();
			reinterpret_cast<duckdb_hugeint *>(data)[row].lower = huge_value.lower;
			reinterpret_cast<duckdb_hugeint *>(data)[row].upper = huge_value.upper;
			return true;
		}
		default:
			error = "Unsupported DECIMAL physical type for native accessor";
			return false;
		}
	} catch (const std::exception &e) {
		error = std::string("Failed to cast BigDecimal to DECIMAL: ") + e.what();
		return false;
	}
}

jbyteArray _duckdb_jdbc_udf_get_varchar_bytes(JNIEnv *env, jclass, jobject vector_ref_buf, jint row) {
	auto vector = udf_vector_ref_buf_to_vector(env, vector_ref_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	if (row < 0) {
		env->ThrowNew(J_SQLException, "Invalid negative row index");
		return nullptr;
	}
	return udf_get_varlen_bytes(env, vector, row);
}

void _duckdb_jdbc_udf_set_varchar_bytes(JNIEnv *env, jclass, jobject vector_ref_buf, jint row, jbyteArray value) {
	auto vector = udf_vector_ref_buf_to_vector(env, vector_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (row < 0) {
		env->ThrowNew(J_SQLException, "Invalid negative row index");
		return;
	}
	udf_set_varlen_bytes(env, vector, row, value, "Invalid null string bytes");
}

jbyteArray _duckdb_jdbc_udf_get_blob_bytes(JNIEnv *env, jclass, jobject vector_ref_buf, jint row) {
	auto vector = udf_vector_ref_buf_to_vector(env, vector_ref_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	if (row < 0) {
		env->ThrowNew(J_SQLException, "Invalid negative row index");
		return nullptr;
	}
	return udf_get_varlen_bytes(env, vector, row);
}

void _duckdb_jdbc_udf_set_blob_bytes(JNIEnv *env, jclass, jobject vector_ref_buf, jint row, jbyteArray value) {
	auto vector = udf_vector_ref_buf_to_vector(env, vector_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (row < 0) {
		env->ThrowNew(J_SQLException, "Invalid negative row index");
		return;
	}
	udf_set_varlen_bytes(env, vector, row, value, "Invalid null blob bytes");
}

jobject _duckdb_jdbc_udf_get_decimal(JNIEnv *env, jclass, jobject vector_ref_buf, jint row) {
	auto vector = udf_vector_ref_buf_to_vector(env, vector_ref_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	if (row < 0) {
		env->ThrowNew(J_SQLException, "Invalid negative row index");
		return nullptr;
	}
	duckdb_decimal decimal;
	std::string error;
	if (!read_decimal_from_vector(vector, row, decimal, error)) {
		env->ThrowNew(J_SQLException, error.c_str());
		return nullptr;
	}
	auto result = decimal_to_bigdecimal(env, decimal, error);
	if (!result) {
		if (error.empty()) {
			error = "Failed to convert DECIMAL value to BigDecimal";
		}
		env->ThrowNew(J_SQLException, error.c_str());
		return nullptr;
	}
	return result;
}

void _duckdb_jdbc_udf_set_decimal(JNIEnv *env, jclass, jobject vector_ref_buf, jint row, jobject value) {
	auto vector = udf_vector_ref_buf_to_vector(env, vector_ref_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (row < 0) {
		env->ThrowNew(J_SQLException, "Invalid negative row index");
		return;
	}
	std::string error;
	if (!write_decimal_to_vector(env, vector, row, value, error)) {
		env->ThrowNew(J_SQLException, error.c_str());
	}
}

jobject _duckdb_jdbc_connect(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection_ref(env, conn_ref_buf);
	auto config = ClientConfig::GetConfig(*conn_ref->connection->context);
	auto conn = new ConnectionHolder(conn_ref->db);
	conn->connection->context->config = config;
	return env->NewDirectByteBuffer(conn, 0);
}

jobject _duckdb_jdbc_create_db_ref(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection_ref(env, conn_ref_buf);
	auto db_ref = conn_ref->create_db_ref();
	return env->NewDirectByteBuffer(db_ref, 0);
}

void _duckdb_jdbc_destroy_db_ref(JNIEnv *env, jclass, jobject db_ref_buf) {
	if (nullptr == db_ref_buf) {
		return;
	}
	auto db_ref = (DBHolder *)env->GetDirectBufferAddress(db_ref_buf);
	if (db_ref) {
		delete db_ref;
	}
}

jstring _duckdb_jdbc_get_schema(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}

	auto entry = ClientData::Get(*conn_ref->context).catalog_search_path->GetDefault();

	return env->NewStringUTF(entry.schema.c_str());
}

static void set_catalog_search_path(JNIEnv *env, jobject conn_ref_buf, CatalogSearchEntry search_entry) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return;
	}

	conn_ref->context->RunFunctionInTransaction([&]() {
		ClientData::Get(*conn_ref->context).catalog_search_path->Set(search_entry, CatalogSetPathType::SET_SCHEMA);
	});
}

void _duckdb_jdbc_set_schema(JNIEnv *env, jclass, jobject conn_ref_buf, jstring schema) {
	set_catalog_search_path(env, conn_ref_buf, CatalogSearchEntry(INVALID_CATALOG, jstring_to_string(env, schema)));
}

void _duckdb_jdbc_set_catalog(JNIEnv *env, jclass, jobject conn_ref_buf, jstring catalog) {
	set_catalog_search_path(env, conn_ref_buf, CatalogSearchEntry(jstring_to_string(env, catalog), DEFAULT_SCHEMA));
}

jstring _duckdb_jdbc_get_catalog(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}

	auto entry = ClientData::Get(*conn_ref->context).catalog_search_path->GetDefault();
	if (entry.catalog == INVALID_CATALOG) {
		entry.catalog = DatabaseManager::GetDefaultDatabase(*conn_ref->context);
	}

	return env->NewStringUTF(entry.catalog.c_str());
}

void _duckdb_jdbc_set_auto_commit(JNIEnv *env, jclass, jobject conn_ref_buf, jboolean auto_commit) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return;
	}
	conn_ref->context->RunFunctionInTransaction([&]() { conn_ref->SetAutoCommit(auto_commit); });
}

jboolean _duckdb_jdbc_get_auto_commit(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return false;
	}
	return conn_ref->IsAutoCommit();
}

void _duckdb_jdbc_interrupt(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return;
	}
	conn_ref->Interrupt();
}

jobject _duckdb_jdbc_query_progress(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}
	duckdb_query_progress_type qpc = duckdb_query_progress(reinterpret_cast<duckdb_connection>(conn_ref));
	return env->NewObject(J_QueryProgress, J_QueryProgress_init, static_cast<jdouble>(qpc.percentage),
	                      uint64_to_jlong(qpc.rows_processed), uint64_to_jlong(qpc.total_rows_to_process));
}

void _duckdb_jdbc_disconnect(JNIEnv *env, jclass, jobject conn_ref_buf) {
	if (nullptr == conn_ref_buf) {
		return;
	}
	auto conn_ref = (ConnectionHolder *)env->GetDirectBufferAddress(conn_ref_buf);
	if (conn_ref) {
		delete conn_ref;
	}
}

#include "utf8proc_wrapper.hpp"

jobject _duckdb_jdbc_prepare(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray query_j) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}

	auto query = jbyteArray_to_string(env, query_j);

	auto statements = conn_ref->ExtractStatements(query.c_str());
	if (statements.empty()) {
		throw InvalidInputException("No statements to execute.");
	}

	// if there are multiple statements, we directly execute the statements besides the last one
	// we only return the result of the last statement to the user, unless one of the previous statements fails
	for (idx_t i = 0; i + 1 < statements.size(); i++) {
		auto res = conn_ref->Query(std::move(statements[i]));
		if (res->HasError()) {
			res->ThrowError();
		}
	}

	auto stmt_ref = make_uniq<StatementHolder>();
	stmt_ref->stmt = conn_ref->Prepare(std::move(statements.back()));
	if (stmt_ref->stmt->HasError()) {
		string error_msg = string(stmt_ref->stmt->GetError());
		stmt_ref->stmt = nullptr;
		ThrowJNI(env, error_msg.c_str());
		return nullptr;
	}
	return env->NewDirectByteBuffer(stmt_ref.release(), 0);
}

jobject _duckdb_jdbc_pending_query(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray query_j) {
	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}

	auto query = jbyteArray_to_string(env, query_j);

	auto statements = conn_ref->ExtractStatements(query.c_str());
	if (statements.empty()) {
		throw InvalidInputException("No statements to execute.");
	}

	// if there are multiple statements, we directly execute the statements besides the last one
	// we only return the result of the last statement to the user, unless one of the previous statements fails
	for (idx_t i = 0; i + 1 < statements.size(); i++) {
		auto res = conn_ref->Query(std::move(statements[i]));
		if (res->HasError()) {
			res->ThrowError();
		}
	}

	Value result;
	bool stream_results =
	    conn_ref->context->TryGetCurrentSetting("jdbc_stream_results", result) ? result.GetValue<bool>() : false;
	QueryParameters query_parameters;
	query_parameters.output_type =
	    stream_results ? QueryResultOutputType::ALLOW_STREAMING : QueryResultOutputType::FORCE_MATERIALIZED;

	auto pending_ref = make_uniq<PendingHolder>();
	pending_ref->pending = conn_ref->PendingQuery(std::move(statements.back()), query_parameters);

	return env->NewDirectByteBuffer(pending_ref.release(), 0);
}

jobject _duckdb_jdbc_execute(JNIEnv *env, jclass, jobject stmt_ref_buf, jobjectArray params) {
	auto stmt_ref = reinterpret_cast<StatementHolder *>(env->GetDirectBufferAddress(stmt_ref_buf));
	if (!stmt_ref) {
		throw InvalidInputException("Invalid statement");
	}

	auto res_ref = make_uniq<ResultHolder>();
	duckdb::vector<Value> duckdb_params;

	idx_t param_len = env->GetArrayLength(params);

	if (param_len != stmt_ref->stmt->named_param_map.size()) {
		throw InvalidInputException("Parameter count mismatch");
	}

	auto &context = stmt_ref->stmt->context;

	if (param_len > 0) {
		for (idx_t i = 0; i < param_len; i++) {
			auto param = env->GetObjectArrayElement(params, i);
			duckdb::Value val = to_duckdb_value(env, param, *context);
			duckdb_params.push_back(std::move(val));
		}
	}

	Value result;
	bool stream_results =
	    stmt_ref->stmt->context->TryGetCurrentSetting("jdbc_stream_results", result) ? result.GetValue<bool>() : false;

	res_ref->res = stmt_ref->stmt->Execute(duckdb_params, stream_results);
	if (res_ref->res->HasError()) {
		std::string error_msg = std::string(res_ref->res->GetError());
		duckdb::ExceptionType error_type = res_ref->res->GetErrorType();
		res_ref->res = nullptr;
		jclass exc_type = duckdb::ExceptionType::INTERRUPT == error_type ? J_SQLTimeoutException : J_SQLException;
		env->ThrowNew(exc_type, error_msg.c_str());
		return nullptr;
	}
	return env->NewDirectByteBuffer(res_ref.release(), 0);
}

jobject _duckdb_jdbc_execute_pending(JNIEnv *env, jclass, jobject pending_ref_buf) {
	auto pending_ref = reinterpret_cast<PendingHolder *>(env->GetDirectBufferAddress(pending_ref_buf));
	if (!pending_ref) {
		throw InvalidInputException("Invalid pending query");
	}

	auto res_ref = make_uniq<ResultHolder>();
	res_ref->res = pending_ref->pending->Execute();
	if (res_ref->res->HasError()) {
		std::string error_msg = std::string(res_ref->res->GetError());
		duckdb::ExceptionType error_type = res_ref->res->GetErrorType();
		res_ref->res = nullptr;
		jclass exc_type = duckdb::ExceptionType::INTERRUPT == error_type ? J_SQLTimeoutException : J_SQLException;
		env->ThrowNew(exc_type, error_msg.c_str());
		return nullptr;
	}
	return env->NewDirectByteBuffer(res_ref.release(), 0);
}

void _duckdb_jdbc_release(JNIEnv *env, jclass, jobject stmt_ref_buf) {
	if (nullptr == stmt_ref_buf) {
		return;
	}
	auto stmt_ref = reinterpret_cast<StatementHolder *>(env->GetDirectBufferAddress(stmt_ref_buf));
	if (stmt_ref) {
		delete stmt_ref;
	}
}

void _duckdb_jdbc_release_pending(JNIEnv *env, jclass, jobject pending_ref_buf) {
	if (nullptr == pending_ref_buf) {
		return;
	}
	auto pending_ref = reinterpret_cast<PendingHolder *>(env->GetDirectBufferAddress(pending_ref_buf));
	if (pending_ref) {
		delete pending_ref;
	}
}

void _duckdb_jdbc_free_result(JNIEnv *env, jclass, jobject res_ref_buf) {
	if (nullptr == res_ref_buf) {
		return;
	}
	auto res_ref = reinterpret_cast<ResultHolder *>(env->GetDirectBufferAddress(res_ref_buf));
	if (res_ref) {
		delete res_ref;
	}
}

static jobject build_meta(JNIEnv *env, size_t column_count, size_t n_param, const duckdb::vector<string> &names,
                          const duckdb::vector<LogicalType> &types, StatementProperties properties,
                          const duckdb::vector<LogicalType> &param_types) {
	auto name_array = env->NewObjectArray(column_count, J_String, nullptr);
	auto type_array = env->NewObjectArray(column_count, J_String, nullptr);
	auto type_detail_array = env->NewObjectArray(column_count, J_String, nullptr);

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		std::string col_name;
		if (types[col_idx].id() == LogicalTypeId::ENUM) {
			col_name = "ENUM";
		} else {
			col_name = types[col_idx].ToString();
		}

		env->SetObjectArrayElement(name_array, col_idx,
		                           decode_charbuffer_to_jstring(env, names[col_idx].c_str(), names[col_idx].length()));
		env->SetObjectArrayElement(type_array, col_idx, env->NewStringUTF(col_name.c_str()));
		env->SetObjectArrayElement(type_detail_array, col_idx,
		                           env->NewStringUTF(type_to_jduckdb_type(types[col_idx]).c_str()));
	}

	auto param_type_array = env->NewObjectArray(n_param, J_String, nullptr);
	auto param_type_detail_array = env->NewObjectArray(n_param, J_String, nullptr);

	for (idx_t param_idx = 0; param_idx < n_param; param_idx++) {
		std::string param_name;
		if (param_types[param_idx].id() == LogicalTypeId::ENUM) {
			param_name = "ENUM";
		} else {
			param_name = param_types[param_idx].ToString();
		}

		env->SetObjectArrayElement(param_type_array, param_idx, env->NewStringUTF(param_name.c_str()));
		env->SetObjectArrayElement(param_type_detail_array, param_idx,
		                           env->NewStringUTF(type_to_jduckdb_type(param_types[param_idx]).c_str()));
	}

	auto return_type = env->NewStringUTF(StatementReturnTypeToString(properties.return_type).c_str());

	return env->NewObject(J_DuckResultSetMeta, J_DuckResultSetMeta_init, n_param, column_count, name_array, type_array,
	                      type_detail_array, return_type, param_type_array, param_type_detail_array);
}

jobject _duckdb_jdbc_query_result_meta(JNIEnv *env, jclass, jobject res_ref_buf) {
	auto res_ref = (ResultHolder *)env->GetDirectBufferAddress(res_ref_buf);
	if (!res_ref || !res_ref->res || res_ref->res->HasError()) {
		throw InvalidInputException("Invalid result set");
	}
	auto &result = res_ref->res;

	auto n_param = 0; // no params now
	duckdb::vector<LogicalType> param_types(n_param);

	return build_meta(env, result->ColumnCount(), n_param, result->names, result->types, result->properties,
	                  param_types);
}

jobject _duckdb_jdbc_prepared_statement_meta(JNIEnv *env, jclass, jobject stmt_ref_buf) {

	auto stmt_ref = (StatementHolder *)env->GetDirectBufferAddress(stmt_ref_buf);
	if (!stmt_ref || !stmt_ref->stmt || stmt_ref->stmt->HasError()) {
		throw InvalidInputException("Invalid statement");
	}

	auto &stmt = stmt_ref->stmt;
	auto n_param = stmt->named_param_map.size();
	duckdb::vector<LogicalType> param_types(n_param);
	if (n_param > 0) {
		auto expected_parameter_types = stmt->GetExpectedParameterTypes();
		for (auto &it : stmt->named_param_map) {
			param_types[it.second - 1] = expected_parameter_types[it.first];
		}
	}

	return build_meta(env, stmt->ColumnCount(), n_param, stmt->GetNames(), stmt->GetTypes(),
	                  stmt->GetStatementProperties(), param_types);
}

jobject ProcessVector(JNIEnv *env, Connection *conn_ref, Vector &vec, idx_t row_count);

jobjectArray _duckdb_jdbc_fetch(JNIEnv *env, jclass, jobject res_ref_buf, jobject conn_ref_buf) {
	auto res_ref = (ResultHolder *)env->GetDirectBufferAddress(res_ref_buf);
	if (!res_ref || !res_ref->res || res_ref->res->HasError()) {
		throw InvalidInputException("Invalid result set");
	}

	auto conn_ref = get_connection(env, conn_ref_buf);
	if (conn_ref == nullptr) {
		return nullptr;
	}

	res_ref->chunk = res_ref->res->Fetch();
	if (!res_ref->chunk) {
		res_ref->chunk = make_uniq<DataChunk>();
	}
	auto row_count = res_ref->chunk->size();
	auto vec_array = (jobjectArray)env->NewObjectArray(res_ref->chunk->ColumnCount(), J_DuckVector, nullptr);

	for (idx_t col_idx = 0; col_idx < res_ref->chunk->ColumnCount(); col_idx++) {
		auto &vec = res_ref->chunk->data[col_idx];

		auto jvec = ProcessVector(env, conn_ref, vec, row_count);

		env->SetObjectArrayElement(vec_array, col_idx, jvec);
	}

	return vec_array;
}

jobjectArray _duckdb_jdbc_cast_result_to_strings(JNIEnv *env, jclass, jobject res_ref_buf, jobject conn_ref_buf,
                                                 jlong col_idx) {
	auto res_ref = reinterpret_cast<ResultHolder *>(env->GetDirectBufferAddress(res_ref_buf));
	if (!res_ref || !res_ref->res || res_ref->res->HasError()) {
		throw InvalidInputException("Invalid result set");
	}

	if (!res_ref->chunk) {
		return nullptr;
	}

	auto conn_ref = get_connection(env, conn_ref_buf);
	if (conn_ref == nullptr) {
		return nullptr;
	}

	auto row_count = res_ref->chunk->size();
	auto &complex_vec = res_ref->chunk->data[col_idx];
	Vector vec(LogicalType::VARCHAR);
	VectorOperations::Cast(*conn_ref->context, complex_vec, vec, row_count);

	jobjectArray string_data = env->NewObjectArray(row_count, J_String, nullptr);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (FlatVector::IsNull(vec, row_idx)) {
			continue;
		}
		auto d_str = (reinterpret_cast<string_t *>(FlatVector::GetData(vec)))[row_idx];
		auto j_str = decode_charbuffer_to_jstring(env, d_str.GetData(), d_str.GetSize());
		env->SetObjectArrayElement(string_data, row_idx, j_str);
	}

	return string_data;
}

jobject ProcessVector(JNIEnv *env, Connection *conn_ref, Vector &vec, idx_t row_count) {
	auto type_str = env->NewStringUTF(type_to_jduckdb_type(vec.GetType()).c_str());
	// construct nullmask
	auto null_array = env->NewBooleanArray(row_count);
	jboolean *null_unique_array = env->GetBooleanArrayElements(null_array, nullptr);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		null_unique_array[row_idx] = FlatVector::IsNull(vec, row_idx);
	}
	env->ReleaseBooleanArrayElements(null_array, null_unique_array, 0);

	auto jvec = env->NewObject(J_DuckVector, J_DuckVector_init, type_str, (int)row_count, null_array);

	jobject constlen_data = nullptr;
	jobjectArray varlen_data = nullptr;

	switch (vec.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(bool));
		break;
	case LogicalTypeId::TINYINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int8_t));
		break;
	case LogicalTypeId::SMALLINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int16_t));
		break;
	case LogicalTypeId::INTEGER:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int32_t));
		break;
	case LogicalTypeId::BIGINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int64_t));
		break;
	case LogicalTypeId::UTINYINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint8_t));
		break;
	case LogicalTypeId::USMALLINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint16_t));
		break;
	case LogicalTypeId::UINTEGER:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint32_t));
		break;
	case LogicalTypeId::UBIGINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint64_t));
		break;
	case LogicalTypeId::HUGEINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(hugeint_t));
		break;
	case LogicalTypeId::UHUGEINT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uhugeint_t));
		break;
	case LogicalTypeId::FLOAT:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(float));
		break;
	case LogicalTypeId::DECIMAL: {
		auto physical_type = vec.GetType().InternalType();

		switch (physical_type) {
		case PhysicalType::INT16:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int16_t));
			break;
		case PhysicalType::INT32:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int32_t));
			break;
		case PhysicalType::INT64:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int64_t));
			break;
		case PhysicalType::INT128:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(hugeint_t));
			break;
		default:
			throw InternalException("Unimplemented physical type for decimal");
		}
		break;
	}
	case LogicalTypeId::DOUBLE:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(double));
		break;
	case LogicalTypeId::DATE:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(date_t));
		break;
	case LogicalTypeId::TIME:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(dtime_t));
		break;
	case LogicalTypeId::TIME_NS:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(dtime_ns_t));
		break;
	case LogicalTypeId::TIME_TZ:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(dtime_tz_t));
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(timestamp_t));
		break;
	case LogicalTypeId::ENUM:
		varlen_data = env->NewObjectArray(row_count, J_String, nullptr);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (FlatVector::IsNull(vec, row_idx)) {
				continue;
			}
			auto d_str = vec.GetValue(row_idx).ToString();
			jstring j_str = env->NewStringUTF(d_str.c_str());
			env->SetObjectArrayElement(varlen_data, row_idx, j_str);
		}
		break;
	case LogicalTypeId::UNION:
	case LogicalTypeId::STRUCT: {
		varlen_data = env->NewObjectArray(row_count, J_DuckStruct, nullptr);

		auto &entries = StructVector::GetEntries(vec);
		auto columns = env->NewObjectArray(entries.size(), J_DuckVector, nullptr);
		auto names = env->NewObjectArray(entries.size(), J_String, nullptr);

		for (idx_t entry_i = 0; entry_i < entries.size(); entry_i++) {
			auto j_vec = ProcessVector(env, conn_ref, *entries[entry_i], row_count);
			env->SetObjectArrayElement(columns, entry_i, j_vec);
			env->SetObjectArrayElement(names, entry_i,
			                           env->NewStringUTF(StructType::GetChildName(vec.GetType(), entry_i).c_str()));
		}
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			env->SetObjectArrayElement(varlen_data, row_idx,
			                           env->NewObject(J_DuckStruct, J_DuckStruct_init, names, columns, row_idx,
			                                          env->NewStringUTF(vec.GetType().ToString().c_str())));
		}

		break;
	}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::GEOMETRY:
		varlen_data = env->NewObjectArray(row_count, J_ByteArray, nullptr);

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (FlatVector::IsNull(vec, row_idx)) {
				continue;
			}
			auto &d_str = ((string_t *)FlatVector::GetData(vec))[row_idx];

			auto j_arr = env->NewByteArray(d_str.GetSize());
			auto j_arr_el = env->GetByteArrayElements(j_arr, nullptr);
			memcpy((void *)j_arr_el, (void *)d_str.GetData(), d_str.GetSize());
			env->ReleaseByteArrayElements(j_arr, j_arr_el, 0);

			env->SetObjectArrayElement(varlen_data, row_idx, j_arr);
		}
		break;
	case LogicalTypeId::UUID:
		constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(hugeint_t));
		break;
	case LogicalTypeId::ARRAY: {
		varlen_data = env->NewObjectArray(row_count, J_DuckArray, nullptr);
		auto &array_vector = ArrayVector::GetEntry(vec);
		auto total_size = row_count * ArrayType::GetSize(vec.GetType());
		auto j_vec = ProcessVector(env, conn_ref, array_vector, total_size);

		auto limit = ArrayType::GetSize(vec.GetType());

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (FlatVector::IsNull(vec, row_idx)) {
				continue;
			}

			auto offset = row_idx * limit;

			auto j_obj = env->NewObject(J_DuckArray, J_DuckArray_init, j_vec, offset, limit);

			env->SetObjectArrayElement(varlen_data, row_idx, j_obj);
		}
		break;
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST: {
		varlen_data = env->NewObjectArray(row_count, J_DuckArray, nullptr);

		auto list_entries = FlatVector::GetData<list_entry_t>(vec);

		auto list_size = ListVector::GetListSize(vec);
		auto &list_vector = ListVector::GetEntry(vec);
		auto j_vec = ProcessVector(env, conn_ref, list_vector, list_size);

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (FlatVector::IsNull(vec, row_idx)) {
				continue;
			}

			auto offset = list_entries[row_idx].offset;
			auto limit = list_entries[row_idx].length;

			auto j_obj = env->NewObject(J_DuckArray, J_DuckArray_init, j_vec, offset, limit);

			env->SetObjectArrayElement(varlen_data, row_idx, j_obj);
		}
		break;
	}
	case LogicalTypeId::VARIANT: {
		RecursiveUnifiedVectorFormat format;
		Vector::RecursiveToUnifiedFormat(vec, 1, format);
		UnifiedVariantVectorData vector_data(format);
		varlen_data = env->NewObjectArray(row_count, J_Object, nullptr);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			auto variant_val = VariantUtils::ConvertVariantToValue(vector_data, row_idx, 0);
			if (variant_val.IsNull()) {
				continue;
			}
			Vector variant_vec(variant_val);
			variant_vec.Flatten(1);
			jobject variant_j_vec = ProcessVector(env, conn_ref, variant_vec, 1);
			env->CallVoidMethod(variant_j_vec, J_DuckVector_retainConstlenData);
			env->SetObjectArrayElement(varlen_data, row_idx, variant_j_vec);
		}
		break;
	}
	default: {
		Vector string_vec(LogicalType::VARCHAR);
		VectorOperations::Cast(*conn_ref->context, vec, string_vec, row_count);
		vec.ReferenceAndSetType(string_vec);
		// fall through on purpose
	}
	case LogicalTypeId::VARCHAR:
		varlen_data = env->NewObjectArray(row_count, J_String, nullptr);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (FlatVector::IsNull(vec, row_idx)) {
				continue;
			}
			auto d_str = ((string_t *)FlatVector::GetData(vec))[row_idx];
			auto j_str = decode_charbuffer_to_jstring(env, d_str.GetData(), d_str.GetSize());
			env->SetObjectArrayElement(varlen_data, row_idx, j_str);
		}
		break;
	}

	env->SetObjectField(jvec, J_DuckVector_constlen, constlen_data);
	env->SetObjectField(jvec, J_DuckVector_varlen, varlen_data);

	return jvec;
}

jint _duckdb_jdbc_fetch_size(JNIEnv *, jclass) {
	return STANDARD_VECTOR_SIZE;
}

jobject _duckdb_jdbc_create_appender(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray schema_name_j,
                                     jbyteArray table_name_j) {

	auto conn_ref = get_connection(env, conn_ref_buf);
	if (!conn_ref) {
		return nullptr;
	}
	auto schema_name = jbyteArray_to_string(env, schema_name_j);
	auto table_name = jbyteArray_to_string(env, table_name_j);
	auto appender = new Appender(*conn_ref, schema_name, table_name);
	return env->NewDirectByteBuffer(appender, 0);
}

static Appender *get_appender(JNIEnv *env, jobject appender_ref_buf) {
	auto appender_ref = (Appender *)env->GetDirectBufferAddress(appender_ref_buf);
	if (!appender_ref) {
		throw InvalidInputException("Invalid appender");
	}
	return appender_ref;
}

void _duckdb_jdbc_appender_begin_row(JNIEnv *env, jclass, jobject appender_ref_buf) {
	get_appender(env, appender_ref_buf)->BeginRow();
}

void _duckdb_jdbc_appender_end_row(JNIEnv *env, jclass, jobject appender_ref_buf) {
	get_appender(env, appender_ref_buf)->EndRow();
}

void _duckdb_jdbc_appender_flush(JNIEnv *env, jclass, jobject appender_ref_buf) {
	get_appender(env, appender_ref_buf)->Flush();
}

void _duckdb_jdbc_appender_close(JNIEnv *env, jclass, jobject appender_ref_buf) {
	auto appender = get_appender(env, appender_ref_buf);
	appender->Close();
	delete appender;
}

void _duckdb_jdbc_appender_append_boolean(JNIEnv *env, jclass, jobject appender_ref_buf, jboolean value) {
	get_appender(env, appender_ref_buf)->Append((bool)value);
}

void _duckdb_jdbc_appender_append_byte(JNIEnv *env, jclass, jobject appender_ref_buf, jbyte value) {
	get_appender(env, appender_ref_buf)->Append((int8_t)value);
}

void _duckdb_jdbc_appender_append_short(JNIEnv *env, jclass, jobject appender_ref_buf, jshort value) {
	get_appender(env, appender_ref_buf)->Append((int16_t)value);
}

void _duckdb_jdbc_appender_append_int(JNIEnv *env, jclass, jobject appender_ref_buf, jint value) {
	get_appender(env, appender_ref_buf)->Append((int32_t)value);
}

void _duckdb_jdbc_appender_append_long(JNIEnv *env, jclass, jobject appender_ref_buf, jlong value) {
	get_appender(env, appender_ref_buf)->Append((int64_t)value);
}

void _duckdb_jdbc_appender_append_float(JNIEnv *env, jclass, jobject appender_ref_buf, jfloat value) {
	get_appender(env, appender_ref_buf)->Append((float)value);
}

void _duckdb_jdbc_appender_append_double(JNIEnv *env, jclass, jobject appender_ref_buf, jdouble value) {
	get_appender(env, appender_ref_buf)->Append((double)value);
}

void _duckdb_jdbc_appender_append_timestamp(JNIEnv *env, jclass, jobject appender_ref_buf, jlong value) {
	timestamp_t timestamp = timestamp_t((int64_t)value);
	get_appender(env, appender_ref_buf)->Append(Value::TIMESTAMP(timestamp));
}

void _duckdb_jdbc_appender_append_decimal(JNIEnv *env, jclass, jobject appender_ref_buf, jobject value) {
	Value val = create_value_from_bigdecimal(env, value);
	get_appender(env, appender_ref_buf)->Append(val);
}

void _duckdb_jdbc_appender_append_string(JNIEnv *env, jclass, jobject appender_ref_buf, jbyteArray value) {
	if (env->IsSameObject(value, NULL)) {
		get_appender(env, appender_ref_buf)->Append<std::nullptr_t>(nullptr);
		return;
	}

	auto string_value = jbyteArray_to_string(env, value);
	get_appender(env, appender_ref_buf)->Append(string_value.c_str());
}

void _duckdb_jdbc_appender_append_bytes(JNIEnv *env, jclass, jobject appender_ref_buf, jbyteArray value) {
	if (env->IsSameObject(value, NULL)) {
		get_appender(env, appender_ref_buf)->Append<std::nullptr_t>(nullptr);
		return;
	}

	auto string_value = jbyteArray_to_string(env, value);
	get_appender(env, appender_ref_buf)->Append(Value::BLOB_RAW(string_value));
}

void _duckdb_jdbc_appender_append_null(JNIEnv *env, jclass, jobject appender_ref_buf) {
	get_appender(env, appender_ref_buf)->Append<std::nullptr_t>(nullptr);
}

jlong _duckdb_jdbc_arrow_stream(JNIEnv *env, jclass, jobject res_ref_buf, jlong batch_size) {
	if (!res_ref_buf) {
		throw InvalidInputException("Invalid result set");
	}
	auto res_ref = (ResultHolder *)env->GetDirectBufferAddress(res_ref_buf);
	if (!res_ref || !res_ref->res || res_ref->res->HasError()) {
		throw InvalidInputException("Invalid result set");
	}

	auto wrapper = new ResultArrowArrayStreamWrapper(std::move(res_ref->res), batch_size);
	return (jlong)&wrapper->stream;
}

class JavaArrowTabularStreamFactory {
public:
	JavaArrowTabularStreamFactory(ArrowArrayStream *stream_ptr_p) : stream_ptr(stream_ptr_p) {};

	static duckdb::unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory_p, ArrowStreamParameters &parameters) {

		auto factory = (JavaArrowTabularStreamFactory *)factory_p;
		if (!factory->stream_ptr->release) {
			throw InvalidInputException("This stream has been released");
		}
		auto res = make_uniq<ArrowArrayStreamWrapper>();
		res->arrow_array_stream = *factory->stream_ptr;
		factory->stream_ptr->release = nullptr;
		return res;
	}

	static void GetSchema(uintptr_t factory_p, ArrowSchemaWrapper &schema) {
		auto factory = (JavaArrowTabularStreamFactory *)factory_p;
		auto stream_ptr = factory->stream_ptr;
		if (!stream_ptr->release) {
			throw InvalidInputException("This stream has been released");
		}
		stream_ptr->get_schema(stream_ptr, &schema.arrow_schema);
		auto error = stream_ptr->get_last_error(stream_ptr);
		if (error != nullptr) {
			throw InvalidInputException(error);
		}
	}

	ArrowArrayStream *stream_ptr;
};

void _duckdb_jdbc_arrow_register(JNIEnv *env, jclass, jobject conn_ref_buf, jlong arrow_array_stream_pointer,
                                 jbyteArray name_j) {

	auto conn = get_connection(env, conn_ref_buf);
	if (conn == nullptr) {
		return;
	}
	auto name = jbyteArray_to_string(env, name_j);

	auto arrow_array_stream = (ArrowArrayStream *)(uintptr_t)arrow_array_stream_pointer;

	auto factory = new JavaArrowTabularStreamFactory(arrow_array_stream);
	duckdb::vector<Value> parameters;
	parameters.push_back(Value::POINTER((uintptr_t)factory));
	parameters.push_back(Value::POINTER((uintptr_t)JavaArrowTabularStreamFactory::Produce));
	parameters.push_back(Value::POINTER((uintptr_t)JavaArrowTabularStreamFactory::GetSchema));
	conn->TableFunction("arrow_scan_dumb", parameters)->CreateView(name, true, true);
}

void _duckdb_jdbc_create_extension_type(JNIEnv *env, jclass, jobject conn_buf) {

	auto connection = get_connection(env, conn_buf);
	if (!connection) {
		return;
	}

	auto &db_instance = DatabaseInstance::GetDatabase(*connection->context);
	ExtensionLoader loader(db_instance, "jdbc");
	child_list_t<LogicalType> children = {{"hello", LogicalType::VARCHAR}, {"world", LogicalType::VARCHAR}};
	auto hello_world_type = LogicalType::STRUCT(children);
	hello_world_type.SetAlias("test_type");
	loader.RegisterType("test_type", hello_world_type);

	LogicalType byte_test_type_type = LogicalTypeId::BLOB;
	byte_test_type_type.SetAlias("byte_test_type");
	loader.RegisterType("byte_test_type", byte_test_type_type);
}

static ProfilerPrintFormat GetProfilerPrintFormat(JNIEnv *env, jobject format) {
	if (env->IsSameObject(format, J_ProfilerPrintFormat_QUERY_TREE)) {
		return ProfilerPrintFormat::QUERY_TREE;
	}
	if (env->IsSameObject(format, J_ProfilerPrintFormat_JSON)) {
		return ProfilerPrintFormat::JSON;
	}
	if (env->IsSameObject(format, J_ProfilerPrintFormat_QUERY_TREE_OPTIMIZER)) {
		return ProfilerPrintFormat::QUERY_TREE_OPTIMIZER;
	}
	if (env->IsSameObject(format, J_ProfilerPrintFormat_NO_OUTPUT)) {
		return ProfilerPrintFormat::NO_OUTPUT;
	}
	if (env->IsSameObject(format, J_ProfilerPrintFormat_HTML)) {
		return ProfilerPrintFormat::HTML;
	}
	if (env->IsSameObject(format, J_ProfilerPrintFormat_GRAPHVIZ)) {
		return ProfilerPrintFormat::GRAPHVIZ;
	}
	throw InvalidInputException("Invalid profiling format");
}

jstring _duckdb_jdbc_get_profiling_information(JNIEnv *env, jclass, jobject conn_ref_buf, jobject j_format) {
	auto connection = get_connection(env, conn_ref_buf);
	if (!connection) {
		throw InvalidInputException("Invalid connection");
	}
	auto format = GetProfilerPrintFormat(env, j_format);
	auto profiling_info = connection->GetProfilingInformation(format);
	return env->NewStringUTF(profiling_info.c_str());
}
