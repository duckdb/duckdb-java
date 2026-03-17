extern "C" {
#include "duckdb.h"
}

#include "refs.hpp"
#include "udf_types.hpp"
#include "util.hpp"

#include <string>
#include <vector>

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

const char *UNSUPPORTED_SCALAR_UDF_TYPE_ERROR =
    "Supported scalar UDF types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR, DECIMAL, "
    "BLOB, DATE, TIME, TIME_NS, TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, UTINYINT, USMALLINT, "
    "UINTEGER, UBIGINT, HUGEINT, UHUGEINT, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, UUID";

const char *UNSUPPORTED_TABLE_FUNCTION_PARAMETER_TYPE_ERROR =
    "Supported table function parameter types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, "
    "VARCHAR, DECIMAL, BLOB, DATE, TIME, TIME_NS, TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, "
    "UTINYINT, USMALLINT, UINTEGER, UBIGINT, HUGEINT, UHUGEINT, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE, "
    "UUID";

static const UdfTypeSpec *find_udf_type_spec_by_name(const std::string &name) {
	for (const auto &spec : UDF_TYPE_SPECS) {
		if (name == spec.duckdb_column_type_name) {
			return &spec;
		}
	}
	return nullptr;
}

static bool is_supported_table_function_parameter_type(duckdb_type type) {
	auto spec = find_udf_type_spec(type);
	return spec != nullptr && spec->udf_vector_supported;
}

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

static bool table_any_column_type_from_java(JNIEnv *env, jobject duckdb_column_type_obj, duckdb_type &out_type) {
	auto name_j = reinterpret_cast<jstring>(env->CallObjectMethod(duckdb_column_type_obj, J_Enum_name));
	if (env->ExceptionCheck() || !name_j) {
		return false;
	}
	auto name = jstring_to_string(env, name_j);
	env->DeleteLocalRef(name_j);
	return duckdb_type_from_java_column_type_name(name, out_type);
}

duckdb_logical_type create_udf_logical_type(duckdb_type type) {
	if (type == DUCKDB_TYPE_DECIMAL) {
		return duckdb_create_decimal_type(DEFAULT_DECIMAL_WIDTH, DEFAULT_DECIMAL_SCALE);
	}
	return duckdb_create_logical_type(type);
}

const UdfTypeSpec *find_udf_type_spec(duckdb_type type) {
	for (const auto &spec : UDF_TYPE_SPECS) {
		if (spec.type == type) {
			return &spec;
		}
	}
	return nullptr;
}

bool capi_type_id_to_duckdb_type(jint type_id, duckdb_type &out_type) {
	auto requested_type = static_cast<duckdb_type>(type_id);
	auto spec = find_udf_type_spec(requested_type);
	if (!spec) {
		return false;
	}
	out_type = spec->type;
	return true;
}

bool is_supported_scalar_udf_type(duckdb_type type) {
	auto spec = find_udf_type_spec(type);
	return spec != nullptr && spec->scalar_udf_implemented;
}

bool table_column_type_from_java(JNIEnv *env, jobject duckdb_column_type_obj, duckdb_type &out_type) {
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

bool is_supported_table_bind_parameter_logical_type(duckdb_logical_type logical_type, std::string &error) {
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

duckdb_logical_type create_table_logical_type_from_java(JNIEnv *env, jobject logical_type_obj, std::string &error);

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

duckdb_logical_type create_table_logical_type_from_java(JNIEnv *env, jobject logical_type_obj, std::string &error) {
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
