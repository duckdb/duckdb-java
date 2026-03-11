extern "C" {
#include "duckdb.h"
}

#include "duckdb/common/assert.hpp"
#include "refs.hpp"
#include "udf_table_bind_conversion.hpp"
#include "util.hpp"

#include <cstdint>
#include <cstring>

static jobject table_bind_parameter_to_java_internal(JNIEnv *env, duckdb_value val, duckdb_logical_type logical_type,
                                                     std::string &error);

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

jobject table_bind_parameter_to_java(JNIEnv *env, duckdb_value val, duckdb_logical_type logical_type,
                                     std::vector<jobject> &local_refs, std::string &error) {
	auto param_obj = table_bind_parameter_to_java_internal(env, val, logical_type, error);
	if (param_obj != nullptr) {
		local_refs.push_back(param_obj);
	}
	return param_obj;
}
