extern "C" {
#include "duckdb.h"
}

#include "duckdb.hpp"
#include "refs.hpp"
#include "types.hpp"
#include "udf_vector_accessors.hpp"
#include "util.hpp"

#include <cstdint>
#include <cstring>
#include <string>

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
		auto casted = decimal_value.DefaultCastAs(duckdb::LogicalType::DECIMAL(width, scale));
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
			auto huge_value = casted.GetValueUnsafe<duckdb::hugeint_t>();
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
