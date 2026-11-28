#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>

/*
 * Values are passed to Java as owned C API v2 handles.
 */
static duckdb_v2_value_handle value_buf_to_value(JNIEnv *env, jobject value_buf) {
	if (value_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid value buffer");
		return nullptr;
	}

	auto value = reinterpret_cast<duckdb_v2_value_handle>(env->GetDirectBufferAddress(value_buf));
	if (value == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid value");
		return nullptr;
	}
	return value;
}

/*
 * Same as in C API v1, the value is converted to the requested type and a "NULL value" of this type
 * (see duckdb::NullValue) is returned if the conversion fails.
 */
template <class T, DUCKDB_V2_ERROR (*GET)(duckdb_v2_value_handle, T *, duckdb_v2_error_info_handle *)>
static T get_value(duckdb_v2_value_handle val, T null_value) {
	T res;
	if (GET(val, &res, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return null_value;
	}
	return res;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_is_null_value
 * Signature: (Ljava/nio/ByteBuffer;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1is_1null_1value(JNIEnv *env, jclass, jobject value) {
	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return false;
	}

	bool flag = false;
	duckdb_v2_value_is_null(val, &flag, nullptr);
	return static_cast<jboolean>(flag);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_value_type
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1value_1type(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return 0;
	}

	v2_logical_type lt;
	v2_error err;
	if (duckdb_v2_value_get_logical_type(val, lt.out(), err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return -1;
	}

	return static_cast<jint>(logical_type_to_capi(lt.get()));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_value
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1value(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_v2_value_destroy(&val);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_bool
 * Signature: (Ljava/nio/ByteBuffer;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1bool(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return false;
	}
	bool flag = get_value<bool, duckdb_v2_value_get_bool>(val, false);
	return static_cast<jboolean>(flag);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_int8
 * Signature: (Ljava/nio/ByteBuffer;)B
 */
JNIEXPORT jbyte JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1int8(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jbyte>(0);
	}
	int8_t num = get_value<int8_t, duckdb_v2_value_get_tinyint>(val, std::numeric_limits<int8_t>::min());
	return static_cast<jbyte>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_uint8
 * Signature: (Ljava/nio/ByteBuffer;)S
 */
JNIEXPORT jshort JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1uint8(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jshort>(0);
	}
	uint8_t num = get_value<uint8_t, duckdb_v2_value_get_utinyint>(val, 0);
	return static_cast<jshort>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_int16
 * Signature: (Ljava/nio/ByteBuffer;)S
 */
JNIEXPORT jshort JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1int16(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jshort>(0);
	}
	int16_t num = get_value<int16_t, duckdb_v2_value_get_smallint>(val, std::numeric_limits<int16_t>::min());
	return static_cast<jshort>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_uint16
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1uint16(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(0);
	}
	uint16_t num = get_value<uint16_t, duckdb_v2_value_get_usmallint>(val, 0);
	return static_cast<jint>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_int32
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1int32(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(0);
	}
	int32_t num = get_value<int32_t, duckdb_v2_value_get_int>(val, std::numeric_limits<int32_t>::min());
	return static_cast<jint>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_uint32
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1uint32(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}
	uint32_t num = get_value<uint32_t, duckdb_v2_value_get_uint>(val, 0);
	return static_cast<jlong>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_int64
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1int64(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}
	int64_t num = get_value<int64_t, duckdb_v2_value_get_bigint>(val, std::numeric_limits<int64_t>::min());
	return static_cast<jlong>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_uint64
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1uint64(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}
	uint64_t num = get_value<uint64_t, duckdb_v2_value_get_ubigint>(val, 0);
	return static_cast<jlong>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_hugeint
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/math/BigInteger;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1hugeint(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_hugeint_t hi = get_value<duckdb_v2_hugeint_t, duckdb_v2_value_get_hugeint>(
	    val, duckdb_v2_hugeint_t {0, std::numeric_limits<int64_t>::min()});
	jlong lower = static_cast<jlong>(hi.lower);
	jlong upper = static_cast<jlong>(hi.upper);
	return env->CallStaticObjectMethod(J_HugeInt, J_HugeInt_toBigInteger, lower, upper);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_uhugeint
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/math/BigInteger;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1uhugeint(JNIEnv *env, jclass, jobject value) {
	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_uhugeint_t uhi =
	    get_value<duckdb_v2_uhugeint_t, duckdb_v2_value_get_uhugeint>(val, duckdb_v2_uhugeint_t {0, 0});
	jlong lower = static_cast<jlong>(uhi.lower);
	jlong upper = static_cast<jlong>(uhi.upper);
	return env->CallStaticObjectMethod(J_HugeInt, J_HugeInt_toBigInteger, lower, upper);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_bignum
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/math/BigInteger;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1bignum(JNIEnv *env, jclass, jobject value) {
	(void)env;
	(void)value;
	return nullptr;
}

/*
 * Reads a DECIMAL that is stored as a BIGNUM blob (wider than 38 digits), returns nullptr for other values.
 */
static jobject get_wide_decimal(JNIEnv *env, duckdb_v2_value_handle val) {
	v2_logical_type lt;
	if (duckdb_v2_value_get_logical_type(val, lt.out(), nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	if (logical_type_to_capi(lt.get()) != capi_type::DECIMAL) {
		return nullptr;
	}
	const char *name_ptr = nullptr;
	idx_t name_length = 0;
	v2_value scale_value;
	uint32_t scale = 0;
	if (duckdb_v2_logical_type_get_param(lt.get(), 1, &name_ptr, &name_length, scale_value.out(), nullptr) !=
	        DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_value_get_uint(scale_value.get(), &scale, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	const char *blob_ptr = nullptr;
	idx_t blob_length = 0;
	if (duckdb_v2_value_get_blob(val, &blob_ptr, &blob_length, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return nullptr;
	}
	jbyteArray blob = make_jbyteArray(env, blob_ptr, blob_length);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	return env->CallStaticObjectMethod(J_Bignum, J_Bignum_toBigDecimal, blob, static_cast<jint>(scale));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_decimal
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/math/BigDecimal;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1decimal(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	// same as in C API v1, zero is returned for non-decimal and NULL values
	duckdb_v2_hugeint_t dec_value = {0, 0};
	uint32_t dec_width = 0;
	uint32_t dec_scale = 0;
	if (duckdb_v2_value_get_decimal(val, &dec_value, &dec_width, &dec_scale, nullptr) != DUCKDB_V2_ERROR_NONE) {
		// a decimal that is wider than 255 digits, or whose unscaled value does not fit in 128 bits, is read
		// exactly from its BIGNUM storage
		jobject wide = get_wide_decimal(env, val);
		if (wide != nullptr || env->ExceptionCheck()) {
			return wide;
		}
		dec_value = {0, 0};
		dec_scale = 0;
	}
	jlong lower = static_cast<jlong>(dec_value.lower);
	jlong upper = static_cast<jlong>(dec_value.upper);
	jint scale = static_cast<jint>(dec_scale);
	return env->CallStaticObjectMethod(J_HugeInt, J_HugeInt_toBigDecimal, lower, upper, scale);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_float
 * Signature: (Ljava/nio/ByteBuffer;)F
 */
JNIEXPORT jfloat JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1float(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jfloat>(0);
	}
	float num = get_value<float, duckdb_v2_value_get_float>(val, NAN);
	return static_cast<jfloat>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_double
 * Signature: (Ljava/nio/ByteBuffer;)D
 */
JNIEXPORT jdouble JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1double(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jdouble>(0);
	}
	double num = get_value<double, duckdb_v2_value_get_double>(val, NAN);
	return static_cast<jdouble>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_date
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1date(JNIEnv *env, jclass, jobject value) {
	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(0);
	}
	int32_t days = get_value<int32_t, duckdb_v2_value_get_date>(val, 0);
	return static_cast<jint>(days);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_time
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1time(JNIEnv *env, jclass, jobject value) {
	(void)env;
	(void)value;
	return 0;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_time_ns
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1time_1ns(JNIEnv *env, jclass, jobject value) {
	(void)env;
	(void)value;
	return 0;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_time_tz
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1time_1tz(JNIEnv *env, jclass, jobject value) {
	(void)env;
	(void)value;
	return 0;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_timestamp
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1timestamp(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}

	int64_t micros = get_value<int64_t, duckdb_v2_value_get_timestamp>(val, 0);
	return static_cast<jlong>(micros);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_timestamp_tz
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1timestamp_1tz(JNIEnv *env, jclass, jobject value) {
	(void)env;
	(void)value;
	return 0;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_timestamp_s
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1timestamp_1s(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}

	int64_t seconds = get_value<int64_t, duckdb_v2_value_get_timestamp_sec>(val, 0);
	return static_cast<jlong>(seconds);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_timestamp_ms
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1timestamp_1ms(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}

	int64_t millis = get_value<int64_t, duckdb_v2_value_get_timestamp_ms>(val, 0);
	return static_cast<jlong>(millis);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_timestamp_ns
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1timestamp_1ns(JNIEnv *env, jclass, jobject value) {

	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}

	int64_t nanos = get_value<int64_t, duckdb_v2_value_get_timestamp_ns>(val, 0);
	return static_cast<jlong>(nanos);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_varchar
 * Signature: (Ljava/nio/ByteBuffer;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1varchar(JNIEnv *env, jclass, jobject value) {
	auto val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	// same as in C API v1, the value is cast to VARCHAR
	v2_value varchar_val;
	duckdb_v2_value_handle str_val = val;
	v2_logical_type value_type;
	DUCKDB_V2_LOGICAL_TYPE_ID type_id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
	if (duckdb_v2_value_get_logical_type(val, value_type.out(), nullptr) == DUCKDB_V2_ERROR_NONE) {
		duckdb_v2_logical_type_get_id(value_type.get(), &type_id, nullptr);
	}
	v2_error err;
	if (type_id != DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR) {
		auto factory = default_factory();
		v2_logical_type varchar_type;
		if (duckdb_v2_factory_create_type_from_id(factory, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR, nullptr, nullptr, nullptr,
		                                          0, varchar_type.out(), err.out()) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_value_cast(factory, val, varchar_type.get(), varchar_val.out(), err.out()) !=
		        DUCKDB_V2_ERROR_NONE) {
			throw_sql_exception(env, err);
			return nullptr;
		}
		str_val = varchar_val.get();
	}
	const char *str = nullptr;
	idx_t str_len = 0;
	if (duckdb_v2_value_get_varchar(str_val, &str, &str_len, err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return nullptr;
	}
	// same as in C API v1, the string is returned up to the first NUL character
	std::string cstr(str, str_len);
	idx_t len = static_cast<idx_t>(std::strlen(cstr.c_str()));
	return make_jbyteArray(env, cstr.c_str(), len);
}
