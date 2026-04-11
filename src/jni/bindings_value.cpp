#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <cstdint>
#include <cstring>

static duckdb_value value_buf_to_value(JNIEnv *env, jobject value_buf) {
	if (value_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid value buffer");
		return nullptr;
	}

	auto value = reinterpret_cast<duckdb_value>(env->GetDirectBufferAddress(value_buf));
	if (value == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid value");
		return nullptr;
	}
	return value;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_is_null_value
 * Signature: (Ljava/nio/ByteBuffer;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1is_1null_1value(JNIEnv *env, jclass, jobject value) {
	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return false;
	}

	bool flag = duckdb_is_null_value(val);
	return static_cast<jboolean>(flag);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_value_type
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1value_1type(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return 0;
	}

	duckdb_logical_type lt = duckdb_get_value_type(val);
	duckdb_type dt = duckdb_get_type_id(lt);
	return static_cast<jint>(dt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_value
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1value(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_destroy_value(&val);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_bool
 * Signature: (Ljava/nio/ByteBuffer;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1bool(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return false;
	}
	bool flag = duckdb_get_bool(val);
	return static_cast<jboolean>(flag);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_int8
 * Signature: (Ljava/nio/ByteBuffer;)B
 */
JNIEXPORT jbyte JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1int8(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jbyte>(0);
	}
	int8_t num = duckdb_get_int8(val);
	return static_cast<jbyte>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_uint8
 * Signature: (Ljava/nio/ByteBuffer;)S
 */
JNIEXPORT jshort JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1uint8(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jshort>(0);
	}
	uint8_t num = duckdb_get_uint8(val);
	return static_cast<jshort>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_int16
 * Signature: (Ljava/nio/ByteBuffer;)S
 */
JNIEXPORT jshort JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1int16(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jshort>(0);
	}
	int16_t num = duckdb_get_int16(val);
	return static_cast<jshort>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_uint16
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1uint16(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(0);
	}
	uint16_t num = duckdb_get_uint16(val);
	return static_cast<jint>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_int32
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1int32(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(0);
	}
	int32_t num = duckdb_get_int32(val);
	return static_cast<jint>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_uint32
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1uint32(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}
	uint32_t num = duckdb_get_uint32(val);
	return static_cast<jlong>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_int64
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1int64(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}
	int64_t num = duckdb_get_int64(val);
	return static_cast<jlong>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_uint64
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1uint64(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}
	uint64_t num = duckdb_get_uint64(val);
	return static_cast<jlong>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_hugeint
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/math/BigInteger;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1hugeint(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_hugeint hi = duckdb_get_hugeint(val);
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
	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_uhugeint uhi = duckdb_get_uhugeint(val);
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
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_decimal
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/math/BigDecimal;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1decimal(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_decimal dec = duckdb_get_decimal(val);
	jlong lower = static_cast<jlong>(dec.value.lower);
	jlong upper = static_cast<jlong>(dec.value.upper);
	jint scale = static_cast<jint>(dec.scale);
	return env->CallStaticObjectMethod(J_HugeInt, J_HugeInt_toBigDecimal, lower, upper, scale);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_float
 * Signature: (Ljava/nio/ByteBuffer;)F
 */
JNIEXPORT jfloat JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1float(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jfloat>(0);
	}
	float num = duckdb_get_float(val);
	return static_cast<jfloat>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_double
 * Signature: (Ljava/nio/ByteBuffer;)D
 */
JNIEXPORT jdouble JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1double(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jdouble>(0);
	}
	double num = duckdb_get_double(val);
	return static_cast<jdouble>(num);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_date
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1date(JNIEnv *env, jclass, jobject value) {
	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(0);
	}
	duckdb_date dt = duckdb_get_date(val);
	return static_cast<jint>(dt.days);
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

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}

	duckdb_timestamp ts = duckdb_get_timestamp(val);
	return static_cast<jlong>(ts.micros);
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

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}

	duckdb_timestamp_s ts = duckdb_get_timestamp_s(val);
	return static_cast<jlong>(ts.seconds);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_timestamp_ms
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1timestamp_1ms(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}

	duckdb_timestamp_ms ts = duckdb_get_timestamp_ms(val);
	return static_cast<jlong>(ts.millis);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_timestamp_ns
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1timestamp_1ns(JNIEnv *env, jclass, jobject value) {

	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return static_cast<jlong>(0);
	}

	duckdb_timestamp_ns ts = duckdb_get_timestamp_ns(val);
	return static_cast<jlong>(ts.nanos);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_get_varchar
 * Signature: (Ljava/nio/ByteBuffer;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1get_1varchar(JNIEnv *env, jclass, jobject value) {
	duckdb_value val = value_buf_to_value(env, value);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	char *cstr = duckdb_get_varchar(val);
	idx_t len = static_cast<idx_t>(std::strlen(cstr));
	jbyteArray result = make_jbyteArray(env, cstr, len);

	duckdb_free(cstr);

	return result;
}
