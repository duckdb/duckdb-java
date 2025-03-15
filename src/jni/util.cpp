#include "util.hpp"

#include "refs.hpp"

void check_java_exception_and_rethrow(JNIEnv *env) {
	if (env->ExceptionCheck()) {
		jthrowable exc = env->ExceptionOccurred();
		env->ExceptionClear();
		jclass clazz = env->GetObjectClass(exc);
		jstring jmsg = reinterpret_cast<jstring>(env->CallObjectMethod(exc, J_Throwable_getMessage));
		if (env->ExceptionCheck()) {
			throw std::runtime_error("Error getting details of the Java exception");
		}
		std::string msg = jstring_to_string(env, jmsg);
		throw std::runtime_error(msg);
	}
}

std::string byte_array_to_string(JNIEnv *env, jbyteArray ba_j) {
	idx_t len = env->GetArrayLength(ba_j);
	std::string ret;
	ret.resize(len);

	jbyte *bytes = (jbyte *)env->GetByteArrayElements(ba_j, NULL);

	for (idx_t i = 0; i < len; i++) {
		ret[i] = bytes[i];
	}
	env->ReleaseByteArrayElements(ba_j, bytes, 0);

	return ret;
}

std::string jstring_to_string(JNIEnv *env, jstring string_j) {
	jbyteArray bytes = (jbyteArray)env->CallObjectMethod(string_j, J_String_getBytes, J_Charset_UTF8);
	return byte_array_to_string(env, bytes);
}

jobject decode_charbuffer_to_jstring(JNIEnv *env, const char *d_str, idx_t d_str_len) {
	auto bb = env->NewDirectByteBuffer((void *)d_str, d_str_len);
	auto j_cb = env->CallObjectMethod(J_Charset_UTF8, J_Charset_decode, bb);
	auto j_str = env->CallObjectMethod(j_cb, J_CharBuffer_toString);
	return j_str;
}

duckdb::Value create_value_from_bigdecimal(JNIEnv *env, jobject decimal) {
	jint precision = env->CallIntMethod(decimal, J_Decimal_precision);
	jint scale = env->CallIntMethod(decimal, J_Decimal_scale);

	// Java BigDecimal type can have scale that exceeds the precision
	// Which our DECIMAL type does not support (assert(width >= scale))
	if (scale > precision) {
		precision = scale;
	}

	// DECIMAL scale is unsigned, so negative values are not supported
	if (scale < 0) {
		throw duckdb::InvalidInputException("Converting from a BigDecimal with negative scale is not supported");
	}

	duckdb::Value val;

	if (precision <= 18) { // normal sizes -> avoid string processing
		jobject no_point_dec = env->CallObjectMethod(decimal, J_Decimal_scaleByPowTen, scale);
		jlong result = env->CallLongMethod(no_point_dec, J_Decimal_longValue);
		val = duckdb::Value::DECIMAL((int64_t)result, (uint8_t)precision, (uint8_t)scale);
	} else if (precision <= 38) { // larger than int64 -> get string and cast
		jobject str_val = env->CallObjectMethod(decimal, J_Decimal_toPlainString);
		auto *str_char = env->GetStringUTFChars((jstring)str_val, 0);
		val = duckdb::Value(str_char);
		val = val.DefaultCastAs(duckdb::LogicalType::DECIMAL(precision, scale));
		env->ReleaseStringUTFChars((jstring)str_val, str_char);
	}

	return val;
}
