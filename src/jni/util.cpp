#include "util.hpp"

#include "refs.hpp"

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>

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

std::string jbyteArray_to_string(JNIEnv *env, jbyteArray jbytes) {
	if (nullptr == jbytes) {
		return std::string();
	}
	size_t len = static_cast<size_t>(env->GetArrayLength(jbytes));
	if (len == 0) {
		return std::string();
	}
	std::string ret;
	ret.resize(len);

	jbyte *bytes = reinterpret_cast<jbyte *>(env->GetByteArrayElements(jbytes, nullptr));
	if (bytes == nullptr) {
		env->ThrowNew(J_SQLException, "GetByteArrayElements error");
		return std::string();
	}

	std::memcpy(&ret[0], bytes, len);
	env->ReleaseByteArrayElements(jbytes, bytes, 0);
	return ret;
}

std::string jstring_to_string(JNIEnv *env, jstring jstr) {
	jbyteArray bytes = reinterpret_cast<jbyteArray>(env->CallObjectMethod(jstr, J_String_getBytes, J_Charset_UTF8));
	check_java_exception_and_rethrow(env);
	std::string res = jbyteArray_to_string(env, bytes);
	env->DeleteLocalRef(bytes);
	return res;
}

jstring decode_charbuffer_to_jstring(JNIEnv *env, const char *cstr, idx_t len) {
	auto bytebuf = env->NewDirectByteBuffer(reinterpret_cast<void *>(const_cast<char *>(cstr)), len);
	check_java_exception_and_rethrow(env);
	auto jcharbuf = env->CallObjectMethod(J_Charset_UTF8, J_Charset_decode, bytebuf);
	check_java_exception_and_rethrow(env);
	env->DeleteLocalRef(bytebuf);
	auto jstr = env->CallObjectMethod(jcharbuf, J_CharBuffer_toString);
	check_java_exception_and_rethrow(env);
	env->DeleteLocalRef(jcharbuf);
	return reinterpret_cast<jstring>(jstr);
}

jlong uint64_to_jlong(uint64_t value) {
	if (value <= std::numeric_limits<int64_t>::max()) {
		return static_cast<jlong>(value);
	}
	return static_cast<jlong>(std::numeric_limits<int64_t>::max());
}

idx_t jlong_to_idx(JNIEnv *env, jlong value) {
	if (value < 0) {
		env->ThrowNew(J_SQLException, "Invalid index");
		return 0;
	}
	return static_cast<idx_t>(value);
}

void check_out_param(JNIEnv *env, jobjectArray out_param) {
	if (out_param == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null output parameter");
		return;
	}
	if (env->GetArrayLength(out_param) != 1) {
		env->ThrowNew(J_SQLException, "Invalid output parameter");
		return;
	}
}

void set_out_param(JNIEnv *env, jobjectArray out_param, jobject value) {
	if (out_param == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid null output parameter");
		return;
	}
	env->SetObjectArrayElement(out_param, 0, value);
}

jbyteArray_ptr make_jbyteArray_ptr(JNIEnv *env, jbyteArray jbytes) {
	if (jbytes == nullptr) {
		return jbyteArray_ptr(nullptr, [](char *) {});
	}
	jbyte *bytes = env->GetByteArrayElements(jbytes, nullptr);
	if (bytes == nullptr) {
		env->ThrowNew(J_SQLException, "GetByteArrayElements error");
		return jbyteArray_ptr(nullptr, [](char *) {});
	}

	char *chars = reinterpret_cast<char *>(bytes);

	return jbyteArray_ptr(chars, [env, jbytes](char *ptr) {
		jbyte *bytes = reinterpret_cast<jbyte *>(ptr);
		env->ReleaseByteArrayElements(jbytes, bytes, 0);
	});
}

jbyteArray make_jbyteArray(JNIEnv *env, const char *data, idx_t len) {
	if (data == nullptr) {
		return nullptr;
	}

	jbyteArray jbytes = env->NewByteArray(static_cast<jsize>(len));
	if (jbytes == nullptr) {
		env->ThrowNew(J_SQLException, "NewByteArray error");
		return nullptr;
	}

	jbyte *bytes = env->GetByteArrayElements(jbytes, nullptr);
	if (bytes == nullptr) {
		env->ThrowNew(J_SQLException, "GetByteArrayElements error");
		return nullptr;
	}

	std::memcpy(bytes, data, static_cast<size_t>(len));

	env->ReleaseByteArrayElements(jbytes, bytes, 0);

	return jbytes;
}

jobject make_ptr_buf(JNIEnv *env, void *ptr) {
	if (ptr != nullptr) {
		return env->NewDirectByteBuffer(ptr, 0);
	}

	return nullptr;
}

jobject make_data_buf(JNIEnv *env, void *data, idx_t len) {
	if (data != nullptr) {
		jobject buf = env->NewDirectByteBuffer(data, uint64_to_jlong(len));
		env->CallObjectMethod(buf, J_ByteBuffer_order, J_ByteOrder_NATIVE);
		if (env->ExceptionCheck()) {
			return nullptr;
		}
		return buf;
	}

	return nullptr;
}
