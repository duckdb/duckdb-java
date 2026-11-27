#include "duckdb.hpp"
#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "functions.hpp"
#include "refs.hpp"
#include "util.hpp"

using namespace duckdb;
using namespace std;

static jint JNI_VERSION = JNI_VERSION_1_6;

void ThrowJNI(JNIEnv *env, const char *message) {
	D_ASSERT(J_SQLException);
	env->ThrowNew(J_SQLException, message);
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
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

static ProfilerPrintFormat GetProfilerPrintFormat(JNIEnv *env, jobject format) {
	jobject jname = env->CallObjectMethod(format, J_ProfilerPrintFormat_getName);
	check_java_exception_and_rethrow(env);
	const std::string name = jstring_to_string(env, static_cast<jstring>(jname));
	return ProfilerPrintFormat(name);
}

// The C API v2 has no call for the profiling information of a connection, so this is the only entry point that
// uses the engine: the C API v2 connection handle is the engine connection
jstring _duckdb_jdbc_get_profiling_information(JNIEnv *env, jclass, jobject conn_ref_buf, jobject j_format) {
	if (conn_ref_buf == nullptr) {
		throw InvalidInputException("Invalid connection");
	}
	auto handle = reinterpret_cast<duckdb_v2_connection_handle>(env->GetDirectBufferAddress(conn_ref_buf));
	if (!handle) {
		throw InvalidInputException("Invalid connection");
	}
	auto connection = duckdb::capiv2::Convert(handle);
	auto format = GetProfilerPrintFormat(env, j_format);
	auto profiling_info = connection->GetProfilingInformation(format);
	return env->NewStringUTF(profiling_info.c_str());
}
