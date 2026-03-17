#include "bindings.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "udf_registration.hpp"
#include "util.hpp"

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

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_scalar_function
 * Signature: ()Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1scalar_1function(JNIEnv *env, jclass) {
	auto scalar_function = duckdb_create_scalar_function();
	return make_ptr_buf(env, scalar_function);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_scalar_function
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1scalar_1function(JNIEnv *env, jclass,
                                                                                        jobject scalar_function) {
	auto scalar_function_ptr = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_destroy_scalar_function(&scalar_function_ptr);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_name
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1name(JNIEnv *env, jclass,
                                                                                          jobject scalar_function,
                                                                                          jbyteArray name) {
	auto scalar_function_ptr = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}

	auto name_string = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_scalar_function_set_name(scalar_function_ptr, name_string.c_str());
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_add_parameter
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1add_1parameter(JNIEnv *env, jclass,
                                                                                               jobject scalar_function,
                                                                                               jobject logical_type) {
	auto scalar_function_ptr = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	auto logical_type_ptr = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_scalar_function_add_parameter(scalar_function_ptr, logical_type_ptr);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_return_type
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1return_1type(
    JNIEnv *env, jclass, jobject scalar_function, jobject logical_type) {
	auto scalar_function_ptr = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	auto logical_type_ptr = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_scalar_function_set_return_type(scalar_function_ptr, logical_type_ptr);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_volatile
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1volatile(JNIEnv *env, jclass,
                                                                                              jobject scalar_function) {
	auto scalar_function_ptr = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_scalar_function_set_volatile(scalar_function_ptr);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_special_handling
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1special_1handling(
    JNIEnv *env, jclass, jobject scalar_function) {
	auto scalar_function_ptr = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_scalar_function_set_special_handling(scalar_function_ptr);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_register_scalar_function
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1register_1scalar_1function(JNIEnv *env, jclass,
                                                                                         jobject connection,
                                                                                         jobject scalar_function) {
	auto conn = conn_ref_buf_to_conn(env, connection);
	if (env->ExceptionCheck()) {
		return -1;
	}
	auto scalar_function_ptr = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return -1;
	}

	auto state = duckdb_register_scalar_function(conn, scalar_function_ptr);
	return static_cast<jint>(state);
}

extern "C" JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1register_1scalar_1function_1java(
    JNIEnv *env, jclass, jobject connection, jbyteArray name, jobject callback, jobjectArray argument_logical_types,
    jobject return_logical_type, jboolean null_special_handling, jboolean return_null_on_exception,
    jboolean deterministic, jboolean var_args) {
	try {
		_duckdb_jdbc_register_scalar_udf(env, nullptr, connection, name, callback, argument_logical_types,
		                                 return_logical_type, null_special_handling, return_null_on_exception,
		                                 deterministic, var_args);
	} catch (const std::exception &e) {
		duckdb::ErrorData error(e);
		ThrowJNI(env, error.Message().c_str());
	}
}

extern "C" JNIEXPORT void JNICALL
Java_org_duckdb_DuckDBBindings_duckdb_1register_1scalar_1function_1java_1with_1function(
    JNIEnv *env, jclass, jobject connection, jobject scalar_function, jobject callback,
    jobjectArray argument_logical_types, jobject return_logical_type, jboolean return_null_on_exception,
    jboolean var_args) {
	try {
		_duckdb_jdbc_register_scalar_udf_on_function(env, nullptr, connection, scalar_function, callback,
		                                             argument_logical_types, return_logical_type,
		                                             return_null_on_exception, var_args);
	} catch (const std::exception &e) {
		duckdb::ErrorData error(e);
		ThrowJNI(env, error.Message().c_str());
	}
}
