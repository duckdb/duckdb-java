#include "bindings.hpp"
#include "functions.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "util.hpp"

static duckdb_scalar_function scalar_function_buf_to_scalar_function(JNIEnv *env, jobject scalar_function_buf) {

	if (scalar_function_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function buffer");
		return nullptr;
	}

	duckdb_scalar_function scalar_function =
	    reinterpret_cast<duckdb_scalar_function>(env->GetDirectBufferAddress(scalar_function_buf));
	if (scalar_function == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function");
		return nullptr;
	}

	return scalar_function;
}

static duckdb_function_info function_info_buf_to_function_info(JNIEnv *env, jobject function_info_buf) {
	if (function_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function info buffer");
		return nullptr;
	}

	auto function_info = reinterpret_cast<duckdb_function_info>(env->GetDirectBufferAddress(function_info_buf));
	if (function_info == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function info");
		return nullptr;
	}
	return function_info;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_scalar_function
 * Signature: ()Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1scalar_1function(JNIEnv *env, jclass) {
	return make_ptr_buf(env, duckdb_create_scalar_function());
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_scalar_function
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1scalar_1function(JNIEnv *env, jclass,
                                                                                        jobject scalar_function) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_destroy_scalar_function(&function);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_name
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1name(JNIEnv *env, jclass,
                                                                                          jobject scalar_function,
                                                                                          jbyteArray name) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	if (name == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function name");
		return;
	}
	auto function_name = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_set_name(function, function_name.c_str());
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_add_parameter
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1add_1parameter(JNIEnv *env, jclass,
                                                                                               jobject scalar_function,
                                                                                               jobject logical_type) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	auto type = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_add_parameter(function, type);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_return_type
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1return_1type(
    JNIEnv *env, jclass, jobject scalar_function, jobject logical_type) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	auto type = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_set_return_type(function, type);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_varargs
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1varargs(JNIEnv *env, jclass,
                                                                                             jobject scalar_function,
                                                                                             jobject logical_type) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	auto type = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_set_varargs(function, type);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_volatile
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1volatile(JNIEnv *env, jclass,
                                                                                              jobject scalar_function) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_set_volatile(function);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_special_handling
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1special_1handling(
    JNIEnv *env, jclass, jobject scalar_function) {
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_set_special_handling(function);
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
		return static_cast<jint>(DuckDBError);
	}
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(DuckDBError);
	}
	return static_cast<jint>(duckdb_register_scalar_function(conn, function));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_extra_info
 * Signature: (Ljava/nio/ByteBuffer;Ljava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1extra_1info(
    JNIEnv *env, jclass, jobject scalar_function, jobject callback) {

	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Specified callback must be not null");
		return;
	}

	auto sf = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}

	auto callback_holder = std::unique_ptr<GlobalRefHolder>(new GlobalRefHolder(env, callback));
	if (callback_holder->vm == nullptr) {
		env->ThrowNew(J_SQLException, "Unable to create a global reference to the specified scalar function callback");
		return;
	}

	duckdb_scalar_function_set_extra_info(sf, callback_holder.release(), GlobalRefHolder::destroy);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_function
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1function(JNIEnv *env, jclass,
                                                                                              jobject scalar_function) {
	auto sf = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_scalar_function_set_function(
	    sf, [](duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
		    auto callback_holder = reinterpret_cast<GlobalRefHolder *>(duckdb_scalar_function_get_extra_info(info));
		    AttachedJNIEnv attached = callback_holder->attach_current_thread();
		    if (attached.env == nullptr) {
			    duckdb_scalar_function_set_error(info, "Unable to attach JNI environment");
			    return;
		    }
		    jobject info_buf = make_ptr_buf(attached.env, info);
		    if (info_buf == nullptr) {
			    duckdb_scalar_function_set_error(info, "Unable to create function info buffer");
			    return;
		    }
		    LocalRefHolder info_buf_holder(attached.env, info_buf);
		    jobject input_buf = make_ptr_buf(attached.env, input);
		    if (input_buf == nullptr) {
			    duckdb_scalar_function_set_error(info, "Unable to create input buffer");
			    return;
		    }
		    LocalRefHolder input_buf_holder(attached.env, input_buf);
		    jobject output_buf = make_ptr_buf(attached.env, output);
		    if (output_buf == nullptr) {
			    duckdb_scalar_function_set_error(info, "Unable to create output buffer");
			    return;
		    }
		    LocalRefHolder output_buf_holder(attached.env, output_buf);

		    attached.env->CallVoidMethod(callback_holder->global_ref, J_DuckDBScalarFunctionWrapper_execute, info_buf,
		                                 input_buf, output_buf);
		    if (attached.env->ExceptionCheck()) {
			    duckdb_scalar_function_set_error(info, "Java callback system error");
		    }
	    });
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_scalar_function_set_error
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1scalar_1function_1set_1error(JNIEnv *env, jclass,
                                                                                           jobject function_info_buf,
                                                                                           jbyteArray error) {
	auto function_info = function_info_buf_to_function_info(env, function_info_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (error == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function error");
		return;
	}
	auto error_message = jbyteArray_to_string(env, error);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_scalar_function_set_error(function_info, error_message.c_str());
}
