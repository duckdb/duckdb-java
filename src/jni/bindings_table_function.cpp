
#include "bindings.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "util.hpp"

static duckdb_table_function table_function_buf_to_table_function(JNIEnv *env, jobject table_function_buf) {
	if (table_function_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function buffer");
		return nullptr;
	}

	auto table_function = reinterpret_cast<duckdb_table_function>(env->GetDirectBufferAddress(table_function_buf));
	if (table_function == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid table function");
		return nullptr;
	}
	return table_function;
}

static duckdb_function_info function_info_buf_to_function_info(JNIEnv *env, jobject function_info_buf) {
	if (function_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid function info buffer");
		return nullptr;
	}

	auto function_info = reinterpret_cast<duckdb_function_info>(env->GetDirectBufferAddress(function_info_buf));
	if (function_info == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid function info");
		return nullptr;
	}
	return function_info;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_table_function
 * Signature: ()Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1table_1function(JNIEnv *env, jclass) {

	duckdb_table_function tf = duckdb_create_table_function();

	return make_ptr_buf(env, tf);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_destroy_table_function
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1destroy_1table_1function(JNIEnv *env, jclass,
                                                                                       jobject table_function) {

	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_destroy_table_function(&tf);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_name
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1name(JNIEnv *env, jclass,
                                                                                         jobject table_function,
                                                                                         jbyteArray name) {

	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}
	std::string name_str = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return;
	}
	if (name_str.empty()) {
		env->ThrowNew(J_SQLException, "Specified function name must be not empty");
		return;
	}

	duckdb_table_function_set_name(tf, name_str.c_str());
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_add_parameter
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1add_1parameter(JNIEnv *env, jclass,
                                                                                              jobject table_function,
                                                                                              jobject logical_type) {

	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}
	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_table_function_add_parameter(tf, lt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_add_named_parameter
 * Signature: (Ljava/nio/ByteBuffer;[BLjava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1add_1named_1parameter(
    JNIEnv *env, jclass, jobject table_function, jbyteArray name, jobject logical_type) {

	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}
	std::string name_str = jbyteArray_to_string(env, name);
	if (env->ExceptionCheck()) {
		return;
	}
	if (name_str.empty()) {
		env->ThrowNew(J_SQLException, "Specified parameter name must be not empty");
		return;
	}
	duckdb_logical_type lt = logical_type_buf_to_logical_type(env, logical_type);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_table_function_add_named_parameter(tf, name_str.c_str(), lt);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_extra_info
 * Signature: (Ljava/nio/ByteBuffer;Lorg/duckdb/DuckDBTableFunctionWrapper;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1extra_1info(JNIEnv *env, jclass,
                                                                                                jobject table_function,
                                                                                                jobject callback) {

	if (callback == nullptr) {
		env->ThrowNew(J_SQLException, "Specified callback must be not null");
		return;
	}

	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	auto callback_holder = std::unique_ptr<GlobalRefHolder>(new GlobalRefHolder(env, callback));
	if (callback_holder->vm == nullptr) {
		env->ThrowNew(J_SQLException, "Unable to create a global reference to the specified table function callback");
		return;
	}

	duckdb_table_function_set_extra_info(tf, callback_holder.release(), GlobalRefHolder::destroy);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_bind
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1bind(JNIEnv *env, jclass,
                                                                                         jobject table_function) {

	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_table_function_set_bind(tf, [](duckdb_bind_info info) {
		auto callback_holder = reinterpret_cast<GlobalRefHolder *>(duckdb_bind_get_extra_info(info));
		if (callback_holder == nullptr) {
			duckdb_bind_set_error(info, "Table function callback not found");
			return;
		}
		AttachedJNIEnv attached = callback_holder->attach_current_thread();
		if (attached.env == nullptr) {
			duckdb_bind_set_error(info, "Unable to attach JNI environment");
			return;
		}
		jobject info_buf = make_ptr_buf(attached.env, info);
		if (info_buf == nullptr) {
			duckdb_bind_set_error(info, "Unable to create bind info buffer");
			return;
		}
		LocalRefHolder info_buf_holder(attached.env, info_buf);

		attached.env->CallVoidMethod(callback_holder->global_ref, J_DuckDBTableFunctionWrapper_executeBind, info_buf);
		if (attached.env->ExceptionCheck()) {
			duckdb_bind_set_error(info, "Java callback system error");
		}
	});
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_init
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1init(JNIEnv *env, jclass,
                                                                                         jobject table_function) {

	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_table_function_set_init(tf, [](duckdb_init_info info) {
		auto callback_holder = reinterpret_cast<GlobalRefHolder *>(duckdb_init_get_extra_info(info));
		if (callback_holder == nullptr) {
			duckdb_init_set_error(info, "Table function callback not found");
			return;
		}
		AttachedJNIEnv attached = callback_holder->attach_current_thread();
		if (attached.env == nullptr) {
			duckdb_init_set_error(info, "Unable to attach JNI environment");
			return;
		}
		jobject info_buf = make_ptr_buf(attached.env, info);
		if (info_buf == nullptr) {
			duckdb_init_set_error(info, "Unable to create global init info buffer");
			return;
		}
		LocalRefHolder info_buf_holder(attached.env, info_buf);

		attached.env->CallVoidMethod(callback_holder->global_ref, J_DuckDBTableFunctionWrapper_executeGlobalInit,
		                             info_buf);
		if (attached.env->ExceptionCheck()) {
			duckdb_init_set_error(info, "Java callback system error");
		}
	});
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_local_init
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL
Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1local_1init(JNIEnv *env, jclass, jobject table_function) {

	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_table_function_set_local_init(tf, [](duckdb_init_info info) {
		auto callback_holder = reinterpret_cast<GlobalRefHolder *>(duckdb_init_get_extra_info(info));
		if (callback_holder == nullptr) {
			duckdb_init_set_error(info, "Table function callback not found");
			return;
		}
		AttachedJNIEnv attached = callback_holder->attach_current_thread();
		if (attached.env == nullptr) {
			duckdb_init_set_error(info, "Unable to attach JNI environment");
			return;
		}
		jobject info_buf = make_ptr_buf(attached.env, info);
		if (info_buf == nullptr) {
			duckdb_init_set_error(info, "Unable to create local init info buffer");
			return;
		}
		LocalRefHolder info_buf_holder(attached.env, info_buf);

		attached.env->CallVoidMethod(callback_holder->global_ref, J_DuckDBTableFunctionWrapper_executeLocalInit,
		                             info_buf);
		if (attached.env->ExceptionCheck()) {
			duckdb_init_set_error(info, "Java callback system error");
		}
	});
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_set_function
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1set_1function(JNIEnv *env, jclass,
                                                                                             jobject table_function) {

	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_table_function_set_function(tf, [](duckdb_function_info info, duckdb_data_chunk output) {
		auto callback_holder = reinterpret_cast<GlobalRefHolder *>(duckdb_function_get_extra_info(info));
		if (callback_holder == nullptr) {
			duckdb_function_set_error(info, "Table function callback not found");
			return;
		}
		AttachedJNIEnv attached = callback_holder->attach_current_thread();
		if (attached.env == nullptr) {
			duckdb_function_set_error(info, "Unable to attach JNI environment");
			return;
		}
		jobject info_buf = make_ptr_buf(attached.env, info);
		if (info_buf == nullptr) {
			duckdb_function_set_error(info, "Unable to create function info buffer");
			return;
		}
		LocalRefHolder info_buf_holder(attached.env, info_buf);
		jobject output_buf = make_ptr_buf(attached.env, output);
		if (output_buf == nullptr) {
			duckdb_function_set_error(info, "Unable to create output buffer");
			return;
		}
		LocalRefHolder output_buf_holder(attached.env, output_buf);

		attached.env->CallVoidMethod(callback_holder->global_ref, J_DuckDBTableFunctionWrapper_executeFunction,
		                             info_buf, output_buf);
		if (attached.env->ExceptionCheck()) {
			duckdb_function_set_error(info, "Java callback system error");
		}
	});
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_table_function_supports_projection_pushdown
 * Signature: (Ljava/nio/ByteBuffer;Z)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1table_1function_1supports_1projection_1pushdown(
    JNIEnv *env, jclass, jobject table_function, jboolean pushdown) {

	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return;
	}

	bool pushdown_flag = !!pushdown;
	duckdb_table_function_supports_projection_pushdown(tf, pushdown_flag);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_register_table_function
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1register_1table_1function(JNIEnv *env, jclass,
                                                                                        jobject connection,
                                                                                        jobject table_function) {

	auto conn = conn_ref_buf_to_conn(env, connection);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(DuckDBError);
	}
	duckdb_table_function tf = table_function_buf_to_table_function(env, table_function);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(DuckDBError);
	}

	duckdb_state state = duckdb_register_table_function(conn, tf);

	return static_cast<jint>(state);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_function_get_bind_data
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1function_1get_1bind_1data(JNIEnv *env, jclass,
                                                                                           jobject function_info) {

	duckdb_function_info fi = function_info_buf_to_function_info(env, function_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	void *bind_data = duckdb_function_get_bind_data(fi);
	if (bind_data == nullptr) {
		return nullptr;
	}

	GlobalRefHolder *holder = reinterpret_cast<GlobalRefHolder *>(bind_data);
	return holder->global_ref;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_function_get_init_data
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1function_1get_1init_1data(JNIEnv *env, jclass,
                                                                                           jobject function_info) {

	duckdb_function_info fi = function_info_buf_to_function_info(env, function_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	void *global_init_data = duckdb_function_get_init_data(fi);
	if (global_init_data == nullptr) {
		return nullptr;
	}

	GlobalRefHolder *holder = reinterpret_cast<GlobalRefHolder *>(global_init_data);
	return holder->global_ref;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_function_get_local_init_data
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL
Java_org_duckdb_DuckDBBindings_duckdb_1function_1get_1local_1init_1data(JNIEnv *env, jclass, jobject function_info) {

	duckdb_function_info fi = function_info_buf_to_function_info(env, function_info);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	void *local_init_data = duckdb_function_get_local_init_data(fi);
	if (local_init_data == nullptr) {
		return nullptr;
	}

	GlobalRefHolder *holder = reinterpret_cast<GlobalRefHolder *>(local_init_data);
	return holder->global_ref;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_function_set_error
 * Signature: (Ljava/nio/ByteBuffer;[B)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1function_1set_1error(JNIEnv *env, jclass,
                                                                                   jobject function_info,
                                                                                   jbyteArray error) {

	duckdb_function_info fi = function_info_buf_to_function_info(env, function_info);
	if (env->ExceptionCheck()) {
		return;
	}

	std::string error_str = jbyteArray_to_string(env, error);
	if (env->ExceptionCheck()) {
		return;
	}

	duckdb_function_set_error(fi, error_str.c_str());
}
