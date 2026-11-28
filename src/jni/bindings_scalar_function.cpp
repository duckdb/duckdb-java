#include "bindings.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <memory>
#include <string>
#include <vector>

/*
 * The scalar function as it is configured from Java.
 *
 * In C API v1 a scalar function is created without a connection, the C API v2 requires the function to be
 * created with a factory of the database it is going to be registered in. So the configuration is collected
 * here and the C API v2 function is created when the function is registered.
 */
struct ScalarFunctionSpec {
	std::string name;
	std::vector<spec_logical_type> parameters;
	spec_logical_type return_type;
	spec_logical_type varargs;
	bool is_volatile = false;
	bool special_handling = false;
	bool has_function = false;
	std::unique_ptr<GlobalRefHolder> callback;
};

/*
 * User data of the registered C API v2 function, the same as "extra info" in C API v1.
 */
struct ScalarFunctionUserData {
	std::unique_ptr<GlobalRefHolder> callback;
};

/*
 * Per-thread state of the function, holds the data chunk, that references the arguments of the function and
 * is passed to Java as an input chunk.
 */
struct ScalarFunctionExecState {
	v2_data_chunk input;
};

/*
 * Function info that is passed to Java during the function call, it is used to report the error.
 */
struct ScalarFunctionInfoHolder {
	bool has_error = false;
	std::string error;
};

static ScalarFunctionSpec *scalar_function_buf_to_scalar_function(JNIEnv *env, jobject scalar_function_buf) {

	if (scalar_function_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function buffer");
		return nullptr;
	}

	auto scalar_function = reinterpret_cast<ScalarFunctionSpec *>(env->GetDirectBufferAddress(scalar_function_buf));
	if (scalar_function == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function");
		return nullptr;
	}

	return scalar_function;
}

static ScalarFunctionInfoHolder *function_info_buf_to_function_info(JNIEnv *env, jobject function_info_buf) {
	if (function_info_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function info buffer");
		return nullptr;
	}

	auto function_info = reinterpret_cast<ScalarFunctionInfoHolder *>(env->GetDirectBufferAddress(function_info_buf));
	if (function_info == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid scalar function info");
		return nullptr;
	}
	return function_info;
}

template <class T>
static void delete_opaque(void *ptr) {
	delete reinterpret_cast<T *>(ptr);
}

static void scalar_function_init(duckdb_v2_scalar_function_init_info_handle info, duckdb_v2_context_handle,
                                 duckdb_v2_error_info_handle *err) {
	duckdb_v2_opaque init_data = {new ScalarFunctionExecState(), delete_opaque<ScalarFunctionExecState>, nullptr};
	v2_error init_err;
	if (duckdb_v2_scalar_function_init_set_init_data(info, &init_data, init_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, init_err.code(), init_err.raw_message());
	}
}

/*
 * Prepares the input chunk, same as in C API v1, the input vectors are flattened. The input chunk is created once
 * per thread and references the argument vectors.
 */
static bool scalar_function_prepare_input(duckdb_v2_scalar_function_exec_info_handle info,
                                          duckdb_v2_context_handle context, ScalarFunctionExecState &state,
                                          v2_error &err) {
	idx_t positional_fixed = 0;
	idx_t positional_variadic = 0;
	idx_t named_fixed = 0;
	idx_t named_variadic = 0;
	if (duckdb_v2_scalar_function_exec_get_arg_count(info, &positional_fixed, &positional_variadic, &named_fixed,
	                                                 &named_variadic, err.out()) != DUCKDB_V2_ERROR_NONE) {
		return false;
	}
	idx_t arg_count = positional_fixed + positional_variadic + named_fixed + named_variadic;
	std::vector<duckdb_v2_vector_handle> args(arg_count);
	for (idx_t i = 0; i < arg_count; i++) {
		if (duckdb_v2_scalar_function_exec_get_arg(info, i, &args[i], err.out()) != DUCKDB_V2_ERROR_NONE) {
			return false;
		}
	}

	if (!state.input) {
		std::vector<v2_logical_type> types(arg_count);
		std::vector<duckdb_v2_logical_type_handle> type_handles(arg_count);
		for (idx_t i = 0; i < arg_count; i++) {
			if (duckdb_v2_vector_get_logical_type(args[i], types[i].out(), err.out()) != DUCKDB_V2_ERROR_NONE) {
				return false;
			}
			type_handles[i] = types[i].get();
		}
		duckdb_v2_factory_handle factory = nullptr;
		if (duckdb_v2_context_get_factory(context, &factory, err.out()) != DUCKDB_V2_ERROR_NONE) {
			return false;
		}
		// the input of a function without arguments is a chunk without columns, the types array must not be null
		duckdb_v2_logical_type_handle no_types[] = {nullptr};
		if (duckdb_v2_data_chunk_create(factory, arg_count > 0 ? type_handles.data() : no_types, arg_count,
		                                state.input.out(), err.out()) != DUCKDB_V2_ERROR_NONE) {
			return false;
		}
	}

	for (idx_t i = 0; i < arg_count; i++) {
		duckdb_v2_vector_handle input_vec = nullptr;
		if (duckdb_v2_vector_flatten(args[i], err.out()) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_data_chunk_get_vector(state.input.get(), i, &input_vec, err.out()) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vector_reference(input_vec, args[i], err.out()) != DUCKDB_V2_ERROR_NONE) {
			return false;
		}
	}
	return true;
}

/*
 * Calls the Java callback, returns an error message on failure.
 */
static bool scalar_function_call_java(duckdb_v2_scalar_function_exec_info_handle info, GlobalRefHolder *callback_holder,
                                      duckdb_v2_data_chunk_handle input, std::string &error) {
	duckdb_v2_vector_handle output = nullptr;
	if (duckdb_v2_scalar_function_exec_get_result(info, &output, nullptr) != DUCKDB_V2_ERROR_NONE) {
		error = "Unable to get function result vector";
		return false;
	}

	ScalarFunctionInfoHolder info_holder;
	AttachedJNIEnv attached = callback_holder->attach_current_thread();
	if (attached.env == nullptr) {
		error = "Unable to attach JNI environment";
		return false;
	}
	jobject info_buf = make_ptr_buf(attached.env, &info_holder);
	if (info_buf == nullptr) {
		error = "Unable to create function info buffer";
		return false;
	}
	LocalRefHolder info_buf_holder(attached.env, info_buf);
	jobject input_buf = make_ptr_buf(attached.env, input);
	if (input_buf == nullptr) {
		error = "Unable to create input buffer";
		return false;
	}
	LocalRefHolder input_buf_holder(attached.env, input_buf);
	jobject output_buf = make_ptr_buf(attached.env, output);
	if (output_buf == nullptr) {
		error = "Unable to create output buffer";
		return false;
	}
	LocalRefHolder output_buf_holder(attached.env, output_buf);

	attached.env->CallVoidMethod(callback_holder->global_ref, J_DuckDBScalarFunctionWrapper_execute, info_buf,
	                             input_buf, output_buf);
	if (attached.env->ExceptionCheck()) {
		error = "Java callback system error";
		return false;
	}
	if (info_holder.has_error) {
		error = info_holder.error;
		return false;
	}
	return true;
}

static void scalar_function_exec(duckdb_v2_scalar_function_exec_info_handle info, duckdb_v2_context_handle context,
                                 duckdb_v2_error_info_handle *err) {
	void *user_data = nullptr;
	void *init_data = nullptr;
	idx_t row_count = 0;
	v2_error exec_err;
	if (duckdb_v2_scalar_function_exec_get_user_data(info, &user_data, exec_err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_scalar_function_exec_get_init_data(info, &init_data, exec_err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_scalar_function_exec_get_row_count(info, &row_count, exec_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, exec_err.code(), exec_err.raw_message());
		return;
	}
	auto callback_holder =
	    user_data != nullptr ? reinterpret_cast<ScalarFunctionUserData *>(user_data)->callback.get() : nullptr;
	if (callback_holder == nullptr) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, "Scalar function callback not found");
		return;
	}
	if (init_data == nullptr) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, "Scalar function state not found");
		return;
	}
	auto &state = *reinterpret_cast<ScalarFunctionExecState *>(init_data);
	if (!scalar_function_prepare_input(info, context, state, exec_err)) {
		set_callback_error(err, exec_err.code(), exec_err.raw_message());
		return;
	}

	// the row count of the input chunk is reported to Java separately, see ScalarFunctionInputRowCount
	scalar_function_input_row_count.chunk = state.input.get();
	scalar_function_input_row_count.row_count = row_count;
	std::string error;
	bool success = scalar_function_call_java(info, callback_holder, state.input.get(), error);
	scalar_function_input_row_count.chunk = nullptr;
	scalar_function_input_row_count.row_count = 0;

	// same as in C API v1, the errors are raised as InvalidInputException
	if (!success) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, error);
	}
}

/*
 * Creates the C API v2 function from the specified configuration and registers it, the checks are the same as in
 * C API v1.
 */
static jint register_scalar_function(duckdb_v2_connection_handle conn, ScalarFunctionSpec &spec) {
	if (spec.name.empty() || !spec.has_function) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	auto return_type_id = logical_type_to_capi(spec.return_type.get());
	if (return_type_id == capi_type::INVALID || return_type_id == capi_type::ANY) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	for (auto &param : spec.parameters) {
		if (!param.is_valid()) {
			return static_cast<jint>(CAPI_STATE_ERROR);
		}
	}

	duckdb_v2_factory_handle factory = nullptr;
	if (duckdb_v2_connection_get_factory(conn, &factory, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	v2_scalar_function function;
	if (duckdb_v2_scalar_function_create(factory, function.out(), nullptr) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_scalar_function_set_name(function.get(), spec.name.data(), spec.name.size(), nullptr) !=
	        DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}

	duckdb_v2_function_signature_handle signature = nullptr;
	if (duckdb_v2_scalar_function_get_signature(function.get(), &signature, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	// C API v1 declares no parameter names, the parameters are positional-only and are named "colN"
	for (size_t i = 0; i < spec.parameters.size(); i++) {
		std::string param_name = "col" + std::to_string(i);
		if (duckdb_v2_function_signature_add_parameter(
		        signature, param_name.data(), param_name.size(), spec.parameters[i].get(), nullptr,
		        DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_ONLY, nullptr) != DUCKDB_V2_ERROR_NONE) {
			return static_cast<jint>(CAPI_STATE_ERROR);
		}
	}
	// same as in C API v1, varargs also receive the named arguments that are not declared
	if (spec.varargs.is_valid()) {
		if (duckdb_v2_function_signature_add_parameter(signature, "args", 4, spec.varargs.get(), nullptr,
		                                               DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_VARIADIC,
		                                               nullptr) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_function_signature_add_parameter(signature, "kwargs", 6, spec.varargs.get(), nullptr,
		                                               DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_VARIADIC,
		                                               nullptr) != DUCKDB_V2_ERROR_NONE) {
			return static_cast<jint>(CAPI_STATE_ERROR);
		}
	}
	if (duckdb_v2_function_signature_set_return_type(signature, spec.return_type.get(), nullptr) !=
	    DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}

	if (spec.is_volatile && duckdb_v2_scalar_function_set_property(
	                            function.get(), DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
	                            DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	if (spec.special_handling &&
	    duckdb_v2_scalar_function_set_property(function.get(), DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
	                                           DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL,
	                                           nullptr) != DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	if (duckdb_v2_scalar_function_set_init_callback(function.get(), scalar_function_init, nullptr) !=
	        DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_scalar_function_set_exec_callback(function.get(), scalar_function_exec, nullptr) !=
	        DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}

	// same as the "extra info" in C API v1, the callback is owned by the registered function
	auto user_data = std::unique_ptr<ScalarFunctionUserData>(new ScalarFunctionUserData());
	user_data->callback = std::move(spec.callback);
	duckdb_v2_opaque opaque = {user_data.get(), delete_opaque<ScalarFunctionUserData>, nullptr};
	if (duckdb_v2_scalar_function_set_user_data(function.get(), &opaque, nullptr) != DUCKDB_V2_ERROR_NONE) {
		spec.callback = std::move(user_data->callback);
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	// the function owns the user data now
	user_data.release();

	if (duckdb_v2_connection_register_scalar_function(conn, function.get(), nullptr) != DUCKDB_V2_ERROR_NONE) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}

	return static_cast<jint>(CAPI_STATE_SUCCESS);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_create_scalar_function
 * Signature: ()Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1create_1scalar_1function(JNIEnv *env, jclass) {
	auto function = new ScalarFunctionSpec();
	jobject buf = make_ptr_buf(env, function);
	if (buf == nullptr) {
		delete function;
	}
	return buf;
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
	delete function;
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
	function->name = function_name;
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
	function->parameters.emplace_back(type);
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
	function->return_type = spec_logical_type(type);
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
	function->varargs = spec_logical_type(type);
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
	function->is_volatile = true;
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
	function->special_handling = true;
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
		return static_cast<jint>(CAPI_STATE_ERROR);
	}
	auto function = scalar_function_buf_to_scalar_function(env, scalar_function);
	if (env->ExceptionCheck()) {
		return static_cast<jint>(CAPI_STATE_ERROR);
	}

	return register_scalar_function(conn, *function);
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

	sf->callback = std::move(callback_holder);
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

	sf->has_function = true;
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
	function_info->has_error = true;
	function_info->error = error_message;
}
