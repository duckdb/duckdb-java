extern "C" {
#include "duckdb.h"
}

#include "duckdb.hpp"
#include "functions.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "scalar_functions.hpp"
#include "util.hpp"

using namespace duckdb;

struct JNIEnvGuard {
	JavaVM *vm;
	JNIEnv *env;
	bool detach_when_done;

	explicit JNIEnvGuard(JavaVM *vm_p) : vm(vm_p), env(nullptr), detach_when_done(false) {
		if (!vm) {
			throw InvalidInputException("JVM is not available");
		}
		auto get_env_status = vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6);
		if (get_env_status == JNI_OK) {
			return;
		}
		if (get_env_status != JNI_EDETACHED) {
			throw InvalidInputException("Failed to get JNI environment");
		}
		auto attach_status = vm->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr);
		if (attach_status != JNI_OK || !env) {
			throw InvalidInputException("Failed to attach current thread to JVM");
		}
		detach_when_done = true;
	}

	~JNIEnvGuard() {
		if (detach_when_done && vm) {
			vm->DetachCurrentThread();
		}
	}
};

struct JavaScalarFunctionState {
	JavaVM *vm;
	jobject callback;
	jmethodID apply_method;

	JavaScalarFunctionState(JavaVM *vm_p, jobject callback_p, jmethodID apply_method_p)
	    : vm(vm_p), callback(callback_p), apply_method(apply_method_p) {
	}

	~JavaScalarFunctionState() {
		if (!vm || !callback) {
			return;
		}
		try {
			JNIEnvGuard env_guard(vm);
			env_guard.env->DeleteGlobalRef(callback);
		} catch (...) {
			// noop in destructor
		}
	}
};

struct JavaScalarFunctionLocalState {
	JavaVM *vm;
	JNIEnv *env;
	bool detach_when_done;
};

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

static string consume_java_exception_message(JNIEnv *env) {
	auto throwable = env->ExceptionOccurred();
	if (!throwable) {
		return "Java exception";
	}
	env->ExceptionClear();

	string message = "Java exception";
	auto msg = (jstring)env->CallObjectMethod(throwable, J_Throwable_getMessage);
	if (!env->ExceptionCheck() && msg) {
		message = jstring_to_string(env, msg);
	}
	if (env->ExceptionCheck()) {
		env->ExceptionClear();
	}

	env->DeleteLocalRef(throwable);
	if (msg) {
		env->DeleteLocalRef(msg);
	}

	return message;
}

static void get_or_attach_jni_env(JavaVM *vm, JNIEnv *&env, bool &detach_when_done) {
	if (!vm) {
		throw InvalidInputException("JVM is not available");
	}

	detach_when_done = false;
	auto get_env_status = vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6);
	if (get_env_status == JNI_OK) {
		return;
	}
	if (get_env_status != JNI_EDETACHED) {
		throw InvalidInputException("Failed to get JNI environment");
	}

	auto attach_status = vm->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr);
	if (attach_status != JNI_OK || !env) {
		throw InvalidInputException("Failed to attach current thread to JVM");
	}
	detach_when_done = true;
}

static void execute_java_vectorized_scalar_function(JNIEnv *env, JavaScalarFunctionState &state, DataChunk &input,
                                                    Vector &output) {
	auto row_count = input.size();
	jobject input_chunk_buf = make_ptr_buf(env, &input);
	jobject output_vector_buf = make_ptr_buf(env, &output);
	auto input_reader = env->NewObject(J_DuckDataChunkReader, J_DuckDataChunkReader_init, input_chunk_buf,
	                                   static_cast<jint>(row_count));
	if (env->ExceptionCheck()) {
		if (input_chunk_buf) {
			env->DeleteLocalRef(input_chunk_buf);
		}
		if (output_vector_buf) {
			env->DeleteLocalRef(output_vector_buf);
		}
		throw InvalidInputException("Could not create DuckDBDataChunkReader: %s", consume_java_exception_message(env));
	}

	auto output_writer = env->NewObject(J_DuckWritableVector, J_DuckWritableVector_init, output_vector_buf,
	                                    static_cast<jint>(row_count));
	if (env->ExceptionCheck()) {
		env->DeleteLocalRef(input_reader);
		if (input_chunk_buf) {
			env->DeleteLocalRef(input_chunk_buf);
		}
		if (output_vector_buf) {
			env->DeleteLocalRef(output_vector_buf);
		}
		throw InvalidInputException("Could not create DuckDBWritableVector: %s", consume_java_exception_message(env));
	}

	env->CallVoidMethod(state.callback, state.apply_method, input_reader, static_cast<jint>(row_count), output_writer);

	env->DeleteLocalRef(output_writer);
	env->DeleteLocalRef(input_reader);
	if (input_chunk_buf) {
		env->DeleteLocalRef(input_chunk_buf);
	}
	if (output_vector_buf) {
		env->DeleteLocalRef(output_vector_buf);
	}

	if (env->ExceptionCheck()) {
		throw InvalidInputException("Java scalar function threw exception: %s", consume_java_exception_message(env));
	}
}

static void destroy_java_scalar_function_state(void *extra_info);
static void init_java_scalar_function_capi(duckdb_init_info info);
static void execute_java_scalar_function_capi(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);

static jmethodID get_scalar_callback_method(JNIEnv *env, jobject function_j, const char *signature,
                                            const char *error_message) {
	auto callback_class = env->GetObjectClass(function_j);
	auto apply_method = env->GetMethodID(callback_class, "apply", signature);
	env->DeleteLocalRef(callback_class);
	if (!apply_method || env->ExceptionCheck()) {
		consume_java_exception_message(env);
		throw InvalidInputException("%s", error_message);
	}
	return apply_method;
}

void duckdb_jdbc_install_scalar_function_callback(JNIEnv *env, jobject conn_ref_buf, jobject scalar_function_buf,
                                                  jobject function_j) {
	auto connection = get_connection(env, conn_ref_buf);
	if (!connection) {
		throw InvalidInputException("Invalid connection");
	}
	auto scalar_function = scalar_function_buf_to_scalar_function(env, scalar_function_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	if (!function_j) {
		throw InvalidInputException("Invalid scalar function callback");
	}

	JavaVM *vm = nullptr;
	if (env->GetJavaVM(&vm) != JNI_OK || !vm) {
		throw InvalidInputException("Failed to get JVM reference");
	}

	auto callback_ref = env->NewGlobalRef(function_j);
	if (!callback_ref) {
		throw InvalidInputException("Could not create global reference for scalar function callback");
	}

	try {
		auto apply_method = get_scalar_callback_method(
		    env, function_j, "(Lorg/duckdb/DuckDBDataChunkReader;ILorg/duckdb/DuckDBWritableVector;)V",
		    "Could not find apply(DuckDBDataChunkReader, int, DuckDBWritableVector) on scalar function callback");
		auto state = new JavaScalarFunctionState(vm, callback_ref, apply_method);
		duckdb_scalar_function_set_extra_info(scalar_function, state, destroy_java_scalar_function_state);
		duckdb_scalar_function_set_function(scalar_function, execute_java_scalar_function_capi);
		duckdb_scalar_function_set_init(scalar_function, init_java_scalar_function_capi);
	} catch (...) {
		env->DeleteGlobalRef(callback_ref);
		throw;
	}
}

static void destroy_java_scalar_function_state(void *extra_info) {
	if (!extra_info) {
		return;
	}
	delete reinterpret_cast<JavaScalarFunctionState *>(extra_info);
}

static void destroy_java_scalar_function_local_state(void *state_ptr) {
	if (!state_ptr) {
		return;
	}

	auto state = reinterpret_cast<JavaScalarFunctionLocalState *>(state_ptr);
	if (state->detach_when_done && state->vm) {
		state->vm->DetachCurrentThread();
	}
	delete state;
}

static void init_java_scalar_function_capi(duckdb_init_info info) {
	JavaScalarFunctionLocalState *local_state = nullptr;
	try {
		auto state = reinterpret_cast<JavaScalarFunctionState *>(duckdb_scalar_function_init_get_extra_info(info));
		if (!state) {
			duckdb_scalar_function_init_set_error(info, "Invalid Java scalar function callback state");
			return;
		}

		local_state = new JavaScalarFunctionLocalState();
		local_state->vm = state->vm;
		local_state->env = nullptr;
		local_state->detach_when_done = false;
		get_or_attach_jni_env(local_state->vm, local_state->env, local_state->detach_when_done);
		duckdb_scalar_function_init_set_state(info, local_state, destroy_java_scalar_function_local_state);
		local_state = nullptr;
	} catch (const std::exception &e) {
		if (local_state) {
			destroy_java_scalar_function_local_state(local_state);
		}
		duckdb_scalar_function_init_set_error(info, e.what());
	}
}

static void execute_java_scalar_function_capi(duckdb_function_info info, duckdb_data_chunk input,
                                              duckdb_vector output) {
	auto state = reinterpret_cast<JavaScalarFunctionState *>(duckdb_scalar_function_get_extra_info(info));
	auto local_state = reinterpret_cast<JavaScalarFunctionLocalState *>(duckdb_scalar_function_get_state(info));
	if (!state || !local_state || !local_state->env || !input || !output) {
		duckdb_scalar_function_set_error(info, "Invalid Java scalar function callback state");
		return;
	}

	try {
		auto &input_chunk = *reinterpret_cast<DataChunk *>(input);
		auto &output_vector = *reinterpret_cast<Vector *>(output);
		execute_java_vectorized_scalar_function(local_state->env, *state, input_chunk, output_vector);
	} catch (const std::exception &e) {
		duckdb_scalar_function_set_error(info, e.what());
	}
}

extern "C" JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1jdbc_1scalar_1function_1set_1callback(
    JNIEnv *env, jclass, jobject conn_ref_buf, jobject scalar_function_buf, jobject function_j) {
	try {
		duckdb_jdbc_install_scalar_function_callback(env, conn_ref_buf, scalar_function_buf, function_j);
	} catch (const std::exception &e) {
		duckdb::ErrorData error(e);
		ThrowJNI(env, error.Message().c_str());
	}
}
