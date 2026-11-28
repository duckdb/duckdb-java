#include "holders.hpp"

#include <algorithm>
#include <iostream>
#include <stdexcept>

duckdb_v2_connection_handle conn_ref_buf_to_conn(JNIEnv *env, jobject conn_ref_buf) {
	if (conn_ref_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid connection buffer");
		return nullptr;
	}
	auto conn = reinterpret_cast<duckdb_v2_connection_handle>(env->GetDirectBufferAddress(conn_ref_buf));
	if (conn == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid connection");
		return nullptr;
	}
	return conn;
}

ChunkedResultHolder::ChunkedResultHolder(duckdb_v2_result_handle result_p) : result(result_p) {
	v2_schema schema;
	v2_error err;
	if (duckdb_v2_result_get_schema(result.get(), schema.out(), err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw std::runtime_error(err.message());
	}
	idx_t count = 0;
	if (duckdb_v2_schema_get_count(schema.get(), &count, nullptr) != DUCKDB_V2_ERROR_NONE) {
		throw std::runtime_error("Unable to read result schema");
	}
	for (idx_t i = 0; i < count; i++) {
		const char *name = nullptr;
		idx_t name_length = 0;
		// the field type is borrowed from the schema
		duckdb_v2_logical_type_handle type = nullptr;
		if (duckdb_v2_schema_get_field(schema.get(), i, &name, &name_length, &type, nullptr) != DUCKDB_V2_ERROR_NONE) {
			throw std::runtime_error("Unable to read result schema");
		}
		column_names.emplace_back(name, name_length);
		column_types.push_back(logical_type_to_capi(type));
	}
}

AttachedJNIEnv::AttachedJNIEnv() {
}

AttachedJNIEnv::AttachedJNIEnv(JavaVM *vm_in, JNIEnv *env_in, bool need_to_detach_in)
    : vm(vm_in), env(env_in), need_to_detach(need_to_detach_in) {
}

AttachedJNIEnv::~AttachedJNIEnv() noexcept {
	if (vm == nullptr) {
		return;
	}
	if (need_to_detach) {
		vm->DetachCurrentThread();
	}
}

GlobalRefHolder::GlobalRefHolder(JNIEnv *env, jobject local_ref) {
	if (env->GetJavaVM(&this->vm) != JNI_OK || this->vm == nullptr) {
		this->vm = nullptr;
		return;
	}
	if (local_ref != nullptr) {
		this->global_ref = env->NewGlobalRef(local_ref);
		if (this->global_ref == nullptr) {
			this->vm = nullptr;
		}
	}
}

GlobalRefHolder::~GlobalRefHolder() noexcept {
	if (global_ref == nullptr) {
		return;
	}
	AttachedJNIEnv attached = attach_current_thread();
	if (attached.env == nullptr) {
		return;
	}
	attached.env->DeleteGlobalRef(global_ref);
}

AttachedJNIEnv GlobalRefHolder::attach_current_thread() {
	if (vm == nullptr) {
		return AttachedJNIEnv();
	}
	JNIEnv *env = nullptr;
	auto env_status = vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_6);
	if (env_status != JNI_OK && env_status != JNI_EDETACHED) {
		std::cerr << "ERROR: vm->GetEnv() failure, status: " << env_status << std::endl;
		return AttachedJNIEnv();
	}
	bool need_to_detach = false;
	if (env_status == JNI_EDETACHED) {
		auto attach_status = vm->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr);
		if (attach_status != JNI_OK || env == nullptr) {
			std::cerr << "ERROR: vm->AttachCurrentThread() failure, status: " << attach_status << std::endl;
			return AttachedJNIEnv();
		}
		need_to_detach = true;
	}

	return AttachedJNIEnv(vm, env, need_to_detach);
}

void GlobalRefHolder::destroy(void *holder_in) noexcept {
	auto holder = reinterpret_cast<GlobalRefHolder *>(holder_in);
	delete holder;
}

void GlobalRefHolder::destroy_table_function_state(void *holder_in) noexcept {
	auto holder = reinterpret_cast<GlobalRefHolder *>(holder_in);
	if (holder == nullptr) {
		return;
	}

	try {
		AttachedJNIEnv attached = holder->attach_current_thread();
		if (attached.env != nullptr) {
			try {
				attached.env->CallStaticVoidMethod(J_DuckDBTableFunctionWrapper,
				                                   J_DuckDBTableFunctionWrapper_closeTableFunctionState,
				                                   holder->global_ref);
			} catch (...) {
				// Teardown must not allow a C++ exception to escape a native destructor.
			}
			try {
				if (attached.env->ExceptionCheck()) {
					attached.env->ExceptionClear();
				}
			} catch (...) {
				// JNI cleanup is best effort; the holder is still deleted below.
			}
			// Keep the callback's attachment alive through reference deletion and holder destruction.
			try {
				if (holder->global_ref != nullptr) {
					jobject global_ref = holder->global_ref;
					holder->global_ref = nullptr;
					attached.env->DeleteGlobalRef(global_ref);
				}
			} catch (...) {
				// The reference was cleared before the JNI call, so the holder destructor cannot
				// redundantly attempt another attach if the call fails unexpectedly.
			}
			delete holder;
			return;
		}

		// attach_current_thread already attempted the only safe cleanup path. Without a valid
		// JNIEnv, JNI does not permit DeleteGlobalRef; clear the reference so the holder destructor
		// does not redundantly attempt another attach.
		holder->global_ref = nullptr;
	} catch (...) {
		// A failed attach or unexpected JNI failure must not skip holder deletion or trigger a
		// second attach from the holder destructor.
		holder->global_ref = nullptr;
	}

	delete holder;
}

LocalRefHolder::LocalRefHolder(JNIEnv *env_in, jobject local_ref_in) : env(env_in), local_ref(local_ref_in) {
}

LocalRefHolder::~LocalRefHolder() noexcept {
	if (env == nullptr || local_ref == nullptr) {
		return;
	}
	env->DeleteLocalRef(local_ref);
}
