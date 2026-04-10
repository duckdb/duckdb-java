#include "holders.hpp"

ConnectionHolder *get_connection_ref(JNIEnv *env, jobject conn_ref_buf) {
	if (!conn_ref_buf) {
		throw duckdb::ConnectionException("Invalid connection buffer ref");
	}
	auto conn_holder = reinterpret_cast<ConnectionHolder *>(env->GetDirectBufferAddress(conn_ref_buf));
	if (!conn_holder) {
		throw duckdb::ConnectionException("Invalid connection buffer");
	}
	return conn_holder;
}

/**
 * Throws a SQLException and returns nullptr if a valid Connection can't be retrieved from the buffer.
 */
duckdb::Connection *get_connection(JNIEnv *env, jobject conn_ref_buf) {
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	auto conn_ref = conn_holder->connection.get();
	if (!conn_ref || !conn_ref->context) {
		throw duckdb::ConnectionException("Invalid connection");
	}

	return conn_ref;
}

duckdb_connection conn_ref_buf_to_conn(JNIEnv *env, jobject conn_ref_buf) {
	if (conn_ref_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid connection buffer");
		return nullptr;
	}
	auto conn_holder = reinterpret_cast<ConnectionHolder *>(env->GetDirectBufferAddress(conn_ref_buf));
	if (conn_holder == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid connection holder");
		return nullptr;
	}
	auto conn_ref = conn_holder->connection.get();
	if (conn_ref == nullptr || conn_ref->context == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid connection");
		return nullptr;
	}

	return reinterpret_cast<duckdb_connection>(conn_ref);
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
		return AttachedJNIEnv();
	}
	bool need_to_detach = false;
	if (env_status == JNI_EDETACHED) {
		auto attach_status = vm->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr);
		if (attach_status != JNI_OK || env == nullptr) {
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

LocalRefHolder::LocalRefHolder(JNIEnv *env_in, jobject local_ref_in) : env(env_in), local_ref(local_ref_in) {
}

LocalRefHolder::~LocalRefHolder() noexcept {
	if (env == nullptr || local_ref == nullptr) {
		return;
	}
	env->DeleteLocalRef(local_ref);
}
