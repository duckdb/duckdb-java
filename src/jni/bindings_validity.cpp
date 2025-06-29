#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <vector>

static uint64_t *validity_buf_to_validity(JNIEnv *env, jobject validity_buf) {

	if (validity_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid validity buffer");
		return nullptr;
	}

	uint64_t *validity = reinterpret_cast<uint64_t *>(env->GetDirectBufferAddress(validity_buf));

	if (validity == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid validity");
		return nullptr;
	}

	return validity;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_validity_row_is_valid
 * Signature: (Ljava/nio/ByteBuffer;J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1validity_1row_1is_1valid(JNIEnv *env, jclass,
                                                                                           jobject validity,
                                                                                           jlong row) {

	uint64_t *val = validity_buf_to_validity(env, validity);
	if (env->ExceptionCheck()) {
		return false;
	}

	idx_t row_idx = jlong_to_idx(env, row);
	if (env->ExceptionCheck()) {
		return false;
	}

	bool res = duckdb_validity_row_is_valid(val, row_idx);

	return static_cast<jboolean>(res);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_validity_set_row_validity
 * Signature: (Ljava/nio/ByteBuffer;JZ)V
 */
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1validity_1set_1row_1validity(JNIEnv *env, jclass,
                                                                                           jobject validity, jlong row,
                                                                                           jboolean valid) {

	uint64_t *val = validity_buf_to_validity(env, validity);
	if (env->ExceptionCheck()) {
		return;
	}

	idx_t row_idx = jlong_to_idx(env, row);
	if (env->ExceptionCheck()) {
		return;
	}

	bool valid_flag = static_cast<bool>(valid);

	duckdb_validity_set_row_validity(val, row_idx, valid_flag);
}
