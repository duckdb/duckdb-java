#include "bindings.hpp"
#include "holders.hpp"
#include "util.hpp"

static duckdb_appender appender_buf_to_appender(JNIEnv *env, jobject appender_buf) {

	if (appender_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid appender buffer");
		return nullptr;
	}

	duckdb_appender appender = reinterpret_cast<duckdb_appender>(env->GetDirectBufferAddress(appender_buf));
	if (appender == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid appender");
		return nullptr;
	}

	return appender;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_create_ext
 * Signature: (Ljava/nio/ByteBuffer;[B[B[B[Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1create_1ext(JNIEnv *env, jclass,
                                                                                    jobject connection,
                                                                                    jbyteArray catalog,
                                                                                    jbyteArray schema, jbyteArray table,
                                                                                    jobjectArray out_appender) {

	duckdb_connection conn = conn_ref_buf_to_conn(env, connection);
	if (env->ExceptionCheck()) {
		return -1;
	}

	std::string catalog_str = jbyteArray_to_string(env, catalog);
	if (env->ExceptionCheck()) {
		return -1;
	}
	const char *catalog_ptr = nullptr != catalog ? catalog_str.c_str() : nullptr;

	std::string schema_str = jbyteArray_to_string(env, schema);
	if (env->ExceptionCheck()) {
		return -1;
	}
	const char *schema_ptr = nullptr != schema ? schema_str.c_str() : nullptr;

	std::string table_str = jbyteArray_to_string(env, table);
	if (env->ExceptionCheck()) {
		return -1;
	}
	const char *table_ptr = nullptr != table ? table_str.c_str() : nullptr;

	check_out_param(env, out_appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_appender appender = nullptr;

	duckdb_state state = duckdb_appender_create_ext(conn, catalog_ptr, schema_ptr, table_ptr, &appender);

	if (state == DuckDBSuccess) {
		jobject appender_ref_buf = env->NewDirectByteBuffer(appender, 0);
		set_out_param(env, out_appender, appender_ref_buf);
	}

	return static_cast<jint>(state);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_error
 * Signature: (Ljava/nio/ByteBuffer;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1error(JNIEnv *env, jclass,
                                                                                    jobject appender) {

	duckdb_appender app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	const char *error_msg = duckdb_appender_error(app);
	if (error_msg == nullptr) {
		return nullptr;
	}

	idx_t len = static_cast<idx_t>(std::strlen(error_msg));

	return make_jbyteArray(env, error_msg, len);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_flush
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1flush(JNIEnv *env, jclass, jobject appender) {

	duckdb_appender app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_state state = duckdb_appender_flush(app);

	return static_cast<jint>(state);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_close
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1close(JNIEnv *env, jclass, jobject appender) {

	duckdb_appender app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_state state = duckdb_appender_close(app);

	return static_cast<jint>(state);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_destroy
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1destroy(JNIEnv *env, jclass, jobject appender) {

	duckdb_appender app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_state state = duckdb_appender_destroy(&app);

	return static_cast<jint>(state);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_column_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1column_1count(JNIEnv *env, jclass,
                                                                                       jobject appender) {

	duckdb_appender app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	idx_t count = duckdb_appender_column_count(app);

	return static_cast<jlong>(count);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_column_type
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1column_1type(JNIEnv *env, jclass,
                                                                                        jobject appender,
                                                                                        jlong col_idx) {

	duckdb_appender app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	idx_t idx = jlong_to_idx(env, col_idx);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	duckdb_logical_type logical_type = duckdb_appender_column_type(app, idx);

	return make_ptr_buf(env, logical_type);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_append_data_chunk
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1append_1data_1chunk(JNIEnv *env, jclass, jobject appender,
                                                                                  jobject chunk) {

	duckdb_appender app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_data_chunk dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_state state = duckdb_append_data_chunk(app, dc);

	return static_cast<jint>(state);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_append_default_to_chunk
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;JJ)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1append_1default_1to_1chunk(JNIEnv *env, jclass,
                                                                                         jobject appender,
                                                                                         jobject chunk, jlong col,
                                                                                         jlong row) {

	duckdb_appender app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}
	duckdb_data_chunk dc = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return -1;
	}
	idx_t col_idx = jlong_to_idx(env, col);
	if (env->ExceptionCheck()) {
		return -1;
	}
	idx_t row_idx = jlong_to_idx(env, row);
	if (env->ExceptionCheck()) {
		return -1;
	}

	duckdb_state state = duckdb_append_default_to_chunk(app, dc, col_idx, row_idx);

	return static_cast<jint>(state);
}
