// Thin JNI entry points over the C API v2 calls, one entry point per call. They contain no driver logic:
// the logic that uses them is implemented in Java.
#include "bindings.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <cstring>
#include <vector>

template <class H>
static H buf_to_handle(JNIEnv *env, jobject buf, const char *what) {
	if (buf == nullptr) {
		env->ThrowNew(J_SQLException, (std::string("Invalid ") + what + " buffer").c_str());
		return nullptr;
	}
	auto handle = reinterpret_cast<H>(env->GetDirectBufferAddress(buf));
	if (handle == nullptr) {
		env->ThrowNew(J_SQLException, (std::string("Invalid ") + what).c_str());
		return nullptr;
	}
	return handle;
}

// An interrupted query is reported to Java as a timeout, same as the C++ part of the driver does
static void throw_v2_error(JNIEnv *env, const v2_error &err) {
	if (env->ExceptionCheck()) {
		return;
	}
	jclass type = err.code() == DUCKDB_V2_ERROR_RUNTIME_INTERRUPT ? J_SQLTimeoutException : J_SQLException;
	env->ThrowNew(type, err.message().c_str());
}

#define CHECK_BUF(var, buf, T, what)                                                                                   \
	auto var = buf_to_handle<T>(env, buf, what);                                                                       \
	if (env->ExceptionCheck()) {                                                                                       \
		return;                                                                                                        \
	}

#define CHECK_BUF_RET(var, buf, T, what, ret)                                                                          \
	auto var = buf_to_handle<T>(env, buf, what);                                                                       \
	if (env->ExceptionCheck()) {                                                                                       \
		return ret;                                                                                                    \
	}

#define CALL_V2(call, ret)                                                                                             \
	do {                                                                                                               \
		v2_error err;                                                                                                  \
		if ((call) != DUCKDB_V2_ERROR_NONE) {                                                                          \
			throw_v2_error(env, err);                                                                                  \
			return ret;                                                                                                \
		}                                                                                                              \
	} while (0)

// The `err` out parameter of a call, used inside CALL_V2 only
#define ERR_OUT err.out()

/* --- connection --- */

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1interrupt(JNIEnv *env, jclass,
                                                                                        jobject conn_buf) {
	CHECK_BUF(conn, conn_buf, duckdb_v2_connection_handle, "connection");
	CALL_V2(duckdb_v2_connection_interrupt(conn, ERR_OUT), );
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1progress_1get(JNIEnv *env, jclass,
                                                                                               jobject conn_buf) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	double percentage = -1;
	uint64_t rows_processed = 0;
	uint64_t total_rows = 0;
	CALL_V2(duckdb_v2_connection_progress_get(conn, &percentage, &rows_processed, &total_rows, ERR_OUT), nullptr);
	return env->NewObject(J_QueryProgress, J_QueryProgress_init, static_cast<jdouble>(percentage),
	                      uint64_to_jlong(rows_processed), uint64_to_jlong(total_rows));
}

/* --- parsing --- */

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1parse_1sql(JNIEnv *env, jclass, jobject conn_buf,
                                                                                jbyteArray sql_j) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	auto sql = jbyteArray_to_string(env, sql_j);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_statement_iterator_handle it = nullptr;
	CALL_V2(duckdb_v2_parse_sql(conn, sql.data(), sql.size(), false, &it, ERR_OUT), nullptr);
	return make_ptr_buf(env, it);
}

// Returns null when there are no more statements
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1statement_1iterator_1next(JNIEnv *env, jclass,
                                                                                               jobject it_buf) {
	CHECK_BUF_RET(it, it_buf, duckdb_v2_statement_iterator_handle, "statement iterator", nullptr);
	duckdb_v2_sql_statement_handle statement = nullptr;
	CALL_V2(duckdb_v2_statement_iterator_next(it, &statement, ERR_OUT), nullptr);
	return make_ptr_buf(env, statement);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1statement_1iterator_1destroy(JNIEnv *env, jclass,
                                                                                               jobject it_buf) {
	CHECK_BUF(it, it_buf, duckdb_v2_statement_iterator_handle, "statement iterator");
	duckdb_v2_statement_iterator_destroy(&it);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1sql_1statement_1destroy(JNIEnv *env, jclass,
                                                                                          jobject st_buf) {
	CHECK_BUF(st, st_buf, duckdb_v2_sql_statement_handle, "statement");
	duckdb_v2_sql_statement_destroy(&st);
}

JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1sql_1statement_1get_1type(JNIEnv *env, jclass,
                                                                                            jobject st_buf) {
	CHECK_BUF_RET(st, st_buf, duckdb_v2_sql_statement_handle, "statement", 0);
	DUCKDB_V2_STATEMENT_TYPE type = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	CALL_V2(duckdb_v2_sql_statement_get_type(st, &type, ERR_OUT), 0);
	return static_cast<jint>(type);
}

JNIEXPORT jlong JNICALL
Java_org_duckdb_DuckDBBindings_duckdb_1v2_1sql_1statement_1get_1parameter_1count(JNIEnv *env, jclass, jobject st_buf) {
	CHECK_BUF_RET(st, st_buf, duckdb_v2_sql_statement_handle, "statement", 0);
	idx_t count = 0;
	CALL_V2(duckdb_v2_sql_statement_get_parameter_count(st, &count, ERR_OUT), 0);
	return uint64_to_jlong(count);
}

/* --- prepared statements --- */

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1prepared_1statement_1create(
    JNIEnv *env, jclass, jobject conn_buf, jobject st_buf, jboolean require_cacheable) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	CHECK_BUF_RET(st, st_buf, duckdb_v2_sql_statement_handle, "statement", nullptr);
	duckdb_v2_prepared_statement_handle prepared = nullptr;
	CALL_V2(duckdb_v2_prepared_statement_create(conn, st, require_cacheable == JNI_TRUE, &prepared, ERR_OUT), nullptr);
	return make_ptr_buf(env, prepared);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1prepared_1statement_1destroy(JNIEnv *env, jclass,
                                                                                               jobject prepared_buf) {
	CHECK_BUF(prepared, prepared_buf, duckdb_v2_prepared_statement_handle, "prepared statement");
	duckdb_v2_prepared_statement_destroy(&prepared);
}

// The values of the positional parameters are borrowed for the call
static bool read_values(JNIEnv *env, jobjectArray values_j, std::vector<duckdb_v2_value_handle> &values) {
	if (values_j == nullptr) {
		return true;
	}
	jsize count = env->GetArrayLength(values_j);
	for (jsize i = 0; i < count; i++) {
		jobject value_buf = env->GetObjectArrayElement(values_j, i);
		auto value = buf_to_handle<duckdb_v2_value_handle>(env, value_buf, "value");
		env->DeleteLocalRef(value_buf);
		if (env->ExceptionCheck()) {
			return false;
		}
		values.push_back(value);
	}
	return true;
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1prepared_1statement_1execute(
    JNIEnv *env, jclass, jobject prepared_buf, jobjectArray values_j) {
	CHECK_BUF_RET(prepared, prepared_buf, duckdb_v2_prepared_statement_handle, "prepared statement", nullptr);
	std::vector<duckdb_v2_value_handle> values;
	if (!read_values(env, values_j, values)) {
		return nullptr;
	}
	duckdb_v2_result_handle result = nullptr;
	CALL_V2(duckdb_v2_prepared_statement_execute(prepared, nullptr, nullptr, values.data(), values.size(), &result,
	                                             ERR_OUT),
	        nullptr);
	return make_ptr_buf(env, result);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1statement_1execute(JNIEnv *env, jclass,
                                                                                        jobject conn_buf,
                                                                                        jobject st_buf,
                                                                                        jobjectArray values_j) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	CHECK_BUF_RET(st, st_buf, duckdb_v2_sql_statement_handle, "statement", nullptr);
	std::vector<duckdb_v2_value_handle> values;
	if (!read_values(env, values_j, values)) {
		return nullptr;
	}
	duckdb_v2_result_handle result = nullptr;
	CALL_V2(duckdb_v2_statement_execute(conn, st, nullptr, nullptr, values.data(), values.size(), &result, ERR_OUT),
	        nullptr);
	return make_ptr_buf(env, result);
}

// Returns the output schema and the parameters schema
JNIEXPORT jobjectArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1statement_1bind(JNIEnv *env, jclass,
                                                                                          jobject conn_buf,
                                                                                          jobject st_buf) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	CHECK_BUF_RET(st, st_buf, duckdb_v2_sql_statement_handle, "statement", nullptr);
	v2_schema output;
	v2_schema parameters;
	CALL_V2(duckdb_v2_statement_bind(conn, st, output.out(), parameters.out(), ERR_OUT), nullptr);
	auto res = env->NewObjectArray(2, J_ByteBuffer, nullptr);
	if (res == nullptr) {
		return nullptr;
	}
	jobject output_buf = make_ptr_buf(env, output.get());
	if (output_buf == nullptr) {
		return nullptr;
	}
	output.release();
	env->SetObjectArrayElement(res, 0, output_buf);
	jobject parameters_buf = make_ptr_buf(env, parameters.get());
	if (parameters_buf != nullptr) {
		parameters.release();
		env->SetObjectArrayElement(res, 1, parameters_buf);
	}
	return res;
}

/* --- schemas --- */

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1schema_1destroy(JNIEnv *env, jclass,
                                                                                  jobject schema_buf) {
	CHECK_BUF(schema, schema_buf, duckdb_v2_schema_handle, "schema");
	duckdb_v2_schema_destroy(&schema);
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1schema_1get_1count(JNIEnv *env, jclass,
                                                                                      jobject schema_buf) {
	CHECK_BUF_RET(schema, schema_buf, duckdb_v2_schema_handle, "schema", 0);
	idx_t count = 0;
	CALL_V2(duckdb_v2_schema_get_count(schema, &count, ERR_OUT), 0);
	return uint64_to_jlong(count);
}

JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1schema_1get_1field_1name(JNIEnv *env, jclass,
                                                                                                 jobject schema_buf,
                                                                                                 jlong index) {
	CHECK_BUF_RET(schema, schema_buf, duckdb_v2_schema_handle, "schema", nullptr);
	idx_t idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	const char *name = nullptr;
	idx_t name_len = 0;
	duckdb_v2_logical_type_handle type = nullptr;
	CALL_V2(duckdb_v2_schema_get_field(schema, idx, &name, &name_len, &type, ERR_OUT), nullptr);
	return make_jbyteArray(env, name, name_len);
}

// The returned type is borrowed from the schema and must not be destroyed
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1schema_1get_1field_1type(JNIEnv *env, jclass,
                                                                                              jobject schema_buf,
                                                                                              jlong index) {
	CHECK_BUF_RET(schema, schema_buf, duckdb_v2_schema_handle, "schema", nullptr);
	idx_t idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	const char *name = nullptr;
	idx_t name_len = 0;
	duckdb_v2_logical_type_handle type = nullptr;
	CALL_V2(duckdb_v2_schema_get_field(schema, idx, &name, &name_len, &type, ERR_OUT), nullptr);
	return make_ptr_buf(env, type);
}

/* --- results --- */

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1destroy(JNIEnv *env, jclass,
                                                                                  jobject result_buf) {
	CHECK_BUF(result, result_buf, duckdb_v2_result_handle, "result");
	duckdb_v2_result_destroy(&result);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1get_1schema(JNIEnv *env, jclass,
                                                                                         jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", nullptr);
	duckdb_v2_schema_handle schema = nullptr;
	CALL_V2(duckdb_v2_result_get_schema(result, &schema, ERR_OUT), nullptr);
	return make_ptr_buf(env, schema);
}

JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1get_1result_1type(JNIEnv *env, jclass,
                                                                                            jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", 0);
	DUCKDB_V2_RESULT_TYPE type = DUCKDB_V2_RESULT_TYPE_NOTHING;
	CALL_V2(duckdb_v2_result_get_result_type(result, &type, ERR_OUT), 0);
	return static_cast<jint>(type);
}

JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1get_1statement_1type(JNIEnv *env, jclass,
                                                                                               jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", 0);
	DUCKDB_V2_STATEMENT_TYPE type = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	CALL_V2(duckdb_v2_result_get_statement_type(result, &type, ERR_OUT), 0);
	return static_cast<jint>(type);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1wait(JNIEnv *env, jclass,
                                                                               jobject result_buf) {
	CHECK_BUF(result, result_buf, duckdb_v2_result_handle, "result");
	CALL_V2(duckdb_v2_result_wait(result, ERR_OUT), );
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1drain(JNIEnv *env, jclass,
                                                                                 jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", 0);
	idx_t rows_changed = 0;
	CALL_V2(duckdb_v2_result_drain(result, &rows_changed, ERR_OUT), 0);
	return uint64_to_jlong(rows_changed);
}

// Steps the result once, the status (DUCKDB_V2_RESULT_STEP_STATUS) is written to status_out[0], the returned chunk is
// null unless the status is CHUNK
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1step(JNIEnv *env, jclass,
                                                                                  jobject result_buf,
                                                                                  jintArray status_out) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", nullptr);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	CALL_V2(duckdb_v2_result_step(result, &chunk, &status, ERR_OUT), nullptr);
	jint status_j = static_cast<jint>(status);
	env->SetIntArrayRegion(status_out, 0, 1, &status_j);
	return make_data_chunk_buf(env, chunk);
}

// Returns null at the end of the result
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1fetch_1chunk(JNIEnv *env, jclass,
                                                                                          jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", nullptr);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CALL_V2(duckdb_v2_result_fetch_chunk(result, &chunk, ERR_OUT), nullptr);
	return make_data_chunk_buf(env, chunk);
}

/* --- factory and values --- */

// The factory is borrowed from the connection
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1get_1factory(JNIEnv *env, jclass,
                                                                                              jobject conn_buf) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	duckdb_v2_factory_handle factory = nullptr;
	CALL_V2(duckdb_v2_connection_get_factory(conn, &factory, ERR_OUT), nullptr);
	return make_ptr_buf(env, factory);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1factory_1create_1type_1from_1text(
    JNIEnv *env, jclass, jobject factory_buf, jbyteArray text_j) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	auto text = jbyteArray_to_string(env, text_j);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_logical_type_handle type = nullptr;
	CALL_V2(duckdb_v2_factory_create_type_from_text(factory, text.data(), text.size(), &type, ERR_OUT), nullptr);
	return make_logical_type_buf(env, type);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1destroy(JNIEnv *env, jclass,
                                                                                 jobject value_buf) {
	CHECK_BUF(value, value_buf, duckdb_v2_value_handle, "value");
	duckdb_v2_value_destroy(&value);
}

#define VALUE_CREATE_SCALAR(FN, JTYPE, CALL)                                                                           \
	JNIEXPORT jobject JNICALL FN(JNIEnv *env, jclass, jobject factory_buf, JTYPE in_value) {                           \
		CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);                             \
		duckdb_v2_value_handle value = nullptr;                                                                        \
		CALL_V2(CALL, nullptr);                                                                                        \
		return make_ptr_buf(env, value);                                                                               \
	}

VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1bool, jboolean,
                    duckdb_v2_value_create_bool(factory, in_value == JNI_TRUE, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1tinyint, jbyte,
                    duckdb_v2_value_create_tinyint(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1smallint, jshort,
                    duckdb_v2_value_create_smallint(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1int, jint,
                    duckdb_v2_value_create_int(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1bigint, jlong,
                    duckdb_v2_value_create_bigint(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1float, jfloat,
                    duckdb_v2_value_create_float(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1double, jdouble,
                    duckdb_v2_value_create_double(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1date, jint,
                    duckdb_v2_value_create_date(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1time, jlong,
                    duckdb_v2_value_create_time(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1timestamp, jlong,
                    duckdb_v2_value_create_timestamp(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1timestamp_1ns, jlong,
                    duckdb_v2_value_create_timestamp_ns(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1timestamp_1tz, jlong,
                    duckdb_v2_value_create_timestamp_tz(factory, in_value, &value, ERR_OUT))

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1null(JNIEnv *env, jclass,
                                                                                         jobject factory_buf,
                                                                                         jobject type_buf) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	auto type = logical_type_buf_to_logical_type(env, type_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_null(factory, type, &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1hugeint(JNIEnv *env, jclass,
                                                                                            jobject factory_buf,
                                                                                            jlong lower, jlong upper) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	duckdb_v2_hugeint_t in_value {static_cast<uint64_t>(lower), static_cast<int64_t>(upper)};
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_hugeint(factory, &in_value, &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1uuid(JNIEnv *env, jclass,
                                                                                         jobject factory_buf,
                                                                                         jlong lower, jlong upper) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	duckdb_v2_hugeint_t in_value {static_cast<uint64_t>(lower), static_cast<int64_t>(upper)};
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_uuid(factory, &in_value, &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1decimal(JNIEnv *env, jclass,
                                                                                            jobject factory_buf,
                                                                                            jlong lower, jlong upper,
                                                                                            jint width, jint scale) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	duckdb_v2_hugeint_t in_value {static_cast<uint64_t>(lower), static_cast<int64_t>(upper)};
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_decimal(factory, &in_value, static_cast<uint32_t>(width),
	                                       static_cast<uint32_t>(scale), &value, ERR_OUT),
	        nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1varchar(JNIEnv *env, jclass,
                                                                                            jobject factory_buf,
                                                                                            jbyteArray in_value_j) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	auto in_value = jbyteArray_to_string(env, in_value_j);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_varchar(factory, in_value.data(), in_value.size(), &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1blob(JNIEnv *env, jclass,
                                                                                         jobject factory_buf,
                                                                                         jbyteArray in_value_j) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	auto in_value = jbyteArray_to_string(env, in_value_j);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_blob(factory, in_value.data(), in_value.size(), &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

// The child values are borrowed for the call
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1list(JNIEnv *env, jclass,
                                                                                         jobject factory_buf,
                                                                                         jobject type_buf,
                                                                                         jobjectArray children_j) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	auto type = logical_type_buf_to_logical_type(env, type_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	std::vector<duckdb_v2_value_handle> children;
	if (!read_values(env, children_j, children)) {
		return nullptr;
	}
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_list(factory, type, children.data(), children.size(), &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1struct(JNIEnv *env, jclass,
                                                                                           jobject factory_buf,
                                                                                           jobjectArray names_j,
                                                                                           jobjectArray children_j) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	std::vector<duckdb_v2_value_handle> children;
	if (!read_values(env, children_j, children)) {
		return nullptr;
	}
	std::vector<std::string> names;
	jsize count = names_j == nullptr ? 0 : env->GetArrayLength(names_j);
	for (jsize i = 0; i < count; i++) {
		auto name_j = reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(names_j, i));
		names.push_back(jbyteArray_to_string(env, name_j));
		env->DeleteLocalRef(name_j);
		if (env->ExceptionCheck()) {
			return nullptr;
		}
	}
	if (names.size() != children.size()) {
		env->ThrowNew(J_SQLException, "Struct names and values count mismatch");
		return nullptr;
	}
	std::vector<const char *> name_ptrs;
	std::vector<idx_t> name_lens;
	for (auto &name : names) {
		name_ptrs.push_back(name.data());
		name_lens.push_back(name.size());
	}
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_struct(factory, name_ptrs.data(), name_lens.data(), children.data(), children.size(),
	                                      &value, ERR_OUT),
	        nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1map(
    JNIEnv *env, jclass, jobject factory_buf, jobject key_type_buf, jobject value_type_buf, jobjectArray keys_j,
    jobjectArray values_j) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	auto key_type = logical_type_buf_to_logical_type(env, key_type_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	auto value_type = logical_type_buf_to_logical_type(env, value_type_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	std::vector<duckdb_v2_value_handle> keys;
	std::vector<duckdb_v2_value_handle> values;
	if (!read_values(env, keys_j, keys) || !read_values(env, values_j, values)) {
		return nullptr;
	}
	if (keys.size() != values.size()) {
		env->ThrowNew(J_SQLException, "Map keys and values count mismatch");
		return nullptr;
	}
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_map(factory, key_type, value_type, keys.data(), values.data(), keys.size(), &value,
	                                   ERR_OUT),
	        nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1type(JNIEnv *env, jclass,
                                                                                         jobject factory_buf,
                                                                                         jobject type_buf) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	auto type = logical_type_buf_to_logical_type(env, type_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_type(factory, type, &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1to_1string(JNIEnv *env, jclass,
                                                                                          jobject value_buf) {
	CHECK_BUF_RET(value, value_buf, duckdb_v2_value_handle, "value", nullptr);
	idx_t length = 0;
	CALL_V2(duckdb_v2_value_to_string(value, nullptr, 0, &length, ERR_OUT), nullptr);
	std::vector<char> text(length + 1);
	CALL_V2(duckdb_v2_value_to_string(value, text.data(), text.size(), &length, ERR_OUT), nullptr);
	return make_jbyteArray(env, text.data(), length);
}

/* --- vectors --- */

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1vector_1flatten(JNIEnv *env, jclass,
                                                                                  jobject vector_buf) {
	CHECK_BUF(vector, vector_buf, duckdb_v2_vector_handle, "vector");
	CALL_V2(duckdb_v2_vector_flatten(vector, ERR_OUT), );
}

// The returned value is owned by the caller
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1vector_1get_1value(JNIEnv *env, jclass,
                                                                                        jobject vector_buf, jlong row) {
	CHECK_BUF_RET(vector, vector_buf, duckdb_v2_vector_handle, "vector", nullptr);
	idx_t idx = jlong_to_idx(env, row);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_vector_get_value(vector, idx, &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

/* --- logical type parameters --- */

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1logical_1type_1get_1param_1count(JNIEnv *env, jclass,
                                                                                                    jobject type_buf) {
	auto type = logical_type_buf_to_logical_type(env, type_buf);
	if (env->ExceptionCheck()) {
		return 0;
	}
	idx_t count = 0;
	CALL_V2(duckdb_v2_logical_type_get_param_count(type, &count, ERR_OUT), 0);
	return uint64_to_jlong(count);
}

// The returned value is owned by the caller
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1logical_1type_1get_1param_1value(JNIEnv *env,
                                                                                                      jclass,
                                                                                                      jobject type_buf,
                                                                                                      jlong index) {
	auto type = logical_type_buf_to_logical_type(env, type_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	const char *name = nullptr;
	idx_t name_len = 0;
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_logical_type_get_param(type, idx, &name, &name_len, &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

// Returns null for a positional parameter
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1logical_1type_1get_1param_1name(
    JNIEnv *env, jclass, jobject type_buf, jlong index) {
	auto type = logical_type_buf_to_logical_type(env, type_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	idx_t idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	const char *name = nullptr;
	idx_t name_len = 0;
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_logical_type_get_param(type, idx, &name, &name_len, &value, ERR_OUT), nullptr);
	duckdb_v2_value_destroy(&value);
	if (name == nullptr || name_len == 0) {
		return nullptr;
	}
	return make_jbyteArray(env, name, name_len);
}

// Unwraps the type of a TYPE value, the returned type is owned by the caller
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1get_1type(JNIEnv *env, jclass,
                                                                                      jobject value_buf) {
	CHECK_BUF_RET(value, value_buf, duckdb_v2_value_handle, "value", nullptr);
	duckdb_v2_logical_type_handle type = nullptr;
	CALL_V2(duckdb_v2_value_get_type(value, &type, ERR_OUT), nullptr);
	return make_logical_type_buf(env, type);
}

/* --- configuration --- */

// The returned config is borrowed from the connection
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1get_1config(JNIEnv *env, jclass,
                                                                                             jobject conn_buf) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	duckdb_v2_config_handle config = nullptr;
	CALL_V2(duckdb_v2_connection_get_config(conn, &config, ERR_OUT), nullptr);
	return make_ptr_buf(env, config);
}

// The returned value is owned by the caller
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1config_1get_1option_1value(JNIEnv *env, jclass,
                                                                                                jobject config_buf,
                                                                                                jbyteArray name_j) {
	CHECK_BUF_RET(config, config_buf, duckdb_v2_config_handle, "config", nullptr);
	auto name = jbyteArray_to_string(env, name_j);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_value_handle value = nullptr;
	DUCKDB_V2_SETTING_SCOPE scope = DUCKDB_V2_SETTING_SCOPE_DEFAULT;
	CALL_V2(duckdb_v2_config_get_option_value(config, name.data(), name.size(), &value, &scope, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

/* --- transitional --- */

// Wraps the result into a chunked result of the bindings API, the holder takes the ownership of the result
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1chunked_1result_1create(JNIEnv *env, jclass,
                                                                                             jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", nullptr);
	try {
		return env->NewDirectByteBuffer(new ChunkedResultHolder(result), 0);
	} catch (const std::exception &e) {
		duckdb_v2_result_destroy(&result);
		env->ThrowNew(J_SQLException, e.what());
		return nullptr;
	}
}

/* --- variants --- */

// The returned value is owned by the caller
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1variant_1to_1value(JNIEnv *env, jclass,
                                                                                               jobject value_buf) {
	CHECK_BUF_RET(value, value_buf, duckdb_v2_value_handle, "value", nullptr);
	duckdb_v2_value_handle out_value = nullptr;
	CALL_V2(duckdb_v2_value_variant_to_value(value, &out_value, ERR_OUT), nullptr);
	return make_ptr_buf(env, out_value);
}

// The returned type is owned by the caller
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1get_1logical_1type(JNIEnv *env, jclass,
                                                                                               jobject value_buf) {
	CHECK_BUF_RET(value, value_buf, duckdb_v2_value_handle, "value", nullptr);
	duckdb_v2_logical_type_handle type = nullptr;
	CALL_V2(duckdb_v2_value_get_logical_type(value, &type, ERR_OUT), nullptr);
	return make_logical_type_buf(env, type);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1vector_1set_1value(JNIEnv *env, jclass,
                                                                                     jobject vector_buf, jlong row,
                                                                                     jobject value_buf) {
	CHECK_BUF(vector, vector_buf, duckdb_v2_vector_handle, "vector");
	CHECK_BUF(value, value_buf, duckdb_v2_value_handle, "value");
	idx_t idx = jlong_to_idx(env, row);
	if (env->ExceptionCheck()) {
		return;
	}
	CALL_V2(duckdb_v2_vector_set_value(vector, idx, value, ERR_OUT), );
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1vector_1set_1size(JNIEnv *env, jclass,
                                                                                    jobject vector_buf, jlong size) {
	CHECK_BUF(vector, vector_buf, duckdb_v2_vector_handle, "vector");
	idx_t sz = jlong_to_idx(env, size);
	if (env->ExceptionCheck()) {
		return;
	}
	CALL_V2(duckdb_v2_vector_set_size(vector, sz, ERR_OUT), );
}

/* --- appending --- */

// The returned type is owned by the caller
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1logical_1type_1copy(JNIEnv *env, jclass,
                                                                                         jobject type_buf) {
	auto type = logical_type_buf_to_logical_type(env, type_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_logical_type_handle copy = nullptr;
	CALL_V2(duckdb_v2_logical_type_copy(type, &copy, ERR_OUT), nullptr);
	return make_logical_type_buf(env, copy);
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1vector_1get_1size(JNIEnv *env, jclass,
                                                                                     jobject vector_buf) {
	CHECK_BUF_RET(vector, vector_buf, duckdb_v2_vector_handle, "vector", 0);
	idx_t size = 0;
	CALL_V2(duckdb_v2_vector_get_size(vector, &size, ERR_OUT), 0);
	return uint64_to_jlong(size);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1column_1data_1collection_1create(
    JNIEnv *env, jclass, jobject factory_buf, jobjectArray types_j) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	std::vector<duckdb_v2_logical_type_handle> types;
	jsize count = types_j == nullptr ? 0 : env->GetArrayLength(types_j);
	for (jsize i = 0; i < count; i++) {
		jobject type_buf = env->GetObjectArrayElement(types_j, i);
		auto type = logical_type_buf_to_logical_type(env, type_buf);
		env->DeleteLocalRef(type_buf);
		if (env->ExceptionCheck()) {
			return nullptr;
		}
		types.push_back(type);
	}
	duckdb_v2_column_data_collection_handle collection = nullptr;
	CALL_V2(duckdb_v2_column_data_collection_create(factory, types.data(), types.size(), &collection, ERR_OUT),
	        nullptr);
	return make_ptr_buf(env, collection);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1column_1data_1collection_1destroy(
    JNIEnv *env, jclass, jobject collection_buf) {
	CHECK_BUF(collection, collection_buf, duckdb_v2_column_data_collection_handle, "collection");
	duckdb_v2_column_data_collection_destroy(&collection);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1column_1data_1collection_1clear(
    JNIEnv *env, jclass, jobject collection_buf) {
	CHECK_BUF(collection, collection_buf, duckdb_v2_column_data_collection_handle, "collection");
	CALL_V2(duckdb_v2_column_data_collection_clear(collection, ERR_OUT), );
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1column_1data_1collection_1row_1count(
    JNIEnv *env, jclass, jobject collection_buf) {
	CHECK_BUF_RET(collection, collection_buf, duckdb_v2_column_data_collection_handle, "collection", 0);
	idx_t count = 0;
	CALL_V2(duckdb_v2_column_data_collection_row_count(collection, &count, ERR_OUT), 0);
	return uint64_to_jlong(count);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1column_1data_1collection_1append_1state_1create(
    JNIEnv *env, jclass, jobject collection_buf) {
	CHECK_BUF_RET(collection, collection_buf, duckdb_v2_column_data_collection_handle, "collection", nullptr);
	duckdb_v2_column_data_collection_append_state_handle state = nullptr;
	CALL_V2(duckdb_v2_column_data_collection_append_state_create(collection, &state, ERR_OUT), nullptr);
	return make_ptr_buf(env, state);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1column_1data_1collection_1append_1state_1destroy(
    JNIEnv *env, jclass, jobject state_buf) {
	CHECK_BUF(state, state_buf, duckdb_v2_column_data_collection_append_state_handle, "append state");
	duckdb_v2_column_data_collection_append_state_destroy(&state);
}

// The chunk is borrowed for the call
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1column_1data_1collection_1append(
    JNIEnv *env, jclass, jobject collection_buf, jobject state_buf, jobject chunk_buf) {
	CHECK_BUF(collection, collection_buf, duckdb_v2_column_data_collection_handle, "collection");
	CHECK_BUF(state, state_buf, duckdb_v2_column_data_collection_append_state_handle, "append state");
	auto chunk = chunk_buf_to_chunk(env, chunk_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	CALL_V2(duckdb_v2_column_data_collection_append(collection, state, chunk, ERR_OUT), );
}

// Binds the collection to the statement under the name, the collection must outlive the statement executions
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1statement_1add_1collection(
    JNIEnv *env, jclass, jobject st_buf, jbyteArray name_j, jobject collection_buf, jobjectArray names_j) {
	CHECK_BUF(st, st_buf, duckdb_v2_sql_statement_handle, "statement");
	CHECK_BUF(collection, collection_buf, duckdb_v2_column_data_collection_handle, "collection");
	auto name = jbyteArray_to_string(env, name_j);
	if (env->ExceptionCheck()) {
		return;
	}
	std::vector<std::string> names;
	jsize count = names_j == nullptr ? 0 : env->GetArrayLength(names_j);
	for (jsize i = 0; i < count; i++) {
		auto name_arr = reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(names_j, i));
		names.push_back(jbyteArray_to_string(env, name_arr));
		env->DeleteLocalRef(name_arr);
		if (env->ExceptionCheck()) {
			return;
		}
	}
	std::vector<const char *> name_ptrs;
	std::vector<idx_t> name_lens;
	for (auto &column_name : names) {
		name_ptrs.push_back(column_name.data());
		name_lens.push_back(column_name.size());
	}
	CALL_V2(duckdb_v2_statement_add_collection(st, name.data(), name.size(), collection, name_ptrs.data(),
	                                           name_lens.data(), names.size(), ERR_OUT), );
}

/* --- environments, instances and connections --- */

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1environment_1create(JNIEnv *env, jclass) {
	duckdb_v2_environment_handle environment = nullptr;
	CALL_V2(duckdb_v2_environment_create(&environment, ERR_OUT), nullptr);
	return make_ptr_buf(env, environment);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1environment_1destroy(JNIEnv *env, jclass,
                                                                                       jobject env_buf) {
	CHECK_BUF(environment, env_buf, duckdb_v2_environment_handle, "environment");
	CALL_V2(duckdb_v2_environment_destroy(&environment), );
}

// Reads the byte arrays of the startup options
static bool read_options(JNIEnv *env, jobjectArray names_j, jobjectArray values_j, std::vector<std::string> &names,
                         std::vector<std::string> &values) {
	jsize count = names_j == nullptr ? 0 : env->GetArrayLength(names_j);
	for (jsize i = 0; i < count; i++) {
		auto name_arr = reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(names_j, i));
		auto value_arr = reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(values_j, i));
		names.push_back(jbyteArray_to_string(env, name_arr));
		values.push_back(jbyteArray_to_string(env, value_arr));
		env->DeleteLocalRef(name_arr);
		env->DeleteLocalRef(value_arr);
		if (env->ExceptionCheck()) {
			return false;
		}
	}
	return true;
}

struct OptionPointers {
	std::vector<const char *> name_ptrs;
	std::vector<idx_t> name_lens;
	std::vector<const char *> value_ptrs;
	std::vector<idx_t> value_lens;

	OptionPointers(const std::vector<std::string> &names, const std::vector<std::string> &values) {
		for (size_t i = 0; i < names.size(); i++) {
			name_ptrs.push_back(names[i].data());
			name_lens.push_back(names[i].size());
			value_ptrs.push_back(values[i].data());
			value_lens.push_back(values[i].size());
		}
	}
};

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1instance_1create_1with_1options(
    JNIEnv *env, jclass, jobject env_buf, jobjectArray names_j, jobjectArray values_j) {
	CHECK_BUF_RET(environment, env_buf, duckdb_v2_environment_handle, "environment", nullptr);
	std::vector<std::string> names;
	std::vector<std::string> values;
	if (!read_options(env, names_j, values_j, names, values)) {
		return nullptr;
	}
	OptionPointers options(names, values);
	duckdb_v2_instance_handle instance = nullptr;
	CALL_V2(duckdb_v2_instance_create_with_options(environment, options.name_ptrs.data(), options.name_lens.data(),
	                                               options.value_ptrs.data(), options.value_lens.data(), names.size(),
	                                               &instance, ERR_OUT),
	        nullptr);
	return make_ptr_buf(env, instance);
}

// The call of the Java initializer of a new shared instance
struct InstanceInitContext {
	JNIEnv *env;
	jobject callback;
};

static void instance_init_trampoline(duckdb_v2_instance_handle instance, void *user_data,
                                     duckdb_v2_error_info_handle *err) {
	auto ctx = reinterpret_cast<InstanceInitContext *>(user_data);
	JNIEnv *env = ctx->env;
	jclass callback_class = env->GetObjectClass(ctx->callback);
	jmethodID init_method = env->GetMethodID(callback_class, "init", "(Ljava/nio/ByteBuffer;)V");
	env->DeleteLocalRef(callback_class);
	if (init_method == nullptr) {
		env->ExceptionClear();
		set_callback_error(err, DUCKDB_V2_ERROR_API, "Instance initializer not found");
		return;
	}
	jobject instance_buf = make_ptr_buf(env, instance);
	env->CallVoidMethod(ctx->callback, init_method, instance_buf);
	env->DeleteLocalRef(instance_buf);
	if (env->ExceptionCheck()) {
		jthrowable exception = env->ExceptionOccurred();
		env->ExceptionClear();
		std::string message = "Instance initializer failed";
		jmethodID get_message = env->GetMethodID(env->GetObjectClass(exception), "getMessage", "()Ljava/lang/String;");
		if (get_message != nullptr) {
			auto message_j = reinterpret_cast<jstring>(env->CallObjectMethod(exception, get_message));
			if (env->ExceptionCheck()) {
				env->ExceptionClear();
			} else if (message_j != nullptr) {
				message = jstring_to_string(env, message_j);
			}
		}
		env->DeleteLocalRef(exception);
		set_callback_error(err, DUCKDB_V2_ERROR_DATABASE_CATALOG, message);
	}
}

// Opens the shared instance of the path, the initializer is called with the instance when it is created
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1instance_1open_1shared(
    JNIEnv *env, jclass, jobject env_buf, jbyteArray path_j, jobjectArray names_j, jobjectArray values_j,
    jobject init) {
	CHECK_BUF_RET(environment, env_buf, duckdb_v2_environment_handle, "environment", nullptr);
	auto path = jbyteArray_to_string(env, path_j);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	std::vector<std::string> names;
	std::vector<std::string> values;
	if (!read_options(env, names_j, values_j, names, values)) {
		return nullptr;
	}
	OptionPointers options(names, values);
	InstanceInitContext ctx {env, init};
	duckdb_v2_instance_handle instance = nullptr;
	v2_error err;
	if (duckdb_v2_instance_open_shared(environment, path.data(), path.size(), options.name_ptrs.data(),
	                                   options.name_lens.data(), options.value_ptrs.data(), options.value_lens.data(),
	                                   names.size(), init == nullptr ? nullptr : instance_init_trampoline,
	                                   init == nullptr ? nullptr : &ctx, &instance,
	                                   err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_v2_error(env, err);
		return nullptr;
	}
	return make_ptr_buf(env, instance);
}

// The name can be null
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1instance_1attach(JNIEnv *env, jclass,
                                                                                   jobject instance_buf,
                                                                                   jbyteArray name_j, jbyteArray path_j,
                                                                                   jboolean make_default) {
	CHECK_BUF(instance, instance_buf, duckdb_v2_instance_handle, "instance");
	std::string name;
	if (name_j != nullptr) {
		name = jbyteArray_to_string(env, name_j);
	}
	auto path = jbyteArray_to_string(env, path_j);
	if (env->ExceptionCheck()) {
		return;
	}
	CALL_V2(duckdb_v2_instance_attach(instance, name_j == nullptr ? nullptr : name.data(), name.size(), path.data(),
	                                  path.size(), nullptr, make_default == JNI_TRUE, ERR_OUT), );
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1instance_1destroy(JNIEnv *env, jclass,
                                                                                    jobject instance_buf) {
	CHECK_BUF(instance, instance_buf, duckdb_v2_instance_handle, "instance");
	duckdb_v2_instance_destroy(&instance);
}

// The returned config is borrowed from the instance
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1instance_1get_1config(JNIEnv *env, jclass,
                                                                                           jobject instance_buf) {
	CHECK_BUF_RET(instance, instance_buf, duckdb_v2_instance_handle, "instance", nullptr);
	duckdb_v2_config_handle config = nullptr;
	CALL_V2(duckdb_v2_instance_get_config(instance, &config, ERR_OUT), nullptr);
	return make_ptr_buf(env, config);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1create(JNIEnv *env, jclass,
                                                                                        jobject instance_buf) {
	CHECK_BUF_RET(instance, instance_buf, duckdb_v2_instance_handle, "instance", nullptr);
	duckdb_v2_connection_handle conn = nullptr;
	CALL_V2(duckdb_v2_connection_create(instance, &conn, ERR_OUT), nullptr);
	return make_ptr_buf(env, conn);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1destroy(JNIEnv *env, jclass,
                                                                                      jobject conn_buf) {
	CHECK_BUF(conn, conn_buf, duckdb_v2_connection_handle, "connection");
	duckdb_v2_connection_destroy(&conn);
}

/* --- configuration --- */

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1config_1set_1option_1text(
    JNIEnv *env, jclass, jobject config_buf, jbyteArray name_j, jbyteArray value_j, jint scope) {
	CHECK_BUF(config, config_buf, duckdb_v2_config_handle, "config");
	auto name = jbyteArray_to_string(env, name_j);
	auto value = jbyteArray_to_string(env, value_j);
	if (env->ExceptionCheck()) {
		return;
	}
	CALL_V2(duckdb_v2_config_set_option_text(config, name.data(), name.size(), value.data(), value.size(),
	                                         static_cast<DUCKDB_V2_SETTING_SCOPE>(scope), ERR_OUT), );
}

// The value is borrowed for the call
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1config_1set_1option_1value(
    JNIEnv *env, jclass, jobject config_buf, jbyteArray name_j, jobject value_buf, jint scope) {
	CHECK_BUF(config, config_buf, duckdb_v2_config_handle, "config");
	CHECK_BUF(value, value_buf, duckdb_v2_value_handle, "value");
	auto name = jbyteArray_to_string(env, name_j);
	if (env->ExceptionCheck()) {
		return;
	}
	CALL_V2(duckdb_v2_config_set_option_value(config, name.data(), name.size(), value,
	                                          static_cast<DUCKDB_V2_SETTING_SCOPE>(scope), ERR_OUT), );
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1config_1get_1option_1count(JNIEnv *env, jclass,
                                                                                              jobject config_buf) {
	CHECK_BUF_RET(config, config_buf, duckdb_v2_config_handle, "config", 0);
	idx_t count = 0;
	CALL_V2(duckdb_v2_config_get_option_count(config, &count, ERR_OUT), 0);
	return uint64_to_jlong(count);
}

// The returned option is owned by the caller
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1config_1get_1option_1by_1index(JNIEnv *env, jclass,
                                                                                                    jobject config_buf,
                                                                                                    jlong index) {
	CHECK_BUF_RET(config, config_buf, duckdb_v2_config_handle, "config", nullptr);
	idx_t idx = jlong_to_idx(env, index);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_option_handle option = nullptr;
	CALL_V2(duckdb_v2_config_get_option_by_index(config, idx, &option, ERR_OUT), nullptr);
	return make_ptr_buf(env, option);
}

JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1option_1get_1name(JNIEnv *env, jclass,
                                                                                          jobject option_buf) {
	CHECK_BUF_RET(option, option_buf, duckdb_v2_option_handle, "option", nullptr);
	const char *name = nullptr;
	idx_t name_len = 0;
	CALL_V2(duckdb_v2_option_get_name(option, &name, &name_len, ERR_OUT), nullptr);
	return make_jbyteArray(env, name, name_len);
}

JNIEXPORT jboolean JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1option_1supports_1scope(JNIEnv *env, jclass,
                                                                                              jobject option_buf,
                                                                                              jint scope) {
	CHECK_BUF_RET(option, option_buf, duckdb_v2_option_handle, "option", JNI_FALSE);
	bool supported = false;
	CALL_V2(duckdb_v2_option_supports_scope(option, static_cast<DUCKDB_V2_SETTING_SCOPE>(scope), &supported, ERR_OUT),
	        JNI_FALSE);
	return supported ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1option_1destroy(JNIEnv *env, jclass,
                                                                                  jobject option_buf) {
	CHECK_BUF(option, option_buf, duckdb_v2_option_handle, "option");
	duckdb_v2_option_destroy(&option);
}

// Reads the option value, the scope that the value comes from is written to scope_out[0] when it is given
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1config_1get_1option_1value_1scoped(
    JNIEnv *env, jclass, jobject config_buf, jbyteArray name_j, jintArray scope_out) {
	CHECK_BUF_RET(config, config_buf, duckdb_v2_config_handle, "config", nullptr);
	auto name = jbyteArray_to_string(env, name_j);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_value_handle value = nullptr;
	DUCKDB_V2_SETTING_SCOPE scope = DUCKDB_V2_SETTING_SCOPE_DEFAULT;
	CALL_V2(duckdb_v2_config_get_option_value(config, name.data(), name.size(), &value, &scope, ERR_OUT), nullptr);
	if (scope_out != nullptr) {
		jint scope_j = static_cast<jint>(scope);
		env->SetIntArrayRegion(scope_out, 0, 1, &scope_j);
	}
	return make_ptr_buf(env, value);
}

/* --- custom options --- */

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1custom_1option_1create(JNIEnv *env, jclass,
                                                                                            jobject factory_buf) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	duckdb_v2_custom_option_handle option = nullptr;
	CALL_V2(duckdb_v2_custom_option_create(factory, &option, ERR_OUT), nullptr);
	return make_ptr_buf(env, option);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1custom_1option_1destroy(JNIEnv *env, jclass,
                                                                                          jobject option_buf) {
	CHECK_BUF(option, option_buf, duckdb_v2_custom_option_handle, "custom option");
	duckdb_v2_custom_option_destroy(&option);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1custom_1option_1set_1name(JNIEnv *env, jclass,
                                                                                            jobject option_buf,
                                                                                            jbyteArray name_j) {
	CHECK_BUF(option, option_buf, duckdb_v2_custom_option_handle, "custom option");
	auto name = jbyteArray_to_string(env, name_j);
	if (env->ExceptionCheck()) {
		return;
	}
	CALL_V2(duckdb_v2_custom_option_set_name(option, name.data(), name.size(), ERR_OUT), );
}

// The type is borrowed for the call
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1custom_1option_1set_1type(JNIEnv *env, jclass,
                                                                                            jobject option_buf,
                                                                                            jobject type_buf) {
	CHECK_BUF(option, option_buf, duckdb_v2_custom_option_handle, "custom option");
	auto type = logical_type_buf_to_logical_type(env, type_buf);
	if (env->ExceptionCheck()) {
		return;
	}
	CALL_V2(duckdb_v2_custom_option_set_type(option, type, ERR_OUT), );
}

// The value is borrowed for the call
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1custom_1option_1set_1default_1value(
    JNIEnv *env, jclass, jobject option_buf, jobject value_buf) {
	CHECK_BUF(option, option_buf, duckdb_v2_custom_option_handle, "custom option");
	CHECK_BUF(value, value_buf, duckdb_v2_value_handle, "value");
	CALL_V2(duckdb_v2_custom_option_set_default_value(option, value, ERR_OUT), );
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1custom_1option_1set_1description(
    JNIEnv *env, jclass, jobject option_buf, jbyteArray description_j) {
	CHECK_BUF(option, option_buf, duckdb_v2_custom_option_handle, "custom option");
	auto description = jbyteArray_to_string(env, description_j);
	if (env->ExceptionCheck()) {
		return;
	}
	CALL_V2(duckdb_v2_custom_option_set_description(option, description.data(), description.size(), ERR_OUT), );
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1custom_1option_1set_1default_1scope(
    JNIEnv *env, jclass, jobject option_buf, jint scope) {
	CHECK_BUF(option, option_buf, duckdb_v2_custom_option_handle, "custom option");
	CALL_V2(duckdb_v2_custom_option_set_default_scope(option, static_cast<DUCKDB_V2_SETTING_SCOPE>(scope), ERR_OUT), );
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1custom_1option_1set_1supports_1scope(
    JNIEnv *env, jclass, jobject option_buf, jint scope, jboolean supported) {
	CHECK_BUF(option, option_buf, duckdb_v2_custom_option_handle, "custom option");
	CALL_V2(duckdb_v2_custom_option_set_supports_scope(option, static_cast<DUCKDB_V2_SETTING_SCOPE>(scope),
	                                                   supported == JNI_TRUE, ERR_OUT), );
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1register_1custom_1option(
    JNIEnv *env, jclass, jobject conn_buf, jobject option_buf) {
	CHECK_BUF(conn, conn_buf, duckdb_v2_connection_handle, "connection");
	CHECK_BUF(option, option_buf, duckdb_v2_custom_option_handle, "custom option");
	CALL_V2(duckdb_v2_connection_register_custom_option(conn, option, ERR_OUT), );
}

/* --- profiling --- */

static void text_sink(const char *text, idx_t text_length, void *user_data, duckdb_v2_error_info_handle *) {
	static_cast<std::string *>(user_data)->assign(text, text_length);
}

// The profiling information of the last query of the connection in the format ("" is the default one)
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1get_1profiling_1information(
    JNIEnv *env, jclass, jobject conn_buf, jbyteArray format_j) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	auto format = jbyteArray_to_string(env, format_j);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	std::string text;
	CALL_V2(
	    duckdb_v2_connection_get_profiling_information(conn, format.data(), format.size(), text_sink, &text, ERR_OUT),
	    nullptr);
	return make_jbyteArray(env, text.data(), text.size());
}

/* --- reading strings --- */

// Copies the strings (or BLOBs) of the rows of the vector data, that do not fit into the 16 bytes of the entry, into
// one array. The string of the i-th row is at [offsets_out[i], offsets_out[i + 1]) of the result. The short strings are
// read from the data buffer on Java side, without any call.
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1vector_1get_1long_1strings(
    JNIEnv *env, jclass, jobject data_buf, jintArray rows_j, jint count, jintArray offsets_out) {
	if (data_buf == nullptr || rows_j == nullptr || offsets_out == nullptr || count < 0) {
		env->ThrowNew(J_SQLException, "Invalid strings buffer");
		return nullptr;
	}
	auto data = reinterpret_cast<duckdb_v2_bytes *>(env->GetDirectBufferAddress(data_buf));
	if (data == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid vector data");
		return nullptr;
	}
	std::vector<jint> rows(static_cast<size_t>(count));
	env->GetIntArrayRegion(rows_j, 0, count, rows.data());
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	std::vector<jint> offsets(static_cast<size_t>(count) + 1);
	uint64_t total = 0;
	for (jint i = 0; i < count; i++) {
		offsets[static_cast<size_t>(i)] = static_cast<jint>(total);
		total += v2_bytes_length(data[rows[static_cast<size_t>(i)]]);
		if (total > static_cast<uint64_t>(INT32_MAX)) {
			env->ThrowNew(J_SQLException, "The strings of the vector are too large");
			return nullptr;
		}
	}
	offsets[static_cast<size_t>(count)] = static_cast<jint>(total);
	jbyteArray res = env->NewByteArray(static_cast<jsize>(total));
	if (res == nullptr) {
		return nullptr;
	}
	if (total > 0) {
		auto dst = static_cast<char *>(env->GetPrimitiveArrayCritical(res, nullptr));
		if (dst == nullptr) {
			return nullptr;
		}
		for (jint i = 0; i < count; i++) {
			auto &bytes = data[rows[static_cast<size_t>(i)]];
			std::memcpy(dst + offsets[static_cast<size_t>(i)], v2_bytes_data(bytes), v2_bytes_length(bytes));
		}
		env->ReleasePrimitiveArrayCritical(res, dst, 0);
	}
	env->SetIntArrayRegion(offsets_out, 0, count + 1, offsets.data());
	return res;
}

// Casts the rows of the source vector to the type of the target vector, the target is replaced by the result
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1vector_1cast(JNIEnv *env, jclass, jobject factory_buf,
                                                                               jobject source_buf, jobject target_buf) {
	CHECK_BUF(factory, factory_buf, duckdb_v2_factory_handle, "factory");
	CHECK_BUF(source, source_buf, duckdb_v2_vector_handle, "vector");
	CHECK_BUF(target, target_buf, duckdb_v2_vector_handle, "vector");
	bool converted = false;
	CALL_V2(duckdb_v2_vector_cast(factory, source, target, DUCKDB_V2_CAST_MODE_NORMAL, &converted, nullptr, ERR_OUT), );
}

/* --- bulk reads --- */

// The names of all the fields of the schema
JNIEXPORT jobjectArray JNICALL
Java_org_duckdb_DuckDBBindings_duckdb_1v2_1schema_1get_1field_1names(JNIEnv *env, jclass, jobject schema_buf) {
	CHECK_BUF_RET(schema, schema_buf, duckdb_v2_schema_handle, "schema", nullptr);
	idx_t count = 0;
	CALL_V2(duckdb_v2_schema_get_count(schema, &count, ERR_OUT), nullptr);
	jobjectArray res = env->NewObjectArray(static_cast<jsize>(count), J_ByteArray, nullptr);
	if (res == nullptr) {
		return nullptr;
	}
	for (idx_t i = 0; i < count; i++) {
		const char *name = nullptr;
		idx_t name_len = 0;
		duckdb_v2_logical_type_handle type = nullptr;
		CALL_V2(duckdb_v2_schema_get_field(schema, i, &name, &name_len, &type, ERR_OUT), nullptr);
		jbyteArray name_j = make_jbyteArray(env, name, name_len);
		if (name_j == nullptr) {
			return nullptr;
		}
		env->SetObjectArrayElement(res, static_cast<jsize>(i), name_j);
		env->DeleteLocalRef(name_j);
	}
	return res;
}

// The type IDs (of the C API v1) of all the fields of the schema
JNIEXPORT jintArray JNICALL
Java_org_duckdb_DuckDBBindings_duckdb_1v2_1schema_1get_1field_1type_1ids(JNIEnv *env, jclass, jobject schema_buf) {
	CHECK_BUF_RET(schema, schema_buf, duckdb_v2_schema_handle, "schema", nullptr);
	idx_t count = 0;
	CALL_V2(duckdb_v2_schema_get_count(schema, &count, ERR_OUT), nullptr);
	std::vector<jint> ids(count);
	for (idx_t i = 0; i < count; i++) {
		const char *name = nullptr;
		idx_t name_len = 0;
		duckdb_v2_logical_type_handle type = nullptr;
		CALL_V2(duckdb_v2_schema_get_field(schema, i, &name, &name_len, &type, ERR_OUT), nullptr);
		ids[i] = static_cast<jint>(logical_type_to_capi(type));
	}
	jintArray res = env->NewIntArray(static_cast<jsize>(count));
	if (res != nullptr && count > 0) {
		env->SetIntArrayRegion(res, 0, static_cast<jsize>(count), ids.data());
	}
	return res;
}

// Reads a BOOLEAN option, returns 1 or 0, or -1 if the option is not set or is not a BOOLEAN
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1config_1get_1bool_1option(JNIEnv *env, jclass,
                                                                                            jobject config_buf,
                                                                                            jbyteArray name_j) {
	CHECK_BUF_RET(config, config_buf, duckdb_v2_config_handle, "config", -1);
	auto name = jbyteArray_to_string(env, name_j);
	if (env->ExceptionCheck()) {
		return -1;
	}
	v2_value value;
	DUCKDB_V2_SETTING_SCOPE scope = DUCKDB_V2_SETTING_SCOPE_DEFAULT;
	if (duckdb_v2_config_get_option_value(config, name.data(), name.size(), value.out(), &scope, nullptr) !=
	    DUCKDB_V2_ERROR_NONE) {
		return -1;
	}
	bool res = false;
	if (duckdb_v2_value_get_bool(value.get(), &res, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return -1;
	}
	return res ? 1 : 0;
}
