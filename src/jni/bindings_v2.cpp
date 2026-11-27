// Thin JNI entry points over the C API v2 calls, one entry point per call. They contain no driver logic:
// the logic that uses them is implemented in Java.
#include "bindings.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "util.hpp"

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

// Transitional: the C API v2 handle of the connection of a connection holder
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1from_1ref(JNIEnv *env, jclass, jobject conn_ref) {
	auto conn = conn_ref_buf_to_conn(env, conn_ref);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	return make_ptr_buf(env, conn);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1interrupt(JNIEnv *env, jclass, jobject conn_buf) {
	CHECK_BUF(conn, conn_buf, duckdb_v2_connection_handle, "connection");
	CALL_V2(duckdb_v2_connection_interrupt(conn, ERR_OUT), );
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1progress_1get(JNIEnv *env, jclass, jobject conn_buf) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	double percentage = -1;
	uint64_t rows_processed = 0;
	uint64_t total_rows = 0;
	CALL_V2(duckdb_v2_connection_progress_get(conn, &percentage, &rows_processed, &total_rows, ERR_OUT), nullptr);
	return env->NewObject(J_QueryProgress, J_QueryProgress_init, static_cast<jdouble>(percentage),
	                      uint64_to_jlong(rows_processed), uint64_to_jlong(total_rows));
}

/* --- parsing --- */

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1parse_1sql(JNIEnv *env, jclass, jobject conn_buf, jbyteArray sql_j) {
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
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1statement_1iterator_1next(JNIEnv *env, jclass, jobject it_buf) {
	CHECK_BUF_RET(it, it_buf, duckdb_v2_statement_iterator_handle, "statement iterator", nullptr);
	duckdb_v2_sql_statement_handle statement = nullptr;
	CALL_V2(duckdb_v2_statement_iterator_next(it, &statement, ERR_OUT), nullptr);
	return make_ptr_buf(env, statement);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1statement_1iterator_1destroy(JNIEnv *env, jclass, jobject it_buf) {
	CHECK_BUF(it, it_buf, duckdb_v2_statement_iterator_handle, "statement iterator");
	duckdb_v2_statement_iterator_destroy(&it);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1sql_1statement_1destroy(JNIEnv *env, jclass, jobject st_buf) {
	CHECK_BUF(st, st_buf, duckdb_v2_sql_statement_handle, "statement");
	duckdb_v2_sql_statement_destroy(&st);
}

JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1sql_1statement_1get_1type(JNIEnv *env, jclass, jobject st_buf) {
	CHECK_BUF_RET(st, st_buf, duckdb_v2_sql_statement_handle, "statement", 0);
	DUCKDB_V2_STATEMENT_TYPE type = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	CALL_V2(duckdb_v2_sql_statement_get_type(st, &type, ERR_OUT), 0);
	return static_cast<jint>(type);
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1sql_1statement_1get_1parameter_1count(JNIEnv *env, jclass, jobject st_buf) {
	CHECK_BUF_RET(st, st_buf, duckdb_v2_sql_statement_handle, "statement", 0);
	idx_t count = 0;
	CALL_V2(duckdb_v2_sql_statement_get_parameter_count(st, &count, ERR_OUT), 0);
	return uint64_to_jlong(count);
}

/* --- prepared statements --- */

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1prepared_1statement_1create(JNIEnv *env, jclass, jobject conn_buf, jobject st_buf,
                                                                jboolean require_cacheable) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	CHECK_BUF_RET(st, st_buf, duckdb_v2_sql_statement_handle, "statement", nullptr);
	duckdb_v2_prepared_statement_handle prepared = nullptr;
	CALL_V2(duckdb_v2_prepared_statement_create(conn, st, require_cacheable == JNI_TRUE, &prepared, ERR_OUT), nullptr);
	return make_ptr_buf(env, prepared);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1prepared_1statement_1destroy(JNIEnv *env, jclass, jobject prepared_buf) {
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

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1prepared_1statement_1execute(JNIEnv *env, jclass, jobject prepared_buf,
                                                                 jobjectArray values_j) {
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

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1statement_1execute(JNIEnv *env, jclass, jobject conn_buf, jobject st_buf,
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
JNIEXPORT jobjectArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1statement_1bind(JNIEnv *env, jclass, jobject conn_buf, jobject st_buf) {
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

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1schema_1destroy(JNIEnv *env, jclass, jobject schema_buf) {
	CHECK_BUF(schema, schema_buf, duckdb_v2_schema_handle, "schema");
	duckdb_v2_schema_destroy(&schema);
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1schema_1get_1count(JNIEnv *env, jclass, jobject schema_buf) {
	CHECK_BUF_RET(schema, schema_buf, duckdb_v2_schema_handle, "schema", 0);
	idx_t count = 0;
	CALL_V2(duckdb_v2_schema_get_count(schema, &count, ERR_OUT), 0);
	return uint64_to_jlong(count);
}

JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1schema_1get_1field_1name(JNIEnv *env, jclass, jobject schema_buf, jlong index) {
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
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1schema_1get_1field_1type(JNIEnv *env, jclass, jobject schema_buf, jlong index) {
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

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1destroy(JNIEnv *env, jclass, jobject result_buf) {
	CHECK_BUF(result, result_buf, duckdb_v2_result_handle, "result");
	duckdb_v2_result_destroy(&result);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1get_1schema(JNIEnv *env, jclass, jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", nullptr);
	duckdb_v2_schema_handle schema = nullptr;
	CALL_V2(duckdb_v2_result_get_schema(result, &schema, ERR_OUT), nullptr);
	return make_ptr_buf(env, schema);
}

JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1get_1result_1type(JNIEnv *env, jclass, jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", 0);
	DUCKDB_V2_RESULT_TYPE type = DUCKDB_V2_RESULT_TYPE_NOTHING;
	CALL_V2(duckdb_v2_result_get_result_type(result, &type, ERR_OUT), 0);
	return static_cast<jint>(type);
}

JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1get_1statement_1type(JNIEnv *env, jclass, jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", 0);
	DUCKDB_V2_STATEMENT_TYPE type = DUCKDB_V2_STATEMENT_TYPE_INVALID;
	CALL_V2(duckdb_v2_result_get_statement_type(result, &type, ERR_OUT), 0);
	return static_cast<jint>(type);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1wait(JNIEnv *env, jclass, jobject result_buf) {
	CHECK_BUF(result, result_buf, duckdb_v2_result_handle, "result");
	CALL_V2(duckdb_v2_result_wait(result, ERR_OUT), );
}

JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1drain(JNIEnv *env, jclass, jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", 0);
	idx_t rows_changed = 0;
	CALL_V2(duckdb_v2_result_drain(result, &rows_changed, ERR_OUT), 0);
	return uint64_to_jlong(rows_changed);
}

// Steps the result once, the status (DUCKDB_V2_RESULT_STEP_STATUS) is written to status_out[0], the returned chunk is
// null unless the status is CHUNK
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1step(JNIEnv *env, jclass, jobject result_buf, jintArray status_out) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", nullptr);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	DUCKDB_V2_RESULT_STEP_STATUS status = DUCKDB_V2_RESULT_STEP_STATUS_WAITING;
	CALL_V2(duckdb_v2_result_step(result, &chunk, &status, ERR_OUT), nullptr);
	jint status_j = static_cast<jint>(status);
	env->SetIntArrayRegion(status_out, 0, 1, &status_j);
	return make_data_chunk_buf(env, chunk);
}

// Returns null at the end of the result
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1result_1fetch_1chunk(JNIEnv *env, jclass, jobject result_buf) {
	CHECK_BUF_RET(result, result_buf, duckdb_v2_result_handle, "result", nullptr);
	duckdb_v2_data_chunk_handle chunk = nullptr;
	CALL_V2(duckdb_v2_result_fetch_chunk(result, &chunk, ERR_OUT), nullptr);
	return make_data_chunk_buf(env, chunk);
}

/* --- factory and values --- */

// The factory is borrowed from the connection
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1connection_1get_1factory(JNIEnv *env, jclass, jobject conn_buf) {
	CHECK_BUF_RET(conn, conn_buf, duckdb_v2_connection_handle, "connection", nullptr);
	duckdb_v2_factory_handle factory = nullptr;
	CALL_V2(duckdb_v2_connection_get_factory(conn, &factory, ERR_OUT), nullptr);
	return make_ptr_buf(env, factory);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1factory_1create_1type_1from_1text(JNIEnv *env, jclass, jobject factory_buf,
                                                                    jbyteArray text_j) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	auto text = jbyteArray_to_string(env, text_j);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_logical_type_handle type = nullptr;
	CALL_V2(duckdb_v2_factory_create_type_from_text(factory, text.data(), text.size(), &type, ERR_OUT), nullptr);
	return make_logical_type_buf(env, type);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1destroy(JNIEnv *env, jclass, jobject value_buf) {
	CHECK_BUF(value, value_buf, duckdb_v2_value_handle, "value");
	duckdb_v2_value_destroy(&value);
}

#define VALUE_CREATE_SCALAR(FN, JTYPE, CALL)                                                                         \
	JNIEXPORT jobject JNICALL FN(JNIEnv *env, jclass, jobject factory_buf, JTYPE in_value) {                                           \
		CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);                             \
		duckdb_v2_value_handle value = nullptr;                                                                        \
		CALL_V2(CALL, nullptr);                                                                                        \
		return make_ptr_buf(env, value);                                                                               \
	}

VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1bool, jboolean, duckdb_v2_value_create_bool(factory, in_value == JNI_TRUE, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1tinyint, jbyte, duckdb_v2_value_create_tinyint(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1smallint, jshort, duckdb_v2_value_create_smallint(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1int, jint, duckdb_v2_value_create_int(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1bigint, jlong, duckdb_v2_value_create_bigint(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1float, jfloat, duckdb_v2_value_create_float(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1double, jdouble, duckdb_v2_value_create_double(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1date, jint, duckdb_v2_value_create_date(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1time, jlong, duckdb_v2_value_create_time(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1timestamp, jlong, duckdb_v2_value_create_timestamp(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1timestamp_1ns, jlong, duckdb_v2_value_create_timestamp_ns(factory, in_value, &value, ERR_OUT))
VALUE_CREATE_SCALAR(Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1timestamp_1tz, jlong, duckdb_v2_value_create_timestamp_tz(factory, in_value, &value, ERR_OUT))

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1null(JNIEnv *env, jclass, jobject factory_buf, jobject type_buf) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	auto type = logical_type_buf_to_logical_type(env, type_buf);
	if (env->ExceptionCheck()) {
		return nullptr;
	}
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_null(factory, type, &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1hugeint(JNIEnv *env, jclass, jobject factory_buf, jlong lower,
                                                           jlong upper) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	duckdb_v2_hugeint_t in_value {static_cast<uint64_t>(lower), static_cast<int64_t>(upper)};
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_hugeint(factory, &in_value, &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1uuid(JNIEnv *env, jclass, jobject factory_buf, jlong lower,
                                                        jlong upper) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	duckdb_v2_hugeint_t in_value {static_cast<uint64_t>(lower), static_cast<int64_t>(upper)};
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_uuid(factory, &in_value, &value, ERR_OUT), nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1decimal(JNIEnv *env, jclass, jobject factory_buf, jlong lower,
                                                           jlong upper, jint width, jint scale) {
	CHECK_BUF_RET(factory, factory_buf, duckdb_v2_factory_handle, "factory", nullptr);
	duckdb_v2_hugeint_t in_value {static_cast<uint64_t>(lower), static_cast<int64_t>(upper)};
	duckdb_v2_value_handle value = nullptr;
	CALL_V2(duckdb_v2_value_create_decimal(factory, &in_value, static_cast<uint32_t>(width),
	                                       static_cast<uint32_t>(scale), &value, ERR_OUT),
	        nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1varchar(JNIEnv *env, jclass, jobject factory_buf,
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

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1blob(JNIEnv *env, jclass, jobject factory_buf,
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
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1list(JNIEnv *env, jclass, jobject factory_buf, jobject type_buf,
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

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1struct(JNIEnv *env, jclass, jobject factory_buf,
                                                          jobjectArray names_j, jobjectArray children_j) {
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
	CALL_V2(duckdb_v2_value_create_struct(factory, name_ptrs.data(), name_lens.data(), children.data(),
	                                      children.size(), &value, ERR_OUT),
	        nullptr);
	return make_ptr_buf(env, value);
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1value_1create_1map(JNIEnv *env, jclass, jobject factory_buf, jobject key_type_buf,
                                                       jobject value_type_buf, jobjectArray keys_j,
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
