#include "bindings.hpp"
#include "holders.hpp"
#include "util.hpp"

#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

/*
 * The C API v2 does not have an appender, the appender of the stable C++ API is used. The connection, data chunks and
 * logical types are passed between Java and JNI as C API v2 handles.
 */

namespace {

struct AppenderHolder {
	//! The appender does not outlive the connection that it is created on
	std::shared_ptr<cxx::Connection> connection;
	std::optional<cxx::Appender> appender;
	std::string error;
	//! The qualified name of the table, to look up the default values of its columns
	std::string catalog;
	std::string schema;
	std::string table;
	//! The SQL texts of the default values of the columns, the lookup is done when the first default is appended
	std::vector<std::optional<std::string>> defaults;
	bool defaults_loaded = false;
};

std::string quote_identifier(const std::string &name) {
	std::string res = "\"";
	for (char c : name) {
		if (c == '"') {
			res += "\"\"";
		} else {
			res += c;
		}
	}
	res += "\"";
	return res;
}

std::string quote_literal(const std::string &value) {
	std::string res = "'";
	for (char c : value) {
		if (c == '\'') {
			res += "''";
		} else {
			res += c;
		}
	}
	res += "'";
	return res;
}

AppenderHolder *appender_buf_to_appender(JNIEnv *env, jobject appender_buf) {
	if (appender_buf == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid appender buffer");
		return nullptr;
	}

	auto appender = reinterpret_cast<AppenderHolder *>(env->GetDirectBufferAddress(appender_buf));
	if (appender == nullptr) {
		env->ThrowNew(J_SQLException, "Invalid appender");
		return nullptr;
	}

	return appender;
}

//! Wraps the chunk that is owned by the Java side, it is not destroyed with the wrapper
cxx::DataChunk borrow_chunk(duckdb_v2_data_chunk_handle chunk) {
	return cxx::detail::HandleFactory::Make<cxx::DataChunk>(static_cast<void *>(chunk), false);
}

void load_defaults(AppenderHolder &app) {
	if (app.defaults_loaded) {
		return;
	}
	auto &conn = *app.connection;
	// the default values of all columns of the table, in the order of the columns
	std::string sql = "SELECT column_default FROM duckdb_columns() WHERE table_name = " + quote_literal(app.table);
	if (!app.schema.empty()) {
		sql += " AND schema_name = " + quote_literal(app.schema);
	}
	if (!app.catalog.empty()) {
		sql += " AND database_name = " + quote_literal(app.catalog);
	}
	sql += " ORDER BY column_index";
	auto result = conn.Execute(sql);
	while (true) {
		auto chunk = result.FetchChunk();
		if (!chunk) {
			break;
		}
		auto vec = chunk.GetVector(0);
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			auto value = vec.GetValue(i);
			if (value.IsNull()) {
				app.defaults.emplace_back(std::nullopt);
			} else {
				app.defaults.emplace_back(value.ToText());
			}
		}
	}
	app.defaults_loaded = true;
}

} // namespace

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

	ConnectionHolder *conn_holder = get_connection_ref(env, connection);
	if (env->ExceptionCheck()) {
		return -1;
	}

	std::string catalog_str = jbyteArray_to_string(env, catalog);
	if (env->ExceptionCheck()) {
		return -1;
	}
	std::string schema_str = jbyteArray_to_string(env, schema);
	if (env->ExceptionCheck()) {
		return -1;
	}
	std::string table_str = jbyteArray_to_string(env, table);
	if (env->ExceptionCheck()) {
		return -1;
	}
	if (nullptr == table) {
		return CAPI_STATE_ERROR;
	}

	check_out_param(env, out_appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	auto holder = std::make_unique<AppenderHolder>();
	holder->connection = conn_holder->connection;
	if (nullptr != catalog) {
		holder->catalog = catalog_str;
	}
	if (nullptr != schema) {
		holder->schema = schema_str;
	}
	holder->table = table_str;

	std::string qualified_name;
	if (nullptr != catalog) {
		qualified_name += quote_identifier(catalog_str) + ".";
	}
	if (nullptr != schema) {
		qualified_name += quote_identifier(schema_str) + ".";
	}
	qualified_name += quote_identifier(table_str);

	try {
		holder->appender.emplace(*holder->connection, qualified_name);
	} catch (const std::exception &) {
		return CAPI_STATE_ERROR;
	}

	jobject appender_ref_buf = env->NewDirectByteBuffer(holder.release(), 0);
	set_out_param(env, out_appender, appender_ref_buf);

	return CAPI_STATE_SUCCESS;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_error
 * Signature: (Ljava/nio/ByteBuffer;)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1error(JNIEnv *env, jclass,
                                                                                    jobject appender) {

	auto app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	if (app->error.empty()) {
		return nullptr;
	}

	return make_jbyteArray(env, app->error.c_str(), static_cast<idx_t>(app->error.size()));
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_flush
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1flush(JNIEnv *env, jclass, jobject appender) {

	auto app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	try {
		app->appender->Flush();
	} catch (const std::exception &e) {
		app->error = e.what();
		return CAPI_STATE_ERROR;
	}
	return CAPI_STATE_SUCCESS;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_close
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1close(JNIEnv *env, jclass, jobject appender) {

	auto app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	try {
		app->appender->Flush();
	} catch (const std::exception &e) {
		app->error = e.what();
		return CAPI_STATE_ERROR;
	}
	return CAPI_STATE_SUCCESS;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_destroy
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1destroy(JNIEnv *env, jclass, jobject appender) {

	auto app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	delete app;

	return CAPI_STATE_SUCCESS;
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_column_count
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1column_1count(JNIEnv *env, jclass,
                                                                                       jobject appender) {

	auto app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	return static_cast<jlong>(app->appender->ColumnTypes().size());
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_appender_column_type
 * Signature: (Ljava/nio/ByteBuffer;J)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1appender_1column_1type(JNIEnv *env, jclass,
                                                                                        jobject appender,
                                                                                        jlong col_idx) {

	auto app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	idx_t idx = jlong_to_idx(env, col_idx);
	if (env->ExceptionCheck()) {
		return nullptr;
	}

	auto &types = app->appender->ColumnTypes();
	if (idx >= types.size()) {
		return make_logical_type_buf(env, nullptr);
	}

	// the type is passed to Java as an owned handle
	duckdb_v2_logical_type_handle copy = nullptr;
	if (duckdb_v2_logical_type_copy(types[idx].handle(), &copy, nullptr) != DUCKDB_V2_ERROR_NONE) {
		return make_logical_type_buf(env, nullptr);
	}
	return make_logical_type_buf(env, copy);
}

/*
 * Class:     org_duckdb_DuckDBBindings
 * Method:    duckdb_append_data_chunk
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1append_1data_1chunk(JNIEnv *env, jclass, jobject appender,
                                                                                  jobject chunk) {

	auto app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}

	auto raw_chunk = chunk_buf_to_chunk(env, chunk);
	if (env->ExceptionCheck()) {
		return -1;
	}
	auto dc = borrow_chunk(raw_chunk);

	try {
		app->appender->AppendChunk(dc);
	} catch (const std::exception &e) {
		app->error = e.what();
		return CAPI_STATE_ERROR;
	}
	return CAPI_STATE_SUCCESS;
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

	auto app = appender_buf_to_appender(env, appender);
	if (env->ExceptionCheck()) {
		return -1;
	}
	auto raw_chunk = chunk_buf_to_chunk(env, chunk);
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

	try {
		load_defaults(*app);
		auto dc = borrow_chunk(raw_chunk);
		auto vec = dc.GetVector(col_idx);
		// the size of the vector is set by the caller when the chunk is complete
		if (vec.GetSize() <= row_idx) {
			vec.SetSize(row_idx + 1);
		}
		if (col_idx < app->defaults.size() && app->defaults[col_idx]) {
			// the default value is an expression, that is evaluated every time as it can be volatile
			auto result = app->connection->Execute("SELECT " + *app->defaults[col_idx]);
			auto value_chunk = result.FetchChunk();
			if (!value_chunk || value_chunk.GetRowCount() == 0) {
				throw JdbcException("Invalid Input Error: Unable to evaluate the default value");
			}
			auto value = value_chunk.GetVector(0).GetValue(0);
			result.Drain();
			vec.SetValue(row_idx, value);
		} else {
			// a column without a default value gets the NULL default
			vec.SetNull(row_idx);
		}
	} catch (const std::exception &e) {
		app->error = e.what();
		return CAPI_STATE_ERROR;
	}
	return CAPI_STATE_SUCCESS;
}
