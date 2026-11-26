#include "arrow.hpp"
#include "bindings.hpp"
#include "config.hpp"
#include "functions.hpp"
#include "holders.hpp"
#include "refs.hpp"
#include "types.hpp"
#include "util.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>

using namespace std;

static jint JNI_VERSION = JNI_VERSION_1_6;

void ThrowJNI(JNIEnv *env, const char *message) {
	env->ThrowNew(J_SQLException, message);
}

void ThrowJNIException(JNIEnv *env, const std::exception &e) {
	if (dynamic_cast<const cxx::InterruptException *>(&e) != nullptr) {
		env->ThrowNew(J_SQLTimeoutException, e.what());
		return;
	}
	env->ThrowNew(J_SQLException, e.what());
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
	JNIEnv *env;
	if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
		return JNI_ERR;
	}

	try {
		create_refs(env);
	} catch (const std::exception &e) {
		if (!env->ExceptionCheck()) {
			auto re_class = env->FindClass("java/lang/RuntimeException");
			if (nullptr != re_class) {
				env->ThrowNew(re_class, e.what());
			}
		}
		return JNI_ERR;
	}

	return JNI_VERSION;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *vm, void *reserved) {
	JNIEnv *env;
	if (vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION) != JNI_OK) {
		return;
	}
	delete_global_refs(env);
}

//! Quotes the string for the use in SQL
static std::string quote_literal(const std::string &value) {
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

//! Quotes the identifier for the use in SQL
static std::string quote_identifier(const std::string &name) {
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

//! The connection of the statement or result, throws if it was closed
static std::shared_ptr<cxx::Connection> lock_connection(const std::weak_ptr<cxx::Connection> &connection) {
	auto res = connection.lock();
	if (!res) {
		throw JdbcException("Invalid Input Error: Attempting to use a result or a statement after its connection was "
		                    "closed!");
	}
	return res;
}

//! The lock of the connection of the statement or result, throws if the connection was closed
static std::shared_ptr<std::mutex> lock_connection_mutex(const std::weak_ptr<std::mutex> &lock) {
	auto res = lock.lock();
	if (!res) {
		throw JdbcException("Invalid Input Error: Attempting to use a result or a statement after its connection was "
		                    "closed!");
	}
	return res;
}

//! Reads the value of the first row and the first column of the query result as a string
static std::string query_single_string(cxx::Connection &conn, const std::string &sql) {
	auto result = conn.Execute(sql);
	std::string res;
	bool found = false;
	while (true) {
		auto chunk = result.FetchChunk();
		if (!chunk) {
			break;
		}
		if (!found && chunk.GetRowCount() > 0) {
			auto value = chunk.GetVector(0).GetValue(0);
			if (!value.IsNull()) {
				res = value.ToText();
			}
			found = true;
		}
	}
	return res;
}

jobject _duckdb_jdbc_startup(JNIEnv *env, jclass, jbyteArray database_j, jboolean read_only, jobject props,
                             jboolean cache_instance) {
	auto database = jbyteArray_to_string(env, database_j);
	auto instance = open_instance(env, database, read_only, props, cache_instance);
	auto conn_ref = new ConnectionHolder(std::move(instance));

	return env->NewDirectByteBuffer(conn_ref, 0);
}

jobject _duckdb_jdbc_connect(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection_ref(env, conn_ref_buf);
	auto conn = std::make_unique<ConnectionHolder>(conn_ref->db);

	// the session settings of the new connection are the same as the ones of this connection
	copy_session_settings(*conn_ref->connection, *conn->connection);
	return env->NewDirectByteBuffer(conn.release(), 0);
}

jobject _duckdb_jdbc_create_db_ref(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection_ref(env, conn_ref_buf);
	auto db_ref = conn_ref->create_db_ref();
	return env->NewDirectByteBuffer(db_ref, 0);
}

jlong _duckdb_jdbc_db_address(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_ref = get_connection_ref(env, conn_ref_buf);
	return instance_address(*conn_ref->db);
}

void _duckdb_jdbc_destroy_db_ref(JNIEnv *env, jclass, jobject db_ref_buf) {
	if (nullptr == db_ref_buf) {
		return;
	}
	auto db_ref = (DBHolder *)env->GetDirectBufferAddress(db_ref_buf);
	if (db_ref) {
		delete db_ref;
	}
}

jstring _duckdb_jdbc_get_schema(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	std::lock_guard<std::mutex> guard(*conn_holder->lock);
	auto schema = query_single_string(*conn_holder->connection, "SELECT current_schema()");
	return env->NewStringUTF(schema.c_str());
}

void _duckdb_jdbc_set_schema(JNIEnv *env, jclass, jobject conn_ref_buf, jstring schema) {
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	std::lock_guard<std::mutex> guard(*conn_holder->lock);
	conn_holder->connection->Execute("SET schema = " + quote_literal(jstring_to_string(env, schema))).Drain();
}

void _duckdb_jdbc_set_catalog(JNIEnv *env, jclass, jobject conn_ref_buf, jstring catalog) {
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	std::lock_guard<std::mutex> guard(*conn_holder->lock);
	conn_holder->connection->Execute("USE " + quote_identifier(jstring_to_string(env, catalog))).Drain();
}

jstring _duckdb_jdbc_get_catalog(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	std::lock_guard<std::mutex> guard(*conn_holder->lock);
	auto catalog = query_single_string(*conn_holder->connection, "SELECT current_database()");
	return env->NewStringUTF(catalog.c_str());
}

void _duckdb_jdbc_set_auto_commit(JNIEnv *env, jclass, jobject conn_ref_buf, jboolean auto_commit) {
	// the auto-commit mode is handled by the Java side with the explicit transactions, the connection is always in
	// the auto-commit mode here
	get_connection(env, conn_ref_buf);
}

jboolean _duckdb_jdbc_get_auto_commit(JNIEnv *env, jclass, jobject conn_ref_buf) {
	get_connection(env, conn_ref_buf);
	return true;
}

void _duckdb_jdbc_interrupt(JNIEnv *env, jclass, jobject conn_ref_buf) {
	get_connection(env, conn_ref_buf).Interrupt();
}

jobject _duckdb_jdbc_query_progress(JNIEnv *env, jclass, jobject conn_ref_buf) {
	auto progress = get_connection(env, conn_ref_buf).GetQueryProgress();
	return env->NewObject(J_QueryProgress, J_QueryProgress_init, static_cast<jdouble>(progress.percentage),
	                      uint64_to_jlong(progress.rows_processed), uint64_to_jlong(progress.total_rows_to_process));
}

void _duckdb_jdbc_disconnect(JNIEnv *env, jclass, jobject conn_ref_buf) {
	if (nullptr == conn_ref_buf) {
		return;
	}
	auto conn_ref = (ConnectionHolder *)env->GetDirectBufferAddress(conn_ref_buf);
	if (conn_ref) {
		delete conn_ref;
	}
}

//! Parses the query and returns its last statement. The statements before it are run, unless the query is prepared:
//! preparing must not have side effects, so a query with multiple statements is rejected then.
static cxx::SqlStatement extract_last_statement(cxx::Connection &conn, const std::string &query, bool is_prepare) {
	auto iterator = conn.ParseSQL(query);
	std::vector<cxx::SqlStatement> statements;
	while (true) {
		auto statement = iterator.Next();
		if (!statement) {
			break;
		}
		statements.push_back(std::move(statement));
	}
	if (statements.empty()) {
		throw JdbcException("Invalid Input Error: No statements to execute.");
	}

	if (is_prepare && statements.size() > 1) {
		throw JdbcException("Invalid Input Error: Cannot prepare multiple statements at once, statements count: " +
		                    std::to_string(statements.size()));
	}

	// if there are multiple statements, we directly execute the statements besides the last one
	// we only return the result of the last statement to the user, unless one of the previous statements fails
	for (size_t i = 0; i + 1 < statements.size(); i++) {
		conn.Execute(statements[i]).Drain();
	}
	return std::move(statements.back());
}

jobject _duckdb_jdbc_prepare(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray query_j) {
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	auto &conn = get_connection(env, conn_ref_buf);
	std::lock_guard<std::mutex> guard(*conn_holder->lock);

	auto query = jbyteArray_to_string(env, query_j);

	auto stmt_ref = std::make_unique<StatementHolder>();
	stmt_ref->connection = conn_holder->connection;
	stmt_ref->lock = conn_holder->lock;
	stmt_ref->statement.emplace(extract_last_statement(conn, query, true));
	stmt_ref->statement_type = stmt_ref->statement->GetStatementType();
	stmt_ref->stmt.emplace(conn.Prepare(*stmt_ref->statement));
	stmt_ref->signature.emplace(conn.Bind(*stmt_ref->statement));
	return env->NewDirectByteBuffer(stmt_ref.release(), 0);
}

jobject _duckdb_jdbc_pending_query(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray query_j) {
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	auto &conn = get_connection(env, conn_ref_buf);
	std::lock_guard<std::mutex> guard(*conn_holder->lock);

	auto query = jbyteArray_to_string(env, query_j);

	auto statement = extract_last_statement(conn, query, false);

	bool stream_results = conn.GetConfig().GetOption("jdbc_stream_results").value.Get<bool>();

	auto pending_ref = std::make_unique<PendingHolder>();
	pending_ref->connection = conn_holder->connection;
	pending_ref->lock = conn_holder->lock;
	pending_ref->pending.emplace(conn.Execute(statement));
	pending_ref->streaming = stream_results;

	return env->NewDirectByteBuffer(pending_ref.release(), 0);
}

static std::string result_type_to_string(cxx::ResultType type) {
	switch (type) {
	case cxx::ResultType::QUERY_RESULT:
		return "QUERY_RESULT";
	case cxx::ResultType::CHANGED_ROWS:
		return "CHANGED_ROWS";
	default:
		return "NOTHING";
	}
}

//! Reads the schema and the result type of the result, fails if the result has an error
static void read_result_metadata(ResultHolder &holder, cxx::QueryResult &result) {
	auto schema = result.GetSchema();
	holder.names.clear();
	holder.types.clear();
	for (idx_t i = 0; i < schema.GetFieldCount(); i++) {
		holder.names.emplace_back(schema.GetFieldName(i));
		holder.types.push_back(schema.GetFieldType(i));
	}
	holder.result_type = result.GetResultType();
}

//! Whether the result is streamed instead of being read into the memory, streaming is only used for
//! the SELECT statements
static bool streams_by_default(cxx::StatementType written_type, cxx::ResultType result_type) {
	return written_type == cxx::StatementType::SELECT && result_type == cxx::ResultType::QUERY_RESULT;
}

//! Completes the execution of the result into the holder: the rows are read into the memory, or the result is kept
//! to be streamed
static std::unique_ptr<ResultHolder> complete_result(cxx::Connection &conn,
                                                     const std::weak_ptr<cxx::Connection> &conn_weak,
                                                     cxx::QueryResult result, cxx::StatementType written_type,
                                                     bool stream_results_user) {
	auto holder = std::make_unique<ResultHolder>();
	holder->connection = conn_weak;

	// the first chunk is read to find out if the query is running without errors, also to complete the metadata
	auto first_chunk = result.FetchChunk();
	read_result_metadata(*holder, result);

	bool stream_results = stream_results_user && streams_by_default(written_type, holder->result_type);
	if (stream_results) {
		if (first_chunk) {
			holder->first_chunk.emplace(std::move(first_chunk));
		}
		holder->stream.emplace(std::move(result));
		return holder;
	}

	auto &factory = conn.GetFactory();
	holder->materialized = std::make_unique<MaterializedData>();
	auto &data = *holder->materialized;
	data.collection.emplace(factory.CreateColumnDataCollection(holder->types));
	auto append_state = data.collection->CreateAppendState();
	auto chunk = std::move(first_chunk);
	while (chunk) {
		data.collection->Append(append_state, chunk);
		chunk = result.FetchChunk();
	}
	data.shared_state.emplace(data.collection->CreateSharedScanState());
	data.worker_state.emplace(data.collection->CreateWorkerScanState());
	return holder;
}

static cxx::QueryResult execute_prepared_statement(JNIEnv *env, StatementHolder &stmt_ref, jobjectArray params,
                                                   cxx::Connection &conn) {
	idx_t param_len = env->GetArrayLength(params);

	if (param_len != stmt_ref.signature->parameters.GetFieldCount()) {
		throw JdbcException("Invalid Input Error: Parameter count mismatch");
	}

	std::vector<cxx::Value> duckdb_params;
	for (idx_t i = 0; i < param_len; i++) {
		auto param = env->GetObjectArrayElement(params, i);
		auto val = to_duckdb_value(env, param, conn.GetFactory());
		env->DeleteLocalRef(param);
		duckdb_params.push_back(std::move(val));
	}

	return stmt_ref.stmt->Execute(duckdb_params);
}

static StatementHolder &get_statement(JNIEnv *env, jobject stmt_ref_buf) {
	auto stmt_ref = reinterpret_cast<StatementHolder *>(env->GetDirectBufferAddress(stmt_ref_buf));
	if (!stmt_ref || !stmt_ref->stmt) {
		throw JdbcException("Invalid Input Error: Invalid statement");
	}
	return *stmt_ref;
}

jobject _duckdb_jdbc_execute(JNIEnv *env, jclass, jobject stmt_ref_buf, jobjectArray params) {
	auto &stmt_ref = get_statement(env, stmt_ref_buf);
	auto conn = stmt_ref.connection.lock();
	if (!conn) {
		throw JdbcException("Invalid Input Error: Attempting to execute a prepared statement after its connection was "
		                    "closed!");
	}
	auto conn_lock = lock_connection_mutex(stmt_ref.lock);
	std::lock_guard<std::mutex> guard(*conn_lock);
	bool stream_results_user = conn->GetConfig().GetOption("jdbc_stream_results").value.Get<bool>();

	auto result = execute_prepared_statement(env, stmt_ref, params, *conn);
	auto holder =
	    complete_result(*conn, stmt_ref.connection, std::move(result), stmt_ref.statement_type, stream_results_user);
	return env->NewDirectByteBuffer(holder.release(), 0);
}

jobject _duckdb_jdbc_execute_capi(JNIEnv *env, jclass, jobject stmt_ref_buf, jobjectArray params) {
	auto &stmt_ref = get_statement(env, stmt_ref_buf);
	auto conn = lock_connection(stmt_ref.connection);
	auto conn_lock = lock_connection_mutex(stmt_ref.lock);
	std::lock_guard<std::mutex> guard(*conn_lock);

	idx_t param_len = env->GetArrayLength(params);
	if (param_len != stmt_ref.signature->parameters.GetFieldCount()) {
		throw JdbcException("Invalid Input Error: Parameter count mismatch");
	}
	std::vector<cxx::Value> values;
	std::vector<duckdb_v2_value_handle> value_handles;
	for (idx_t i = 0; i < param_len; i++) {
		auto param = env->GetObjectArrayElement(params, i);
		values.push_back(to_duckdb_value(env, param, conn->GetFactory()));
		env->DeleteLocalRef(param);
		value_handles.push_back(values.back().handle());
	}

	// the result is read through the C API v2 by the Java side
	v2_result result;
	v2_error err;
	if (duckdb_v2_prepared_statement_execute(stmt_ref.stmt->handle(), nullptr, nullptr, value_handles.data(),
	                                         static_cast<idx_t>(value_handles.size()), result.out(),
	                                         err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw JdbcException(err.message());
	}
	auto out = std::make_unique<ChunkedResultHolder>(result.release());
	// the first chunk is read to find out if the query is running without errors
	if (duckdb_v2_result_fetch_chunk(out->result.get(), out->first_chunk.out(), err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw JdbcException(err.message());
	}
	out->has_first_chunk = true;
	return env->NewDirectByteBuffer(out.release(), 0);
}

jobject _duckdb_jdbc_execute_pending(JNIEnv *env, jclass, jobject pending_ref_buf) {
	auto pending_ref = reinterpret_cast<PendingHolder *>(env->GetDirectBufferAddress(pending_ref_buf));
	if (!pending_ref || !pending_ref->pending) {
		throw JdbcException("Invalid Input Error: Invalid pending query");
	}

	auto conn = lock_connection(pending_ref->connection);
	auto conn_lock = lock_connection_mutex(pending_ref->lock);
	std::lock_guard<std::mutex> guard(*conn_lock);
	auto result = std::move(*pending_ref->pending);
	pending_ref->pending.reset();

	// the pending result was submitted from the SQL statement as written, so its type is the written type
	auto written_type = result.GetStatementType();
	auto holder =
	    complete_result(*conn, pending_ref->connection, std::move(result), written_type, pending_ref->streaming);
	return env->NewDirectByteBuffer(holder.release(), 0);
}

void _duckdb_jdbc_release(JNIEnv *env, jclass, jobject stmt_ref_buf) {
	if (nullptr == stmt_ref_buf) {
		return;
	}
	auto stmt_ref = reinterpret_cast<StatementHolder *>(env->GetDirectBufferAddress(stmt_ref_buf));
	if (stmt_ref) {
		delete stmt_ref;
	}
}

void _duckdb_jdbc_release_pending(JNIEnv *env, jclass, jobject pending_ref_buf) {
	if (nullptr == pending_ref_buf) {
		return;
	}
	auto pending_ref = reinterpret_cast<PendingHolder *>(env->GetDirectBufferAddress(pending_ref_buf));
	if (pending_ref) {
		delete pending_ref;
	}
}

void _duckdb_jdbc_free_result(JNIEnv *env, jclass, jobject res_ref_buf) {
	if (nullptr == res_ref_buf) {
		return;
	}
	auto res_ref = reinterpret_cast<ResultHolder *>(env->GetDirectBufferAddress(res_ref_buf));
	if (res_ref) {
		delete res_ref;
	}
}

static jobject build_meta(JNIEnv *env, size_t column_count, size_t n_param, const std::vector<std::string> &names,
                          const std::vector<cxx::LogicalType> &types, const std::string &return_type_str,
                          const std::vector<cxx::LogicalType> &param_types) {
	auto name_array = env->NewObjectArray(column_count, J_String, nullptr);
	auto type_array = env->NewObjectArray(column_count, J_String, nullptr);
	auto type_detail_array = env->NewObjectArray(column_count, J_String, nullptr);

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		std::string col_name;
		if (types[col_idx].GetTypeId() == cxx::LogicalTypeId::ENUM) {
			col_name = "ENUM";
		} else {
			col_name = types[col_idx].ToText();
		}

		const std::string &sname = names[col_idx];
		jstring jname = decode_charbuffer_to_jstring(env, sname.c_str(), sname.length());
		env->SetObjectArrayElement(name_array, col_idx, jname);
		env->DeleteLocalRef(jname);
		jstring jcolname = env->NewStringUTF(col_name.c_str());
		env->SetObjectArrayElement(type_array, col_idx, jcolname);
		env->DeleteLocalRef(jcolname);
		jstring jtype = env->NewStringUTF(type_to_jduckdb_type(types[col_idx]).c_str());
		env->SetObjectArrayElement(type_detail_array, col_idx, jtype);
		env->DeleteLocalRef(jtype);
	}

	jobjectArray param_type_array = env->NewObjectArray(n_param, J_String, nullptr);
	jobjectArray param_type_detail_array = env->NewObjectArray(n_param, J_String, nullptr);

	for (idx_t param_idx = 0; param_idx < n_param; param_idx++) {
		std::string param_name;
		if (param_types[param_idx].GetTypeId() == cxx::LogicalTypeId::ENUM) {
			param_name = "ENUM";
		} else {
			param_name = param_types[param_idx].ToText();
		}

		jstring jname = env->NewStringUTF(param_name.c_str());
		env->SetObjectArrayElement(param_type_array, param_idx, jname);
		env->DeleteLocalRef(jname);
		jstring jdetail = env->NewStringUTF(type_to_jduckdb_type(param_types[param_idx]).c_str());
		env->SetObjectArrayElement(param_type_detail_array, param_idx, jdetail);
		env->DeleteLocalRef(jdetail);
	}

	jstring return_type = env->NewStringUTF(return_type_str.c_str());

	jobject res = env->NewObject(J_DuckResultSetMeta, J_DuckResultSetMeta_init, n_param, column_count, name_array,
	                             type_array, type_detail_array, return_type, param_type_array, param_type_detail_array);
	env->DeleteLocalRef(return_type);
	env->DeleteLocalRef(param_type_detail_array);
	env->DeleteLocalRef(param_type_array);
	env->DeleteLocalRef(type_detail_array);
	env->DeleteLocalRef(type_array);
	env->DeleteLocalRef(name_array);
	return res;
}

static ResultHolder &get_result(JNIEnv *env, jobject res_ref_buf) {
	auto res_ref = reinterpret_cast<ResultHolder *>(env->GetDirectBufferAddress(res_ref_buf));
	if (!res_ref || (!res_ref->stream && !res_ref->materialized)) {
		throw JdbcException("Invalid Input Error: Invalid result set");
	}
	return *res_ref;
}

jobject _duckdb_jdbc_query_result_meta(JNIEnv *env, jclass, jobject res_ref_buf) {
	auto &res_ref = get_result(env, res_ref_buf);
	std::vector<cxx::LogicalType> param_types; // no params now
	std::vector<cxx::LogicalType> types;
	for (auto &type : res_ref.types) {
		types.push_back(type.Copy());
	}
	return build_meta(env, res_ref.types.size(), 0, res_ref.names, types, result_type_to_string(res_ref.result_type),
	                  param_types);
}

//! The return type of a prepared statement is not known before it is executed, it is derived from its columns
static std::string prepared_return_type(const StatementHolder &stmt_ref) {
	auto &output = stmt_ref.signature->output;
	if (output.GetFieldCount() == 0) {
		return "NOTHING";
	}
	if (output.GetFieldCount() == 1 && output.GetFieldName(0) == "Count" &&
	    output.GetFieldType(0).GetTypeId() == cxx::LogicalTypeId::BIGINT) {
		switch (stmt_ref.statement_type) {
		case cxx::StatementType::INSERT:
		case cxx::StatementType::UPDATE:
		case cxx::StatementType::DELETE:
		case cxx::StatementType::COPY:
		case cxx::StatementType::MERGE_INTO:
			return "CHANGED_ROWS";
		default:
			break;
		}
	}
	return "QUERY_RESULT";
}

jobject _duckdb_jdbc_prepared_statement_meta(JNIEnv *env, jclass, jobject stmt_ref_buf) {
	auto &stmt_ref = get_statement(env, stmt_ref_buf);

	auto &signature = *stmt_ref.signature;
	auto n_param = signature.parameters.GetFieldCount();
	std::vector<cxx::LogicalType> param_types;
	for (idx_t i = 0; i < n_param; i++) {
		param_types.push_back(signature.parameters.GetFieldType(i));
	}
	std::vector<std::string> names;
	std::vector<cxx::LogicalType> types;
	for (idx_t i = 0; i < signature.output.GetFieldCount(); i++) {
		names.emplace_back(signature.output.GetFieldName(i));
		types.push_back(signature.output.GetFieldType(i));
	}

	return build_meta(env, types.size(), n_param, names, types, prepared_return_type(stmt_ref), param_types);
}

jobject ProcessVector(JNIEnv *env, cxx::Factory &factory, cxx::Vector &vec, idx_t row_count);

jobjectArray _duckdb_jdbc_fetch(JNIEnv *env, jclass, jobject res_ref_buf, jobject conn_ref_buf) {
	auto &res_ref = get_result(env, res_ref_buf);
	auto &conn = get_connection(env, conn_ref_buf);
	auto &factory = conn.GetFactory();

	res_ref.chunk.reset();
	if (res_ref.materialized) {
		auto &data = *res_ref.materialized;
		auto chunk = factory.CreateDataChunk(res_ref.types);
		if (data.collection->Scan(*data.shared_state, *data.worker_state, chunk) && chunk.GetRowCount() > 0) {
			res_ref.chunk.emplace(std::move(chunk));
		}
	} else if (res_ref.first_chunk) {
		res_ref.chunk.emplace(std::move(*res_ref.first_chunk));
		res_ref.first_chunk.reset();
	} else {
		auto chunk = res_ref.stream->FetchChunk();
		if (chunk) {
			res_ref.chunk.emplace(std::move(chunk));
		}
	}

	if (!res_ref.chunk) {
		return env->NewObjectArray(0, J_DuckVector, nullptr);
	}
	auto row_count = res_ref.chunk->GetRowCount();
	auto column_count = res_ref.chunk->GetVectorCount();
	jobjectArray vec_array = reinterpret_cast<jobjectArray>(env->NewObjectArray(column_count, J_DuckVector, nullptr));

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		auto vec = res_ref.chunk->GetVector(col_idx);

		jobject jvec = ProcessVector(env, factory, vec, row_count);

		env->SetObjectArrayElement(vec_array, col_idx, jvec);
		env->DeleteLocalRef(jvec);
	}

	return vec_array;
}

//! Casts the vector to strings, the chunk that holds the result vector is returned to keep the result alive
static cxx::DataChunk cast_to_strings(cxx::Factory &factory, const cxx::Vector &vec) {
	std::vector<cxx::LogicalType> types;
	types.push_back(factory.CreateType(cxx::LogicalTypeId::VARCHAR));
	auto chunk = factory.CreateDataChunk(types);
	auto target = chunk.GetVector(0);
	vec.Cast(factory, target);
	target.Flatten();
	return chunk;
}

jobjectArray _duckdb_jdbc_cast_result_to_strings(JNIEnv *env, jclass, jobject res_ref_buf, jobject conn_ref_buf,
                                                 jlong col_idx) {
	auto &res_ref = get_result(env, res_ref_buf);
	if (!res_ref.chunk) {
		return nullptr;
	}
	auto &conn = get_connection(env, conn_ref_buf);

	auto row_count = res_ref.chunk->GetRowCount();
	auto complex_vec = res_ref.chunk->GetVector(static_cast<idx_t>(col_idx));
	auto cast_chunk = cast_to_strings(conn.GetFactory(), complex_vec);
	auto vec = cast_chunk.GetVector(0);
	auto view = vec.GetView();

	jobjectArray string_data = env->NewObjectArray(row_count, J_String, nullptr);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (!view.IsValid(row_idx)) {
			continue;
		}
		auto d_str = view.Data<cxx::varchar_t>()[view.SelAt(row_idx)].view();
		auto j_str = decode_charbuffer_to_jstring(env, d_str.data(), d_str.size());
		env->SetObjectArrayElement(string_data, row_idx, j_str);
	}

	return string_data;
}

template <typename T>
static jobject make_vec_data_buf(JNIEnv *env, cxx::Vector &vec, idx_t row_count) {
	T *vec_data = vec.GetDataMutable<T>();
	idx_t len = row_count * sizeof(T);
	return make_data_buf(env, vec_data, len);
}

//! The type of the entries of a MAP, that are STRUCTs of the keys and the values
static cxx::LogicalType map_entry_type(cxx::Factory &factory, const cxx::LogicalType &map_type) {
	return factory.ParseType("STRUCT(\"key\" " + map_type.GetMapKeyType().ToText() + ", \"value\" " +
	                         map_type.GetMapValueType().ToText() + ")");
}

//! Creates a Java vector of the specified type from a single value
static jobject process_value(JNIEnv *env, cxx::Factory &factory, const cxx::Value &value);

static jobject process_vector_with_type(JNIEnv *env, cxx::Factory &factory, cxx::Vector &vec,
                                        const cxx::LogicalType &type, idx_t row_count);

jobject ProcessVector(JNIEnv *env, cxx::Factory &factory, cxx::Vector &vec, idx_t row_count) {
	auto type = vec.GetLogicalType();
	return process_vector_with_type(env, factory, vec, type, row_count);
}

static jobject process_vector_with_type(JNIEnv *env, cxx::Factory &factory, cxx::Vector &vec,
                                        const cxx::LogicalType &type, idx_t row_count) {
	using cxx::LogicalTypeId;
	auto type_id = type.GetTypeId();
	bool is_variant = type_id == LogicalTypeId::VARIANT;
	if (vec.GetVectorType() != cxx::VectorType::FLAT && !is_variant) {
		vec.Flatten();
	}
	cxx::VectorView view {};
	if (!is_variant) {
		view = vec.GetView();
	}
	jstring type_str = env->NewStringUTF(type_to_jduckdb_type(type).c_str());
	// construct nullmask
	jbooleanArray null_array = env->NewBooleanArray(row_count);
	jboolean *null_unique_array = env->GetBooleanArrayElements(null_array, nullptr);
	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		if (is_variant) {
			null_unique_array[row_idx] = vec.GetValue(row_idx).IsNull();
		} else {
			null_unique_array[row_idx] = !view.IsValid(row_idx);
		}
	}
	env->ReleaseBooleanArrayElements(null_array, null_unique_array, 0);

	jobject jvec = env->NewObject(J_DuckVector, J_DuckVector_init, type_str, static_cast<int>(row_count), null_array);

	jobject constlen_data = nullptr;
	jobjectArray varlen_data = nullptr;
	// the source vector must not be modified, it is a part of the result chunk that can be cast again
	// in getString() (with this vector as a child of a nested type)
	std::optional<cxx::DataChunk> cast_chunk;
	std::optional<cxx::Vector> cast_vec;
	cxx::VectorView string_view_data = view;

	switch (type_id) {
	case LogicalTypeId::BOOLEAN:
		constlen_data = make_vec_data_buf<bool>(env, vec, row_count);
		break;
	case LogicalTypeId::TINYINT:
		constlen_data = make_vec_data_buf<int8_t>(env, vec, row_count);
		break;
	case LogicalTypeId::SMALLINT:
		constlen_data = make_vec_data_buf<int16_t>(env, vec, row_count);
		break;
	case LogicalTypeId::INTEGER:
		constlen_data = make_vec_data_buf<int32_t>(env, vec, row_count);
		break;
	case LogicalTypeId::BIGINT:
		constlen_data = make_vec_data_buf<int64_t>(env, vec, row_count);
		break;
	case LogicalTypeId::UTINYINT:
		constlen_data = make_vec_data_buf<uint8_t>(env, vec, row_count);
		break;
	case LogicalTypeId::USMALLINT:
		constlen_data = make_vec_data_buf<uint16_t>(env, vec, row_count);
		break;
	case LogicalTypeId::UINTEGER:
		constlen_data = make_vec_data_buf<uint32_t>(env, vec, row_count);
		break;
	case LogicalTypeId::UBIGINT:
		constlen_data = make_vec_data_buf<uint64_t>(env, vec, row_count);
		break;
	case LogicalTypeId::HUGEINT:
		constlen_data = make_vec_data_buf<cxx::int128_t>(env, vec, row_count);
		break;
	case LogicalTypeId::UHUGEINT:
		constlen_data = make_vec_data_buf<cxx::uint128_t>(env, vec, row_count);
		break;
	case LogicalTypeId::FLOAT:
		constlen_data = make_vec_data_buf<float>(env, vec, row_count);
		break;
	case LogicalTypeId::DECIMAL: {
		switch (type.GetDecimalInternalTypeId()) {
		case LogicalTypeId::SMALLINT:
			constlen_data = make_vec_data_buf<int16_t>(env, vec, row_count);
			break;
		case LogicalTypeId::INTEGER:
			constlen_data = make_vec_data_buf<int32_t>(env, vec, row_count);
			break;
		case LogicalTypeId::BIGINT:
			constlen_data = make_vec_data_buf<int64_t>(env, vec, row_count);
			break;
		case LogicalTypeId::HUGEINT:
			constlen_data = make_vec_data_buf<cxx::int128_t>(env, vec, row_count);
			break;
		default:
			throw JdbcException("Internal Error: Unimplemented physical type for decimal");
		}
		break;
	}
	case LogicalTypeId::DOUBLE:
		constlen_data = make_vec_data_buf<double>(env, vec, row_count);
		break;
	case LogicalTypeId::DATE:
		constlen_data = make_vec_data_buf<cxx::date_t>(env, vec, row_count);
		break;
	case LogicalTypeId::TIME:
		constlen_data = make_vec_data_buf<cxx::dtime_t>(env, vec, row_count);
		break;
	case LogicalTypeId::TIME_NS:
		constlen_data = make_vec_data_buf<cxx::dtime_ns_t>(env, vec, row_count);
		break;
	case LogicalTypeId::TIME_TZ:
		constlen_data = make_vec_data_buf<cxx::dtime_tz_t>(env, vec, row_count);
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		constlen_data = make_vec_data_buf<cxx::timestamp_t>(env, vec, row_count);
		break;
	case LogicalTypeId::ENUM:
		varlen_data = env->NewObjectArray(row_count, J_String, nullptr);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (!view.IsValid(row_idx)) {
				continue;
			}
			auto dstr = vec.GetValue(row_idx).ToText();
			jstring jstr = env->NewStringUTF(dstr.c_str());
			env->SetObjectArrayElement(varlen_data, row_idx, jstr);
			env->DeleteLocalRef(jstr);
		}
		break;
	case LogicalTypeId::UNION:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TUPLE: {
		varlen_data = env->NewObjectArray(row_count, J_DuckStruct, nullptr);

		idx_t child_count = vec.GetChildCount();
		auto columns = env->NewObjectArray(child_count, J_DuckVector, nullptr);
		auto names = env->NewObjectArray(child_count, J_String, nullptr);

		for (idx_t entry_i = 0; entry_i < child_count; entry_i++) {
			auto child = vec.GetChild(entry_i);
			std::string name;
			if (type_id == LogicalTypeId::UNION) {
				// the first entry of a union is its tag, that has no name
				name = entry_i == 0 ? "" : type.GetUnionMemberName(entry_i - 1);
			} else {
				name = type.GetStructChildName(entry_i);
				// TUPLE members are unnamed, use the positional names synthesized by DuckDB: "element1", ...
				if (name.empty() && type_id == LogicalTypeId::TUPLE) {
					name = "element" + std::to_string(entry_i + 1);
				}
			}
			jobject j_vec = ProcessVector(env, factory, child, row_count);
			env->SetObjectArrayElement(columns, entry_i, j_vec);
			env->DeleteLocalRef(j_vec);
			jstring jstr = env->NewStringUTF(name.c_str());
			env->SetObjectArrayElement(names, entry_i, jstr);
			env->DeleteLocalRef(jstr);
		}
		auto type_text = type.ToText();
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			jstring jstr = env->NewStringUTF(type_text.c_str());
			jobject jstruct = env->NewObject(J_DuckStruct, J_DuckStruct_init, names, columns, row_idx, jstr);
			env->SetObjectArrayElement(varlen_data, row_idx, jstruct);
			env->DeleteLocalRef(jstruct);
			env->DeleteLocalRef(jstr);
		}
		env->DeleteLocalRef(names);
		env->DeleteLocalRef(columns);

		break;
	}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::GEOMETRY:
		varlen_data = env->NewObjectArray(row_count, J_ByteArray, nullptr);

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (!view.IsValid(row_idx)) {
				continue;
			}

			auto d_str = view.Data<cxx::blob_t>()[view.SelAt(row_idx)].view();

			auto j_arr = env->NewByteArray(d_str.size());
			auto j_arr_el = env->GetByteArrayElements(j_arr, nullptr);
			memcpy(j_arr_el, d_str.data(), d_str.size());
			env->ReleaseByteArrayElements(j_arr, j_arr_el, 0);

			env->SetObjectArrayElement(varlen_data, row_idx, j_arr);
			env->DeleteLocalRef(j_arr);
		}
		break;
	case LogicalTypeId::UUID:
		constlen_data = make_vec_data_buf<cxx::uuid_t>(env, vec, row_count);
		break;
	case LogicalTypeId::ARRAY: {
		varlen_data = env->NewObjectArray(row_count, J_DuckArray, nullptr);
		auto array_vector = vec.GetChild(0);
		auto limit = type.GetArraySize();
		auto total_size = row_count * limit;
		jobject j_vec = ProcessVector(env, factory, array_vector, total_size);

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (!view.IsValid(row_idx)) {
				continue;
			}

			auto offset = row_idx * limit;

			auto j_obj = env->NewObject(J_DuckArray, J_DuckArray_init, j_vec, offset, limit);

			env->SetObjectArrayElement(varlen_data, row_idx, j_obj);
			env->DeleteLocalRef(j_obj);
		}
		env->DeleteLocalRef(j_vec);
		break;
	}
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST: {
		varlen_data = env->NewObjectArray(row_count, J_DuckArray, nullptr);

		auto list_entries = view.Data<cxx::list_entry_t>();

		// the child vector has the capacity of the list, the size is the end of the last list
		idx_t list_size = 0;
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (!view.IsValid(row_idx)) {
				continue;
			}
			auto &entry = list_entries[view.SelAt(row_idx)];
			list_size = std::max<idx_t>(list_size, entry.offset + entry.length);
		}
		auto list_vector = vec.GetChild(0);
		jobject j_vec;
		if (type_id == LogicalTypeId::MAP) {
			j_vec = process_vector_with_type(env, factory, list_vector, map_entry_type(factory, type), list_size);
		} else {
			j_vec = process_vector_with_type(env, factory, list_vector, type.GetListChildType(), list_size);
		}

		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (!view.IsValid(row_idx)) {
				continue;
			}

			auto offset = list_entries[view.SelAt(row_idx)].offset;
			auto limit = list_entries[view.SelAt(row_idx)].length;

			auto j_obj = env->NewObject(J_DuckArray, J_DuckArray_init, j_vec, offset, limit);

			env->SetObjectArrayElement(varlen_data, row_idx, j_obj);
			env->DeleteLocalRef(j_obj);
		}
		env->DeleteLocalRef(j_vec);
		break;
	}
	case LogicalTypeId::VARIANT: {
		varlen_data = env->NewObjectArray(row_count, J_Object, nullptr);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			auto variant_val = vec.GetValue(row_idx).VariantToValue();
			if (variant_val.IsNull()) {
				continue;
			}
			jobject variant_j_vec = process_value(env, factory, variant_val);
			env->SetObjectArrayElement(varlen_data, row_idx, variant_j_vec);
			env->DeleteLocalRef(variant_j_vec);
		}
		break;
	}
	default: {
		cast_chunk.emplace(cast_to_strings(factory, vec));
		cast_vec.emplace(cast_chunk->GetVector(0));
		string_view_data = cast_vec->GetView();
		// fall through on purpose
	}
	case LogicalTypeId::VARCHAR:
		varlen_data = env->NewObjectArray(row_count, J_String, nullptr);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			if (!string_view_data.IsValid(row_idx)) {
				continue;
			}
			auto d_str = string_view_data.Data<cxx::varchar_t>()[string_view_data.SelAt(row_idx)].view();
			auto j_str = decode_charbuffer_to_jstring(env, d_str.data(), d_str.size());
			env->SetObjectArrayElement(varlen_data, row_idx, j_str);
			env->DeleteLocalRef(j_str);
		}
		break;
	}

	env->SetObjectField(jvec, J_DuckVector_constlen, constlen_data);
	if (constlen_data != nullptr) {
		env->DeleteLocalRef(constlen_data);
	}
	env->SetObjectField(jvec, J_DuckVector_varlen, varlen_data);
	if (varlen_data != nullptr) {
		env->DeleteLocalRef(varlen_data);
	}

	env->DeleteLocalRef(null_array);
	env->DeleteLocalRef(type_str);

	return jvec;
}

static jobject process_value(JNIEnv *env, cxx::Factory &factory, const cxx::Value &value) {
	auto type = value.GetLogicalType();
	std::vector<cxx::LogicalType> types;
	types.push_back(type.Copy());
	auto chunk = factory.CreateDataChunk(types);
	auto vec = chunk.GetVector(0);
	vec.SetSize(1);
	vec.SetValue(0, value);
	jobject j_vec = ProcessVector(env, factory, vec, 1);
	// the fixed-size data of the Java vector points into the chunk, that is destroyed here, so it is copied
	env->CallVoidMethod(j_vec, J_DuckVector_retainConstlenData);
	check_java_exception_and_rethrow(env);
	return j_vec;
}

jint _duckdb_jdbc_fetch_size(JNIEnv *env, jclass cls) {
	// the size of the chunks of the engine
	return static_cast<jint>(Java_org_duckdb_DuckDBBindings_duckdb_1vector_1size(env, cls));
}

/**
 * An appender of the rows that are appended one value at a time. The values are collected into a data chunk
 * that is appended to the table when it is full or the appender is flushed.
 */
struct RowAppender {
	std::shared_ptr<cxx::Connection> connection;
	std::optional<cxx::Appender> appender;
	std::optional<cxx::DataChunk> chunk;
	std::vector<cxx::LogicalTypeId> column_ids;
	idx_t capacity = 0;
	//! The index of the row that is being appended, and of the column to append to
	idx_t row = 0;
	idx_t column = 0;
	bool in_row = false;
	bool closed = false;

	cxx::Factory &factory() {
		return connection->GetFactory();
	}

	void reset_chunk() {
		chunk->Clear();
		for (idx_t i = 0; i < column_ids.size(); i++) {
			chunk->GetVector(i).SetSize(capacity);
		}
		row = 0;
		column = 0;
	}

	//! Appends the collected rows to the table
	void collect() {
		if (row == 0) {
			return;
		}
		for (idx_t i = 0; i < column_ids.size(); i++) {
			chunk->GetVector(i).SetSize(row);
		}
		try {
			appender->AppendChunk(*chunk);
		} catch (...) {
			reset_chunk();
			throw;
		}
		reset_chunk();
	}
};

jobject _duckdb_jdbc_create_appender(JNIEnv *env, jclass, jobject conn_ref_buf, jbyteArray schema_name_j,
                                     jbyteArray table_name_j) {
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	auto &conn = get_connection(env, conn_ref_buf);
	auto schema_name = jbyteArray_to_string(env, schema_name_j);
	auto table_name = jbyteArray_to_string(env, table_name_j);

	auto appender = std::make_unique<RowAppender>();
	appender->connection = conn_holder->connection;
	std::string table = schema_name.empty() ? quote_identifier(table_name)
	                                        : quote_identifier(schema_name) + "." + quote_identifier(table_name);
	appender->appender.emplace(conn, table);
	auto &types = appender->appender->ColumnTypes();
	for (auto &type : types) {
		appender->column_ids.push_back(type.GetTypeId());
	}
	appender->chunk.emplace(conn.GetFactory().CreateDataChunk(types));
	appender->capacity = appender->chunk->GetCapacity();
	appender->reset_chunk();
	return env->NewDirectByteBuffer(appender.release(), 0);
}

static RowAppender &get_appender(JNIEnv *env, jobject appender_ref_buf) {
	auto appender_ref = (RowAppender *)env->GetDirectBufferAddress(appender_ref_buf);
	if (!appender_ref || appender_ref->closed) {
		throw JdbcException("Invalid Input Error: Invalid appender");
	}
	return *appender_ref;
}

void _duckdb_jdbc_appender_begin_row(JNIEnv *env, jclass, jobject appender_ref_buf) {
	// the values are appended to the columns in order, no matter how many times the row is begun
	get_appender(env, appender_ref_buf).in_row = true;
}

void _duckdb_jdbc_appender_end_row(JNIEnv *env, jclass, jobject appender_ref_buf) {
	auto &appender = get_appender(env, appender_ref_buf);
	if (appender.column != appender.column_ids.size()) {
		// the values of the incomplete row are dropped
		appender.column = 0;
		appender.in_row = false;
		throw JdbcException("Invalid Input Error: Call to EndRow before all columns have been appended to");
	}
	appender.in_row = false;
	appender.column = 0;
	appender.row++;
	if (appender.row == appender.capacity) {
		appender.collect();
	}
}

void _duckdb_jdbc_appender_flush(JNIEnv *env, jclass, jobject appender_ref_buf) {
	auto &appender = get_appender(env, appender_ref_buf);
	appender.collect();
	appender.appender->Flush();
}

void _duckdb_jdbc_appender_close(JNIEnv *env, jclass, jobject appender_ref_buf) {
	auto appender = (RowAppender *)env->GetDirectBufferAddress(appender_ref_buf);
	if (!appender) {
		throw JdbcException("Invalid Input Error: Invalid appender");
	}
	std::unique_ptr<RowAppender> owned(appender);
	if (owned->closed) {
		return;
	}
	owned->closed = true;
	owned->collect();
	owned->appender->Flush();
}

//! The vector of the column that is appended to
static cxx::Vector appender_vector(RowAppender &appender) {
	if (appender.column >= appender.column_ids.size()) {
		throw JdbcException("Invalid Input Error: Too many appends for chunk!");
	}
	return appender.chunk->GetVector(appender.column);
}

//! Appends the value to the column, that is cast to the type of the column
static void append_value(RowAppender &appender, const cxx::Value &value) {
	auto vec = appender_vector(appender);
	vec.SetValue(appender.row, value);
	appender.column++;
}

//! Appends the primitive value directly if the column has the same type, otherwise the value is cast
template <typename T>
static void append_primitive(RowAppender &appender, cxx::LogicalTypeId id, T value) {
	if (appender.column < appender.column_ids.size() && appender.column_ids[appender.column] == id) {
		auto vec = appender.chunk->GetVector(appender.column);
		vec.GetDataMutable<T>()[appender.row] = value;
		vec.GetValidityMutable().SetValid(appender.row);
		appender.column++;
		return;
	}
	append_value(appender, cxx::Value::Create(appender.factory(), value));
}

void _duckdb_jdbc_appender_append_boolean(JNIEnv *env, jclass, jobject appender_ref_buf, jboolean value) {
	append_primitive<bool>(get_appender(env, appender_ref_buf), cxx::LogicalTypeId::BOOLEAN, value != 0);
}

void _duckdb_jdbc_appender_append_byte(JNIEnv *env, jclass, jobject appender_ref_buf, jbyte value) {
	append_primitive<int8_t>(get_appender(env, appender_ref_buf), cxx::LogicalTypeId::TINYINT, (int8_t)value);
}

void _duckdb_jdbc_appender_append_short(JNIEnv *env, jclass, jobject appender_ref_buf, jshort value) {
	append_primitive<int16_t>(get_appender(env, appender_ref_buf), cxx::LogicalTypeId::SMALLINT, (int16_t)value);
}

void _duckdb_jdbc_appender_append_int(JNIEnv *env, jclass, jobject appender_ref_buf, jint value) {
	append_primitive<int32_t>(get_appender(env, appender_ref_buf), cxx::LogicalTypeId::INTEGER, (int32_t)value);
}

void _duckdb_jdbc_appender_append_long(JNIEnv *env, jclass, jobject appender_ref_buf, jlong value) {
	append_primitive<int64_t>(get_appender(env, appender_ref_buf), cxx::LogicalTypeId::BIGINT, (int64_t)value);
}

void _duckdb_jdbc_appender_append_float(JNIEnv *env, jclass, jobject appender_ref_buf, jfloat value) {
	append_primitive<float>(get_appender(env, appender_ref_buf), cxx::LogicalTypeId::FLOAT, (float)value);
}

void _duckdb_jdbc_appender_append_double(JNIEnv *env, jclass, jobject appender_ref_buf, jdouble value) {
	append_primitive<double>(get_appender(env, appender_ref_buf), cxx::LogicalTypeId::DOUBLE, (double)value);
}

void _duckdb_jdbc_appender_append_timestamp(JNIEnv *env, jclass, jobject appender_ref_buf, jlong value) {
	auto &appender = get_appender(env, appender_ref_buf);
	append_value(appender, cxx::Value::Create(appender.factory(), cxx::timestamp_t {static_cast<int64_t>(value)}));
}

void _duckdb_jdbc_appender_append_decimal(JNIEnv *env, jclass, jobject appender_ref_buf, jobject value) {
	auto &appender = get_appender(env, appender_ref_buf);
	append_value(appender, create_value_from_bigdecimal(env, value, appender.factory()));
}

static void append_null(RowAppender &appender) {
	auto vec = appender_vector(appender);
	vec.SetNull(appender.row);
	appender.column++;
}

void _duckdb_jdbc_appender_append_string(JNIEnv *env, jclass, jobject appender_ref_buf, jbyteArray value) {
	auto &appender = get_appender(env, appender_ref_buf);
	if (env->IsSameObject(value, NULL)) {
		append_null(appender);
		return;
	}

	auto string_value = jbyteArray_to_string(env, value);
	append_value(appender, cxx::Value::Create(appender.factory(), cxx::varchar_t(string_value)));
}

void _duckdb_jdbc_appender_append_bytes(JNIEnv *env, jclass, jobject appender_ref_buf, jbyteArray value) {
	auto &appender = get_appender(env, appender_ref_buf);
	if (env->IsSameObject(value, NULL)) {
		append_null(appender);
		return;
	}

	auto string_value = jbyteArray_to_string(env, value);
	append_value(appender, cxx::Value::Create(appender.factory(), cxx::blob_t(string_value)));
}

void _duckdb_jdbc_appender_append_null(JNIEnv *env, jclass, jobject appender_ref_buf) {
	append_null(get_appender(env, appender_ref_buf));
}

jlong _duckdb_jdbc_arrow_stream(JNIEnv *env, jclass, jobject res_ref_buf, jobject conn_ref_buf, jlong batch_size) {
	if (!res_ref_buf) {
		throw JdbcException("Invalid Input Error: Invalid result set");
	}
	auto &res_ref = get_result(env, res_ref_buf);
	if (batch_size <= 0) {
		throw JdbcException("Invalid Input Error: Arrow batch size must be greater than 0");
	}
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	std::lock_guard<std::mutex> guard(*conn_holder->lock);
	res_ref.chunk.reset();
	return reinterpret_cast<jlong>(create_result_arrow_stream(*conn_holder, res_ref, static_cast<idx_t>(batch_size)));
}

void _duckdb_jdbc_arrow_register(JNIEnv *env, jclass, jobject conn_ref_buf, jlong arrow_array_stream_pointer,
                                 jbyteArray name_j) {
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	auto name = jbyteArray_to_string(env, name_j);
	auto arrow_array_stream = reinterpret_cast<ArrowArrayStream *>(arrow_array_stream_pointer);
	if (!arrow_array_stream || !arrow_array_stream->release) {
		throw JdbcException("Invalid Input Error: This stream has been released");
	}
	register_arrow_stream(*conn_holder, arrow_array_stream, name);
}

void _duckdb_jdbc_arrow_close_streams(JNIEnv *env, jclass, jobject conn_ref_buf) {
	get_connection_ref(env, conn_ref_buf)->close_arrow_streams();
}

jstring _duckdb_jdbc_get_profiling_information(JNIEnv *env, jclass, jobject conn_ref_buf, jobject j_format) {
	auto &conn = get_connection(env, conn_ref_buf);
	jobject jname = env->CallObjectMethod(j_format, J_ProfilerPrintFormat_getName);
	check_java_exception_and_rethrow(env);
	const std::string name = jstring_to_string(env, static_cast<jstring>(jname));
	auto profiling_info = conn.GetProfilingInformation(name == "default" ? std::string() : name);
	return env->NewStringUTF(profiling_info.c_str());
}
