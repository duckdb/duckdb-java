#pragma once

#include "capi_v2.hpp"
#include "cxx_handles.hpp"
#include "refs.hpp"

#include <jni.h>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace cxx = duckdb::cxx;

/**
 * A database instance of the stable C++ API. The instances that are opened with the instance cache enabled are shared
 * between connections to the same database file.
 */
struct InstanceHolder {
	explicit InstanceHolder(cxx::Instance instance_p) : instance(std::move(instance_p)) {
	}

	//! The instances that are not shared are opened in an environment of their own, same named in-memory databases of
	//! such instances are isolated from each other
	InstanceHolder(std::unique_ptr<cxx::Environment> environment_p, cxx::Instance instance_p)
	    : environment(std::move(environment_p)), instance(std::move(instance_p)) {
	}

	InstanceHolder(const InstanceHolder &) = delete;

	InstanceHolder &operator=(const InstanceHolder &) = delete;

	//! The cache key (the normalized database path), is empty for instances that are not cached
	std::string key;
	//! The configuration the instance was opened with, only used to check the compatibility with the new connections
	std::vector<std::pair<std::string, std::string>> options;
	bool read_only = false;
	//! Declared before the instance, as the environment must outlive it
	std::unique_ptr<cxx::Environment> environment;
	cxx::Instance instance;
};

/**
 * Holds a copy of a shared_ptr to an existing DB instance.
 * Is used to keep this DB alive (and accessible from DB cache)
 * even after the last connection to this DB is closed.
 */
struct DBHolder {
	std::shared_ptr<InstanceHolder> db;

	DBHolder(std::shared_ptr<InstanceHolder> _db) : db(std::move(_db)) {};

	DBHolder(const DBHolder &) = delete;

	DBHolder &operator=(const DBHolder &) = delete;
};

/**
 * State of an Arrow stream exported from or registered with a connection. Arrow streams are owned by their consumers
 * and can outlive the connection, connection closes the state of all its live streams before it is disconnected.
 */
struct ArrowStreamState {
	virtual ~ArrowStreamState() = default;

	//! Releases the resources held by the stream, must be idempotent
	virtual void Close() = 0;
};

/**
 * Associates a connection with a database instance. The instance may be shared amongst many ConnectionHolders, but the
 * connection is unique to this holder. Every Java DuckDBConnection has exactly 1 of these holders, and they are never
 * shared. The holder is freed when the DuckDBConnection is closed. When the last holder sharing an instance is freed,
 * the instance is released as well.
 *
 * The connection handle is also used as a connection handle of the C API v2 by the bindings.
 */
struct ConnectionHolder {
private:
	struct State {
		explicit State(cxx::Connection connection_p) : connection(std::move(connection_p)) {
		}

		cxx::Connection connection;
		//! A connection can be used by one thread at a time, the Java side can share a connection between threads
		std::mutex lock;
	};

	std::shared_ptr<State> state;

public:
	const std::shared_ptr<InstanceHolder> db;
	//! Shared with the statements of this connection, so they can find out that the connection was closed
	const std::shared_ptr<cxx::Connection> connection;
	const std::shared_ptr<std::mutex> lock;

	explicit ConnectionHolder(std::shared_ptr<InstanceHolder> _db)
	    : state(std::make_shared<State>(_db->instance.Connect())), db(_db), connection(state, &state->connection),
	      lock(state, &state->lock) {
	}

	~ConnectionHolder() {
		close_arrow_streams();
	}

	void track_arrow_stream(std::shared_ptr<ArrowStreamState> state);

	void close_arrow_streams();

	//! Remembers the stream that is registered with the name, returns the stream that was registered with it before
	std::shared_ptr<ArrowStreamState> replace_registered_stream(const std::string &name,
	                                                            std::shared_ptr<ArrowStreamState> state);

	DBHolder *create_db_ref() {
		return new DBHolder(db);
	}

private:
	std::mutex arrow_streams_lock;
	//! Streams are untracked implicitly when they are released by their consumers
	std::vector<std::weak_ptr<ArrowStreamState>> arrow_streams;
	//! The streams of the views that are created for the registered streams
	std::unordered_map<std::string, std::shared_ptr<ArrowStreamState>> registered_streams;
};

struct StatementHolder {
	std::weak_ptr<cxx::Connection> connection;
	std::weak_ptr<std::mutex> lock;
	std::optional<cxx::SqlStatement> statement;
	std::optional<cxx::PreparedStatement> stmt;
	//! The columns of the result and the parameters of the statement
	std::optional<cxx::Signature> signature;
	cxx::StatementType statement_type = cxx::StatementType::INVALID;
};

/**
 * A query that is submitted but is not yet completed, the execution of the query is deferred until it is read.
 */
struct PendingHolder {
	std::weak_ptr<cxx::Connection> connection;
	std::weak_ptr<std::mutex> lock;
	std::optional<cxx::QueryResult> pending;
	bool streaming = false;
};

/**
 * Column data of a result that is read into memory, it does not hold the connection that the result was created on.
 */
struct MaterializedData {
	std::optional<cxx::ColumnDataCollection> collection;
	std::optional<cxx::ColumnDataCollection::SharedScanState> shared_state;
	std::optional<cxx::ColumnDataCollection::WorkerScanState> worker_state;
};

struct ResultHolder {
	std::weak_ptr<cxx::Connection> connection;
	std::vector<std::string> names;
	std::vector<cxx::LogicalType> types;
	cxx::ResultType result_type = cxx::ResultType::QUERY_RESULT;
	//! Set for the results that are streamed, the connection is busy until the result is freed
	std::optional<cxx::QueryResult> stream;
	//! Set for the results that are read into memory
	std::unique_ptr<MaterializedData> materialized;
	//! The first chunk of a streamed result, that was read to check that the query is running without errors
	std::optional<cxx::DataChunk> first_chunk;
	std::optional<cxx::DataChunk> chunk;
};

/**
 * Result of a prepared statement execution that is read by chunks (see DuckDBChunkedResult) using C API v2.
 * The column names and types are captured when the result is created, the error is set when fetching of
 * a chunk fails, same as with the C API v1 result.
 */
struct ChunkedResultHolder {
	v2_result result;
	std::vector<std::string> column_names;
	std::vector<capi_type> column_types;
	bool has_error = false;
	std::string error;
	//! The first chunk of the result, that was read to check that the query is running without errors
	v2_data_chunk first_chunk;
	bool has_first_chunk = false;

	//! Takes ownership of the result, throws if the result schema cannot be read
	explicit ChunkedResultHolder(duckdb_v2_result_handle result_p);
};

struct AttachedJNIEnv {
	JavaVM *vm = nullptr;
	JNIEnv *env = nullptr;
	bool need_to_detach = false;

	AttachedJNIEnv();

	AttachedJNIEnv(JavaVM *vm_in, JNIEnv *env_in, bool need_to_detach_in);

	~AttachedJNIEnv() noexcept;
};

struct GlobalRefHolder {
	JavaVM *vm = nullptr;
	jobject global_ref = nullptr;

	GlobalRefHolder(JNIEnv *env, jobject local_ref);

	~GlobalRefHolder() noexcept;

	AttachedJNIEnv attach_current_thread();

	void detach_current_thread();

	static void destroy(void *holder_in) noexcept;
	static void destroy_table_function_state(void *holder_in) noexcept;
};

struct LocalRefHolder {
	JNIEnv *env = nullptr;
	jobject local_ref = nullptr;

	LocalRefHolder(JNIEnv *env_in, jobject local_ref_in);

	~LocalRefHolder() noexcept;
};

/**
 * Thrown for the errors that are detected by the driver itself.
 */
struct JdbcException : public std::runtime_error {
	explicit JdbcException(const std::string &message) : std::runtime_error(message) {
	}
};

ConnectionHolder *get_connection_ref(JNIEnv *env, jobject conn_ref_buf);

/**
 * Throws if a valid connection can't be retrieved from the buffer.
 */
cxx::Connection &get_connection(JNIEnv *env, jobject conn_ref_buf);

/**
 * Returns the C API v2 handle of the connection of the specified holder. Throws a SQLException and returns nullptr
 * if a valid connection can't be retrieved from the buffer.
 */
duckdb_v2_connection_handle conn_ref_buf_to_conn(JNIEnv *env, jobject conn_ref_buf);

/**
 * Same as conn_ref_buf_to_conn, returns the connection of the stable C++ API.
 */
cxx::Connection *conn_ref_buf_to_engine_conn(JNIEnv *env, jobject conn_ref_buf);
