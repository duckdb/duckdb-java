#pragma once

#include "duckdb.hpp"

#include <jni.h>
#include <list>
#include <mutex>
#include <stdexcept>
#include <unordered_set>

/**
 * This header contains holders for Connection, Statement and Result.
 * Instances of these objects are shared with Java part as bare poiters
 * inside empty ByteBuffers. When instance is closed - the underlying
 * object is destroyed. Concurrent closure can happen any time, thus
 * the pointer coming from Java cannot be dereferenced without holding
 * the same lock that is held when deleting this object.
 *
 * Locks registry is a global synchonized unordered_map for every type
 * of the object, it is managed using `track/untrack/check_tracked`
 * static methods. Mutex for the corresponding object is created when
 * the object is registered and deleted when the object is deleted.
 * Object, shared with Java, is considered alive (and thus a pointer
 * to it can be dereferenced) only while it has a mutex in the
 * registry. 'init_statics' methods are used to initialize the
 * registries. They are called from JNI_OnLoad to not rely on
 * thread-safety of a static var initialization, that, while required
 * by the standard, is not thread-safe in older versions of MSVC and
 * can be disabled manually in GCC.
 *
 * To dereferenced a pointer coming from Java it is necessary first to
 * lock the instance taking a mutex from registry and then, while
 * holding the lock, re-check that the instance is still tracked.
 * These steps are required in the beginning of every JNI call, they
 * are the same for all object types, example for a Statement:
 *
 * > jobject _duckdb_jdbc_execute(JNIEnv *env, jclass, jobject stmt_ref_buf, ...) {
 * >   auto stmt_ref = StatementHolder::unwrap_ref_buf(env, stmt_ref_buf);
 * >   auto mtx = StatementHolder::mutex(stmt_ref);
 * >   std::lock_guard<std::mutex> guard(*mtx);
 * >   StatementHolder::check_tracked(stmt_ref);
 * >   ...
 *
 * Note, the 'mtx' above is a shared_ptr and must be kept in (at least)
 * the same scope as the lock_guard created on it.
 *
 * Connection maintains an ordered set of Statements opened on it, and
 * Statement maintains an ordered set of Results opened on it (set here
 * is used for consistency, only one active result is expected). When
 * the parent object is closed - these child objects are closed too.
 *
 * Close methods should not throw, so they use *_no_throw versions of
 * the steps above.
 */

struct StatementHolder;
struct ResultHolder;

/**
 * Associates a duckdb::Connection with a duckdb::DuckDB. The DB may be shared amongst many ConnectionHolders, but the
 * Connection is unique to this holder. Every Java DuckDBConnection has exactly 1 of these holders, and they are never
 * shared. The holder is freed when the DuckDBConnection is closed. When the last holder sharing a DuckDB is freed, the
 * DuckDB is released as well.
 */
struct ConnectionHolder {
	duckdb::shared_ptr<duckdb::DuckDB> db;
	duckdb::unique_ptr<duckdb::Connection> connection;
	std::unordered_set<StatementHolder *> stmt_set;
	std::list<StatementHolder *> stmt_list;

	explicit ConnectionHolder(duckdb::shared_ptr<duckdb::DuckDB> db_in);

	explicit ConnectionHolder(duckdb::shared_ptr<duckdb::DuckDB> db_in, duckdb::ClientConfig config);

	duckdb::Connection &conn();

	duckdb::ClientData &client_data();

	void track_stmt(StatementHolder *stmt_ref);

	void untrack_stmt(StatementHolder *stmt_ref);

	static void init_statics();

	static void track(ConnectionHolder *conn_ref);

	static void check_tracked(ConnectionHolder *conn_ref);

	static bool is_tracked(ConnectionHolder *conn_ref);

	static bool untrack(ConnectionHolder *conn_ref);

	static std::shared_ptr<std::mutex> mutex(ConnectionHolder *conn_ref);

	static std::shared_ptr<std::mutex> mutex_no_throw(ConnectionHolder *conn_ref);

	static ConnectionHolder *unwrap_ref_buf(JNIEnv *env, jobject conn_ref_buf);

	static ConnectionHolder *unwrap_ref_buf_no_throw(JNIEnv *env, jobject conn_ref_buf);
};

struct StatementHolder {
	ConnectionHolder *conn_ref;
	duckdb::unique_ptr<duckdb::PreparedStatement> stmt;
	std::unordered_set<ResultHolder *> res_set;
	std::list<ResultHolder *> res_list;

	explicit StatementHolder(ConnectionHolder *conn_ref_in, duckdb::unique_ptr<duckdb::PreparedStatement> stmt_in);

	void track_result(ResultHolder *res_ref);

	void untrack_result(ResultHolder *res_ref);

	static void init_statics();

	static void track(StatementHolder *stmt_ref);

	static void check_tracked(StatementHolder *conn_ref);

	static bool is_tracked(StatementHolder *conn_ref);

	static bool untrack(StatementHolder *stmt_ref);

	static std::shared_ptr<std::mutex> mutex(StatementHolder *stmt_ref);

	static std::shared_ptr<std::mutex> mutex_no_throw(StatementHolder *stmt_ref);

	static StatementHolder *unwrap_ref_buf(JNIEnv *env, jobject stmt_ref_buf);

	static StatementHolder *unwrap_ref_buf_no_throw(JNIEnv *env, jobject stmt_ref_buf);
};

struct ResultHolder {
	StatementHolder *stmt_ref;
	duckdb::unique_ptr<duckdb::QueryResult> res;
	duckdb::unique_ptr<duckdb::DataChunk> chunk;

	ResultHolder(StatementHolder *stmt_ref_in, duckdb::unique_ptr<duckdb::QueryResult> res_in);

	static void init_statics();

	static void track(ResultHolder *res_ref);

	static void check_tracked(ResultHolder *conn_ref);

	static bool is_tracked(ResultHolder *conn_ref);

	static bool untrack(ResultHolder *res_ref);

	static std::shared_ptr<std::mutex> mutex(ResultHolder *res_ref);

	static std::shared_ptr<std::mutex> mutex_no_throw(ResultHolder *res_ref);

	static ResultHolder *unwrap_ref_buf(JNIEnv *env, jobject res_ref_buf);

	static ResultHolder *unwrap_ref_buf_no_throw(JNIEnv *env, jobject res_ref_buf);
};
