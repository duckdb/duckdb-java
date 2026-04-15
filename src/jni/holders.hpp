#pragma once

extern "C" {
#include "duckdb.h"
}
#include "duckdb.hpp"
#include "refs.hpp"

#include <jni.h>

/**
 * Holds a copy of a shared_ptr to an existing DB instance.
 * Is used to keep this DB alive (and accessible from DB cache)
 * even after the last connection to this DB is closed.
 */
struct DBHolder {
	duckdb::shared_ptr<duckdb::DuckDB> db;

	DBHolder(duckdb::shared_ptr<duckdb::DuckDB> _db) : db(std::move(_db)) {};

	DBHolder(const DBHolder &) = delete;

	DBHolder &operator=(const DBHolder &) = delete;
};

/**
 * Associates a duckdb::Connection with a duckdb::DuckDB. The DB may be shared amongst many ConnectionHolders, but the
 * Connection is unique to this holder. Every Java DuckDBConnection has exactly 1 of these holders, and they are never
 * shared. The holder is freed when the DuckDBConnection is closed. When the last holder sharing a DuckDB is freed, the
 * DuckDB is released as well.
 */
struct ConnectionHolder {
	const duckdb::shared_ptr<duckdb::DuckDB> db;
	const duckdb::unique_ptr<duckdb::Connection> connection;

	ConnectionHolder(duckdb::shared_ptr<duckdb::DuckDB> _db)
	    : db(_db), connection(duckdb::make_uniq<duckdb::Connection>(*_db)) {
	}

	DBHolder *create_db_ref() {
		return new DBHolder(db);
	}
};

struct StatementHolder {
	duckdb::unique_ptr<duckdb::PreparedStatement> stmt;
	//! When the preprocessor expands a statement into several (e.g. PIVOT + transaction policy SETs), we prepare the
	//! last SELECT and run any trailing statements (typically SET current_transaction_invalidation_policy) after each
	//! successful execute. Stored as SQL text so repeated execute() on the PreparedStatement remains correct.
	duckdb::vector<std::string> trailing_queries_after_execute;
};

struct PendingHolder {
	duckdb::unique_ptr<duckdb::PendingQueryResult> pending;
	duckdb::vector<std::string> trailing_queries_after_execute;
	duckdb::Connection *connection_for_trailing = nullptr;
};

struct ResultHolder {
	duckdb::unique_ptr<duckdb::QueryResult> res;
	duckdb::unique_ptr<duckdb::DataChunk> chunk;
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
};

struct LocalRefHolder {
	JNIEnv *env = nullptr;
	jobject local_ref = nullptr;

	LocalRefHolder(JNIEnv *env_in, jobject local_ref_in);

	~LocalRefHolder() noexcept;
};

ConnectionHolder *get_connection_ref(JNIEnv *env, jobject conn_ref_buf);

duckdb::Connection *get_connection(JNIEnv *env, jobject conn_ref_buf);

duckdb_connection conn_ref_buf_to_conn(JNIEnv *env, jobject conn_ref_buf);
