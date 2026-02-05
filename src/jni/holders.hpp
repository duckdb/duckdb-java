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
};

struct PendingHolder {
	duckdb::unique_ptr<duckdb::PendingQueryResult> pending;
};

struct ResultHolder {
	duckdb::unique_ptr<duckdb::QueryResult> res;
	duckdb::unique_ptr<duckdb::DataChunk> chunk;
};

inline ConnectionHolder *get_connection_ref(JNIEnv *env, jobject conn_ref_buf) {
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
inline duckdb::Connection *get_connection(JNIEnv *env, jobject conn_ref_buf) {
	auto conn_holder = get_connection_ref(env, conn_ref_buf);
	auto conn_ref = conn_holder->connection.get();
	if (!conn_ref || !conn_ref->context) {
		throw duckdb::ConnectionException("Invalid connection");
	}

	return conn_ref;
}

inline duckdb_connection conn_ref_buf_to_conn(JNIEnv *env, jobject conn_ref_buf) {
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
