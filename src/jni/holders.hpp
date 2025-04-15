#pragma once

#include "duckdb.hpp"

#include <jni.h>

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
};

struct StatementHolder {
	duckdb::unique_ptr<duckdb::PreparedStatement> stmt;
};

struct ResultHolder {
	duckdb::unique_ptr<duckdb::QueryResult> res;
	duckdb::unique_ptr<duckdb::DataChunk> chunk;
};

/**
 * Throws a SQLException and returns nullptr if a valid Connection can't be retrieved from the buffer.
 */
inline duckdb::Connection *get_connection(JNIEnv *env, jobject conn_ref_buf) {
	if (!conn_ref_buf) {
		throw duckdb::ConnectionException("Invalid connection");
	}
	auto conn_holder = (ConnectionHolder *)env->GetDirectBufferAddress(conn_ref_buf);
	if (!conn_holder) {
		throw duckdb::ConnectionException("Invalid connection");
	}
	auto conn_ref = conn_holder->connection.get();
	if (!conn_ref || !conn_ref->context) {
		throw duckdb::ConnectionException("Invalid connection");
	}

	return conn_ref;
}
