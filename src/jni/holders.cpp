#include "holders.hpp"

#include "duckdb/main/client_data.hpp"

#include <memory>
#include <unordered_map>

// All following statics are initialized from JNI_OnLoad

static std::shared_ptr<std::mutex> conn_map_mutex() {
	static auto mutex = std::make_shared<std::mutex>();
	return mutex;
}

static std::shared_ptr<std::unordered_map<ConnectionHolder *, std::shared_ptr<std::mutex>>> conn_map() {
	static auto map = std::make_shared<std::unordered_map<ConnectionHolder *, std::shared_ptr<std::mutex>>>();
	return map;
}

static std::shared_ptr<std::mutex> stmt_map_mutex() {
	static auto mutex = std::make_shared<std::mutex>();
	return mutex;
}

static std::shared_ptr<std::unordered_map<StatementHolder *, std::shared_ptr<std::mutex>>> stmt_map() {
	static auto map = std::make_shared<std::unordered_map<StatementHolder *, std::shared_ptr<std::mutex>>>();
	return map;
}

static std::shared_ptr<std::mutex> res_map_mutex() {
	static auto mutex = std::make_shared<std::mutex>();
	return mutex;
}

static std::shared_ptr<std::unordered_map<ResultHolder *, std::shared_ptr<std::mutex>>> res_map() {
	static auto map = std::make_shared<std::unordered_map<ResultHolder *, std::shared_ptr<std::mutex>>>();
	return map;
}

// Connection

ConnectionHolder::ConnectionHolder(duckdb::shared_ptr<duckdb::DuckDB> db_in) : db(std::move(db_in)) {
	auto conn_ptr = duckdb::make_uniq<duckdb::Connection>(*this->db);
	this->connection = std::move(conn_ptr);
}

ConnectionHolder::ConnectionHolder(duckdb::shared_ptr<duckdb::DuckDB> db_in, duckdb::ClientConfig config)
    : ConnectionHolder(std::move(db_in)) {
	this->connection->context->config = config;
}

duckdb::Connection &ConnectionHolder::conn() {
	return *this->connection;
}

duckdb::ClientData &ConnectionHolder::client_data() {
	return duckdb::ClientData::Get(*this->conn().context);
}

void ConnectionHolder::track_stmt(StatementHolder *stmt_ref) {
	auto it = this->stmt_set.emplace(stmt_ref);
	if (!it.second) {
		throw std::runtime_error("Statement is already registered with the connection");
	}
	this->stmt_list.emplace_back(stmt_ref);
}

void ConnectionHolder::untrack_stmt(StatementHolder *stmt_ref) {
	auto num_removed = this->stmt_set.erase(stmt_ref);
	if (1 != num_removed) {
		return;
	}
	stmt_list.remove(stmt_ref);
}

void ConnectionHolder::init_statics() {
	conn_map_mutex();
	conn_map();
}

void ConnectionHolder::track(ConnectionHolder *conn_ref) {
	if (conn_ref == nullptr) {
		throw std::runtime_error("Invalid connection ref");
	}
	auto mtx = conn_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = conn_map();
	auto pair = map->emplace(conn_ref, std::make_shared<std::mutex>());
	if (!pair.second) {
		throw std::runtime_error("Connection is already registered");
	}
}

void ConnectionHolder::check_tracked(ConnectionHolder *conn_ref) {
	bool tracked = ConnectionHolder::is_tracked(conn_ref);
	if (!tracked) {
		throw std::runtime_error("Connection is closed");
	}
}

bool ConnectionHolder::is_tracked(ConnectionHolder *conn_ref) {
	auto mtx = conn_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = conn_map();
	auto count = map->count(conn_ref);
	return count == 1;
}

bool ConnectionHolder::untrack(ConnectionHolder *conn_ref) {
	if (conn_ref == nullptr) {
		return false;
	}
	auto mtx = conn_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = conn_map();
	auto num_removed = map->erase(conn_ref);
	return num_removed == 1;
}

static std::shared_ptr<std::mutex> lookup_conn_mutex(ConnectionHolder *conn_ref, bool throw_on_not_found) {
	if (conn_ref == nullptr) {
		if (throw_on_not_found) {
			throw std::runtime_error("Invalid connection ref");
		} else {
			return std::shared_ptr<std::mutex>();
		}
	}
	auto mtx = conn_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = conn_map();
	auto it = map->find(conn_ref);
	if (it == map->end()) {
		if (throw_on_not_found) {
			throw std::runtime_error("Connection is closed");
		} else {
			return std::shared_ptr<std::mutex>();
		}
	}
	return it->second;
}

std::shared_ptr<std::mutex> ConnectionHolder::mutex(ConnectionHolder *conn_ref) {
	return lookup_conn_mutex(conn_ref, true);
}

std::shared_ptr<std::mutex> ConnectionHolder::mutex_no_throw(ConnectionHolder *conn_ref) {
	return lookup_conn_mutex(conn_ref, false);
}

static ConnectionHolder *get_conn_from_ref_buf(JNIEnv *env, jobject conn_ref_buf, bool throw_on_not_found) {
	if (conn_ref_buf == nullptr) {
		if (throw_on_not_found) {
			throw std::runtime_error("Invalid connection ref buffer");
		} else {
			return nullptr;
		}
	}
	auto conn_holder = reinterpret_cast<ConnectionHolder *>(env->GetDirectBufferAddress(conn_ref_buf));
	if (conn_holder == nullptr && throw_on_not_found) {
		throw std::runtime_error("Invalid connection");
	}
	return conn_holder;
}

ConnectionHolder *ConnectionHolder::unwrap_ref_buf(JNIEnv *env, jobject conn_ref_buf) {
	return get_conn_from_ref_buf(env, conn_ref_buf, true);
}

ConnectionHolder *ConnectionHolder::unwrap_ref_buf_no_throw(JNIEnv *env, jobject conn_ref_buf) {
	return get_conn_from_ref_buf(env, conn_ref_buf, false);
}

// Statement

StatementHolder::StatementHolder(ConnectionHolder *conn_ref_in, duckdb::unique_ptr<duckdb::PreparedStatement> stmt_in)
    : conn_ref(conn_ref_in), stmt(std::move(stmt_in)) {
}

void StatementHolder::track_result(ResultHolder *res_ref) {
	auto it = this->res_set.emplace(res_ref);
	if (!it.second) {
		throw std::runtime_error("Result is already registered with the connection");
	}
	this->res_list.emplace_back(res_ref);
}

void StatementHolder::untrack_result(ResultHolder *res_ref) {
	auto num_removed = this->res_set.erase(res_ref);
	if (1 != num_removed) {
		return;
	}
	res_list.remove(res_ref);
}

void StatementHolder::init_statics() {
	stmt_map_mutex();
	stmt_map();
}

void StatementHolder::track(StatementHolder *stmt_ref) {
	if (stmt_ref == nullptr) {
		throw std::runtime_error("Invalid statement ref");
	}
	auto mtx = stmt_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = stmt_map();
	auto pair = map->emplace(stmt_ref, std::make_shared<std::mutex>());
	if (!pair.second) {
		throw std::runtime_error("Statement is already registered");
	}
}

void StatementHolder::check_tracked(StatementHolder *stmt_ref) {
	bool tracked = StatementHolder::is_tracked(stmt_ref);
	if (!tracked) {
		throw std::runtime_error("Statement is closed");
	}
}

bool StatementHolder::is_tracked(StatementHolder *stmt_ref) {
	auto mtx = stmt_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = stmt_map();
	auto count = map->count(stmt_ref);
	return count == 1;
}

bool StatementHolder::untrack(StatementHolder *stmt_ref) {
	if (stmt_ref == nullptr) {
		return false;
	}
	auto mtx = stmt_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = stmt_map();
	auto num_removed = map->erase(stmt_ref);
	return num_removed == 1;
}

static std::shared_ptr<std::mutex> lookup_stmt_mutex(StatementHolder *stmt_ref, bool throw_on_not_found) {
	if (stmt_ref == nullptr) {
		if (throw_on_not_found) {
			throw std::runtime_error("Invalid statement ref");
		} else {
			return std::shared_ptr<std::mutex>();
		}
	}
	auto mtx = stmt_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = stmt_map();
	auto it = map->find(stmt_ref);
	if (it == map->end()) {
		if (throw_on_not_found) {
			throw std::runtime_error("Statement is closed");
		} else {
			return std::shared_ptr<std::mutex>();
		}
	}
	return it->second;
}

std::shared_ptr<std::mutex> StatementHolder::mutex(StatementHolder *stmt_ref) {
	return lookup_stmt_mutex(stmt_ref, true);
}

std::shared_ptr<std::mutex> StatementHolder::mutex_no_throw(StatementHolder *stmt_ref) {
	return lookup_stmt_mutex(stmt_ref, false);
}

static StatementHolder *get_stmt_from_ref_buf(JNIEnv *env, jobject stmt_ref_buf, bool throw_on_not_found) {
	if (stmt_ref_buf == nullptr) {
		if (throw_on_not_found) {
			throw std::runtime_error("Invalid statement ref buffer");
		} else {
			return nullptr;
		}
	}
	auto stmt_ref = reinterpret_cast<StatementHolder *>(env->GetDirectBufferAddress(stmt_ref_buf));
	if (stmt_ref == nullptr && throw_on_not_found) {
		throw std::runtime_error("Invalid statement");
	}
	return stmt_ref;
}

StatementHolder *StatementHolder::unwrap_ref_buf(JNIEnv *env, jobject stmt_ref_buf) {
	return get_stmt_from_ref_buf(env, stmt_ref_buf, true);
}

StatementHolder *StatementHolder::unwrap_ref_buf_no_throw(JNIEnv *env, jobject stmt_ref_buf) {
	return get_stmt_from_ref_buf(env, stmt_ref_buf, false);
}

// Result

ResultHolder::ResultHolder(StatementHolder *stmt_ref_in, duckdb::unique_ptr<duckdb::QueryResult> res_in)
    : stmt_ref(stmt_ref_in), res(std::move(res_in)) {
}

void ResultHolder::init_statics() {
	res_map_mutex();
	res_map();
}

void ResultHolder::track(ResultHolder *res_ref) {
	if (res_ref == nullptr) {
		throw std::runtime_error("Invalid result ref");
	}
	auto mtx = res_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = res_map();
	auto pair = map->emplace(res_ref, std::make_shared<std::mutex>());
	if (!pair.second) {
		throw std::runtime_error("Result is already registered");
	}
}

void ResultHolder::check_tracked(ResultHolder *res_ref) {
	bool tracked = ResultHolder::is_tracked(res_ref);
	if (!tracked) {
		throw std::runtime_error("Result is closed");
	}
}

bool ResultHolder::is_tracked(ResultHolder *res_ref) {
	auto mtx = res_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = res_map();
	auto count = map->count(res_ref);
	return count == 1;
}

bool ResultHolder::untrack(ResultHolder *res_ref) {
	if (res_ref == nullptr) {
		return false;
	}
	auto mtx = res_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = res_map();
	auto num_removed = map->erase(res_ref);
	return num_removed == 1;
}

std::shared_ptr<std::mutex> lookup_res_mutex(ResultHolder *res_ref, bool throw_on_not_found) {
	if (res_ref == nullptr) {
		if (throw_on_not_found) {
			throw std::runtime_error("Invalid result ref");
		} else {
			return std::shared_ptr<std::mutex>();
		}
	}
	auto mtx = res_map_mutex();
	std::lock_guard<std::mutex> guard(*mtx);
	auto map = res_map();
	auto it = map->find(res_ref);
	if (it == map->end()) {
		if (throw_on_not_found) {
			throw std::runtime_error("Result is closed");
		} else {
			return std::shared_ptr<std::mutex>();
		}
	}
	return it->second;
}

std::shared_ptr<std::mutex> ResultHolder::mutex(ResultHolder *res_ref) {
	return lookup_res_mutex(res_ref, true);
}

std::shared_ptr<std::mutex> ResultHolder::mutex_no_throw(ResultHolder *res_ref) {
	return lookup_res_mutex(res_ref, false);
}

static ResultHolder *get_res_from_ref_buf(JNIEnv *env, jobject res_ref_buf, bool throw_on_not_found) {
	if (res_ref_buf == nullptr) {
		if (throw_on_not_found) {
			throw std::runtime_error("Invalid result set ref buffer");
		} else {
			return nullptr;
		}
	}
	auto res_ref = reinterpret_cast<ResultHolder *>(env->GetDirectBufferAddress(res_ref_buf));
	if (res_ref == nullptr && throw_on_not_found) {
		throw std::runtime_error("Invalid result set");
	}
	return res_ref;
}

ResultHolder *ResultHolder::unwrap_ref_buf(JNIEnv *env, jobject res_ref_buf) {
	return get_res_from_ref_buf(env, res_ref_buf, true);
}

ResultHolder *ResultHolder::unwrap_ref_buf_no_throw(JNIEnv *env, jobject res_ref_buf) {
	return get_res_from_ref_buf(env, res_ref_buf, false);
}
