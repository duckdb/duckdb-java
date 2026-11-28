// Exports a query result as an Arrow stream, over the C API v2 only.
//
// The rows of the result are bound to a statement (SELECT * FROM the rows) that is executed with the Arrow result
// format, and the result is turned into an ArrowArrayStream. An Arrow stream is a C structure with callbacks, so it
// cannot be implemented in Java. The stream that is handed out owns the statement and the rows, it is released by
// its consumer. When the connection is closed before the consumer, the streams of the connection are closed: the
// reads of the stream fail then.
#include "bindings.hpp"
#include "refs.hpp"
#include "util.hpp"

#include <cerrno>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

struct ExportedStream {
	//! The stream that is handed out, it must be the first member
	ArrowArrayStream stream {};
	//! The stream of the Arrow result
	ArrowArrayStream inner {};
	duckdb_v2_sql_statement_handle statement = nullptr;
	duckdb_v2_column_data_collection_handle collection = nullptr;
	//! The connection of the result that is exported, the stream is closed with it
	duckdb_v2_connection_handle connection = nullptr;
	//! The connection of its own that the statement runs on, so that the connection of the result stays usable
	duckdb_v2_connection_handle helper = nullptr;
	std::mutex lock;
	std::string last_error;
	bool closed = false;

	//! Releases the resources of the stream, the result goes first as it uses the statement and the rows
	void Close() {
		std::lock_guard<std::mutex> guard(lock);
		closed = true;
		if (inner.release) {
			inner.release(&inner);
			inner.release = nullptr;
		}
		if (statement) {
			duckdb_v2_sql_statement_destroy(&statement);
			statement = nullptr;
		}
		if (collection) {
			duckdb_v2_column_data_collection_destroy(&collection);
			collection = nullptr;
		}
		if (helper) {
			duckdb_v2_connection_destroy(&helper);
			helper = nullptr;
		}
	}
};

std::mutex registry_lock;
std::unordered_map<duckdb_v2_connection_handle, std::vector<ExportedStream *>> registry;

ExportedStream &get_exported(ArrowArrayStream *stream) {
	return *reinterpret_cast<ExportedStream *>(stream->private_data);
}

template <class FUNCTION>
int forward(ExportedStream &self, FUNCTION call) {
	std::lock_guard<std::mutex> guard(self.lock);
	if (self.closed || !self.inner.release) {
		self.last_error = "Connection Error: Connection was closed";
		return EIO;
	}
	int res = call();
	if (res != 0) {
		const char *error = self.inner.get_last_error(&self.inner);
		self.last_error = error ? error : "unable to read the result";
	}
	return res;
}

int stream_get_schema(ArrowArrayStream *stream, ArrowSchema *out) {
	if (!stream->release) {
		return EINVAL;
	}
	auto &self = get_exported(stream);
	out->release = nullptr;
	return forward(self, [&]() { return self.inner.get_schema(&self.inner, out); });
}

int stream_get_next(ArrowArrayStream *stream, ArrowArray *out) {
	if (!stream->release) {
		return EINVAL;
	}
	auto &self = get_exported(stream);
	out->release = nullptr;
	return forward(self, [&]() { return self.inner.get_next(&self.inner, out); });
}

const char *stream_get_last_error(ArrowArrayStream *stream) {
	if (!stream->release) {
		return nullptr;
	}
	auto &self = get_exported(stream);
	std::lock_guard<std::mutex> guard(self.lock);
	return self.last_error.empty() ? nullptr : self.last_error.c_str();
}

void stream_release(ArrowArrayStream *stream) {
	if (!stream->release) {
		return;
	}
	auto self = &get_exported(stream);
	stream->release = nullptr;
	{
		// the connection does not close the stream anymore
		std::lock_guard<std::mutex> guard(registry_lock);
		auto it = registry.find(self->connection);
		if (it != registry.end()) {
			auto &streams = it->second;
			for (size_t i = 0; i < streams.size(); i++) {
				if (streams[i] == self) {
					streams.erase(streams.begin() + static_cast<std::ptrdiff_t>(i));
					break;
				}
			}
			if (streams.empty()) {
				registry.erase(it);
			}
		}
	}
	self->Close();
	delete self;
}

} // namespace

// Executes the statement (that reads the collection) on the helper connection with the Arrow result format and returns
// the address of the stream of the result. The stream takes the ownership of the helper connection, of the statement
// and of the collection when the call succeeds, conn is the connection that the stream is closed with.
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1arrow_1export_1stream(
    JNIEnv *env, jclass, jobject conn_buf, jobject helper_buf, jobject st_buf, jobject collection_buf,
    jlong batch_size) {
	auto conn = reinterpret_cast<duckdb_v2_connection_handle>(env->GetDirectBufferAddress(conn_buf));
	auto helper = reinterpret_cast<duckdb_v2_connection_handle>(env->GetDirectBufferAddress(helper_buf));
	auto statement = reinterpret_cast<duckdb_v2_sql_statement_handle>(env->GetDirectBufferAddress(st_buf));
	auto collection =
	    reinterpret_cast<duckdb_v2_column_data_collection_handle>(env->GetDirectBufferAddress(collection_buf));
	if (!conn || !helper || !statement || !collection) {
		env->ThrowNew(J_SQLException, "Invalid Arrow export arguments");
		return 0;
	}
	idx_t batch = jlong_to_idx(env, batch_size);
	if (env->ExceptionCheck()) {
		return 0;
	}

	v2_error err;
	duckdb_v2_arrow_result_handle result = nullptr;
	if (duckdb_v2_statement_execute_arrow(helper, statement, nullptr, nullptr, nullptr, 0, batch, &result, err.out()) !=
	    DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
		return 0;
	}
	auto exported = new ExportedStream();
	if (duckdb_v2_arrow_result_to_arrow_c_stream(&result, &exported->inner, err.out()) != DUCKDB_V2_ERROR_NONE) {
		if (result) {
			duckdb_v2_arrow_result_destroy(&result);
		}
		delete exported;
		throw_sql_exception(env, err);
		return 0;
	}
	exported->statement = statement;
	exported->collection = collection;
	exported->connection = conn;
	exported->helper = helper;
	exported->stream.private_data = exported;
	exported->stream.get_schema = stream_get_schema;
	exported->stream.get_next = stream_get_next;
	exported->stream.get_last_error = stream_get_last_error;
	exported->stream.release = stream_release;
	{
		std::lock_guard<std::mutex> guard(registry_lock);
		registry[conn].push_back(exported);
	}
	return reinterpret_cast<jlong>(&exported->stream);
}

namespace {
void close_registered_streams(duckdb_v2_connection_handle conn);
} // namespace

// Closes the streams of the connection that are not released by their consumers yet
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1arrow_1close_1streams(JNIEnv *env, jclass,
                                                                                        jobject conn_buf) {
	auto conn = reinterpret_cast<duckdb_v2_connection_handle>(env->GetDirectBufferAddress(conn_buf));
	if (!conn) {
		return;
	}
	close_registered_streams(conn);
	std::lock_guard<std::mutex> guard(registry_lock);
	auto it = registry.find(conn);
	if (it == registry.end()) {
		return;
	}
	// the last stream that was created is the first one that is closed
	for (size_t i = it->second.size(); i > 0; i--) {
		it->second[i - 1]->Close();
	}
	registry.erase(it);
}

// ---------------------------------------------------------------------------------------------------------------------
// Registered streams: an Arrow stream of Java is scanned by the table function that a view of the stream selects from
// ---------------------------------------------------------------------------------------------------------------------

namespace {

const char *const ARROW_SCAN_FUNCTION = "__jdbc_arrow_stream_scan";
// The size of the chunks of the engine, that is the capacity of the output chunk of the scan
constexpr idx_t ARROW_SCAN_BATCH_SIZE = 2048;

/**
 * An Arrow stream registered with a connection. The stream is moved out of the Java-owned struct on registration, it
 * is released when it is consumed by a scan, when the view is replaced or when the connection is closed.
 */
struct RegisteredStream {
	std::mutex lock;
	ArrowArrayStream stream {};
	int64_t id = 0;
	duckdb_v2_connection_handle connection = nullptr;

	void Release() {
		std::lock_guard<std::mutex> guard(lock);
		if (stream.release) {
			stream.release(&stream);
			stream.release = nullptr;
		}
	}

	// Moves the stream out, returns false if it is released already
	bool Take(ArrowArrayStream &out) {
		std::lock_guard<std::mutex> guard(lock);
		if (!stream.release) {
			return false;
		}
		out = stream;
		stream.release = nullptr;
		return true;
	}
};

struct RegisteredStreams {
	std::mutex lock;
	int64_t next_id = 1;
	std::unordered_map<int64_t, std::shared_ptr<RegisteredStream>> streams;

	std::shared_ptr<RegisteredStream> Get(int64_t id) {
		std::lock_guard<std::mutex> guard(lock);
		auto it = streams.find(id);
		return it == streams.end() ? nullptr : it->second;
	}

	std::shared_ptr<RegisteredStream> Remove(int64_t id) {
		std::lock_guard<std::mutex> guard(lock);
		auto it = streams.find(id);
		if (it == streams.end()) {
			return nullptr;
		}
		auto res = std::move(it->second);
		streams.erase(it);
		return res;
	}
};

RegisteredStreams &registered_streams() {
	static RegisteredStreams registered;
	return registered;
}

// Releases a stream that was taken from a registered one
struct TakenStream {
	ArrowArrayStream stream {};

	TakenStream() = default;
	TakenStream(const TakenStream &) = delete;
	TakenStream &operator=(const TakenStream &) = delete;

	~TakenStream() {
		if (stream.release) {
			stream.release(&stream);
		}
	}
};

struct ScanBindData {
	std::shared_ptr<RegisteredStream> state;
};

struct ScanGlobalState {
	TakenStream taken;
	duckdb_v2_arrow_importer_handle importer = nullptr;
	bool finished = false;

	~ScanGlobalState() {
		if (importer) {
			duckdb_v2_arrow_importer_destroy(&importer);
		}
	}
};

std::string stream_error(ArrowArrayStream &stream, const char *fallback) {
	const char *error = stream.get_last_error ? stream.get_last_error(&stream) : nullptr;
	return error ? error : fallback;
}

void scan_bind(duckdb_v2_function_bind_info_handle info, duckdb_v2_table_function_bind_info_handle result,
               duckdb_v2_context_handle context, duckdb_v2_error_info_handle *err) {
	v2_value id_value;
	v2_error call_err;
	int64_t id = 0;
	if (duckdb_v2_function_bind_get_arg_value(info, 0, id_value.out(), call_err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_value_get_bigint(id_value.get(), &id, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
		return;
	}
	auto state = registered_streams().Get(id);
	if (!state) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, "This stream has been released");
		return;
	}
	ArrowSchema schema {};
	{
		std::lock_guard<std::mutex> guard(state->lock);
		if (!state->stream.release) {
			set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, "This stream has been released");
			return;
		}
		if (state->stream.get_schema(&state->stream, &schema) != 0) {
			auto message = stream_error(state->stream, "unable to read the schema");
			set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, message);
			return;
		}
	}
	duckdb_v2_arrow_importer_handle importer = nullptr;
	bool ok = duckdb_v2_arrow_importer_create(context, &schema, ARROW_SCAN_BATCH_SIZE, &importer, call_err.out()) ==
	          DUCKDB_V2_ERROR_NONE;
	if (schema.release) {
		schema.release(&schema);
	}
	if (!ok) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
		return;
	}
	v2_schema resolved;
	ok = duckdb_v2_arrow_importer_get_schema(importer, resolved.out(), call_err.out()) == DUCKDB_V2_ERROR_NONE;
	duckdb_v2_arrow_importer_destroy(&importer);
	if (!ok) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
		return;
	}
	idx_t count = 0;
	duckdb_v2_schema_get_count(resolved.get(), &count, nullptr);
	if (count == 0) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID,
		                   "Provided table/dataframe must have at least one column");
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		const char *name = nullptr;
		idx_t name_len = 0;
		duckdb_v2_logical_type_handle type = nullptr;
		if (duckdb_v2_schema_get_field(resolved.get(), i, &name, &name_len, &type, call_err.out()) !=
		        DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_table_function_bind_add_result_column(result, name, name_len, type, call_err.out()) !=
		        DUCKDB_V2_ERROR_NONE) {
			set_callback_error(err, call_err.code(), call_err.raw_message());
			return;
		}
	}
	auto bind_data = new ScanBindData {std::move(state)};
	duckdb_v2_opaque opaque = {bind_data, [](void *data) { delete static_cast<ScanBindData *>(data); }, nullptr};
	if (duckdb_v2_function_bind_set_bind_data(info, &opaque, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		delete bind_data;
		set_callback_error(err, call_err.code(), call_err.raw_message());
	}
}

void scan_init_global(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle context,
                      duckdb_v2_error_info_handle *err) {
	v2_error call_err;
	void *bind_ptr = nullptr;
	if (duckdb_v2_table_function_init_global_get_bind_data(info, &bind_ptr, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
		return;
	}
	auto &bind_data = *static_cast<ScanBindData *>(bind_ptr);
	auto global = std::unique_ptr<ScanGlobalState>(new ScanGlobalState());
	// only the stream of the first scan can be read
	if (!bind_data.state->Take(global->taken.stream)) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID, "This stream has been released");
		return;
	}
	ArrowSchema schema {};
	if (global->taken.stream.get_schema(&global->taken.stream, &schema) != 0) {
		set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID,
		                   stream_error(global->taken.stream, "unable to read the schema"));
		return;
	}
	bool ok = duckdb_v2_arrow_importer_create(context, &schema, ARROW_SCAN_BATCH_SIZE, &global->importer,
	                                          call_err.out()) == DUCKDB_V2_ERROR_NONE;
	if (schema.release) {
		schema.release(&schema);
	}
	if (!ok) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
		return;
	}
	duckdb_v2_table_function_init_global_set_max_threads(info, 1, nullptr);
	duckdb_v2_opaque opaque = {global.get(), [](void *data) { delete static_cast<ScanGlobalState *>(data); }, nullptr};
	if (duckdb_v2_table_function_init_global_set_global_state(info, &opaque, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
		return;
	}
	global.release();
}

void scan_exec(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle,
               duckdb_v2_error_info_handle *err) {
	v2_error call_err;
	void *global_ptr = nullptr;
	if (duckdb_v2_table_function_exec_get_global_state(info, &global_ptr, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
		return;
	}
	auto &global = *static_cast<ScanGlobalState *>(global_ptr);
	v2_data_chunk chunk;
	while (true) {
		if (duckdb_v2_arrow_importer_next_chunk(global.importer, chunk.out(), call_err.out()) != DUCKDB_V2_ERROR_NONE) {
			set_callback_error(err, call_err.code(), call_err.raw_message());
			return;
		}
		if (chunk) {
			break;
		}
		if (global.finished) {
			// an empty output chunk is the end of the scan
			return;
		}
		ArrowArray array {};
		array.release = nullptr;
		if (global.taken.stream.get_next(&global.taken.stream, &array) != 0) {
			set_callback_error(err, DUCKDB_V2_ERROR_INPUT_INVALID,
			                   stream_error(global.taken.stream, "unable to read the stream"));
			return;
		}
		bool end = array.release == nullptr;
		if (end) {
			global.finished = true;
		}
		if (duckdb_v2_arrow_importer_append(global.importer, end ? nullptr : &array, true, end, call_err.out()) !=
		    DUCKDB_V2_ERROR_NONE) {
			if (!end && array.release) {
				array.release(&array);
			}
			set_callback_error(err, call_err.code(), call_err.raw_message());
			return;
		}
	}
	duckdb_v2_data_chunk_handle output = nullptr;
	if (duckdb_v2_table_function_exec_get_output_chunk(info, &output, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
		set_callback_error(err, call_err.code(), call_err.raw_message());
		return;
	}
	idx_t column_count = 0;
	idx_t row_count = 0;
	duckdb_v2_data_chunk_get_vector_count(chunk.get(), &column_count, nullptr);
	duckdb_v2_data_chunk_get_size(chunk.get(), &row_count, nullptr);
	for (idx_t i = 0; i < column_count; i++) {
		duckdb_v2_vector_handle target = nullptr;
		duckdb_v2_vector_handle source = nullptr;
		if (duckdb_v2_data_chunk_get_vector(output, i, &target, call_err.out()) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_data_chunk_get_vector(chunk.get(), i, &source, call_err.out()) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vector_reference(target, source, call_err.out()) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vector_set_size(target, row_count, call_err.out()) != DUCKDB_V2_ERROR_NONE) {
			set_callback_error(err, call_err.code(), call_err.raw_message());
			return;
		}
	}
}

// Closes the registered streams of the connection
void close_registered_streams(duckdb_v2_connection_handle conn) {
	std::vector<std::shared_ptr<RegisteredStream>> closing;
	{
		auto &registered = registered_streams();
		std::lock_guard<std::mutex> guard(registered.lock);
		for (auto it = registered.streams.begin(); it != registered.streams.end();) {
			if (it->second->connection == conn) {
				closing.push_back(std::move(it->second));
				it = registered.streams.erase(it);
			} else {
				++it;
			}
		}
	}
	for (auto &state : closing) {
		state->Release();
	}
}

} // namespace

// Registers the table function that scans the registered streams
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1arrow_1register_1scan_1function(JNIEnv *env, jclass,
                                                                                                  jobject conn_buf) {
	auto conn = reinterpret_cast<duckdb_v2_connection_handle>(env->GetDirectBufferAddress(conn_buf));
	if (!conn) {
		env->ThrowNew(J_SQLException, "Invalid connection");
		return;
	}
	v2_error err;
	duckdb_v2_factory_handle factory = nullptr;
	v2_table_function function;
	v2_logical_type bigint;
	duckdb_v2_function_signature_handle signature = nullptr;
	const char *name = ARROW_SCAN_FUNCTION;
	if (duckdb_v2_connection_get_factory(conn, &factory, err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_create(factory, function.out(), err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_set_name(function.get(), name, std::strlen(name), err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_factory_create_type_from_id(factory, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, nullptr, nullptr, nullptr, 0,
	                                          bigint.out(), err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_get_signature(function.get(), &signature, err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_function_signature_add_parameter(signature, "stream_id", 9, bigint.get(), nullptr,
	                                               DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_ONLY,
	                                               err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_set_bind_callback(function.get(), scan_bind, err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_set_init_global_callback(function.get(), scan_init_global, err.out()) !=
	        DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_set_exec_callback(function.get(), scan_exec, err.out()) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_connection_register_table_function(conn, function.get(), err.out()) != DUCKDB_V2_ERROR_NONE) {
		throw_sql_exception(env, err);
	}
}

// Moves the stream out of the Java-owned struct and registers it with the connection, returns its id
JNIEXPORT jlong JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1arrow_1register_1stream(JNIEnv *env, jclass,
                                                                                           jobject conn_buf,
                                                                                           jlong stream_address) {
	auto conn = reinterpret_cast<duckdb_v2_connection_handle>(env->GetDirectBufferAddress(conn_buf));
	auto source = reinterpret_cast<ArrowArrayStream *>(stream_address);
	if (!conn) {
		env->ThrowNew(J_SQLException, "Invalid connection");
		return 0;
	}
	if (!source || !source->release) {
		env->ThrowNew(J_SQLException, "Invalid Input Error: This stream has been released");
		return 0;
	}
	auto state = std::make_shared<RegisteredStream>();
	state->stream = *source;
	source->release = nullptr;
	state->connection = conn;
	auto &registered = registered_streams();
	std::lock_guard<std::mutex> guard(registered.lock);
	state->id = registered.next_id++;
	registered.streams[state->id] = state;
	return static_cast<jlong>(state->id);
}

// Releases the registered stream, if it is not consumed or released yet
JNIEXPORT void JNICALL Java_org_duckdb_DuckDBBindings_duckdb_1v2_1arrow_1release_1stream(JNIEnv *, jclass, jlong id) {
	auto state = registered_streams().Remove(static_cast<int64_t>(id));
	if (state) {
		state->Release();
	}
}
