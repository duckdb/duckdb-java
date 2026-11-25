#include "arrow.hpp"

#include "util.hpp"

#include <cerrno>
#include <cstring>
#include <mutex>
#include <optional>
#include <unordered_map>

/**
 * State of an Arrow stream that exports the rows of a result. The rows of the result are bound to a statement
 * (SELECT * FROM the rows) that runs on a connection of its own with the Arrow result format, so that the connection
 * of the result stays usable while the stream is open. The rows are moved out of the Java-owned holder, a result
 * that is streamed is read into memory first.
 */
class JavaResultArrowStream : public ArrowStreamState {
public:
	JavaResultArrowStream(ConnectionHolder &conn_holder, ResultHolder &source, idx_t batch_size) {
		auto &factory = conn_holder.connection->GetFactory();
		names = source.names;
		if (source.materialized) {
			collection = std::move(source.materialized->collection);
			source.materialized.reset();
		} else {
			collection.emplace(factory.CreateColumnDataCollection(source.types));
			auto append_state = collection->CreateAppendState();
			if (source.first_chunk) {
				collection->Append(append_state, *source.first_chunk);
				source.first_chunk.reset();
			}
			while (true) {
				auto chunk = source.stream->FetchChunk();
				if (!chunk) {
					break;
				}
				collection->Append(append_state, chunk);
			}
			// the connection of the result is free
			source.stream.reset();
		}

		helper = std::make_unique<cxx::Connection>(conn_holder.db->instance.Connect());
		copy_session_settings(*conn_holder.connection, *helper);
		auto iterator = helper->ParseSQL(std::string("SELECT * FROM ") + ROWS_TABLE_NAME);
		statement.emplace(iterator.Next());
		statement->AddCollection(ROWS_TABLE_NAME, *collection, names);
		auto result = helper->Execute(*statement, cxx::ArrowFormat(batch_size));
		inner.release = nullptr;
		result.ToArrowStream(inner);
	}

	~JavaResultArrowStream() override {
		Close();
	}

	void Close() override {
		std::lock_guard<std::mutex> guard(lock);
		closed = true;
		// the result of the stream goes first, as it uses the statement, the rows and the connection
		if (inner.release) {
			inner.release(&inner);
			inner.release = nullptr;
		}
		statement.reset();
		helper.reset();
		collection.reset();
	}

	//! Creates a stream that shares the state, the stream is deleted when it is released
	static ArrowArrayStream *Export(std::shared_ptr<JavaResultArrowStream> state) {
		auto exported = new Exported();
		exported->state = std::move(state);
		auto &stream = exported->stream;
		stream.private_data = exported;
		stream.get_schema = GetSchema;
		stream.get_next = GetNext;
		stream.get_last_error = GetLastError;
		stream.release = Release;
		return &stream;
	}

private:
	static constexpr const char *ROWS_TABLE_NAME = "__jdbc_result_rows";

	struct Exported {
		ArrowArrayStream stream;
		std::shared_ptr<JavaResultArrowStream> state;
	};

	static JavaResultArrowStream &GetState(ArrowArrayStream *stream) {
		return *reinterpret_cast<Exported *>(stream->private_data)->state;
	}

	//! Calls the function of the stream of the result, the error of the call is kept
	template <class FUNCTION>
	int Forward(FUNCTION call) {
		std::lock_guard<std::mutex> guard(lock);
		if (closed || !inner.release) {
			last_error = "Connection Error: Connection was closed";
			return EIO;
		}
		auto res = call();
		if (res != 0) {
			auto error = inner.get_last_error(&inner);
			last_error = error ? error : "unable to read the result";
		}
		return res;
	}

	static int GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
		if (!stream->release) {
			return -1;
		}
		auto &self = GetState(stream);
		out->release = nullptr;
		return self.Forward([&]() { return self.inner.get_schema(&self.inner, out); });
	}

	static int GetNext(ArrowArrayStream *stream, ArrowArray *out) {
		if (!stream->release) {
			return -1;
		}
		auto &self = GetState(stream);
		out->release = nullptr;
		return self.Forward([&]() { return self.inner.get_next(&self.inner, out); });
	}

	static const char *GetLastError(ArrowArrayStream *stream) {
		if (!stream->release) {
			return nullptr;
		}
		auto &self = GetState(stream);
		std::lock_guard<std::mutex> guard(self.lock);
		return self.last_error.empty() ? nullptr : self.last_error.c_str();
	}

	static void Release(ArrowArrayStream *stream) {
		if (!stream->release) {
			return;
		}
		auto exported = reinterpret_cast<Exported *>(stream->private_data);
		stream->release = nullptr;
		delete exported;
	}

	std::mutex lock;
	std::vector<std::string> names;
	std::optional<cxx::ColumnDataCollection> collection;
	std::unique_ptr<cxx::Connection> helper;
	std::optional<cxx::SqlStatement> statement;
	//! The stream of the result of the statement
	ArrowArrayStream inner {};
	bool closed = false;
	std::string last_error;
};

ArrowArrayStream *create_result_arrow_stream(ConnectionHolder &conn_holder, ResultHolder &result, idx_t batch_size) {
	auto state = std::make_shared<JavaResultArrowStream>(conn_holder, result, batch_size);
	conn_holder.track_arrow_stream(state);
	return JavaResultArrowStream::Export(std::move(state));
}

namespace {

//! The name of the table function that scans a registered Arrow stream
const char *const ARROW_SCAN_FUNCTION = "__jdbc_arrow_stream_scan";

/**
 * State of an Arrow stream registered with a connection. The stream is moved out of the Java-owned struct on
 * registration, it is released when it is consumed by a scan or when the connection is closed.
 */
class JavaRegisteredArrowStream : public ArrowStreamState {
public:
	explicit JavaRegisteredArrowStream(ArrowArrayStream *source) {
		stream = *source;
		source->release = nullptr;
	}

	~JavaRegisteredArrowStream() override {
		Close();
	}

	void Close() override;

	void GetSchema(ArrowSchema &schema) {
		std::lock_guard<std::mutex> guard(lock);
		if (!stream.release) {
			throw JdbcException("Invalid Input Error: This stream has been released");
		}
		schema.release = nullptr;
		if (stream.get_schema(&stream, &schema) != 0) {
			auto error = stream.get_last_error(&stream);
			throw JdbcException(std::string("Invalid Input Error: ") + (error ? error : "unable to read the schema"));
		}
	}

	//! A stream can only be consumed by a single scan
	void Take(ArrowArrayStream &out);

	int64_t id = 0;

private:
	std::mutex lock;
	ArrowArrayStream stream;
};

//! The registered streams by the ids that the views pass to the scan function
struct ArrowStreamRegistry {
	std::mutex lock;
	int64_t next_id = 1;
	//! The streams are owned until they are consumed by a scan or closed with their connection
	std::unordered_map<int64_t, std::shared_ptr<JavaRegisteredArrowStream>> streams;

	void Add(const std::shared_ptr<JavaRegisteredArrowStream> &state) {
		std::lock_guard<std::mutex> guard(lock);
		state->id = next_id++;
		streams[state->id] = state;
	}

	std::shared_ptr<JavaRegisteredArrowStream> Get(int64_t id) {
		std::lock_guard<std::mutex> guard(lock);
		auto it = streams.find(id);
		return it == streams.end() ? nullptr : it->second;
	}

	void Remove(int64_t id) {
		// the state can be destroyed here, it is destroyed after the lock is released
		std::shared_ptr<JavaRegisteredArrowStream> removed;
		{
			std::lock_guard<std::mutex> guard(lock);
			auto it = streams.find(id);
			if (it != streams.end()) {
				removed = std::move(it->second);
				streams.erase(it);
			}
		}
	}
};

ArrowStreamRegistry &arrow_stream_registry() {
	static ArrowStreamRegistry registry;
	return registry;
}

void JavaRegisteredArrowStream::Close() {
	{
		std::lock_guard<std::mutex> guard(lock);
		if (stream.release) {
			stream.release(&stream);
			stream.release = nullptr;
		}
	}
	arrow_stream_registry().Remove(id);
}

void JavaRegisteredArrowStream::Take(ArrowArrayStream &out) {
	{
		std::lock_guard<std::mutex> guard(lock);
		if (!stream.release) {
			throw JdbcException("Invalid Input Error: This stream has been released");
		}
		out = stream;
		stream.release = nullptr;
	}
	arrow_stream_registry().Remove(id);
}

//! Releases an Arrow stream that was taken from a registered stream
struct TakenArrowStream {
	ArrowArrayStream stream {};

	TakenArrowStream() = default;
	TakenArrowStream(const TakenArrowStream &) = delete;
	TakenArrowStream &operator=(const TakenArrowStream &) = delete;

	~TakenArrowStream() {
		if (stream.release) {
			stream.release(&stream);
		}
	}
};

struct ArrowScanBindData {
	explicit ArrowScanBindData(std::shared_ptr<JavaRegisteredArrowStream> state_p) : state(std::move(state_p)) {
	}

	std::shared_ptr<JavaRegisteredArrowStream> state;
};

struct ArrowScanGlobalState {
	ArrowScanGlobalState(const std::shared_ptr<JavaRegisteredArrowStream> &state, cxx::Context &context) {
		// only the stream of the first scan can be read
		state->Take(taken.stream);
		ArrowSchema schema {};
		if (taken.stream.get_schema(&taken.stream, &schema) != 0) {
			auto error = taken.stream.get_last_error(&taken.stream);
			throw JdbcException(std::string("Invalid Input Error: ") + (error ? error : "unable to read the schema"));
		}
		try {
			// the arrays are split into chunks that fit into the output chunk
			std::vector<cxx::LogicalType> types;
			types.push_back(context.GetFactory().CreateType(cxx::LogicalTypeId::BOOLEAN));
			auto capacity = context.GetFactory().CreateDataChunk(types).GetCapacity();
			importer.emplace(context, schema, capacity);
		} catch (...) {
			if (schema.release) {
				schema.release(&schema);
			}
			throw;
		}
		if (schema.release) {
			schema.release(&schema);
		}
	}

	//! The next chunk of the stream, the chunk is empty at the end of the stream
	cxx::DataChunk NextChunk() {
		while (true) {
			auto chunk = importer->NextChunk();
			if (chunk) {
				return chunk;
			}
			if (finished) {
				return chunk;
			}
			ArrowArray array {};
			array.release = nullptr;
			if (taken.stream.get_next(&taken.stream, &array) != 0) {
				auto error = taken.stream.get_last_error(&taken.stream);
				throw JdbcException(std::string("Invalid Input Error: ") +
				                    (error ? error : "unable to read the stream"));
			}
			if (!array.release) {
				// the end of the stream
				finished = true;
				importer->Flush();
				continue;
			}
			importer->Append(array, true);
		}
	}

	TakenArrowStream taken;
	std::optional<cxx::ArrowImporter> importer;
	bool finished = false;
};

void arrow_scan_bind(cxx::TableFunction::BindInput &input) {
	auto id = input.GetConstantArgument(0).Get<int64_t>();
	auto state = arrow_stream_registry().Get(id);
	if (!state) {
		throw JdbcException("Invalid Input Error: This stream has been released");
	}
	ArrowSchema schema {};
	state->GetSchema(schema);
	try {
		cxx::ArrowImporter importer(input.GetContext(), schema);
		auto resolved = importer.GetSchema();
		if (resolved.GetFieldCount() == 0) {
			throw JdbcException("Invalid Input Error: Provided table/dataframe must have at least one column");
		}
		for (idx_t i = 0; i < resolved.GetFieldCount(); i++) {
			input.AddResultColumn(std::string(resolved.GetFieldName(i)), resolved.GetFieldType(i));
		}
	} catch (...) {
		if (schema.release) {
			schema.release(&schema);
		}
		throw;
	}
	if (schema.release) {
		schema.release(&schema);
	}
	input.SetBindData<ArrowScanBindData>(std::move(state));
}

void arrow_scan_init_global(cxx::TableFunction::InitGlobalInput &input) {
	auto &bind_data = input.GetBindData<ArrowScanBindData>();
	input.SetGlobalState<ArrowScanGlobalState>(bind_data.state, input.GetContext());
}

void arrow_scan_exec(cxx::TableFunction::ExecInput &input) {
	auto &global = input.GetGlobalState<ArrowScanGlobalState>();
	auto chunk = global.NextChunk();
	if (!chunk) {
		// an empty output chunk is the end of the scan
		return;
	}
	auto output = input.GetOutputChunk();
	for (idx_t i = 0; i < chunk.GetVectorCount(); i++) {
		auto target = output.GetVector(i);
		target.Reference(chunk.GetVector(i));
	}
}

} // namespace

//! The message of the error without the name of its type, as in "Invalid Input Error: message"
static std::string error_message_without_type(const std::string &message) {
	auto pos = message.find(" Error: ");
	if (pos == std::string::npos || pos > 40) {
		return message;
	}
	return message.substr(pos + 8);
}

void register_arrow_stream(ConnectionHolder &conn_holder, ArrowArrayStream *stream, const std::string &name) {
	auto state = std::make_shared<JavaRegisteredArrowStream>(stream);
	conn_holder.track_arrow_stream(state);
	arrow_stream_registry().Add(state);
	auto id = state->id;

	std::string quoted_name = "\"";
	for (char c : name) {
		if (c == '"') {
			quoted_name += "\"\"";
		} else {
			quoted_name += c;
		}
	}
	quoted_name += "\"";
	try {
		std::lock_guard<std::mutex> guard(*conn_holder.lock);
		conn_holder.connection
		    ->Execute("CREATE OR REPLACE TEMPORARY VIEW " + quoted_name + " AS SELECT * FROM " + ARROW_SCAN_FUNCTION +
		              "(" + std::to_string(id) + ")")
		    .Drain();
	} catch (const std::exception &e) {
		state->Close();
		throw JdbcException(std::string("Failed to create view '") + name +
		                    "': " + error_message_without_type(e.what()));
	}
	// the stream of the view that is replaced is released
	auto replaced = conn_holder.replace_registered_stream(name, state);
	if (replaced) {
		replaced->Close();
	}
}

void register_arrow_scan_function(cxx::Instance &instance) {
	auto conn = instance.Connect();
	auto &factory = conn.GetFactory();
	auto function = cxx::TableFunction::Create(factory);
	function.SetName(ARROW_SCAN_FUNCTION);
	function.WithSignature([&factory](cxx::FunctionSignature &signature) {
		signature.AddParameter("stream_id", factory.CreateType(cxx::LogicalTypeId::BIGINT));
	});
	function.SetBindCallback(arrow_scan_bind);
	function.SetInitGlobalCallback(arrow_scan_init_global);
	function.SetExecCallback(arrow_scan_exec);
	conn.Register(function);
}
