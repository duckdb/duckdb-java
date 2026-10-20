#include "arrow.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

using namespace duckdb;

JavaResultArrowStream::JavaResultArrowStream(duckdb::unique_ptr<QueryResult> res_p,
                                             duckdb::unique_ptr<QueryResultStream<>> stream_p, idx_t batch_size_p)
    : res(std::move(res_p)), res_stream(std::move(stream_p)), batch_size(batch_size_p) {
	if (res) {
		types = res->GetTypes();
		names = IdentifiersToStrings(res->GetNames());
		properties = res->client_properties;
	} else {
		types = res_stream->GetTypes();
		names = IdentifiersToStrings(res_stream->GetNames());
		properties = res_stream->GetClientProperties();
	}
	if (properties.client_context) {
		extension_types = ArrowTypeExtensionData::GetExtensionTypes(*properties.client_context, types);
	}
}

void JavaResultArrowStream::Close() {
	std::lock_guard<std::mutex> guard(lock);
	closed = true;
	chunk = nullptr;
	res = nullptr;
	res_stream = nullptr;
}

ArrowArrayStream *JavaResultArrowStream::Export(duckdb::shared_ptr<JavaResultArrowStream> state) {
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

JavaResultArrowStream &JavaResultArrowStream::GetState(ArrowArrayStream *stream) {
	return *reinterpret_cast<Exported *>(stream->private_data)->state;
}

int JavaResultArrowStream::GetSchema(ArrowArrayStream *stream, ArrowSchema *out) {
	if (!stream->release) {
		return -1;
	}
	auto &self = GetState(stream);
	std::lock_guard<std::mutex> guard(self.lock);
	out->release = nullptr;
	try {
		ArrowConverter::ToArrowSchema(out, self.types, self.names, self.properties);
	} catch (std::exception &e) {
		self.last_error = ErrorData(e);
		return -1;
	}
	return 0;
}

int JavaResultArrowStream::GetNext(ArrowArrayStream *stream, ArrowArray *out) {
	if (!stream->release) {
		return -1;
	}
	auto &self = GetState(stream);
	std::lock_guard<std::mutex> guard(self.lock);
	out->release = nullptr;
	if (self.closed) {
		self.last_error = ErrorData(ConnectionException("Connection was closed"));
		return -1;
	}
	try {
		ArrowAppender appender(self.types, self.batch_size, self.properties, self.extension_types);
		idx_t count = 0;
		while (count < self.batch_size) {
			if (!self.chunk || self.chunk_offset >= self.chunk->size()) {
				if (!self.FetchChunk()) {
					return -1;
				}
				if (!self.chunk) {
					break;
				}
				continue;
			}
			auto &chunk = *self.chunk;
			auto to_append = MinValue(self.batch_size - count, chunk.size() - self.chunk_offset);
			appender.Append(chunk, self.chunk_offset, self.chunk_offset + to_append, chunk.size());
			self.chunk_offset += to_append;
			count += to_append;
		}
		if (count > 0) {
			*out = appender.Finalize();
		}
	} catch (std::exception &e) {
		self.last_error = ErrorData(e);
		return -1;
	}
	return 0;
}

const char *JavaResultArrowStream::GetLastError(ArrowArrayStream *stream) {
	if (!stream->release) {
		return "stream was released";
	}
	auto &self = GetState(stream);
	std::lock_guard<std::mutex> guard(self.lock);
	return self.last_error.HasError() ? self.last_error.Message().c_str() : nullptr;
}

void JavaResultArrowStream::Release(ArrowArrayStream *stream) {
	if (!stream || !stream->release) {
		return;
	}
	stream->release = nullptr;
	delete reinterpret_cast<Exported *>(stream->private_data);
}

bool JavaResultArrowStream::FetchChunk() {
	chunk_offset = 0;
	if (finished) {
		chunk = nullptr;
		return true;
	}
	if (res) {
		chunk = res->Fetch();
		if (res->HasError()) {
			last_error = res->GetErrorObject();
			return false;
		}
	} else {
		chunk = res_stream->Fetch();
		if (res_stream->HasError()) {
			last_error = res_stream->GetErrorObject();
			return false;
		}
	}
	if (!chunk) {
		finished = true;
	}
	return true;
}

JavaRegisteredArrowStream::JavaRegisteredArrowStream(ArrowArrayStream *source) : stream(*source) {
	source->release = nullptr;
}

JavaRegisteredArrowStream::~JavaRegisteredArrowStream() {
	Close();
}

void JavaRegisteredArrowStream::Close() {
	std::lock_guard<std::mutex> guard(lock);
	if (stream.release) {
		stream.release(&stream);
		stream.release = nullptr;
	}
}

void JavaRegisteredArrowStream::GetSchema(ArrowSchema &schema) {
	std::lock_guard<std::mutex> guard(lock);
	if (!stream.release) {
		throw InvalidInputException("This stream has been released");
	}
	if (stream.get_schema(&stream, &schema) != 0) {
		auto error = stream.get_last_error(&stream);
		throw InvalidInputException(error ? error : "Unknown error getting Arrow stream schema");
	}
}

duckdb::unique_ptr<ArrowArrayStreamWrapper> JavaRegisteredArrowStream::Take() {
	std::lock_guard<std::mutex> guard(lock);
	if (!stream.release) {
		throw InvalidInputException("This stream has been released");
	}
	auto res = make_uniq<ArrowArrayStreamWrapper>();
	res->arrow_array_stream = stream;
	stream.release = nullptr;
	return res;
}
