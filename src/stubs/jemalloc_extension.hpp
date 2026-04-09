#pragma once

#include "duckdb.hpp"

namespace duckdb {

class JemallocExtension : public Extension {
public:
	void Load(ExtensionLoader &) override {};
	std::string Name() override {
		return "";
	};
	std::string Version() const override {
		return "";
	};
};

} // namespace duckdb
