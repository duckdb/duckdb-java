#pragma once

#include "duckdb_cpp.hpp"

#include <jni.h>
#include <string>

//! The type name that is used by the Java side, DECIMAL types also have the width and the scale
std::string type_to_jduckdb_type(const duckdb::cxx::LogicalType &logical_type);

//! The name of the type, same as the string representation of the type ID in the engine
std::string type_id_to_string(duckdb::cxx::LogicalTypeId id);

duckdb::cxx::Value create_value_from_bigdecimal(JNIEnv *env, jobject decimal, duckdb::cxx::Factory &factory);

duckdb::cxx::Value to_duckdb_value(JNIEnv *env, jobject param, duckdb::cxx::Factory &factory);
