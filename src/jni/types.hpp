#pragma once

#include "duckdb.hpp"

#include <jni.h>

std::string type_to_jduckdb_type(duckdb::LogicalType logical_type);

duckdb::Value create_value_from_bigdecimal(JNIEnv *env, jobject decimal);

duckdb::Value to_duckdb_value(JNIEnv *env, jobject param, duckdb::ClientContext &context);
