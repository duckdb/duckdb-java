#pragma once

#include "duckdb.hpp"

#include <jni.h>
#include <memory>

std::unique_ptr<duckdb::DBConfig> create_db_config(JNIEnv *env, jboolean read_only, jobject java_config);
