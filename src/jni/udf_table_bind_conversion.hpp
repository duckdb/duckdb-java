#pragma once

extern "C" {
#include "duckdb.h"
}

#include <jni.h>
#include <string>
#include <vector>

jobject table_bind_parameter_to_java(JNIEnv *env, duckdb_value val, duckdb_logical_type logical_type,
                                     std::vector<jobject> &local_refs, std::string &error);
