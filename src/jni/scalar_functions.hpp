#pragma once

#include "bindings.hpp"

void duckdb_jdbc_scalar_function_set_function(JNIEnv *env, jobject scalar_function_buf, jobject function_j);
