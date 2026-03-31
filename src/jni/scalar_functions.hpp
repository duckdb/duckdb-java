#pragma once

#include "bindings.hpp"

void duckdb_jdbc_install_scalar_function_callback(JNIEnv *env, jobject conn_ref_buf, jobject scalar_function_buf,
                                                  jobject function_j);
