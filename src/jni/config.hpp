#pragma once

#include "holders.hpp"

#include <jni.h>
#include <memory>
#include <string>

/**
 * Opens a database instance, the instance is shared with the other connections to the same database file if
 * the instance cache is enabled and the database is not an in-memory one. The configuration is read from the Java
 * properties.
 */
std::shared_ptr<InstanceHolder> open_instance(JNIEnv *env, const std::string &database, jboolean read_only,
                                              jobject java_config, bool cache_instance);

/**
 * An identifier of the database instance, the same for all connections to the same cached instance.
 */
jlong instance_address(const InstanceHolder &holder);
