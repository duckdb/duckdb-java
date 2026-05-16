import os
import sys
import json
import pickle
import platform
import argparse

def sanitize_path(x):
    return x.replace('\\', '/')

parser = argparse.ArgumentParser(description='Inlines DuckDB Sources')

parser.add_argument('--duckdb', action='store',
                    help='Path to the DuckDB Version to be vendored in', required=True, type=str)

args = parser.parse_args()

# list of extensions to bundle
extensions = ['core_functions', 'parquet', 'icu', 'json']

# path to target
basedir = os.getcwd()
target_dir = os.path.join(basedir, 'src', 'duckdb')

# path to package_build.py
os.chdir(os.path.join(args.duckdb))
scripts_dir = 'scripts'

sys.path.append(scripts_dir)
import package_build

defines = ['DUCKDB_EXTENSION_{}_LINKED'.format(ext.upper()) for ext in extensions]

# fresh build - copy over all of the files
(source_list, include_list, original_sources) = package_build.build_package(target_dir, extensions, False)
source_list = [sanitize_path(x) for x in source_list]
include_list = [sanitize_path(x) for x in include_list]

# process jemalloc separately with its own CMake vars
jemalloc_source_list = [x for x in source_list if x.startswith("duckdb/third_party/jemalloc/")]
jemalloc_include_list = [x for x in include_list if x.startswith("third_party/jemalloc/")]

# clean up paths
source_list = [os.path.relpath(x, basedir) if os.path.isabs(x) else os.path.join('src', x)
    for x in source_list if x not in jemalloc_source_list]
source_list = [x.replace("/./", "/") for x in source_list]
jemalloc_source_list = [os.path.relpath(x, basedir) if os.path.isabs(x) else os.path.join('src', x)
    for x in jemalloc_source_list]
jemalloc_source_list = [x for x in jemalloc_source_list if not x.endswith("jemalloc_cpp.cpp")]
include_list = [os.path.join('src', 'duckdb', x) for x in include_list if x not in jemalloc_include_list]
jemalloc_include_list = [os.path.join('src', 'duckdb', x) for x in jemalloc_include_list]

# sort paths
source_list.sort()
jemalloc_source_list.sort()
include_list.sort()
jemalloc_include_list.sort()

os.chdir(basedir)

with open('CMakeLists.txt.in', 'r') as f:
    cmake = f.read()

def replace_entries(cmake, replacement_map):
    cmake.replace()

cmake = cmake.replace('${SOURCES}', '\n  '.join(source_list))
cmake = cmake.replace('${JEMALLOC_SOURCES}', '\n  '.join(jemalloc_source_list))
cmake = cmake.replace('${INCLUDES}', '\n  '.join(include_list))
cmake = cmake.replace('${JEMALLOC_INCLUDES}', '\n  '.join(jemalloc_include_list))
cmake = cmake.replace('${DEFINES}', '\n  '.join(['-D'+x for x in defines]))

with open('CMakeLists.txt', 'w+') as f:
    f.write(cmake)
