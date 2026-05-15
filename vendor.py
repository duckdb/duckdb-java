import os
import sys
import json
import pickle
import platform
import argparse

parser = argparse.ArgumentParser(description='Inlines DuckDB Sources')

parser.add_argument('--duckdb', action='store',
                    help='Path to the DuckDB Version to be vendored in', required=True, type=str)

args = parser.parse_args()

# list of extensions to bundle, we include jemalloc here to have its
# files copied along with all other sources
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

source_list = [os.path.relpath(x, basedir) if os.path.isabs(x) else os.path.join('src', x)
    for x in source_list]
include_list = [os.path.join('src', 'duckdb', x) for x in include_list]

def sanitize_path(x):
    return x.replace('\\', '/')

source_list = [sanitize_path(x) for x in source_list]
include_list = [sanitize_path(x) for x in include_list]

os.chdir(basedir)

with open('CMakeLists.txt.in', 'r') as f:
    cmake = f.read()

def replace_entries(cmake, replacement_map):
    cmake.replace()

cmake = cmake.replace('${SOURCES}', '\n  '.join(source_list))
cmake = cmake.replace('${INCLUDES}', '\n  '.join(include_list))
cmake = cmake.replace('${DEFINES}', '\n  '.join(['-D'+x for x in defines]))

with open('CMakeLists.txt', 'w+') as f:
    f.write(cmake)
