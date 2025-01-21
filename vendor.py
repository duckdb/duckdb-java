import os
import sys
import json
import pickle
import argparse

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

# # Autoloading is on by default for node distributions
# defines.extend(['DUCKDB_EXTENSION_AUTOLOAD_DEFAULT=1', 'DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT=1'])






# fresh build - copy over all of the files
(source_list, include_list, original_sources) = package_build.build_package(target_dir, extensions, False)



# # # the list of all source files (.cpp files) that have been copied into the `duckdb_source_copy` directory
# # print(source_list)
# # # the list of all include files
# # print(include_list)
source_list = [os.path.relpath(x, basedir) if os.path.isabs(x) else os.path.join('src', x) for x in source_list]
include_list = [os.path.join('src', 'duckdb', x) for x in include_list]

libraries = []

def sanitize_path(x):
    return x.replace('\\', '/')


source_list = [sanitize_path(x) for x in source_list]
include_list = [sanitize_path(x) for x in include_list]
libraries = [sanitize_path(x) for x in libraries]

os.chdir(basedir)

with open('CMakeLists.txt.in', 'r') as f:
    cmake = f.read()


def replace_entries(cmake, replacement_map):
    cmake.replace()


cmake = cmake.replace('${SOURCE_FILES}', ' '.join(source_list))
cmake = cmake.replace('${INCLUDE_FILES}', ' '.join(include_list))
cmake = cmake.replace('${DEFINES}', ' '.join(['-D'+x for x in defines]))
cmake = cmake.replace('${LIBRARY_FILES}', ' '.join(libraries))
# cmake.replace('${CFLAGS}', cflags)
# cmake.replace('${WINDOWS_OPTIONS}', windows_options)


with open('CMakeLists.txt', 'w+') as f:
    f.write(cmake)
