from subprocess import check_call
from os import scandir
from glob import glob

for name in ['src/jni/duckdb_java.cpp'] + glob('src/**/*.java', recursive=True):
    print('Formatting', name)
    check_call(['clang-format', '-i', name])

