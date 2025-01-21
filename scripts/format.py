from subprocess import check_call
from os import scandir
from glob import glob
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('--check', action='store_true')
args = parser.parse_args()

template = ['clang-format', '-i']
if args.check:
    template += ['--dry-run', '--Werror']

for name in ['src/jni/duckdb_java.cpp'] + glob('src/**/*.java', recursive=True):
    print('Formatting', name)
    check_call(template + [name])

