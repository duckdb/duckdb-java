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

hpp_files = set(glob('src/jni/*.hpp'))
hpp_files.remove('src/jni/functions.hpp')
cpp_files = set(glob('src/jni/*.cpp'))
cpp_files.remove('src/jni/functions.cpp')
java_files = set(glob('src/**/*.java', recursive=True))

for name in [*hpp_files] + [*cpp_files] + [*java_files]:
    print('Formatting', name)
    check_call(template + [name])

