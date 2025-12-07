#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from os import path
import sys

version_regex = re.compile(r"""^\#define DUCKDB_SOURCE_ID "([a-z0-9]+)"$""")

project_dir = path.dirname(path.dirname(path.abspath(__file__)))
pragma_version_path = path.join(project_dir, "src/duckdb/src/function/table/version/pragma_version.cpp")

with open(pragma_version_path, "r") as fd:
    for line in fd:
        stripped = line.strip()
        fm = version_regex.fullmatch(stripped)
        if fm is not None:
            print(fm.group(1), end="")
            sys.exit(0)

    print("ERROR: DuckDB version not found", file=sys.stderr)
    sys.exit(1)
