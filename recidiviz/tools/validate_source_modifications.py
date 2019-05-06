# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# ============================================================================
"""Tool to validate sets of files that must be modified together.

This is a very rudimentary if-this-then-that check that ensures if one or more
files in a specified set is modified, then all of the files in that set have
been modified.

This check is skipped if any of the commit messages in the given range start
contains the text '[skip validation]'.

Example usage:
$ python -m recidiviz.tools.validate_source_modifications [--commit-range RANGE]
"""

import argparse
import os
import subprocess
import sys
from typing import FrozenSet, List, Tuple

from recidiviz.ingest.models import ingest_info, ingest_info_pb2
from recidiviz.persistence.database.schema.aggregate import \
    schema as aggregate_schema
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.migrations import versions
from recidiviz.persistence.database.schema.state import schema as state_schema

# Sets of prefixes to check. For each set, if the changes modify a file matching
# any prefix in the set, then it must also modify files matching all other
# prefixes in that set.
#
# New sets of file prefixes can be added to this set. This will cause the check
# to be performed for that new set as well.
MODIFIED_FILE_ASSERTIONS = frozenset((
    # ingest info files
    frozenset((
        os.path.relpath(ingest_info.__file__),  # python object
        os.path.relpath(ingest_info.__file__)[:-2] + 'proto',  # proto
        os.path.relpath(ingest_info_pb2.__file__),  # generated proto source
        os.path.relpath(ingest_info_pb2.__file__) + 'i',  # proto type hints
    )),
    # pipfile
    frozenset((
        'Pipfile', 'Pipfile.lock'
    )),
    # aggregate schema
    frozenset((
        os.path.relpath(aggregate_schema.__file__),  # aggregate schema
        os.path.relpath(versions.__file__[:-len('__init__.py')])  # versions
    )),
    # county schema
    frozenset((
        os.path.relpath(county_schema.__file__),  # county schema
        os.path.relpath(versions.__file__[:-len('__init__.py')])  # versions
    )),
    # state schema
    frozenset((
        os.path.relpath(state_schema.__file__),  # state schema
        os.path.relpath(versions.__file__[:-len('__init__.py')])  # versions
    )),
))


def match_assertions(modified_files: FrozenSet[str],
                     assertion_prefixes: FrozenSet[str]) -> FrozenSet[str]:
    return frozenset(file_prefix for file_prefix in assertion_prefixes
                     if any(modified_file.startswith(file_prefix)
                            for modified_file in modified_files))


def check_assertions(modified_files: FrozenSet[str]) \
        -> List[Tuple[FrozenSet[str], FrozenSet[str]]]:
    failed_assertion_files: List[Tuple[FrozenSet[str], FrozenSet[str]]] = []
    for assertion_prefixes in MODIFIED_FILE_ASSERTIONS:
        matched_prefixes = match_assertions(modified_files, assertion_prefixes)
        if frozenset() < matched_prefixes < assertion_prefixes:
            failed_assertion_files.append((
                matched_prefixes, assertion_prefixes - matched_prefixes))
    return failed_assertion_files


def get_modified_files(commit_range: str) -> FrozenSet[str]:
    git = subprocess.Popen(
        ['git', 'diff', '--name-only', commit_range], stdout=subprocess.PIPE)
    modified_files, _ = git.communicate()
    git.wait()
    return frozenset(modified_files.decode().splitlines())


def format_failure(failure: Tuple[FrozenSet[str], FrozenSet[str]]) -> str:
    return \
        'Failure:\n\tModified file(s):\n{}\n\tWithout modifying file(s):\n{}' \
            .format(
                '\n'.join(map(lambda file: '\t\t' + file, failure[0])),
                '\n'.join(map(lambda file: '\t\t' + file, failure[1])))


SKIP_COMMIT_REGEX = r'\[skip validation\]'


def find_skip_commits(commit_range: str) -> FrozenSet[str]:
    git = subprocess.Popen(
        ['git', 'log', '--format=%h', '--grep={}'.format(SKIP_COMMIT_REGEX),
         commit_range], stdout=subprocess.PIPE)
    skip_commits, _ = git.communicate()
    git.wait()
    return frozenset(skip_commits.decode().splitlines())


def main(commit_range: str):
    skip_commits = find_skip_commits(commit_range)
    if skip_commits:
        print("Skipping check due to skip commits ({})".format(
            ', '.join(skip_commits)))
        sys.exit(0)

    failures = check_assertions(get_modified_files(commit_range))
    return_code = 0
    if failures:
        return_code = 1
        for failure in failures:
            print(format_failure(failure))

    sys.exit(return_code)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--commit-range', default='master...HEAD',
                        help='The git commit range to compare against.')
    args = parser.parse_args()
    main(args.commit_range)
