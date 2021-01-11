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
import logging
import os
import re
import subprocess
import sys
from typing import FrozenSet, List, Set, Tuple

from recidiviz.ingest.models import ingest_info, ingest_info_pb2
from recidiviz.persistence.database.schema.aggregate import \
    schema as aggregate_schema
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.migrations.jails import \
    versions as jails_versions
from recidiviz.persistence.database.migrations.operations import \
    versions as operations_versions
from recidiviz.persistence.database.schema.operations import schema as operations_schema
from recidiviz.persistence.database.migrations.state import \
    versions as state_versions
from recidiviz.persistence.database.schema.state import schema as state_schema

# Sets of prefixes to check. For each set, if the changes modify a file matching
# any prefix in the set, then it must also modify files matching all other
# prefixes in that set.
#
# New sets of file prefixes can be added to this set. This will cause the check
# to be performed for that new set as well.

INGEST_KEY = "ingest"
PIPFILE_KEY = "pipfile"
AGGREGATE_KEY = "aggregate"
COUNTY_KEY = "county"
OPERATIONS_KEY = "operations"
STATE_KEY = "state"

MODIFIED_FILE_ASSERTIONS = {
    # ingest info files
    INGEST_KEY:
        frozenset((
            os.path.relpath(ingest_info.__file__),  # python object
            os.path.relpath(ingest_info.__file__)[:-2] + 'proto',  # proto
            os.path.relpath(ingest_info_pb2.__file__),  # generated proto source
            os.path.relpath(ingest_info_pb2.__file__) + 'i',  # proto type hints
        )),
    # pipfile
    PIPFILE_KEY:
        frozenset((
            'Pipfile', 'Pipfile.lock'
        )),
    # aggregate schema
    AGGREGATE_KEY:
        frozenset((
            os.path.relpath(aggregate_schema.__file__),  # aggregate schema
            os.path.relpath(
                jails_versions.__file__[:-len('__init__.py')])  # versions
        )),
    # county schema
    COUNTY_KEY:
        frozenset((
            os.path.relpath(county_schema.__file__),  # county schema
            os.path.relpath(
                jails_versions.__file__[:-len('__init__.py')])  # versions
        )),
    # operations schema
    OPERATIONS_KEY:
        frozenset((
            os.path.relpath(operations_schema.__file__),  # operations schema
            os.path.relpath(
                operations_versions.__file__[:-len('__init__.py')])  # versions
        )),
    # state schema
    STATE_KEY:
        frozenset((
            os.path.relpath(state_schema.__file__),  # state schema
            os.path.relpath(
                state_versions.__file__[:-len('__init__.py')])  # versions
        ))
}


def match_assertions(modified_files: FrozenSet[str],
                     assertion_prefixes: FrozenSet[str]) -> FrozenSet[str]:
    return frozenset(file_prefix for file_prefix in assertion_prefixes
                     if any(modified_file.startswith(file_prefix)
                            for modified_file in modified_files))


def check_assertions(modified_files: FrozenSet[str], sets_to_skip: FrozenSet[str]) \
        -> List[Tuple[FrozenSet[str], FrozenSet[str]]]:
    failed_assertion_files: List[Tuple[FrozenSet[str], FrozenSet[str]]] = []
    for set_to_validate, assertion_prefixes in MODIFIED_FILE_ASSERTIONS.items():
        if set_to_validate in sets_to_skip:
            logging.info('Skipping %s check due to skip commits.', set_to_validate)
            continue
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


SKIP_COMMIT_REGEX = r'\[skip validation.*\]'


def _should_skip(validation_message: str, validation_key: str) -> bool:
    return len(validation_message) < 2 or validation_key in validation_message


def get_assertions_to_skip(commit_range: str) -> FrozenSet[str]:
    git = subprocess.Popen(
        ['git', 'log', '--format=%h %B', '--grep={}'.format(SKIP_COMMIT_REGEX),
         commit_range], stdout=subprocess.PIPE)
    skip_commits, _ = git.communicate()
    git.wait()
    if git.returncode != 0:
        logging.error('git log failed')
        sys.exit(git.returncode)
    sets_to_skip = set()  # type: Set[str]
    full_validation_message = re.findall(r'\[skip validation(.*?)\]', skip_commits.decode())
    if full_validation_message:
        for valid_message in full_validation_message:
            skip_sets = [key for key in MODIFIED_FILE_ASSERTIONS if _should_skip(valid_message, key)]
            sets_to_skip = sets_to_skip.union(skip_sets)
    return frozenset(sets_to_skip)


def main(commit_range: str) -> None:
    assertions_to_skip = get_assertions_to_skip(commit_range)

    failures = check_assertions(get_modified_files(commit_range), assertions_to_skip)
    return_code = 0
    if failures:
        return_code = 1
        for failure in failures:
            logging.warning(format_failure(failure))

    sys.exit(return_code)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--commit-range', default='master...HEAD',
                        help='The git commit range to compare against.')
    args = parser.parse_args()
    main(args.commit_range)
