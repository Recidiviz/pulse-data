# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

This uses a rudimentary if-this-then-that check that ensures if one or more files in a specified set is modified,
then all of the files in that set have been modified.

This entire check is skipped if any of the commit messages in the given range start contains the text
'[skip validation]'. It is also possible to skip specific sets of assertions if any of the commit messages in the
given range contain the text '[skip validation some-validation-key]'. For example, '[skip validation state]'.

Example usage:
$ python -m recidiviz.tools.validate_source_modifications [--commit-range RANGE]
"""

import argparse
import logging
import os
import re
import subprocess
import sys
from collections import defaultdict
from inspect import getmembers, isfunction
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

import attr
from flask import Flask
from werkzeug.routing import Rule

from recidiviz.ingest.models import ingest_info, ingest_info_pb2
from recidiviz.server import get_blueprints_for_documentation
from recidiviz.tools.docs.endpoint_documentation_generator import (
    EndpointDocumentationGenerator,
)
from recidiviz.utils.regions import get_supported_direct_ingest_region_codes

ENDPOINT_DOCS_DIRECTORY = "docs/endpoints"


@attr.s(auto_attribs=True)
class RequiredModificationSets:
    if_modified_files: FrozenSet[str]
    then_modified_files: FrozenSet[str]

    @staticmethod
    def for_symmetric_check(files: FrozenSet[str]) -> "RequiredModificationSets":
        return RequiredModificationSets(
            if_modified_files=files,
            then_modified_files=files,
        )


def _get_file_for_endpoint_rule(rule: Rule) -> Optional[str]:
    """Given a rule, look for the actual file within the codebase that has the method definition
    for the endpoint defined. This is done by crawling through all of our customized modules imported
    and finding the exact function that matches."""
    endpoint_parts = rule.endpoint.split(".")
    endpoint_module = endpoint_parts[0]
    function_name = endpoint_parts[1] if len(endpoint_parts) > 1 else None
    recidiviz_modules = [
        module
        for module in sys.modules
        if "recidiviz" in module and "tools" not in module and "tests" not in module
    ]
    file_for_endpoint: Optional[str] = None
    for module in recidiviz_modules:
        if endpoint_module in module or not file_for_endpoint:
            for func, _ in getmembers(sys.modules[module], isfunction):
                if func == function_name:
                    file_for_endpoint = os.path.relpath(sys.modules[module].__file__)
                    break
    return file_for_endpoint


def _get_modified_endpoints() -> List[RequiredModificationSets]:
    """Returns the dynamic set of documentation for the App Engine endpoints and the corresponding
    source code modifications."""
    temp_app = Flask(__name__)
    all_blueprints_with_url_prefixes = get_blueprints_for_documentation()
    for blueprint, url_prefix in all_blueprints_with_url_prefixes:
        temp_app.register_blueprint(blueprint, url_prefix=url_prefix)

    doc_generator = EndpointDocumentationGenerator()
    endpoint_files_to_markdown_paths: Dict[str, List[str]] = defaultdict(list)
    for rule in temp_app.url_map.iter_rules():
        file_for_endpoint = _get_file_for_endpoint_rule(rule)
        if file_for_endpoint:
            endpoint_files_to_markdown_paths[file_for_endpoint].append(
                doc_generator.generate_markdown_path_for_endpoint(
                    ENDPOINT_DOCS_DIRECTORY, rule.rule
                )
            )

    required_modification_sets = []
    for (
        endpoint_file,
        markdown_paths,
    ) in endpoint_files_to_markdown_paths.items():
        common_path = os.path.commonpath(markdown_paths)
        paths_to_modify = (
            # If an endpoint python file has methods that produce two endpoints with different url_prefixes,
            # the common_path will end up being the docs directory (docs/endpoints). So in this case,
            # we will add the next immediate top-level directories to the set that should be modified,
            # since that directory is the url_prefix that will be affixed to the endpoint.
            # (e.g. docs/endpoints/single_count, docs/endpoints/scraper)
            {"/".join(top_level_dir.split("/")[:3]) for top_level_dir in markdown_paths}
            if common_path == ENDPOINT_DOCS_DIRECTORY
            else {common_path}
        )
        required_modification_sets.append(
            RequiredModificationSets(
                if_modified_files=frozenset({endpoint_file}),
                then_modified_files=frozenset(paths_to_modify),
            )
        )
    return required_modification_sets


# Sets of prefixes to check. For each set, if the changes modify a file matching
# any prefix in the set, then it must also modify files matching all other
# prefixes in that set.
#
# New sets of file prefixes can be added to this set. This will cause the check
# to be performed for that new set as well.

INGEST_KEY = "ingest"
PIPFILE_KEY = "pipfile"
INGEST_DOCS_KEY = "ingest_docs"
CASE_TRIAGE_FIXTURES_KEY = "case_triage_fixtures"
ENDPOINTS_DOCS_KEY = "endpoints_docs"

MODIFIED_FILE_ASSERTIONS: Dict[str, List[RequiredModificationSets]] = {
    # ingest info files
    INGEST_KEY: [
        RequiredModificationSets(
            if_modified_files=frozenset(
                {
                    os.path.relpath(ingest_info.__file__),  # python object
                    os.path.relpath(ingest_info.__file__)[:-2] + "proto",  # proto
                }
            ),
            then_modified_files=frozenset(
                {
                    os.path.relpath(ingest_info.__file__),  # python object
                    os.path.relpath(ingest_info.__file__)[:-2] + "proto",  # proto
                    os.path.relpath(ingest_info_pb2.__file__),  # generated proto source
                    os.path.relpath(ingest_info_pb2.__file__) + "i",  # proto type hints
                }
            ),
        )
    ],
    # pipfile
    PIPFILE_KEY: [
        RequiredModificationSets(
            if_modified_files=frozenset({"Pipfile"}),
            then_modified_files=frozenset({"Pipfile.lock"}),
        )
    ],
    # ingest docs
    INGEST_DOCS_KEY: [
        RequiredModificationSets(
            if_modified_files=frozenset(
                {f"recidiviz/ingest/direct/regions/{region_code}/"}
            ),
            then_modified_files=frozenset({f"docs/ingest/{region_code}/"}),
        )
        for region_code in get_supported_direct_ingest_region_codes()
    ],
    # case triage demo data
    CASE_TRIAGE_FIXTURES_KEY: [
        RequiredModificationSets(
            if_modified_files=frozenset(
                {f"recidiviz/tools/case_triage/fixtures/etl_{data_type}.csv"}
            ),
            then_modified_files=frozenset(
                {f"recidiviz/case_triage/fixtures/demo_{data_type}.json"}
            ),
        )
        for data_type in ["clients", "opportunities"]
    ],
    ENDPOINTS_DOCS_KEY: _get_modified_endpoints(),
}


def _match_filenames(
    modified_files: FrozenSet[str], required_modification_sets: RequiredModificationSets
) -> Tuple[FrozenSet[str], FrozenSet[str]]:
    """Returns all of the assertions in the set of expected assertions which are actually contained within the set of
    modified files."""
    matched_prefixes = frozenset(
        file_prefix
        for file_prefix in required_modification_sets.if_modified_files
        if any(
            modified_file.startswith(file_prefix) for modified_file in modified_files
        )
    )
    if not matched_prefixes:
        return frozenset(), frozenset()
    matched_assertions = frozenset(
        file_prefix
        for file_prefix in required_modification_sets.then_modified_files
        if any(
            modified_file.startswith(file_prefix) for modified_file in modified_files
        )
    )

    if matched_assertions < required_modification_sets.then_modified_files:
        return (
            matched_prefixes,
            required_modification_sets.then_modified_files - matched_assertions,
        )

    return matched_prefixes, frozenset()


def check_assertions(
    modified_files: FrozenSet[str], sets_to_skip: FrozenSet[str]
) -> List[Tuple[FrozenSet[str], FrozenSet[str]]]:
    """Checks that all of the the modified files against the global set of modification assertions, and returns any
    failed assertions.

    If any of the modified files are part of a set of files that must be modified together and the other files in that
    set are not themselves modified, that constitutes a failure. However, if that set is provided as one of the sets
    to skip, then any such failure is ignored.
    """
    failed_assertion_files: List[Tuple[FrozenSet[str], FrozenSet[str]]] = []

    for set_to_validate, modifications in MODIFIED_FILE_ASSERTIONS.items():
        if set_to_validate in sets_to_skip:
            logging.info("Skipping %s check due to skip commits.", set_to_validate)
            continue

        for required_modification_sets in modifications:
            matched_prefixes, failed_prefixes = _match_filenames(
                modified_files, required_modification_sets
            )
            if failed_prefixes:
                failed_assertion_files.append((matched_prefixes, failed_prefixes))

    return failed_assertion_files


def _get_modified_files(commit_range: str) -> FrozenSet[str]:
    """Returns a set of all files that have been modified in the given commit range."""
    git = subprocess.run(
        ["git", "diff", "--name-only", commit_range],
        stdout=subprocess.PIPE,
        check=True,
    )
    return frozenset(git.stdout.decode().splitlines())


def _format_failure(failure: Tuple[FrozenSet[str], FrozenSet[str]]) -> str:
    """Returns the set of source modification assertion failures in a pretty-printable format."""
    return "Failure:\n\tModified file(s):\n{}\n\tWithout modifying file(s):\n{}".format(
        "\n".join(map(lambda file: "\t\t" + file, failure[0])),
        "\n".join(map(lambda file: "\t\t" + file, failure[1])),
    )


_SKIP_COMMIT_REGEX = r"\[skip validation.*\]"


def _should_skip(validation_message: str, validation_key: str) -> bool:
    """Returns whether or not the given validation set should be skipped based on the commit message."""
    return len(validation_message) < 2 or validation_key in validation_message


def _get_assertions_to_skip(commit_range: str) -> FrozenSet[str]:
    """Returns a set of all of the assertion sets that should be skipped for the given commit range.

    This is determined based on the commit messages in this range, i.e. whether commit messages were written to skip
    either all assertion sets or specific assertion sets.
    """
    git = subprocess.run(
        [
            "git",
            "log",
            "--format=%h %B",
            "--grep={}".format(_SKIP_COMMIT_REGEX),
            commit_range,
        ],
        stdout=subprocess.PIPE,
        check=True,
    )

    if git.returncode != 0:
        logging.error("git log failed")
        sys.exit(git.returncode)

    sets_to_skip = set()  # type: Set[str]
    full_validation_message = re.findall(
        r"\[skip validation(.*?)\]", git.stdout.decode()
    )
    if full_validation_message:
        for valid_message in full_validation_message:
            skip_sets = [
                key
                for key in MODIFIED_FILE_ASSERTIONS
                if _should_skip(valid_message, key)
            ]
            sets_to_skip = sets_to_skip.union(skip_sets)

    return frozenset(sets_to_skip)


def main(commit_range: str) -> None:
    assertions_to_skip = _get_assertions_to_skip(commit_range)

    failures = check_assertions(_get_modified_files(commit_range), assertions_to_skip)
    return_code = 0
    if failures:
        return_code = 1
        for failure in failures:
            logging.warning(_format_failure(failure))

    sys.exit(return_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--commit-range",
        default="master...HEAD",
        help="The git commit range to compare against.",
    )
    args = parser.parse_args()
    main(args.commit_range)
