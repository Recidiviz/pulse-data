# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
# =============================================================================
"""
Utility to find TODOs in our codebase.

Find TODOs referencing specific issues:
$ python -m recidiviz.tools.github.find_todos --issues "#123" Recidiviz/pulse-data#789
"""

import argparse
import logging
import sys
from typing import Dict, List, Mapping, Optional, Set

from recidiviz.repo.issue import GithubIssue, Issue
from recidiviz.repo.issue_references import (
    CodeReference,
    get_issue_references,
    to_markdown,
)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(description="Finds TODOs in our codebase.")
    parser.add_argument(
        "--issues",
        nargs="+",
        default=None,
        help="""Only include TODOs for these specific issues, in the form '#XXXX' or
        'owner/repo#XXXX'. E.g. --issues "#123" Recidiviz/pulse-data#789""",
    )
    parser.add_argument(
        "--fail-if-found",
        action="store_true",
        help="Return a non-zero error code if any references exist.",
    )
    parser.add_argument(
        "--minimal-output",
        action="store_true",
        help="Only outputs the issues, for use in automation.",
    )
    return parser


def filter_to_issues(
    issues_to_include: Set[GithubIssue],
    issue_references: Dict[Issue, List[CodeReference]],
) -> Dict[Issue, List[CodeReference]]:
    return {
        issue: issue_references[issue]
        for issue in issues_to_include
        if issue in issue_references
    }


def write_output(
    issue_references: Mapping[Issue, List[CodeReference]], minimal_output: bool
) -> None:
    if not issue_references:
        if not minimal_output:
            print("No matching references found")
        return

    if not minimal_output:
        print("Found the following matching issue references:")
        print()

    print(to_markdown(issue_references))


def main(
    issues: Optional[List[str]],
    fail_if_found: bool,
    minimal_output: bool,
) -> int:
    issue_references = get_issue_references()
    if issues is not None:
        issues_to_include = {GithubIssue.from_string(issue) for issue in issues}
        issue_references = filter_to_issues(issues_to_include, issue_references)
    write_output(issue_references, minimal_output)

    return 1 if fail_if_found and issue_references else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = create_parser().parse_args()
    sys.exit(
        main(
            args.issues,
            args.fail_if_found,
            args.minimal_output,
        )
    )
