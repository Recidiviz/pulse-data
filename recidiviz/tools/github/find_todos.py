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

Find closed TODOs:
$ python -m recidiviz.tools.github.find_todos --closed --github_token $GITHUB_PAT

Note, the $GITHUB_PAT here is a Personal Access Token for Github. If you need one, they
can be created here: https://github.com/settings/tokens

Find TODOs referencing specific issues:
$ python -m recidiviz.tools.github.find_todos --issues "#123" Recidiviz/pulse-data#789
"""

import argparse
import logging
import sys
from typing import Dict, List, Optional, Set

import github

from recidiviz.repo.issue_references import (
    CodeReference,
    GithubIssue,
    get_issue_references,
    to_markdown,
)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser(description="Finds TODOs in our codebase.")
    parser.add_argument(
        "--closed",
        type=bool,
        nargs="?",
        const=True,
        default=False,
        help="Only include references to closed issues.",
    )
    parser.add_argument(
        "--github-token",
        default=None,
        help="Token to use when talking to github, required when filtering to closed issues.",
    )
    parser.add_argument(
        "--issues",
        nargs="+",
        default=None,
        help="""Only include TODOs for these specific issues, in the form '#XXXX' or
        'owner/repo#XXXX'. E.g. --issues "#123" Recidiviz/pulse-data#789""",
    )
    parser.add_argument(
        "--fail-if-found",
        type=bool,
        nargs="?",
        const=True,
        default=False,
        help="Return a non-zero error code if any references exist.",
    )
    parser.add_argument(
        "--minimal-output",
        type=bool,
        nargs="?",
        const=True,
        default=False,
        help="Only outputs the issues, for use in automation.",
    )
    return parser


def filter_to_closed(
    github_token: str, issue_references: Dict[GithubIssue, List[CodeReference]]
) -> Dict[GithubIssue, List[CodeReference]]:
    g = github.Github(login_or_token=github_token)

    closed_issue_references = {}

    for issue in issue_references:
        if issue.number == 0:
            # Issue #0 is a special placeholder used in templates -- skip it.
            continue

        repo = g.get_repo(issue.repo)
        try:
            github_issue = repo.get_issue(number=issue.number)
        except github.UnknownObjectException:
            logging.error("Unable to get issue %s", issue)
            continue

        if github_issue.state == "closed":
            closed_issue_references[issue] = issue_references[issue]

    return closed_issue_references


def filter_to_issues(
    issues_to_include: Set[GithubIssue],
    issue_references: Dict[GithubIssue, List[CodeReference]],
) -> Dict[GithubIssue, List[CodeReference]]:
    return {
        issue: issue_references[issue]
        for issue in issues_to_include
        if issue in issue_references
    }


def write_output(
    issue_references: Dict[GithubIssue, List[CodeReference]], minimal_output: bool
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
    github_token: Optional[str],
    closed: bool,
    issues: Optional[List[str]],
    fail_if_found: bool,
    minimal_output: bool,
) -> int:
    issue_references = get_issue_references()
    if issues is not None:
        issues_to_include = {GithubIssue.from_string(issue) for issue in issues}
        issue_references = filter_to_issues(issues_to_include, issue_references)
    if closed:
        if not github_token:
            raise ValueError("--github-token is required when --closed is provided.")
        issue_references = filter_to_closed(github_token, issue_references)
    write_output(issue_references, minimal_output)

    return 1 if fail_if_found and issue_references else 0


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = create_parser().parse_args()
    sys.exit(
        main(
            args.github_token,
            args.closed,
            args.issues,
            args.fail_if_found,
            args.minimal_output,
        )
    )
