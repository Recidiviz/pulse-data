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
"""Tests for recidiviz.issue_tracking.codebase_todos."""

import unittest

from mock import patch
from mock.mock import MagicMock

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.codebase_todos import (
    CodeReference,
    get_entire_codebase_issue_references,
    parse_issue_string,
    to_markdown,
)
from recidiviz.issue_tracking.issue import Issue, UrlIssue
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue


class GetEntireCodebaseIssueReferencesTest(unittest.TestCase):
    """Tests for get_entire_codebase_issue_references()."""

    @patch("recidiviz.issue_tracking.codebase_todos._find_todo_code_references")
    def test_get_issue_references(self, mock_todo_lines: MagicMock) -> None:
        self.maxDiff = None
        mock_todo_lines.return_value = [
            CodeReference(
                filepath="foo.py",
                line_number=10,
                line_text="# TODO(#123): Get something",
            ),
            CodeReference(
                filepath="bar.yaml",
                line_number=20,
                line_text="  - key: value  # TODO(Recidiviz/pulse-dashboard#456): Remap for frontend",
            ),
            CodeReference(
                filepath="baz.py",
                line_number=30,
                line_text="# TODO(https://issues.apache.org/jira/browse/BEAM-12641): Upgrade once fixed by Apache",
            ),
            CodeReference(
                filepath="dir/qux.py",
                line_number=40,
                line_text="  # TODO(Recidiviz/pulse-data#123): Get something else now",
            ),
            CodeReference(
                filepath="linear.py",
                line_number=50,
                line_text="# TODO(OBT-789): Migrate to Linear",
            ),
            CodeReference(
                filepath="multi.py",
                line_number=60,
                line_text="# TODO(#123), TODO(#456), TODO(OBT-789): Multiple on one line",
            ),
        ]

        references = get_entire_codebase_issue_references(commit_ref="HEAD")

        multi_code_ref = CodeReference(
            filepath="multi.py",
            line_number=60,
            line_text="# TODO(#123), TODO(#456), TODO(OBT-789): Multiple on one line",
        )
        self.assertDictEqual(
            references,
            {
                GithubIssue(repo="Recidiviz/pulse-data", number=123): [
                    CodeReference(
                        filepath="foo.py",
                        line_number=10,
                        line_text="# TODO(#123): Get something",
                    ),
                    CodeReference(
                        filepath="dir/qux.py",
                        line_number=40,
                        line_text="  # TODO(Recidiviz/pulse-data#123): Get something else now",
                    ),
                    multi_code_ref,
                ],
                GithubIssue(repo="Recidiviz/pulse-dashboard", number=456): [
                    CodeReference(
                        filepath="bar.yaml",
                        line_number=20,
                        line_text="  - key: value  # TODO(Recidiviz/pulse-dashboard#456): Remap for frontend",
                    ),
                ],
                LinearIssue(team_prefix="OBT", number=789): [
                    CodeReference(
                        filepath="linear.py",
                        line_number=50,
                        line_text="# TODO(OBT-789): Migrate to Linear",
                    ),
                    multi_code_ref,
                ],
                UrlIssue(url="https://issues.apache.org/jira/browse/BEAM-12641"): [
                    CodeReference(
                        filepath="baz.py",
                        line_number=30,
                        line_text="# TODO(https://issues.apache.org/jira/browse/BEAM-12641): Upgrade once fixed by Apache",
                    ),
                ],
                GithubIssue(repo="Recidiviz/pulse-data", number=456): [
                    multi_code_ref,
                ],
            },
        )


class ParseIssueStringTest(unittest.TestCase):
    """Tests for parse_issue_string()."""

    def test_github_hash(self) -> None:
        issue = parse_issue_string("#123")
        self.assertEqual(
            issue, GithubIssue(repo="Recidiviz/pulse-data", number=123)
        )

    def test_github_repo(self) -> None:
        issue = parse_issue_string("Recidiviz/pulse-data#789")
        self.assertEqual(issue, GithubIssue(repo="Recidiviz/pulse-data", number=789))

    def test_linear(self) -> None:
        issue = parse_issue_string("OBT-12345")
        self.assertEqual(issue, LinearIssue(team_prefix="OBT", number=12345))


class ToMarkdownTest(unittest.TestCase):
    """Tests for to_markdown()."""

    def test_github_only(self) -> None:
        issue_references: dict[Issue, list[CodeReference]] = {
            GithubIssue(repo="Recidiviz/pulse-data", number=123): [
                CodeReference(
                    filepath="foo.py",
                    line_number=10,
                    line_text="# TODO(#123): Get something",
                ),
                CodeReference(
                    filepath="dir/qux.py",
                    line_number=40,
                    line_text="  # TODO(Recidiviz/pulse-data#123): Get something else now",
                ),
            ],
            GithubIssue(repo="Recidiviz/pulse-dashboard", number=456): [
                CodeReference(
                    filepath="bar.yaml",
                    line_number=20,
                    line_text="  - key: value  # TODO(Recidiviz/pulse-dashboard#456): Remap for frontend",
                ),
            ],
        }

        markdown = to_markdown(issue_references)

        self.assertEqual(
            markdown,
            """* Recidiviz/pulse-dashboard#456
  * bar.yaml:20
* Recidiviz/pulse-data#123
  * dir/qux.py:40
  * foo.py:10""",
        )

    def test_mixed_types(self) -> None:
        issue_references: dict[Issue, list[CodeReference]] = {
            GithubIssue(repo="Recidiviz/pulse-data", number=123): [
                CodeReference(
                    filepath="foo.py",
                    line_number=10,
                    line_text="# TODO(#123): Fix",
                ),
            ],
            LinearIssue(team_prefix="OBT", number=456): [
                CodeReference(
                    filepath="bar.py",
                    line_number=20,
                    line_text="# TODO(OBT-456): Migrate",
                ),
            ],
        }

        markdown = to_markdown(issue_references)

        self.assertEqual(
            markdown,
            """* OBT-456
  * bar.py:20
* Recidiviz/pulse-data#123
  * foo.py:10""",
        )
