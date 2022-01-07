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
"""Tests for issue reference utilities."""

import unittest

from mock import patch
from mock.mock import MagicMock

from recidiviz.repo.issue_references import (
    CodeReference,
    GithubIssue,
    get_issue_references,
    to_markdown,
)


class IssueReferencesTest(unittest.TestCase):
    """Tests for issue reference utilities."""

    @patch("recidiviz.repo.issue_references._read_todo_lines_from_codebase")
    def test_get_issue_references(self, mock_todo_lines: MagicMock) -> None:
        self.maxDiff = None
        mock_todo_lines.return_value = [
            "foo.py:10:# TODO(#123): Get something",
            "bar.yaml:20:  - key: value  # TODO(Recidiviz/pulse-data#456): Remap for frontend",
            "baz.py:30:# TODO(https://issues.apache.org/jira/browse/BEAM-12641): Upgrade once fixed by Apache",
            "dir/qux.py:40:  # TODO(Recidiviz/pulse-data#123): Get something else now",
        ]

        references = get_issue_references()

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
                ],
                GithubIssue(repo="Recidiviz/pulse-data", number=456): [
                    CodeReference(
                        filepath="bar.yaml",
                        line_number=20,
                        line_text="  - key: value  # TODO(Recidiviz/pulse-data#456): Remap for frontend",
                    ),
                ],
            },
        )

    def test_to_markdown(self) -> None:
        issue_references = {
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
            GithubIssue(repo="Recidiviz/pulse-data", number=456): [
                CodeReference(
                    filepath="bar.yaml",
                    line_number=20,
                    line_text="  - key: value  # TODO(Recidiviz/pulse-data#456): Remap for frontend",
                ),
            ],
        }

        markdown = to_markdown(issue_references)

        self.assertEqual(
            markdown,
            """* Recidiviz/pulse-data#456
  * bar.yaml:20
* Recidiviz/pulse-data#123
  * dir/qux.py:40
  * foo.py:10""",
        )
