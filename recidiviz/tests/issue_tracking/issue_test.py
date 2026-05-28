# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for recidiviz.issue_tracking.issue."""

import unittest

from recidiviz.issue_tracking.issue import UrlIssue


class TestUrlIssue(unittest.TestCase):
    """Tests for UrlIssue."""

    def test_from_string(self) -> None:
        issue = UrlIssue.from_string("https://issues.apache.org/jira/browse/BEAM-12641")
        self.assertEqual(issue.url, "https://issues.apache.org/jira/browse/BEAM-12641")
        self.assertEqual(str(issue), "https://issues.apache.org/jira/browse/BEAM-12641")

    def test_from_string_invalid(self) -> None:
        with self.assertRaises(ValueError):
            UrlIssue.from_string("not-a-url")

    def test_todo_regex(self) -> None:
        self.assertRegex(
            "TODO(https://issues.apache.org/jira/browse/BEAM-12641)",
            UrlIssue.todo_regex(),
        )
        self.assertNotRegex("TODO(#123)", UrlIssue.todo_regex())
        self.assertNotRegex("TODO(XXXX)", UrlIssue.todo_regex())
