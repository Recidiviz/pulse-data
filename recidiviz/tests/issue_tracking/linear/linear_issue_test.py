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
"""Tests for recidiviz.issue_tracking.linear.linear_issue."""

import unittest

from recidiviz.issue_tracking.linear.linear_issue import LinearIssue


class TestLinearIssue(unittest.TestCase):
    """Tests for LinearIssue."""

    def test_from_string(self) -> None:
        issue = LinearIssue.from_string("OBT-12345")
        self.assertEqual(issue.team_prefix, "OBT")
        self.assertEqual(issue.number, 12345)
        self.assertEqual(str(issue), "OBT-12345")

    def test_from_string_invalid(self) -> None:
        with self.assertRaises(ValueError):
            LinearIssue.from_string("#123")
        with self.assertRaises(ValueError):
            LinearIssue.from_string("XXXX")

    def test_issue_identifier(self) -> None:
        issue = LinearIssue(team_prefix="OBT", number=789)
        self.assertEqual(issue.issue_identifier, "OBT-789")

    def test_todo_regex(self) -> None:
        self.assertRegex("TODO(OBT-12345)", LinearIssue.todo_regex())
        self.assertRegex("TODO(ENG-1)", LinearIssue.todo_regex())
        self.assertNotRegex("TODO(#123)", LinearIssue.todo_regex())
        self.assertNotRegex("TODO(XXXX)", LinearIssue.todo_regex())
