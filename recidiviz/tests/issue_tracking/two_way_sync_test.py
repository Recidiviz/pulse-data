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
"""Tests for GitHub/Linear two-way sync helpers."""

import unittest

from mock import MagicMock, call

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.linear.linear_client import LinearEquivalentIssueGroup
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue
from recidiviz.issue_tracking.two_way_sync import resolve_cross_references

TN_94 = LinearIssue(team_prefix="TN", number=94)
OBT_123 = LinearIssue(team_prefix="OBT", number=123)
ENG_55 = LinearIssue(team_prefix="ENG", number=55)
GH_500 = GithubIssue(repo="Recidiviz/pulse-data", number=500)


class ResolveCrossReferencesTest(unittest.TestCase):
    """Tests for resolve_cross_references()."""

    def test_linear_issue_expands_to_twin_and_previous_identifiers(self) -> None:
        linear_client = MagicMock()
        linear_client.get_equivalent_issue_group_for_linear_issue.return_value = (
            LinearEquivalentIssueGroup(
                linear_issue=TN_94,
                previous_issues={OBT_123, ENG_55},
                github_issue=GH_500,
            )
        )

        result = resolve_cross_references({TN_94}, linear_client)

        self.assertEqual(result, {TN_94, OBT_123, ENG_55, GH_500})

    def test_github_issue_expands_to_linear_and_previous_identifiers(self) -> None:
        linear_client = MagicMock()
        linear_client.get_equivalent_issue_group_for_github_issue.return_value = (
            LinearEquivalentIssueGroup(
                linear_issue=TN_94,
                previous_issues={OBT_123},
                github_issue=GH_500,
            )
        )

        result = resolve_cross_references({GH_500}, linear_client)

        self.assertEqual(result, {GH_500, TN_94, OBT_123})

    def test_previous_identifiers_are_never_re_resolved(self) -> None:
        linear_client = MagicMock()
        linear_client.get_equivalent_issue_group_for_linear_issue.return_value = (
            LinearEquivalentIssueGroup(
                linear_issue=TN_94,
                previous_issues={OBT_123},
                github_issue=None,
            )
        )

        resolve_cross_references({TN_94}, linear_client)

        linear_client.get_equivalent_issue_group_for_linear_issue.assert_has_calls(
            [call(TN_94)]
        )
        self.assertEqual(
            linear_client.get_equivalent_issue_group_for_linear_issue.call_count, 1
        )
        linear_client.get_equivalent_issue_group_for_github_issue.assert_not_called()

    def test_no_issue_group_leaves_set_unchanged(self) -> None:
        linear_client = MagicMock()
        linear_client.get_equivalent_issue_group_for_github_issue.return_value = None

        result = resolve_cross_references({GH_500}, linear_client)

        self.assertEqual(result, {GH_500})

    def test_mixed_inputs_dedupe_into_one_set(self) -> None:
        linear_client = MagicMock()
        linear_client.get_equivalent_issue_group_for_linear_issue.return_value = (
            LinearEquivalentIssueGroup(
                linear_issue=TN_94,
                previous_issues={OBT_123},
                github_issue=GH_500,
            )
        )
        linear_client.get_equivalent_issue_group_for_github_issue.return_value = (
            LinearEquivalentIssueGroup(
                linear_issue=TN_94,
                previous_issues={OBT_123},
                github_issue=GH_500,
            )
        )

        result = resolve_cross_references({TN_94, GH_500}, linear_client)

        self.assertEqual(result, {TN_94, OBT_123, GH_500})
