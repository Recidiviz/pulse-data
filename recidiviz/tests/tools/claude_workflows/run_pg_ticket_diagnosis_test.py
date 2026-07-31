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
"""Tests for pg_ticket_diagnosis/run_pg_ticket_diagnosis.py.

The Cloud Build container ships run_pg_ticket_diagnosis.py flat alongside
claude_agent.py and pii_doc_parser_utils.py, so the script imports those by
bare module name. Reproduce that layout here by putting both directories on
sys.path before importing the module under test.
"""
import os
import sys
import unittest
from unittest import mock

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.linear.linear_client import (
    LinearApiError,
    LinearEquivalentIssueGroup,
)
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue
from recidiviz.tools.claude_workflows import claude_agent as _claude_agent_pkg
from recidiviz.tools.claude_workflows.pg_ticket_diagnosis import (
    pii_doc_parser_utils as _pii_doc_parser_pkg,
)

sys.path.insert(0, os.path.dirname(_claude_agent_pkg.__file__))
sys.path.insert(0, os.path.dirname(_pii_doc_parser_pkg.__file__))

# run_pg_ticket_diagnosis is a bare (non-recidiviz) module imported only after
# the sys.path setup above, so it must sit below the first-party imports.
# pylint: disable=wrong-import-position,wrong-import-order
import run_pg_ticket_diagnosis as run_pg  # type: ignore[import-not-found]  # noqa: E402

_MODULE = "run_pg_ticket_diagnosis"
_ISSUE = GithubIssue(repo="Recidiviz/pulse-data", number=88494)


class TestResolveLinearIdForIssue(unittest.TestCase):
    """Tests for resolve_linear_id_for_issue — best-effort Linear resolution."""

    @mock.patch(f"{_MODULE}.linear_client_from_secret")
    def test_returns_identifier_when_synced(
        self, mock_build_client: mock.MagicMock
    ) -> None:
        mock_client = mock_build_client.return_value
        mock_client.get_equivalent_issue_group_for_github_issue.return_value = (
            LinearEquivalentIssueGroup(
                linear_issue=LinearIssue.from_string("OBT-36212"),
                previous_issues=set(),
                github_issue=_ISSUE,
            )
        )
        self.assertEqual(run_pg.resolve_linear_id_for_issue(_ISSUE), "OBT-36212")
        mock_client.get_equivalent_issue_group_for_github_issue.assert_called_once_with(
            _ISSUE
        )

    @mock.patch(f"{_MODULE}.linear_client_from_secret")
    def test_returns_none_when_not_synced(
        self, mock_build_client: mock.MagicMock
    ) -> None:
        mock_build_client.return_value.get_equivalent_issue_group_for_github_issue.return_value = (
            None
        )
        self.assertIsNone(run_pg.resolve_linear_id_for_issue(_ISSUE))

    @mock.patch(f"{_MODULE}.linear_client_from_secret")
    def test_degrades_to_none_on_linear_api_error(
        self, mock_build_client: mock.MagicMock
    ) -> None:
        mock_build_client.return_value.get_equivalent_issue_group_for_github_issue.side_effect = LinearApiError(
            "boom"
        )
        self.assertIsNone(run_pg.resolve_linear_id_for_issue(_ISSUE))

    @mock.patch(f"{_MODULE}.linear_client_from_secret")
    def test_degrades_to_none_on_credential_failure(
        self, mock_build_client: mock.MagicMock
    ) -> None:
        # A missing-secret failure surfacing from client construction must not
        # abort the diagnosis (GitHub-number lookup still works for pre-Linear
        # tickets).
        mock_build_client.side_effect = KeyError("no linear api key secret")
        self.assertIsNone(run_pg.resolve_linear_id_for_issue(_ISSUE))
