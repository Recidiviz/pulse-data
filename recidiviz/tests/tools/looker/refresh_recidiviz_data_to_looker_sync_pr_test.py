# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for RecidivizDataLookerSyncOrchestrator."""
import unittest
from unittest import mock

from recidiviz.tools.looker.refresh_recidiviz_data_to_looker_sync_pr import (
    LookerGitManager,
    RecidivizDataLookerSyncConfig,
    RecidivizDataLookerSyncGitHubManager,
    RecidivizDataLookerSyncOrchestrator,
)


class TestRecidivizDataLookerSyncOrchestrator(unittest.TestCase):
    """Tests for RecidivizDataLookerSyncOrchestrator."""

    def setUp(self) -> None:
        self.config = RecidivizDataLookerSyncConfig(
            recidiviz_data_commit_sha="fake-sha",
            recidiviz_data_pr_number=123,
            base_branch="main",
            github_token="fake-token",
            looker_branch_name="update-lookml-sync-123",
        )
        self.mock_git_manager = mock.create_autospec(LookerGitManager, instance=True)
        self.mock_github_manager = mock.create_autospec(
            RecidivizDataLookerSyncGitHubManager, instance=True
        )

        self.orchestrator = RecidivizDataLookerSyncOrchestrator(
            self.config, self.mock_git_manager, self.mock_github_manager
        )

    def test_sync_orchestrator_no_changes(self) -> None:
        self.mock_git_manager.has_changes.return_value = False

        self.orchestrator.refresh_recidiviz_data_to_looker_sync_pr()

        self.mock_git_manager.commit_and_push_changes.assert_not_called()
        self.mock_github_manager.create_looker_pr_if_not_exists.assert_not_called()
        self.mock_github_manager.comment_on_recidiviz_data_pr.assert_not_called()

    def test_sync_orchestrator_full_flow(self) -> None:
        self.mock_git_manager.has_changes.return_value = True
        self.mock_github_manager.create_looker_pr_if_not_exists.return_value = (
            "http://github/pr"
        )

        self.orchestrator.refresh_recidiviz_data_to_looker_sync_pr()

        self.mock_github_manager.create_looker_pr_if_not_exists.assert_called_once()
        self.mock_github_manager.comment_on_recidiviz_data_pr.assert_called_once_with(
            "http://github/pr"
        )
