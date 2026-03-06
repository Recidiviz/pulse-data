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
"""Tests for poll_for_recidiviz_data_to_looker_sync_pr_merge."""
import unittest
from unittest import mock

from recidiviz.tools.looker.poll_for_recidiviz_data_to_looker_sync_pr_merge import (
    poll_for_looker_pr_merge,
)


class TestPollForPrMerge(unittest.TestCase):
    """Tests for poll_for_recidiviz_data_to_looker_sync_pr_merge."""

    def setUp(self) -> None:
        self.mock_github_client = mock.MagicMock()
        self.patch_github = mock.patch(
            "recidiviz.tools.looker.poll_for_recidiviz_data_to_looker_sync_pr_merge.Github",
            return_value=self.mock_github_client,
        )
        self.mock_github = self.patch_github.start()

        self.patch_get_pr = mock.patch("recidiviz.utils.github.get_pr_if_exists")
        self.mock_get_pr = self.patch_get_pr.start()

        self.patch_poll = mock.patch(
            "recidiviz.tools.looker.poll_for_recidiviz_data_to_looker_sync_pr_merge.poll_for_pr_merge"
        )
        self.mock_poll = self.patch_poll.start()

    def tearDown(self) -> None:
        self.patch_github.stop()
        self.patch_get_pr.stop()
        self.patch_poll.stop()

    def test_pr_merges(self) -> None:
        poll_for_looker_pr_merge(
            github_token="fake-token",
            commit_sha="abc123def456",
            base_branch="main",
            timeout_minutes=5,
        )

        self.mock_poll.assert_called_once()

    def test_timeout_exceeded(self) -> None:
        self.mock_poll.side_effect = TimeoutError("Timed out")

        with self.assertRaisesRegex(
            TimeoutError,
            "Please check the Looker PR for any failing workflow checks or merge conflicts.",
        ):
            poll_for_looker_pr_merge(
                github_token="fake-token",
                commit_sha="abc123def456",
                base_branch="main",
                timeout_minutes=1,
            )
