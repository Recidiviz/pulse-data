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
"""Tests for recidiviz.utils.github."""
import unittest
from unittest import mock

from recidiviz.utils.github import poll_for_pr_merge


class TestPollForPrMerge(unittest.TestCase):
    """Tests for poll_for_pr_merge."""

    def setUp(self) -> None:
        self.mock_github_client = mock.MagicMock()

        self.patch_get_pr = mock.patch("recidiviz.utils.github.get_pr_if_exists")
        self.mock_get_pr = self.patch_get_pr.start()

        self.patch_time = mock.patch("recidiviz.utils.github.time")
        self.mock_time = self.patch_time.start()

    def tearDown(self) -> None:
        self.patch_get_pr.stop()
        self.patch_time.stop()

    def test_pr_merges(self) -> None:
        mock_pr_url = "https://github.com/Recidiviz/looker/pull/123"

        # First call: PR is open, second call: PR is merged (returns None)
        self.mock_get_pr.side_effect = [mock_pr_url, None]
        self.mock_time.time.side_effect = [0, 30, 60]

        poll_for_pr_merge(
            github_client=self.mock_github_client,
            pr_branch="update-lookml-sync-abc123d",
            base_branch="main",
            repo="Recidiviz/looker",
            timeout_minutes=5,
        )

        self.assertEqual(self.mock_get_pr.call_count, 2)
        self.mock_time.sleep.assert_called_once()

    def test_timeout_exceeded(self) -> None:
        mock_pr_url = "https://github.com/Recidiviz/looker/pull/123"

        self.mock_get_pr.return_value = mock_pr_url
        self.mock_time.time.side_effect = [0, 30, 60, 90]

        with self.assertRaisesRegex(TimeoutError, "Timed out after 1 minutes"):
            poll_for_pr_merge(
                github_client=self.mock_github_client,
                pr_branch="update-lookml-sync-abc123d",
                base_branch="main",
                repo="Recidiviz/looker",
                timeout_minutes=1,
            )
