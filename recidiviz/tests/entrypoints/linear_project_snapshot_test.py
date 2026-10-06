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
"""Tests for recidiviz/entrypoints/linear_project_snapshot.py"""
import argparse
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.entrypoints.linear_project_snapshot import LinearProjectSnapshot

_MODULE = "recidiviz.entrypoints.linear_project_snapshot"


def _fake_get_secret(secret_name: str) -> str | None:
    return {
        "linear_project_snapshot_oauth_client_id": "test-client-id",
        "linear_project_snapshot_oauth_client_secret": "test-client-secret",
    }[secret_name]


class LinearProjectSnapshotTest(unittest.TestCase):
    """Tests for LinearProjectSnapshot.run_entrypoint()."""

    @patch(f"{_MODULE}.write_linear_project_snapshot")
    @patch(f"{_MODULE}.BigQueryClientImpl")
    @patch(f"{_MODULE}.LinearClient")
    @patch(f"{_MODULE}.get_linear_oauth_access_token", return_value="test-token")
    @patch(f"{_MODULE}.get_secret", side_effect=_fake_get_secret)
    def test_writes_snapshot_rows_for_every_project(
        self,
        _mock_get_secret: MagicMock,
        mock_get_token: MagicMock,
        mock_linear_client: MagicMock,
        mock_bq_client: MagicMock,
        mock_write: MagicMock,
    ) -> None:
        mock_linear_client.return_value.get_all_projects_with_milestones.return_value = [
            {
                "id": "proj-1",
                "slugId": "74c48ad27169",
                "name": "[US_XX] Tasks V2",
                "state": "started",
                "status": {"name": "TT"},
                "startDate": None,
                "targetDate": None,
                "health": None,
                "healthUpdatedAt": None,
                "milestones": {"nodes": []},
                "labels": {"nodes": []},
            }
        ]

        LinearProjectSnapshot.run_entrypoint(args=argparse.Namespace())

        mock_get_token.assert_called_once_with(
            client_id="test-client-id",
            client_secret="test-client-secret",
            scope="read",
        )
        mock_linear_client.assert_called_once_with("Bearer test-token")
        mock_write.assert_called_once()
        self.assertIs(
            mock_write.call_args.kwargs["bq_client"], mock_bq_client.return_value
        )
        self.assertEqual(
            [row["linear_project_id"] for row in mock_write.call_args.kwargs["rows"]],
            ["proj-1"],
        )

    @patch(f"{_MODULE}.write_linear_project_snapshot")
    @patch(f"{_MODULE}.get_linear_oauth_access_token")
    @patch(f"{_MODULE}.get_secret", return_value=None)
    def test_raises_when_secrets_have_no_value(
        self,
        _mock_get_secret: MagicMock,
        mock_get_token: MagicMock,
        mock_write: MagicMock,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Secrets \[linear_project_snapshot_oauth_client_id\] and "
            r"\[linear_project_snapshot_oauth_client_secret\] must both have "
            r"values\. Add the Launch Momentum Snapshot Linear OAuth "
            r"application's client ID and client secret as new versions of those "
            r"secrets in this project\.$",
        ):
            LinearProjectSnapshot.run_entrypoint(args=argparse.Namespace())

        mock_get_token.assert_not_called()
        mock_write.assert_not_called()
