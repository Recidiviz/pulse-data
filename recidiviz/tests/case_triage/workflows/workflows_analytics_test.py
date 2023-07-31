# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Implements tests for the WorkflowsSegmentClient."""
from unittest import TestCase
from unittest.mock import MagicMock, patch

from recidiviz.case_triage.workflows.workflows_analytics import WorkflowsSegmentClient


class TestWorkflowsSegmentClient(TestCase):
    """Implements tests for the WorkflowsSegmentClient."""

    @patch("recidiviz.case_triage.workflows.workflows_analytics.secrets")
    @patch("recidiviz.case_triage.workflows.workflows_analytics.get_gcp_environment")
    def test_track_milestones_message_status(
        self, mock_get_gcp_environment: MagicMock, mock_secrets: MagicMock
    ) -> None:
        mock_secrets.get_secret.return_value = "TEST-KEY"
        mock_get_gcp_environment.return_value = "test_env"
        client = WorkflowsSegmentClient()
        with patch.object(client, "track") as track:
            client.track_milestones_message_status(
                user_hash="test-123",
                twilioRawStatus="delivered",
                status="SUCCESS",
                error_code=None,
                error_message=None,
            )
            mock_get_gcp_environment.assert_called_once()
            track.assert_called_once_with(
                "test-123",
                "backend.milestones_message_status",
                {
                    "twilioRawStatus": "delivered",
                    "status": "SUCCESS",
                    "error_code": None,
                    "error_message": None,
                    "gcp_environment": "test_env",
                },
            )
