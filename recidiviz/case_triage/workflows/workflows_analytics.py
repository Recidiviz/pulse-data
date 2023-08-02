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
"""Contains classes used to log events to Segment."""
from typing import Optional

from recidiviz.utils import secrets
from recidiviz.utils.environment import get_gcp_environment
from recidiviz.utils.segment_client import SegmentClient


class WorkflowsSegmentClient(SegmentClient):
    """A Workflows-specific Segment client."""

    def __init__(self) -> None:
        write_key = secrets.get_secret("case_triage_segment_backend_key")
        if write_key:
            super().__init__(write_key)

    def track_milestones_message_status(
        self,
        user_hash: str,
        twilioRawStatus: Optional[str],
        status: str,
        error_code: Optional[str],
        error_message: Optional[str],
    ) -> None:
        self.track(
            user_hash,
            "backend.milestones_message_status",
            {
                "twilioRawStatus": twilioRawStatus,
                "status": status,
                "error_code": error_code,
                "error_message": error_message,
                "gcp_environment": get_gcp_environment(),
            },
        )

    # Opt out types are: STOP, START, and HELP
    def track_milestones_message_opt_out(
        self,
        user_hash: str,
        opt_out_type: str,
    ) -> None:
        self.track(
            user_hash,
            "backend.milestones_message_opt_out",
            {
                "opt_out_type": opt_out_type,
                "gcp_environment": get_gcp_environment(),
            },
        )
