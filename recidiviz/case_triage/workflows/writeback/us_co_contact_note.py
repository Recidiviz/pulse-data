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
"""US_CO contact note writeback implementation (stub)."""
from typing import Literal

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.base import (
    WritebackExecutorInterface,
    WritebackStatusTracker,
)
from recidiviz.case_triage.workflows.writeback.contact_note import (
    ContactNoteRequestData,
)
from recidiviz.common.constants.states import StateCode


class UsCoContactNoteRequestData(ContactNoteRequestData):
    state_code: Literal["US_CO"] = StateCode.US_CO.value
    person_external_id_type: Literal["US_CO_OFFENDERID"]
    staff_id_type: Literal["US_CO_DOC_BADGE_NUMBER"]
    note_body: str
    # TODO(#69066): Add CO-specific fields (e.g., CO auth details, system-specific data)


class UsCoContactNoteStatusTracker(WritebackStatusTracker):
    """Stub status tracker for CO contact notes."""

    def set_status(self, status: ExternalSystemRequestStatus) -> None:
        raise NotImplementedError(
            "US_CO contact note status tracking not yet implemented"
        )


class UsCoContactNoteWritebackExecutor(
    WritebackExecutorInterface[UsCoContactNoteRequestData]
):
    """Stub writeback executor for CO contact notes.

    CO uses different auth and transport than TN. This stub is a placeholder
    for the full implementation.
    """

    @classmethod
    def for_request(
        cls, request: UsCoContactNoteRequestData
    ) -> "UsCoContactNoteWritebackExecutor":
        return cls(request)

    def execute(self) -> None:
        raise NotImplementedError("US_CO contact note writeback not yet implemented")

    def create_status_tracker(self) -> UsCoContactNoteStatusTracker:
        return UsCoContactNoteStatusTracker()

    @property
    def operation_action_description(self) -> str:
        return "Inserting contact note into eOMIS"
