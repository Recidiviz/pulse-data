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
"""US_TN contact note writeback implementation."""
import json
from datetime import datetime, timezone
from typing import Any, List, Optional

import attr
import dateutil.parser

from recidiviz.case_triage.workflows.api_schemas import (
    WorkflowsUsTnInsertTEPEContactNoteSchema,
)
from recidiviz.case_triage.workflows.constants import (
    ExternalSystemRequestStatus,
    WorkflowsUsTnVotersRightsCode,
)
from recidiviz.case_triage.workflows.writeback.base import (
    WritebackConfig,
    WritebackExecutorInterface,
    WritebackStatusTracker,
)
from recidiviz.case_triage.workflows.writeback.transports.rest import (
    BasicAuth,
    RestTransport,
    RestTransportConfig,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import in_gcp_production
from recidiviz.utils.secrets import get_secret


@attr.s(frozen=True)
class WorkflowsUsTnWriteTEPENoteToTomisRequest:
    """Models the required parameters needed for writing a TEPE note to TOMIS"""

    # The justice-impacted individual that the note is relevant for
    offender_id: str = attr.ib(default=None)

    # The state user who authored the note
    staff_id: str = attr.ib(default=None)

    # The time that the request was submitted for the full note
    # All pages from the same note should have the same contact_note_date_time
    contact_note_date_time: str = attr.ib(default=None)

    # The page number
    contact_sequence_number: int = attr.ib(default=None)

    # The page text
    comments: List[str] = attr.ib(default=None)

    # Voter's rights code
    voters_rights_code: Optional[WorkflowsUsTnVotersRightsCode] = attr.ib(default=None)

    def format_request(self) -> str:
        """
        Format the request body for the request to TOMIS.
        If not in production, override the requested id for the JII and user with acceptable test ids.
        """
        try:
            dateutil.parser.parse(self.contact_note_date_time).date()
        except Exception as exc:
            raise ValueError(
                f"{self.contact_note_date_time} is not a valid datetime str"
            ) from exc

        if not in_gcp_production() or self.staff_id == "RECIDIVIZ":
            # If we are not in production or the user is Recidiviz, ensure we do not submit notes to TOMIS with real ids
            offender_id = get_secret("workflows_us_tn_test_offender_id")
            staff_id = get_secret("workflows_us_tn_test_user_id")

            if offender_id is None or staff_id is None:
                raise ValueError("Missing OffenderId and/or StaffId secret")
        else:
            offender_id = self.offender_id
            staff_id = self.staff_id

        request = {
            "ContactNoteDateTime": self.contact_note_date_time,
            "OffenderId": offender_id,
            "StaffId": staff_id,
            "ContactTypeCode1": "TEPE",
            "ContactSequenceNumber": self.contact_sequence_number,
        }

        for idx, line_text in enumerate(self.comments):
            request[f"Comment{idx+1}"] = line_text

        if self.voters_rights_code:
            request["ContactTypeCode2"] = self.voters_rights_code.value

        return json.dumps(request)


TOMIS_TRANSPORT_CONFIG = RestTransportConfig(
    system_name="TOMIS",
    url_secret="workflows_us_tn_insert_contact_note_url",  # nosec
    credential_secret="workflows_us_tn_insert_contact_note_key",
    test_url_secret="workflows_us_tn_insert_contact_note_test_url",
    auth_strategy=BasicAuth(),
)


class UsTnContactNoteStatusTracker(WritebackStatusTracker):
    def __init__(
        self, person_external_id: str, firestore_client: FirestoreClientImpl
    ) -> None:
        self.person_external_id = person_external_id
        self.firestore_client = firestore_client

    def _firestore_path(self) -> str:
        record_id = f"{StateCode.US_TN.value.lower()}_{self.person_external_id}"
        return f"clientUpdatesV2/{record_id}/clientOpportunityUpdates/usTnExpiration"

    def set_status(self, status: ExternalSystemRequestStatus) -> None:
        self.firestore_client.update_document(
            self._firestore_path(),
            {
                "contactNote.status": status.value,
                f"contactNote.{self.firestore_client.timestamp_key}": datetime.now(
                    timezone.utc
                ),
            },
        )

    def set_page_status(
        self, page_number: int, status: ExternalSystemRequestStatus
    ) -> None:
        self.firestore_client.update_document(
            self._firestore_path(),
            {
                f"contactNote.noteStatus.{page_number}": status.value,
                f"contactNote.{self.firestore_client.timestamp_key}": datetime.now(
                    timezone.utc
                ),
            },
        )


@attr.s(frozen=True)
class UsTnContactNoteRequestData:
    person_external_id: str = attr.ib()
    staff_id: str = attr.ib()
    contact_note_date_time: str = attr.ib()
    contact_note: dict[int, list[str]] = attr.ib()
    voters_rights_code: str | None = attr.ib()


class UsTnContactNoteWritebackExecutor(
    WritebackExecutorInterface[UsTnContactNoteRequestData]
):
    """Writeback implementation for TN contact note."""

    def __init__(self, person_external_id: str) -> None:
        self.person_external_id = person_external_id
        self._tracker = UsTnContactNoteStatusTracker(
            person_external_id, FirestoreClientImpl()
        )

    @classmethod
    def parse_request_data(
        cls, raw_request: dict[str, Any]
    ) -> UsTnContactNoteRequestData:
        return UsTnContactNoteRequestData(
            person_external_id=raw_request["person_external_id"],
            staff_id=raw_request["staff_id"],
            contact_note_date_time=raw_request["contact_note_date_time"],
            contact_note=raw_request["contact_note"],
            voters_rights_code=raw_request.get("voters_rights_code"),
        )

    def execute(self, request_data: UsTnContactNoteRequestData) -> None:
        transport = RestTransport(
            TOMIS_TRANSPORT_CONFIG,
            # TODO(#68802): Centralize logic for detecting a Recidiviz user in writeback code.
            use_test_url=in_gcp_production() and request_data.staff_id == "RECIDIVIZ",
        )

        for page_number, page_by_line in request_data.contact_note.items():
            self._tracker.set_page_status(
                page_number, ExternalSystemRequestStatus.IN_PROGRESS
            )
            request_body = WorkflowsUsTnWriteTEPENoteToTomisRequest(
                offender_id=request_data.person_external_id,
                staff_id=request_data.staff_id,
                contact_note_date_time=request_data.contact_note_date_time,
                contact_sequence_number=page_number,
                comments=page_by_line,
                voters_rights_code=(
                    WorkflowsUsTnVotersRightsCode[request_data.voters_rights_code]
                    if request_data.voters_rights_code
                    else None
                ),
            )

            try:
                transport.send(
                    request_body.format_request(),
                    log_context=f"page {page_number}",
                )
                self._tracker.set_page_status(
                    page_number, ExternalSystemRequestStatus.SUCCESS
                )
            except Exception:
                self._tracker.set_page_status(
                    page_number, ExternalSystemRequestStatus.FAILURE
                )
                raise

    def create_status_tracker(self) -> UsTnContactNoteStatusTracker:
        return self._tracker

    @classmethod
    def config(cls) -> WritebackConfig:
        return WritebackConfig(
            state_code=StateCode.US_TN,
            operation_action_description="Inserting contact note into TOMIS",
            api_schema_cls=WorkflowsUsTnInsertTEPEContactNoteSchema,
        )
