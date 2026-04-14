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
from enum import Enum
from typing import List, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.base import (
    WritebackExecutorInterface,
    WritebackRequestData,
    WritebackStatusTracker,
)
from recidiviz.case_triage.workflows.writeback.contact_note import (
    ContactNoteRequestData,
)
from recidiviz.case_triage.workflows.writeback.transports.rest import (
    BasicAuth,
    RestTransport,
    RestTransportConfig,
)
from recidiviz.common.constants.state.external_id_types import (
    US_TN_DOC,
    US_TN_STAFF_TOMIS,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import in_gcp_production
from recidiviz.utils.secrets import get_secret


class UsTnContactTypeCode(Enum):
    # Expiration code
    TEPE = "TEPE"

    # Referral code
    REIO = "REIO"

    # Denial codes
    DEIO = "DEIO"
    DEIR = "DEIR"
    DECF = "DECF"
    DEDF = "DEDF"
    DEDU = "DEDU"
    DECT = "DECT"
    DECR = "DECR"
    DEIJ = "DEIJ"

    # Voter's rights codes
    VRRE = "VRRE"
    VRRI = "VRRI"


class UsTnVotersRightsCode(Enum):
    VRRE = "VRRE"
    VRRI = "VRRI"

    def to_contact_type_code(self) -> UsTnContactTypeCode:
        match self:
            case UsTnVotersRightsCode.VRRE:
                return UsTnContactTypeCode.VRRE
            case UsTnVotersRightsCode.VRRI:
                return UsTnContactTypeCode.VRRI


class UsTnTEPEContactNoteRequestData(WritebackRequestData):
    """Model for the legacy `insert_tepe_contact_note` endpoint."""

    person_external_id: str
    staff_id: str
    contact_note_date_time: datetime
    contact_note: dict[int, list[str]]
    voters_rights_code: UsTnVotersRightsCode | None = None

    @field_validator("contact_note")
    @classmethod
    def validate_contact_note(cls, v: dict[int, list[str]]) -> dict[int, list[str]]:
        return UsTnContactNoteRequestData.validate_contact_note(v)

    def to_new_request_data(self) -> "UsTnContactNoteRequestData":
        return UsTnContactNoteRequestData(
            person_external_id=self.person_external_id,
            person_external_id_type=US_TN_DOC,
            staff_id=self.staff_id,
            staff_id_type=US_TN_STAFF_TOMIS,
            contact_note_date_time=self.contact_note_date_time,
            contact_type_codes=(
                [UsTnContactTypeCode.TEPE]
                + (
                    [self.voters_rights_code.to_contact_type_code()]
                    if self.voters_rights_code
                    else []
                )
            ),
            contact_note=self.contact_note,
            should_queue_task=self.should_queue_task,
        )


class UsTnContactNoteRequestData(ContactNoteRequestData):
    """Writeback request data for inserting a contact note in TN"""

    state_code: Literal["US_TN"] = StateCode.US_TN.value
    person_external_id_type: Literal["US_TN_DOC"]
    staff_id_type: Literal["US_TN_STAFF_TOMIS"]
    contact_note: dict[int, list[str]]
    contact_type_codes: List[UsTnContactTypeCode]

    @field_validator("contact_type_codes")
    @classmethod
    def validate_contact_type_codes(
        cls, v: List[UsTnContactTypeCode]
    ) -> List[UsTnContactTypeCode]:
        """Validates contact type code constraints including count limits and
        allowed code combinations."""
        if not v:
            raise ValueError("At least one contact type code must be specified.")

        if len(v) > 10:
            raise ValueError("No more than ten contact type codes may be specified.")

        voters_rights_codes = {UsTnContactTypeCode.VRRE, UsTnContactTypeCode.VRRI}
        denial_codes = {
            UsTnContactTypeCode.DEIO,
            UsTnContactTypeCode.DEIR,
            UsTnContactTypeCode.DECF,
            UsTnContactTypeCode.DEDF,
            UsTnContactTypeCode.DEDU,
            UsTnContactTypeCode.DECT,
            UsTnContactTypeCode.DECR,
            UsTnContactTypeCode.DEIJ,
        }
        code_set = set(v)

        has_tepe = UsTnContactTypeCode.TEPE in code_set
        has_reio = UsTnContactTypeCode.REIO in code_set
        has_denial = bool(code_set & denial_codes)

        # Voter's rights codes can't be used alone
        if code_set <= voters_rights_codes:
            raise ValueError(
                "Voter's rights codes (VRRE, VRRI) cannot be used without an "
                "accompanying contact type code."
            )

        # TEPE can only be used alone or with a single voter's rights code
        if has_tepe and not code_set <= (
            {UsTnContactTypeCode.TEPE} | voters_rights_codes
        ):
            raise ValueError(
                "TEPE may only be used alone or with a single voter's rights code "
                "(VRRE or VRRI)."
            )
        if has_tepe and len(code_set & voters_rights_codes) > 1:
            raise ValueError("TEPE may be used with at most one voter's rights code.")

        # REIO must be used alone
        if has_reio and len(v) > 1:
            raise ValueError("REIO must be used alone.")

        # Denial codes can be combined together but not mixed with other codes
        if has_denial and not code_set <= denial_codes:
            raise ValueError(
                "Denial codes cannot be mixed with other contact type codes."
            )

        return v

    @field_validator("contact_note")
    @classmethod
    def validate_contact_note(cls, v: dict[int, list[str]]) -> dict[int, list[str]]:
        """
        Validates the shape of the contact note for US_TN.

        The note should be a dictionary, where the key is an integer representing the page number and
        the value is a list of strings (each string is a line in the page).

        External system requirements to be met:
        - Pages must be numbered 1-10.
        - Each page can have a maximum of 10 lines.
        - Each line must be <= 70 characters.

        Since we might receive a subset of pages for a note, i.e. if some pages failed and others succeeded, the
        dictionary structure enables us to specify the page number.
        """
        if not v:
            raise ValueError("Note must be non-empty")

        if min(v.keys()) < 1 or max(v.keys()) > 10:
            raise ValueError("Page number provided is outside the 1-10 range.")

        for page_num in sorted(v):
            lines = v[page_num]
            if len(lines) > 10:
                raise ValueError(f"Page {page_num} has too many lines, maximum is 10")
            if any(len(line) > 70 for line in lines):
                raise ValueError(
                    f"Line in page {page_num} has too many characters, maximum is 70"
                )

        return v


class TomisContactNoteRequest(BaseModel):
    """Models a single-page request body sent to TOMIS for contact note insertion."""

    model_config = ConfigDict(frozen=True)

    offender_id: str
    staff_id: str
    contact_note_date_time: datetime
    contact_type_codes: List[UsTnContactTypeCode]
    page_number: int
    comments: list[str]

    @classmethod
    def from_request_data(
        cls,
        request_data: "UsTnContactNoteRequestData",
        page_number: int,
        comments: list[str],
    ) -> "TomisContactNoteRequest":
        """Build a TOMIS request, swapping in test IDs when not in production."""
        if not in_gcp_production() or request_data.staff_id == "RECIDIVIZ":
            offender_id = get_secret("workflows_us_tn_test_offender_id")
            staff_id = get_secret("workflows_us_tn_test_user_id")
            if offender_id is None or staff_id is None:
                raise ValueError("Missing OffenderId and/or StaffId secret")
        else:
            offender_id = request_data.person_external_id
            staff_id = request_data.staff_id

        return cls(
            offender_id=offender_id,
            staff_id=staff_id,
            contact_note_date_time=request_data.contact_note_date_time,
            contact_type_codes=request_data.contact_type_codes,
            page_number=page_number,
            comments=comments,
        )

    def to_json(self) -> str:
        body: dict[str, str | int] = {
            "ContactNoteDateTime": self.contact_note_date_time.isoformat(),
            "OffenderId": self.offender_id,
            "StaffId": self.staff_id,
            "ContactSequenceNumber": self.page_number,
        }
        for line_idx, line_text in enumerate(self.comments):
            body[f"Comment{line_idx + 1}"] = line_text

        for code_idx, code in enumerate(self.contact_type_codes):
            body[f"ContactTypeCode{code_idx + 1}"] = code.value
        return json.dumps(body)


TOMIS_TRANSPORT_CONFIG = RestTransportConfig(
    system_name="TOMIS",
    url_secret="workflows_us_tn_insert_contact_note_url",  # nosec
    credential_secret="workflows_us_tn_insert_contact_note_key",
    test_url_secret="workflows_us_tn_insert_contact_note_test_url",
    auth_strategy=BasicAuth(),
)


# TODO(#70781): Move status tracking to Postgres and create an API to get
# writeback job status.
class UsTnContactNoteStatusTracker(WritebackStatusTracker):
    """Tracks writeback status for US_TN contact notes in Firestore."""

    def __init__(
        self,
        person_external_id: str,
        contact_note_date_time: datetime,
        contact_type_codes: List[UsTnContactTypeCode],
        firestore_client: FirestoreClientImpl,
    ) -> None:
        self.person_external_id = person_external_id
        self.contact_date_time = contact_note_date_time
        self.contact_type_codes = contact_type_codes
        self.firestore_client = firestore_client

    def _firestore_path(self) -> str:
        record_id = f"{StateCode.US_TN.value.lower()}_{self.person_external_id}"

        # TODO(#70780): Add a proper idempotency key.
        contact_id = (
            # TODO(#70782): Differentiate when multiple expirations are entered
            # for the same person.
            "usTnExpiration"
            if UsTnContactTypeCode.TEPE in self.contact_type_codes
            else f"usTnContactNote_{self.contact_date_time.isoformat()}"
        )
        return f"clientUpdatesV2/{record_id}/clientOpportunityUpdates/{contact_id}"

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


class UsTnContactNoteWritebackExecutor(
    WritebackExecutorInterface[UsTnContactNoteRequestData]
):
    """Writeback implementation for TN contact note."""

    def __init__(self, request: UsTnContactNoteRequestData) -> None:
        super().__init__(request)
        self._tracker = UsTnContactNoteStatusTracker(
            self.request.person_external_id,
            self.request.contact_note_date_time,
            self.request.contact_type_codes,
            FirestoreClientImpl(),
        )

    def execute(self) -> None:
        transport = RestTransport(
            TOMIS_TRANSPORT_CONFIG,
            # TODO(#68802): Centralize logic for detecting a Recidiviz user in writeback code.
            use_test_url=in_gcp_production() and self.request.staff_id == "RECIDIVIZ",
        )

        for page_number, page_by_line in self.request.contact_note.items():
            self._tracker.set_page_status(
                page_number, ExternalSystemRequestStatus.IN_PROGRESS
            )

            tomis_request = TomisContactNoteRequest.from_request_data(
                self.request, page_number, page_by_line
            )

            try:
                transport.send(
                    tomis_request.to_json(),
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

    @property
    def operation_action_description(self) -> str:
        return "Inserting contact note into TOMIS"
