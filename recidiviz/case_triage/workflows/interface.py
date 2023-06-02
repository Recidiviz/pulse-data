# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Implements common interface used to support external requests from Workflows."""
import base64
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import attr
import dateutil.parser
import requests

from recidiviz.case_triage.workflows.constants import (
    ExternalSystemRequestStatus,
    WorkflowsUsTnVotersRightsCode,
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


class WorkflowsUsTnExternalRequestInterface:
    """Implements interface for Workflows-related external requests for Tennessee."""

    def insert_tepe_contact_note(
        self,
        person_external_id: str,
        staff_id: str,
        contact_note_date_time: str,
        contact_note: Dict[int, List[str]],
        voters_rights_code: Optional[str] = None,
    ) -> None:
        """
        Formats a request and sends the request to insert a TEPE contact note to TOMIS.
        """
        firestore_client = FirestoreClientImpl()
        firestore_doc_path = self.get_contact_note_updates_firestore_path(
            person_external_id
        )

        tomis_url = get_secret("workflows_us_tn_insert_contact_note_url")
        tomis_key = get_secret("workflows_us_tn_insert_contact_note_key")

        if in_gcp_production() and staff_id == "RECIDIVIZ":
            tomis_url = get_secret("workflows_us_tn_insert_contact_note_test_url")

        if tomis_url is None or tomis_key is None:
            firestore_client.update_document(
                firestore_doc_path,
                {
                    "contactNote.status": ExternalSystemRequestStatus.FAILURE.value,
                    f"contactNote.{firestore_client.timestamp_key}": datetime.now(
                        timezone.utc
                    ),
                },
            )
            logging.error("Unable to get secrets for TOMIS")
            raise EnvironmentError("Unable to get secrets for TOMIS")

        base64_encoded = base64.b64encode(tomis_key.encode())

        headers = {
            "Authorization": f"Basic {base64_encoded.decode()}",
            "Content-Type": "application/json",
        }

        for page_number, page_by_line in contact_note.items():
            firestore_client.update_document(
                firestore_doc_path,
                {
                    f"contactNote.noteStatus.{page_number}": ExternalSystemRequestStatus.IN_PROGRESS.value,
                    f"contactNote.{firestore_client.timestamp_key}": datetime.now(
                        timezone.utc
                    ),
                },
            )
            request_body = WorkflowsUsTnWriteTEPENoteToTomisRequest(
                offender_id=person_external_id,
                staff_id=staff_id,
                contact_note_date_time=contact_note_date_time,
                contact_sequence_number=page_number,
                comments=page_by_line,
                voters_rights_code=WorkflowsUsTnVotersRightsCode[voters_rights_code]
                if voters_rights_code
                else None,
            )

            try:
                self._write_to_tomis(
                    tomis_url,
                    headers,
                    request_body,
                )

                firestore_client.update_document(
                    firestore_doc_path,
                    {
                        f"contactNote.noteStatus.{page_number}": ExternalSystemRequestStatus.SUCCESS.value,
                        f"contactNote.{firestore_client.timestamp_key}": datetime.now(
                            timezone.utc
                        ),
                    },
                )
            except Exception:
                # Exception should be logged in the _write_to_tomis function
                firestore_client.update_document(
                    firestore_doc_path,
                    {
                        f"contactNote.noteStatus.{page_number}": ExternalSystemRequestStatus.FAILURE.value,
                        f"contactNote.{firestore_client.timestamp_key}": datetime.now(
                            timezone.utc
                        ),
                    },
                )
                firestore_client.update_document(
                    firestore_doc_path,
                    {
                        "contactNote.status": ExternalSystemRequestStatus.FAILURE.value,
                        f"contactNote.{firestore_client.timestamp_key}": datetime.now(
                            timezone.utc
                        ),
                    },
                )
                raise

        firestore_client.update_document(
            firestore_doc_path,
            {
                "contactNote.status": ExternalSystemRequestStatus.SUCCESS.value,
                f"contactNote.{firestore_client.timestamp_key}": datetime.now(
                    timezone.utc
                ),
            },
        )

    @staticmethod
    def _write_to_tomis(
        tomis_url: str,
        headers: Dict[str, Any],
        request: WorkflowsUsTnWriteTEPENoteToTomisRequest,
    ) -> None:
        """
        Makes a request to the provided TOMIS url with the given headers and request body.
        """
        page_number = request.contact_sequence_number
        start_time = time.perf_counter()
        request_body = request.format_request()

        logging.info("Sending request to TOMIS: %s", request_body)

        try:
            tomis_response = requests.put(
                tomis_url,
                headers=headers,
                data=request_body,
                timeout=360,
            )
            if tomis_response.status_code != requests.codes.ok:
                tomis_response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            duration = round(time.perf_counter() - start_time, 2)
            logging.error(
                f"Error response from TOMIS for page %s in % seconds: {e.response.text}",
                page_number,
                duration,
            )
            raise e
        except Exception as e:
            duration = round(time.perf_counter() - start_time, 2)
            logging.error(
                f"Request to TOMIS failed in %s seconds with error: {e}", duration
            )
            raise e

        duration = round(time.perf_counter() - start_time, 2)
        logging.info(
            "Request to TOMIS for page %s completed with status code %s in %s seconds",
            page_number,
            tomis_response.status_code,
            duration,
        )

    @staticmethod
    def get_contact_note_updates_firestore_path(person_external_id: str) -> str:
        record_id = f"{StateCode.US_TN.value.lower()}_{person_external_id}"
        doc_path = (
            f"clientUpdatesV2/{record_id}/clientOpportunityUpdates/usTnExpiration"
        )
        return doc_path
