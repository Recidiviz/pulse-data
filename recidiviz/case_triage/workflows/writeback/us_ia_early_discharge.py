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
"""US_IA early discharge writeback — MCP callback handling.

Provides the data model for MCP's asynchronous callback (McpCallbackData)
and the Firestore status tracker that records per-charge outcomes
(UsIaEarlyDischargeStatusTracker).

The callback endpoint receives ATG's accept/reject result after MCP forwards
our discharge request. The full form-submission writeback
(UsIaEarlyDischargeWritebackExecutor and the XML request builder) is
implemented separately.
"""
from datetime import datetime, timezone
from typing import Self

from defusedxml.ElementTree import fromstring as _safe_fromstring
from google.cloud.firestore_v1 import FieldFilter
from pydantic import BaseModel, ConfigDict

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.base import WritebackStatusTracker
from recidiviz.firestore.firestore_client import FirestoreClientImpl

# Firestore document ID for the early discharge opportunity status.
EARLY_DISCHARGE_STATUS_DOC_ID = "usIaEarlyDischarge"

# XML namespace URIs in MCP's callback body.
# Root element namespace for the AsynchronousNotificationMessage envelope.
_NS_EXCHANGE_NOTIFICATION = "http://www.cjis.iowa.gov/exchange-notification/1.0"
# Namespace for the payload fields (MessageId, TransactionStatus, etc.).
_NS_WS_RESPONSE = "http://www.cjis.iowa.gov/WSResponse/1.0"


class McpCallbackData(BaseModel):
    """Models the callback MCP POSTs to our endpoint after ATG processing.

    MCP sends an ``AsynchronousNotificationMessage`` XML document. The payload
    fields (``MessageId``, ``TransactionStatus``, etc.) carry the namespace
    ``http://www.cjis.iowa.gov/WSResponse/1.0``.

    ``message_id`` echoes the UUID we generated and sent in our original POST;
    we use it to look up which person the callback belongs to.

    ``transaction_status`` is ``"S"`` for success or ``"E"`` for error.
    """

    model_config = ConfigDict(frozen=True)

    # Echoes the MessageID we sent in the original POST.
    message_id: str
    # "S" = ATG accepted, "E" = ATG rejected.
    transaction_status: str
    error_code: str | None = None
    error_message: str | None = None

    @classmethod
    def from_callback_xml(cls, xml_str: str) -> "McpCallbackData":
        """Parse MCP's AsynchronousNotificationMessage callback body."""
        root = _safe_fromstring(xml_str)

        def _text(tag: str) -> str | None:
            el = root.find(f".//{{{_NS_WS_RESPONSE}}}{tag}")
            return el.text if el is not None else None

        message_id = _text("MessageId")
        transaction_status = _text("TransactionStatus")
        if message_id is None or transaction_status is None:
            raise ValueError(
                f"Missing required fields in MCP callback body: [{xml_str}]"
            )
        return cls(
            message_id=message_id,
            transaction_status=transaction_status,
            error_code=_text("ErrorCode"),
            error_message=_text("ErrorMsg"),
        )


class UsIaEarlyDischargeStatusTracker(WritebackStatusTracker):
    """Tracks writeback status for IA early discharge requests in Firestore.

    Status is written per charge ID so the FE can display, for each charge,
    whether it has been submitted and what its current status is. A single form
    submission covers a subset of the person's charges; unsubmitted charges have
    no status document.

    The writeback is two-phase:
      - After the initial POST to MCP succeeds, IN_PROGRESS is written for each
        charge in the submission.
        - If the initial POST to MCP doesn't succeed, FAILURE is written for each
          charge in the submission.
      - The final SUCCESS or FAILURE arrives via the MCP→ATG callback. ATG
        responds per submission (not per charge), so all charges in a submission
        receive the same final status.

    Because of this, ``set_status(SUCCESS)`` is intentionally a no-op: the
    route helper calls it after ``execute()`` returns, but the true success
    signal comes from the callback, not from MCP's 200 acknowledgement.
    """

    def __init__(
        self,
        firestore_paths: list[str],
        firestore_client: FirestoreClientImpl,
    ) -> None:
        self.firestore_paths = firestore_paths
        self.firestore_client = firestore_client

    @classmethod
    def from_message_id(
        cls,
        message_id: str,
        firestore_client: FirestoreClientImpl,
    ) -> Self:
        """Returns a tracker for the submission associated with the given MCP message ID.
        Raises ValueError if no documents are found in clientOpportunityUpdates for the message ID,
        or if the documents span multiple persons.
        """
        docs = (
            firestore_client.client.collection_group("clientOpportunityUpdates")
            .where(filter=FieldFilter("earlyDischargeMessageId", "==", message_id))
            .get()
        )

        if not docs:
            raise ValueError(
                f"No pending early discharge request found for message ID [{message_id}]"
            )

        record_ids = {doc.reference.parent.parent.id for doc in docs}
        if len(record_ids) != 1:
            raise ValueError(
                f"Expected a single person for message ID [{message_id}], "
                f"got [{record_ids}]"
            )

        return cls(
            firestore_paths=[doc.reference.path for doc in docs],
            firestore_client=firestore_client,
        )

    def set_status(self, status: ExternalSystemRequestStatus) -> None:
        if status is ExternalSystemRequestStatus.SUCCESS:
            # SUCCESS is only written via apply_mcp_callback once ATG confirms.
            return
        for path in self.firestore_paths:
            self.firestore_client.set_document(
                path,
                {
                    "status": status.value,
                    self.firestore_client.timestamp_key: datetime.now(timezone.utc),
                },
                merge=True,
            )

    def apply_mcp_callback(self, callback: McpCallbackData) -> None:
        """Write the final SUCCESS or FAILURE status for all charges in the submission."""
        final_status = (
            ExternalSystemRequestStatus.SUCCESS
            if callback.transaction_status == "S"
            else ExternalSystemRequestStatus.FAILURE
        )
        update: dict[str, object] = {
            "status": final_status.value,
            self.firestore_client.timestamp_key: datetime.now(timezone.utc),
        }
        if callback.error_message:
            update["errorMessage"] = callback.error_message
        for path in self.firestore_paths:
            self.firestore_client.set_document(
                path,
                update,
                merge=True,
            )
