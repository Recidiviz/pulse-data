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
"""US_IA early discharge writeback implementation.

Flow:
  1. PO submits the early discharge form on the Recidiviz tool.
  2. We POST the request as XML to MCP. MCP acknowledges receipt immediately;
     the Firestore status is set to IN_PROGRESS.
  3. MCP transfers the request to ATG (the OMS). ATG processes it and sends
     a status message back to MCP.
  4. MCP POSTs the ATG result to our callback endpoint, which calls
     ``UsIaEarlyDischargeStatusTracker.apply_mcp_callback`` to write the
     final SUCCESS or FAILURE status to Firestore.
"""
import io
import uuid
import xml.etree.ElementTree as ET  # nosec B405 — used only to build/write XML, never to parse untrusted input (parsing uses defusedxml below)
from datetime import datetime, timezone
from typing import Self, TypeAlias

from defusedxml.ElementTree import fromstring as _safe_fromstring
from google.cloud.firestore_v1 import FieldFilter
from pydantic import BaseModel, ConfigDict, field_validator

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.base import (
    WritebackExecutorInterface,
    WritebackRequestData,
    WritebackStatusTracker,
)
from recidiviz.case_triage.workflows.writeback.transports.rest import (
    RestTransport,
    RestTransportConfig,
    TokenHeaderAuth,
)
from recidiviz.common.http import HttpMethod
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import in_gcp_production
from recidiviz.utils.secrets import get_secret

# Firestore document ID for the early discharge opportunity status.
EARLY_DISCHARGE_STATUS_DOC_ID = "usIaEarlyDischarge"


class McpCallbackParseError(Exception):
    """Raised when the MCP callback body cannot be parsed as XML."""


class McpCallbackMissingFieldsError(Exception):
    """Raised when the MCP callback XML is missing MessageId or TransactionStatus."""


class McpMessageNotFoundError(Exception):
    """Raised when no Firestore documents match the given MCP message ID."""


class McpMessageAmbiguousError(Exception):
    """Raised when the MCP message ID matches documents belonging to multiple persons."""


# XML namespace URIs in MCP's callback body.
# Root element namespace for the AsynchronousNotificationMessage envelope.
_NS_EXCHANGE_NOTIFICATION = "http://www.cjis.iowa.gov/exchange-notification/1.0"
# Namespace for the payload fields (MessageId, TransactionStatus, etc.).
MCP_WS_RESPONSE_NS = "http://www.cjis.iowa.gov/WSResponse/1.0"

# XML namespace URIs defined by the MCP/NIEM schema.
_NS_NIEM_CORE = "http://release.niem.gov/niem/niem-core/5.0/"
_NS_IA = "http://www.cjis.iowa.gov/WriteDoc/1.0"
_NS_IA_EXT = "http://mcp.com/iowa/extension/v/1.0/"
_NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
_NS_J = "http://release.niem.gov/niem/domains/jxdm/7.2/"

_SCHEMA_LOCATION = f"{_NS_IA} WriteDoc_Iowa.xsd"

# Declarative XML tree node: (namespace_uri, local_tag, body). ``body`` is a
# string for text leaves or a list of child nodes for containers.
_XmlNode: TypeAlias = tuple[str, str, "str | list[_XmlNode]"]


def _append_xml_children(parent: ET.Element, children: list[_XmlNode]) -> None:
    """Recursively append a declarative _XmlNode tree under ``parent``."""
    for ns, tag, body in children:
        el = ET.SubElement(parent, f"{{{ns}}}{tag}")
        if isinstance(body, str):
            el.text = body
        else:
            _append_xml_children(el, body)


# Confirmed with MCP and ATG that _MCP_ACTION_CODE should be set to "A" since
# we are only adding new records in this flow. MessageKey and SystemIdentificationNumber are
# optional (minOccurs="0") so leaving blank for now (until MCP/ATG lets me know otherwise).
_MCP_ACTION_CODE = "A"
_MCP_MESSAGE_KEY = ""
_MCP_SYSTEM_ID = ""

MCP_TRANSPORT_CONFIG = RestTransportConfig(
    system_name="MCP",
    url_secret="workflows_us_ia_early_discharge_url",  # nosec
    credential_secret="workflows_us_ia_early_discharge_key",
    test_url_secret="workflows_us_ia_early_discharge_test_url",
    auth_strategy=TokenHeaderAuth("X-API-Key"),
    http_method=HttpMethod.POST,
    content_type="text/xml",
)


class UsIaEarlyDischargeRequestData(WritebackRequestData):
    """Writeback request data for submitting an early discharge request in IA.

    All fields are populated by the Recidiviz frontend from the early discharge
    form and the person's case data.
    """

    # OffenderCd
    person_external_id: str
    # StaffId of the supervision officer submitting the request.
    officer_external_id: str
    # StaffId of the supervision officer's supervisor
    supervisor_external_id: str
    # List of charge ids submitted on the form
    charge_ids: list[str]
    # District (region) associated with the supervision case.
    region: str
    # Work unit associated with the supervision case.
    work_unit: str
    # Progress of Supervision/Restitution Status/Recommendations Narrative
    # TODO(OBT-42080): Revise if narrative is confirmed to be optional on the IA early discharge form.
    narrative: str

    @field_validator(
        "person_external_id",
        "officer_external_id",
        "supervisor_external_id",
        "region",
        "work_unit",
        "narrative",
    )
    @classmethod
    def validate_non_empty_str(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must be non-empty.")
        return v

    @field_validator("charge_ids")
    @classmethod
    def validate_charge_ids(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one charge ID must be specified.")
        return v


class McpEarlyDischargeXmlRequest(BaseModel):
    """Models the XML request POSTed to MCP."""

    model_config = ConfigDict(frozen=True)

    # Offender code (SubjectCorrectionsIdentification).
    offender_id: str
    # Supervision officer staff ID (CaseOfficial RoleOfPerson).
    staff_manager_id: str
    # Supervisor staff ID (SubjectSupervision SupervisionPerson).
    staff_supervisor_id: str
    # Charge IDs (one ChargeIdentification element per entry).
    charge_identification: list[str]
    # Region name (JudicialOfficialFirm LocationLocale).
    case_region: str
    # Work unit name (JudicialOfficialFirm OrganizationUnitName).
    case_work_unit: str
    # Narrative (CaseOfficialCaseStatusText).
    # TODO(OBT-42080): Revise if narrative becomes optional on the IA early discharge form.
    case_status_notes: str
    # Unique message ID generated per submission.
    message_id: str

    @classmethod
    def from_request_data(
        cls,
        request_data: UsIaEarlyDischargeRequestData,
        *,
        use_test_ids: bool,
    ) -> "McpEarlyDischargeXmlRequest":
        """Build the MCP request, substituting MCP sandbox IDs for Recidiviz test submissions in production."""
        if use_test_ids:
            # TODO(OBT-42944): confirm with MCP how we want to implement the logic for detecting
            # a Recidiviz user in production
            offender_id = get_secret("workflows_us_ia_test_offender_id")
            staff_manager_id = get_secret("workflows_us_ia_test_staff_id")
            if offender_id is None or staff_manager_id is None:
                raise ValueError("Missing OffenderId and/or StaffId secret")
        else:
            offender_id = request_data.person_external_id
            staff_manager_id = request_data.officer_external_id

        return cls(
            offender_id=offender_id,
            staff_manager_id=staff_manager_id,
            staff_supervisor_id=request_data.supervisor_external_id,
            charge_identification=request_data.charge_ids,
            case_region=request_data.region,
            case_work_unit=request_data.work_unit,
            case_status_notes=request_data.narrative,
            message_id=str(uuid.uuid4()),
        )

    def _charge_node(self, charge_id: str) -> _XmlNode:
        # One CaseCharge per charge ID. The XSD requires ChargeIdentification
        # maxOccurs="1" inside ChargeType, so multiple charges must be separate
        # CaseCharge elements (CaseAugmentation allows maxOccurs="unbounded").
        # Each CaseCharge repeats the offender/supervisor IDs in its ChargeSubject.
        # Short aliases (rebound to `_NS_*` constants) keep the nested tree
        # compact enough for black to leave the shape intact.
        core, j = _NS_NIEM_CORE, _NS_J
        return (j, "CaseCharge", [
            (j, "ChargeIdentification", [
                (core, "IdentificationID", charge_id),
            ]),
            (j, "ChargeSubject", [
                (j, "SubjectSupervision", [
                    (core, "SupervisionPerson", [
                        (core, "PersonHumanResourceIdentification", [
                            (core, "IdentificationID", self.staff_supervisor_id),
                        ]),
                    ]),
                ]),
                (j, "SubjectCorrectionsIdentification", [
                    (core, "IdentificationID", self.offender_id),
                ]),
            ]),
        ])  # fmt: skip

    def _case_official_node(self) -> _XmlNode:
        core, j = _NS_NIEM_CORE, _NS_J
        children: list[_XmlNode] = [
            (core, "RoleOfPerson", [
                (core, "PersonHumanResourceIdentification", [
                    (core, "IdentificationID", self.staff_manager_id),
                ]),
            ]),
            (j, "JudicialOfficialFirm", [
                (core, "OrganizationLocation", [
                    (core, "LocationLocale", [
                        (core, "LocaleDistrictName", self.case_region),
                    ]),
                ]),
                (core, "OrganizationUnitName", self.case_work_unit),
            ]),
            (j, "CaseOfficialCaseStatusText", self.case_status_notes),
        ]  # fmt: skip
        return (j, "CaseOfficial", children)

    def to_xml(self) -> str:
        """Serialize the request to a NIEM-conformant XML string."""
        # Register prefixes for readable output (``<j:CaseCharge>`` vs ``ns0:``).
        # Scoped here rather than at import so unrelated imports don't mutate
        # ElementTree's global namespace map.
        ET.register_namespace("", _NS_NIEM_CORE)
        ET.register_namespace("ia", _NS_IA)
        ET.register_namespace("ia-ext", _NS_IA_EXT)
        ET.register_namespace("xsi", _NS_XSI)
        ET.register_namespace("j", _NS_J)

        core, ia_ext, j = _NS_NIEM_CORE, _NS_IA_EXT, _NS_J
        body: list[_XmlNode] = [
            (core, "Case", [
                (j, "CaseAugmentation", [
                    *(self._charge_node(cid) for cid in self.charge_identification),
                    self._case_official_node(),
                ]),
            ]),
            (ia_ext, "TransactionDetails", [
                (core, "MessageID", self.message_id),
                (ia_ext, "MessageAugmentation", [
                    (ia_ext, "ActionCode", _MCP_ACTION_CODE),
                    (ia_ext, "MessageKey", _MCP_MESSAGE_KEY),
                    (ia_ext, "SystemIdentificationNumber", _MCP_SYSTEM_ID),
                ]),
            ]),
        ]  # fmt: skip

        root = ET.Element(f"{{{_NS_IA}}}WriteDocExchange")
        root.set(f"{{{_NS_XSI}}}schemaLocation", _SCHEMA_LOCATION)
        _append_xml_children(root, body)

        buf = io.BytesIO()
        ET.ElementTree(root).write(buf, encoding="utf-8", xml_declaration=True)
        return buf.getvalue().decode("utf-8")


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
        try:
            root = _safe_fromstring(xml_str)
        except Exception as e:
            raise McpCallbackParseError(
                f"Failed to parse MCP callback XML: [{e}]"
            ) from e

        def _text(tag: str) -> str | None:
            el = root.find(f".//{{{MCP_WS_RESPONSE_NS}}}{tag}")
            return el.text if el is not None else None

        message_id = _text("MessageId")
        transaction_status = _text("TransactionStatus")
        if message_id is None or transaction_status is None:
            raise McpCallbackMissingFieldsError(
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
        Raises McpMessageNotFoundError if no documents are found in clientOpportunityUpdates
        for the message ID, or McpMessageAmbiguousError if the documents span multiple persons.
        """
        docs = (
            firestore_client.client.collection_group("clientOpportunityUpdates")
            .where(filter=FieldFilter("earlyDischargeMessageId", "==", message_id))
            .get()
        )

        if not docs:
            raise McpMessageNotFoundError(
                f"No pending early discharge request found for message ID [{message_id}]"
            )

        record_ids = {doc.reference.parent.parent.id for doc in docs}
        if len(record_ids) != 1:
            raise McpMessageAmbiguousError(
                f"Expected a single person for message ID [{message_id}], "
                f"got [{record_ids}]"
            )

        return cls(
            firestore_paths=[doc.reference.path for doc in docs],
            firestore_client=firestore_client,
        )

    def record_pending_message_id(self, message_id: str) -> None:
        """Store the message ID in clientOpportunityUpdates ."""
        for path in self.firestore_paths:
            self.firestore_client.set_document(
                path,
                {
                    "earlyDischargeMessageId": message_id,
                },
                merge=True,
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


class UsIaEarlyDischargeWritebackExecutor(
    WritebackExecutorInterface[UsIaEarlyDischargeRequestData]
):
    """Writeback implementation for IA early discharge requests.

    POSTs the NIEM XML to MCP. A 200 response confirms MCP received the
    request; the final outcome arrives asynchronously via the MCP callback
    and must be handled by a separate endpoint.
    """

    def __init__(self, request: UsIaEarlyDischargeRequestData) -> None:
        super().__init__(request)
        firestore_paths = [
            f"clientUpdatesV2/us_ia_{request.person_external_id}"
            f"/clientOpportunityUpdates/{EARLY_DISCHARGE_STATUS_DOC_ID}_{charge_id}"
            for charge_id in request.charge_ids
        ]
        self._tracker = UsIaEarlyDischargeStatusTracker(
            firestore_paths=firestore_paths,
            firestore_client=FirestoreClientImpl(),
        )

    def execute(self) -> None:
        # TODO(OBT-42944): confirm with MCP how we want to implement the logic for detecting
        # a Recidiviz user in production.  Currently defaulting to officer_external_id == "RECIDIVIZ".
        use_test_ids = (
            in_gcp_production() and self.request.officer_external_id == "RECIDIVIZ"
        )
        transport = RestTransport(
            MCP_TRANSPORT_CONFIG,
            # TODO(#68802): Centralize logic for detecting a Recidiviz user in writeback code.
            use_test_url=use_test_ids,
        )

        mcp_request = McpEarlyDischargeXmlRequest.from_request_data(
            self.request,
            use_test_ids=use_test_ids,
        )
        self._tracker.record_pending_message_id(mcp_request.message_id)
        transport.send(body=mcp_request.to_xml())

    def create_status_tracker(self) -> UsIaEarlyDischargeStatusTracker:
        return self._tracker

    @property
    def operation_action_description(self) -> str:
        return "Submitting early discharge request to MCP"
