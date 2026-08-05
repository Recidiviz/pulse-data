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
"""Tests for the US_IA early discharge writeback."""
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from typing import Generator
from unittest import TestCase

import pytest
import requests
import responses
from flask import Flask
from mock import MagicMock, patch
from pydantic import ValidationError

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.us_ia_early_discharge import (
    _MCP_ACTION_CODE,
    _MCP_MESSAGE_KEY,
    _MCP_SYSTEM_ID,
    _NS_EXCHANGE_NOTIFICATION,
    _NS_IA,
    _NS_IA_EXT,
    _NS_J,
    _NS_NIEM_CORE,
    _NS_WS_RESPONSE,
    EARLY_DISCHARGE_STATUS_DOC_ID,
    McpCallbackData,
    McpEarlyDischargeXmlRequest,
    UsIaEarlyDischargeRequestData,
    UsIaEarlyDischargeStatusTracker,
    UsIaEarlyDischargeWritebackExecutor,
)

PERSON_EXTERNAL_ID = "A123456"
STAFF_ID = "STAFF01"
STAFF_SUPERVISOR_ID = "SUP99"
CHARGE_IDENTIFICATION = ["CHG789", "CHG790"]
CASE_REGION = "District 1"
CASE_WORK_UNIT = "Unit A"
_TEST_MESSAGE_ID = "test-message-id-1234"


def _make_callback_xml(
    message_id: str = _TEST_MESSAGE_ID,
    transaction_status: str = "S",
    error_code: str | None = None,
    error_msg: str | None = None,
) -> str:
    """Build a test AsynchronousNotificationMessage matching the confirmed MCP format."""
    error_code_el = (
        f'<ErrorCode xmlns="{_NS_WS_RESPONSE}">{error_code}</ErrorCode>'
        if error_code
        else ""
    )
    error_el = (
        f'<ErrorMsg xmlns="{_NS_WS_RESPONSE}">{error_msg}</ErrorMsg>'
        if error_msg
        else ""
    )
    return (
        f'<AsynchronousNotificationMessage xmlns="{_NS_EXCHANGE_NOTIFICATION}">'
        f'<MessageId xmlns="{_NS_WS_RESPONSE}">{message_id}</MessageId>'
        f'<Description xmlns="{_NS_WS_RESPONSE}">Recidiviz CBC Discharge Asynchronous Response</Description>'
        f'<TransactionStatus xmlns="{_NS_WS_RESPONSE}">{transaction_status}</TransactionStatus>'
        f"{error_code_el}"
        f"{error_el}"
        f"</AsynchronousNotificationMessage>"
    )


MODULE = "recidiviz.case_triage.workflows.writeback.us_ia_early_discharge"
TRANSPORT_MODULE = "recidiviz.case_triage.workflows.writeback.transports.rest"


def _make_request_data(**overrides: object) -> UsIaEarlyDischargeRequestData:
    return UsIaEarlyDischargeRequestData(
        **{  # type: ignore[arg-type]
            "person_external_id": PERSON_EXTERNAL_ID,
            "officer_external_id": STAFF_ID,
            "supervisor_external_id": STAFF_SUPERVISOR_ID,
            "charge_ids": CHARGE_IDENTIFICATION,
            "region": CASE_REGION,
            "work_unit": CASE_WORK_UNIT,
            "narrative": "Released early per policy.",
            **overrides,
        }
    )


@pytest.fixture(autouse=True)
def app_context() -> Generator[None, None, None]:
    test_app = Flask("test_us_ia_writeback")
    with test_app.app_context():
        yield


# ---------------------------------------------------------------------------
# UsIaEarlyDischargeRequestData
# ---------------------------------------------------------------------------


class TestUsIaEarlyDischargeRequestData(TestCase):
    """Tests model validation for UsIaEarlyDischargeRequestData."""

    def test_valid_camel_case(self) -> None:
        data = UsIaEarlyDischargeRequestData.model_validate(
            {
                "personExternalId": PERSON_EXTERNAL_ID,
                "officerExternalId": STAFF_ID,
                "supervisorExternalId": STAFF_SUPERVISOR_ID,
                "chargeIds": CHARGE_IDENTIFICATION,
                "region": CASE_REGION,
                "workUnit": CASE_WORK_UNIT,
                "narrative": "Released early per policy.",
            }
        )
        self.assertEqual(data.person_external_id, PERSON_EXTERNAL_ID)
        self.assertEqual(data.supervisor_external_id, STAFF_SUPERVISOR_ID)

    def test_valid_snake_case(self) -> None:
        data = UsIaEarlyDischargeRequestData.model_validate(
            {
                "person_external_id": PERSON_EXTERNAL_ID,
                "officer_external_id": STAFF_ID,
                "supervisor_external_id": STAFF_SUPERVISOR_ID,
                "charge_ids": CHARGE_IDENTIFICATION,
                "region": CASE_REGION,
                "work_unit": CASE_WORK_UNIT,
                "narrative": "Released early per policy.",
            }
        )
        self.assertEqual(data.charge_ids, CHARGE_IDENTIFICATION)

    def test_narrative(self) -> None:
        data = _make_request_data(narrative="Released early per policy.")
        self.assertEqual(data.narrative, "Released early per policy.")

    def test_missing_required_field_raises(self) -> None:
        with self.assertRaises(ValidationError):
            UsIaEarlyDischargeRequestData.model_validate(
                {
                    "personExternalId": PERSON_EXTERNAL_ID,
                    "staffId": STAFF_ID,
                    # missing staffSupervisorId and others
                }
            )

    def test_empty_string_fields_raise(self) -> None:
        for field in (
            "person_external_id",
            "officer_external_id",
            "supervisor_external_id",
            "region",
            "work_unit",
            "narrative",
        ):
            with self.subTest(field=field):
                with self.assertRaises(ValidationError):
                    _make_request_data(**{field: ""})

    def test_empty_charge_ids_raises(self) -> None:
        with self.assertRaises(ValidationError):
            _make_request_data(charge_ids=[])

    def test_should_queue_task_defaults_true(self) -> None:
        data = _make_request_data()
        self.assertTrue(data.should_queue_task)

    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_to_cloud_task_payload_sets_should_queue_false(
        self, _mock_firestore: MagicMock
    ) -> None:
        executor = UsIaEarlyDischargeWritebackExecutor(_make_request_data())
        payload = executor.to_cloud_task_payload()
        self.assertFalse(payload["should_queue_task"])
        self.assertEqual(payload["person_external_id"], PERSON_EXTERNAL_ID)


# ---------------------------------------------------------------------------
# McpEarlyDischargeXmlRequest
# ---------------------------------------------------------------------------


class TestMcpEarlyDischargeXmlRequest(TestCase):
    """Tests XML serialization for McpEarlyDischargeXmlRequest."""

    def _build_and_parse(self, **overrides: object) -> ET.Element:
        request_data = _make_request_data(**overrides)
        xml_request = McpEarlyDischargeXmlRequest.from_request_data(
            request_data, use_test_ids=False
        )
        xml_str = xml_request.to_xml()
        return ET.fromstring(xml_str)

    def _find(self, root: ET.Element, ns: str, tag: str) -> ET.Element | None:
        return root.find(f".//{{{ns}}}{tag}")

    def test_root_element_is_write_doc_exchange(self) -> None:
        root = self._build_and_parse()
        self.assertEqual(root.tag, f"{{{_NS_IA}}}WriteDocExchange")

    def test_offender_id_in_corrections_identification(self) -> None:
        root = self._build_and_parse()
        corrections_id = root.find(
            f".//{{{_NS_J}}}SubjectCorrectionsIdentification"
            f"/{{{_NS_NIEM_CORE}}}IdentificationID"
        )
        self.assertIsNotNone(corrections_id)
        self.assertEqual(corrections_id.text, PERSON_EXTERNAL_ID)  # type: ignore[union-attr]

    def test_supervisor_id_in_supervision_person(self) -> None:
        root = self._build_and_parse()
        supervisor_id = root.find(
            f".//{{{_NS_J}}}SubjectSupervision"
            f"/{{{_NS_NIEM_CORE}}}SupervisionPerson"
            f"/{{{_NS_NIEM_CORE}}}PersonHumanResourceIdentification"
            f"/{{{_NS_NIEM_CORE}}}IdentificationID"
        )
        self.assertIsNotNone(supervisor_id)
        self.assertEqual(supervisor_id.text, STAFF_SUPERVISOR_ID)  # type: ignore[union-attr]

    def test_staff_manager_id_in_case_official(self) -> None:
        root = self._build_and_parse()
        manager_id = root.find(
            f".//{{{_NS_J}}}CaseOfficial"
            f"/{{{_NS_NIEM_CORE}}}RoleOfPerson"
            f"/{{{_NS_NIEM_CORE}}}PersonHumanResourceIdentification"
            f"/{{{_NS_NIEM_CORE}}}IdentificationID"
        )
        self.assertIsNotNone(manager_id)
        self.assertEqual(manager_id.text, STAFF_ID)  # type: ignore[union-attr]

    def test_charge_identification(self) -> None:
        root = self._build_and_parse()
        charge_id_els = root.findall(
            f".//{{{_NS_J}}}ChargeIdentification"
            f"/{{{_NS_NIEM_CORE}}}IdentificationID"
        )
        self.assertEqual([el.text for el in charge_id_els], CHARGE_IDENTIFICATION)

    def test_case_region_and_work_unit(self) -> None:
        root = self._build_and_parse()
        district = root.find(f".//{{{_NS_NIEM_CORE}}}LocaleDistrictName")
        unit = root.find(f".//{{{_NS_NIEM_CORE}}}OrganizationUnitName")
        self.assertIsNotNone(district)
        self.assertEqual(district.text, CASE_REGION)  # type: ignore[union-attr]
        self.assertIsNotNone(unit)
        self.assertEqual(unit.text, CASE_WORK_UNIT)  # type: ignore[union-attr]

    def test_narrative_present_when_provided(self) -> None:
        root = self._build_and_parse(narrative="Good behavior noted.")
        status_text = root.find(f".//{{{_NS_J}}}CaseOfficialCaseStatusText")
        self.assertIsNotNone(status_text)
        self.assertEqual(status_text.text, "Good behavior noted.")  # type: ignore[union-attr]

    def test_transaction_details_constants(self) -> None:
        root = self._build_and_parse()
        action_code = root.find(f".//{{{_NS_IA_EXT}}}ActionCode")
        message_key = root.find(f".//{{{_NS_IA_EXT}}}MessageKey")
        system_id = root.find(f".//{{{_NS_IA_EXT}}}SystemIdentificationNumber")
        self.assertIsNotNone(action_code)
        self.assertEqual(action_code.text, _MCP_ACTION_CODE)  # type: ignore[union-attr]
        self.assertIsNotNone(message_key)
        # ET returns None for empty elements after round-trip, normalize to ""
        self.assertEqual(message_key.text or "", _MCP_MESSAGE_KEY)  # type: ignore[union-attr]
        self.assertIsNotNone(system_id)
        self.assertEqual(system_id.text or "", _MCP_SYSTEM_ID)  # type: ignore[union-attr]

    def test_message_id_is_unique_per_call(self) -> None:
        request_data = _make_request_data()
        req_a = McpEarlyDischargeXmlRequest.from_request_data(
            request_data, use_test_ids=False
        )
        req_b = McpEarlyDischargeXmlRequest.from_request_data(
            request_data, use_test_ids=False
        )
        self.assertNotEqual(req_a.message_id, req_b.message_id)

    def test_xml_declaration_present(self) -> None:
        request_data = _make_request_data()
        xml_str = McpEarlyDischargeXmlRequest.from_request_data(
            request_data, use_test_ids=False
        ).to_xml()
        self.assertTrue(xml_str.startswith("<?xml"))

    @patch(f"{MODULE}.get_secret")
    def test_use_test_ids_substitutes_secrets(self, mock_get_secret: MagicMock) -> None:
        mock_get_secret.side_effect = {
            "workflows_us_ia_test_offender_id": "TEST_OFFENDER",
            "workflows_us_ia_test_staff_id": "TEST_STAFF",
        }.get

        request_data = _make_request_data()
        xml_request = McpEarlyDischargeXmlRequest.from_request_data(
            request_data, use_test_ids=True
        )
        self.assertEqual(xml_request.offender_id, "TEST_OFFENDER")
        self.assertEqual(xml_request.staff_manager_id, "TEST_STAFF")

    @patch(f"{MODULE}.get_secret")
    def test_use_test_ids_missing_secrets_raises(
        self, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.return_value = None
        with self.assertRaises(ValueError):
            McpEarlyDischargeXmlRequest.from_request_data(
                _make_request_data(), use_test_ids=True
            )

    # XSD files from the CJIS_WriteDoc_SSP package provided by MCP, stored in
    # schema_documentation/us_ia/ (not committed to git).
    _WRITE_DOC_XSD = "recidiviz/tests/case_triage/workflows/writeback/schema_documentation/us_ia/WriteDoc_Iowa.xsd"

    @pytest.mark.skipif(
        not shutil.which("xmllint")
        or not os.path.exists(
            "recidiviz/tests/case_triage/workflows/writeback/schema_documentation/us_ia/WriteDoc_Iowa_Ext.xsd"
        ),
        reason="xmllint not installed or full MCP SSP package not present",
    )
    def test_xml_validates_against_mcp_schema(self) -> None:
        xml_str = McpEarlyDischargeXmlRequest.from_request_data(
            _make_request_data(charge_ids=["CHG001", "CHG002"]),
            use_test_ids=False,
        ).to_xml()
        result = subprocess.run(
            ["xmllint", "--noout", "--schema", self._WRITE_DOC_XSD, "-"],
            input=xml_str.encode("utf-8"),
            capture_output=True,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            0,
            f"Schema validation failed:\n{result.stderr.decode()}",
        )


# ---------------------------------------------------------------------------
# McpCallbackData
# ---------------------------------------------------------------------------


class TestMcpCallbackData(TestCase):
    """Tests for McpCallbackData XML parsing."""

    def test_parse_success_callback(self) -> None:
        data = McpCallbackData.from_callback_xml(
            _make_callback_xml(message_id=_TEST_MESSAGE_ID, transaction_status="S")
        )
        self.assertEqual(data.message_id, _TEST_MESSAGE_ID)
        self.assertEqual(data.transaction_status, "S")
        self.assertIsNone(data.error_code)
        self.assertIsNone(data.error_message)

    def test_parse_error_callback_with_message(self) -> None:
        data = McpCallbackData.from_callback_xml(
            _make_callback_xml(
                message_id=_TEST_MESSAGE_ID,
                transaction_status="E",
                error_code="1",
                error_msg="ATG rejected: ineligible offense",
            )
        )
        self.assertEqual(data.transaction_status, "E")
        self.assertEqual(data.error_code, "1")
        self.assertEqual(data.error_message, "ATG rejected: ineligible offense")

    def test_parse_missing_message_id_raises(self) -> None:
        xml = (
            f'<ws:WSResponse xmlns:ws="{_NS_WS_RESPONSE}">'
            f"<ws:TransactionStatus>S</ws:TransactionStatus>"
            f"</ws:WSResponse>"
        )
        with self.assertRaises(ValueError):
            McpCallbackData.from_callback_xml(xml)

    def test_parse_missing_transaction_status_raises(self) -> None:
        xml = (
            f'<ws:WSResponse xmlns:ws="{_NS_WS_RESPONSE}">'
            f"<ws:MessageId>{_TEST_MESSAGE_ID}</ws:MessageId>"
            f"</ws:WSResponse>"
        )
        with self.assertRaises(ValueError):
            McpCallbackData.from_callback_xml(xml)

    def test_parse_invalid_xml_raises(self) -> None:
        with self.assertRaises(ET.ParseError):
            McpCallbackData.from_callback_xml("not xml at all")


# ---------------------------------------------------------------------------
# UsIaEarlyDischargeWritebackExecutor
# ---------------------------------------------------------------------------


class TestUsIaEarlyDischargeWritebackExecutor(TestCase):
    """Tests for UsIaEarlyDischargeWritebackExecutor."""

    def setUp(self) -> None:
        self.fake_url = "http://fake-mcp.com"

    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_posts_xml_to_mcp(
        self, _mock_firestore: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.return_value = self.fake_url
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.POST, self.fake_url, status=200)

            UsIaEarlyDischargeWritebackExecutor(_make_request_data()).execute()

            self.assertEqual(len(rsps.calls), 1)
            sent_body = rsps.calls[0].request.body
            assert isinstance(sent_body, str)
            self.assertIn(
                f"<IdentificationID>{PERSON_EXTERNAL_ID}</IdentificationID>",
                sent_body,
            )

    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_sends_text_xml_content_type(
        self, _mock_firestore: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.return_value = self.fake_url
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.POST, self.fake_url, status=200)

            UsIaEarlyDischargeWritebackExecutor(_make_request_data()).execute()

            content_type = rsps.calls[0].request.headers.get("Content-Type", "")
            self.assertEqual(content_type, "text/xml")

    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_missing_secrets_raises(
        self, _mock_firestore: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.return_value = None
        with self.assertRaises(EnvironmentError):
            UsIaEarlyDischargeWritebackExecutor(_make_request_data()).execute()

    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_http_error_raises(
        self, _mock_firestore: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.return_value = self.fake_url
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.POST, self.fake_url, status=500)
            with self.assertRaises(requests.exceptions.HTTPError):
                UsIaEarlyDischargeWritebackExecutor(_make_request_data()).execute()

    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_network_error_raises(
        self, _mock_firestore: MagicMock, mock_get_secret: MagicMock
    ) -> None:
        mock_get_secret.return_value = self.fake_url
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.POST, self.fake_url, body=ConnectionRefusedError())
            with self.assertRaises(ConnectionRefusedError):
                UsIaEarlyDischargeWritebackExecutor(_make_request_data()).execute()

    @patch(f"{MODULE}.get_secret")
    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    @patch(f"{MODULE}.in_gcp_production")
    def test_execute_prod_recidiviz_user_uses_test_url(
        self,
        mock_in_prod: MagicMock,
        _mock_firestore: MagicMock,
        mock_transport_secret: MagicMock,
        mock_module_secret: MagicMock,
    ) -> None:
        mock_in_prod.return_value = True
        mock_transport_secret.return_value = self.fake_url
        mock_module_secret.side_effect = {
            "workflows_us_ia_test_offender_id": "TEST_OFFENDER",
            "workflows_us_ia_test_staff_id": "TEST_STAFF",
        }.get
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.POST, self.fake_url, status=200)
            UsIaEarlyDischargeWritebackExecutor(
                _make_request_data(officer_external_id="RECIDIVIZ")
            ).execute()
            mock_transport_secret.assert_any_call(
                "workflows_us_ia_early_discharge_test_url"
            )

    @patch(
        f"{MODULE}.uuid.uuid4",
        return_value=MagicMock(hex="", __str__=lambda self: _TEST_MESSAGE_ID),
    )
    @patch(f"{TRANSPORT_MODULE}.get_secret")
    @patch(f"{MODULE}.FirestoreClientImpl")
    def test_execute_records_pending_message_id(
        self,
        mock_firestore_cls: MagicMock,
        mock_get_secret: MagicMock,
        _mock_uuid: MagicMock,
    ) -> None:
        mock_get_secret.return_value = self.fake_url
        with responses.RequestsMock() as rsps:
            rsps.add(responses.POST, self.fake_url, status=200)
            UsIaEarlyDischargeWritebackExecutor(_make_request_data()).execute()

        path = mock_firestore_cls.return_value.set_document.call_args.args[0]
        self.assertTrue(path.startswith("clientUpdatesV2"))
        doc = mock_firestore_cls.return_value.set_document.call_args.args[1]
        self.assertEqual(doc["earlyDischargeMessageId"], _TEST_MESSAGE_ID)


# ---------------------------------------------------------------------------
# UsIaEarlyDischargeStatusTracker
# ---------------------------------------------------------------------------


def _expected_firestore_path(charge_id: str) -> str:
    return (
        f"clientUpdatesV2/us_ia_{PERSON_EXTERNAL_ID}"
        f"/clientOpportunityUpdates/usIaEarlyDischarge_{charge_id}"
    )


EXPECTED_FIRESTORE_PATHS = [
    _expected_firestore_path(charge_id) for charge_id in CHARGE_IDENTIFICATION
]


class TestUsIaEarlyDischargeStatusTracker(TestCase):
    """Tests for UsIaEarlyDischargeStatusTracker."""

    def _tracker(self) -> tuple[UsIaEarlyDischargeStatusTracker, MagicMock]:
        mock_firestore = MagicMock()
        mock_firestore.timestamp_key = "serverTimestamp"
        return (
            UsIaEarlyDischargeStatusTracker(
                firestore_paths=EXPECTED_FIRESTORE_PATHS,
                firestore_client=mock_firestore,
            ),
            mock_firestore,
        )

    def test_set_status_in_progress_writes_once_per_charge(self) -> None:
        tracker, mock_firestore = self._tracker()
        tracker.set_status(ExternalSystemRequestStatus.IN_PROGRESS)

        self.assertEqual(
            mock_firestore.set_document.call_count, len(CHARGE_IDENTIFICATION)
        )
        for call, expected_path in zip(
            mock_firestore.set_document.call_args_list, EXPECTED_FIRESTORE_PATHS
        ):
            path, doc, *_ = call.args
            self.assertEqual(path, expected_path)
            self.assertEqual(
                doc["status"], ExternalSystemRequestStatus.IN_PROGRESS.value
            )

    def test_set_status_failure_writes_once_per_charge(self) -> None:
        tracker, mock_firestore = self._tracker()
        tracker.set_status(ExternalSystemRequestStatus.FAILURE)

        self.assertEqual(
            mock_firestore.set_document.call_count, len(CHARGE_IDENTIFICATION)
        )
        for call, expected_path in zip(
            mock_firestore.set_document.call_args_list, EXPECTED_FIRESTORE_PATHS
        ):
            path, doc, *_ = call.args
            self.assertEqual(path, expected_path)
            self.assertEqual(doc["status"], ExternalSystemRequestStatus.FAILURE.value)

    def test_set_status_success_is_noop(self) -> None:
        tracker, mock_firestore = self._tracker()
        tracker.set_status(ExternalSystemRequestStatus.SUCCESS)
        mock_firestore.set_document.assert_not_called()

    def test_apply_mcp_callback_success_writes_once_per_charge(self) -> None:
        tracker, mock_firestore = self._tracker()
        tracker.apply_mcp_callback(
            McpCallbackData(message_id=_TEST_MESSAGE_ID, transaction_status="S")
        )

        self.assertEqual(
            mock_firestore.set_document.call_count, len(CHARGE_IDENTIFICATION)
        )
        for call, expected_path in zip(
            mock_firestore.set_document.call_args_list, EXPECTED_FIRESTORE_PATHS
        ):
            path, doc, *_ = call.args
            self.assertEqual(path, expected_path)
            self.assertEqual(doc["status"], ExternalSystemRequestStatus.SUCCESS.value)
            self.assertNotIn("errorMessage", doc)

    def test_apply_mcp_callback_failure_writes_error_message_to_all_charges(
        self,
    ) -> None:
        tracker, mock_firestore = self._tracker()
        tracker.apply_mcp_callback(
            McpCallbackData(
                message_id=_TEST_MESSAGE_ID,
                transaction_status="E",
                error_message="ATG rejected: ineligible offense",
            )
        )

        self.assertEqual(
            mock_firestore.set_document.call_count, len(CHARGE_IDENTIFICATION)
        )
        for call in mock_firestore.set_document.call_args_list:
            _, doc, *_ = call.args
            self.assertEqual(doc["status"], ExternalSystemRequestStatus.FAILURE.value)
            self.assertEqual(doc["errorMessage"], "ATG rejected: ineligible offense")

    def test_apply_mcp_callback_unknown_status_treated_as_failure(self) -> None:
        tracker, mock_firestore = self._tracker()
        tracker.apply_mcp_callback(
            McpCallbackData(message_id=_TEST_MESSAGE_ID, transaction_status="X")
        )

        for call in mock_firestore.set_document.call_args_list:
            _, doc, *_ = call.args
            self.assertEqual(doc["status"], ExternalSystemRequestStatus.FAILURE.value)

    def _mock_docs(
        self, charge_ids: list[str], person_external_id: str = PERSON_EXTERNAL_ID
    ) -> list[MagicMock]:
        docs = []
        for charge_id in charge_ids:
            doc = MagicMock()
            doc.reference.parent.parent.id = f"us_ia_{person_external_id}"
            doc.reference.path = (
                f"clientUpdatesV2/us_ia_{person_external_id}"
                f"/clientOpportunityUpdates/{EARLY_DISCHARGE_STATUS_DOC_ID}_{charge_id}"
            )
            docs.append(doc)
        return docs

    def test_from_message_id(self) -> None:
        mock_firestore = MagicMock()
        mock_firestore.client.collection_group.return_value.where.return_value.get.return_value = self._mock_docs(
            CHARGE_IDENTIFICATION
        )
        tracker = UsIaEarlyDischargeStatusTracker.from_message_id(
            _TEST_MESSAGE_ID, mock_firestore
        )
        self.assertEqual(
            tracker.firestore_paths,
            [
                "clientUpdatesV2/us_ia_A123456/clientOpportunityUpdates/usIaEarlyDischarge_CHG789",
                "clientUpdatesV2/us_ia_A123456/clientOpportunityUpdates/usIaEarlyDischarge_CHG790",
            ],
        )
        mock_firestore.client.collection_group.assert_called_once_with(
            "clientOpportunityUpdates"
        )

    def test_from_message_id_unknown_raises(self) -> None:
        mock_firestore = MagicMock()
        mock_firestore.client.collection_group.return_value.where.return_value.get.return_value = (
            []
        )
        with self.assertRaisesRegex(ValueError, rf"\[{_TEST_MESSAGE_ID}\]"):
            UsIaEarlyDischargeStatusTracker.from_message_id(
                _TEST_MESSAGE_ID, mock_firestore
            )

    def test_from_message_id_multiple_persons_raises(self) -> None:
        mock_firestore = MagicMock()
        doc1 = self._mock_docs(["CHG789"], person_external_id="A123456")[0]
        doc2 = self._mock_docs(["CHG789"], person_external_id="B999999")[0]
        mock_firestore.client.collection_group.return_value.where.return_value.get.return_value = [
            doc1,
            doc2,
        ]
        with self.assertRaisesRegex(ValueError, r"Expected a single person"):
            UsIaEarlyDischargeStatusTracker.from_message_id(
                _TEST_MESSAGE_ID, mock_firestore
            )

    def test_firestore_path_uses_correct_state_prefix(self) -> None:
        tracker, mock_firestore = self._tracker()
        tracker.set_status(ExternalSystemRequestStatus.IN_PROGRESS)

        for call in mock_firestore.set_document.call_args_list:
            path = call.args[0]
            self.assertTrue(path.startswith("clientUpdatesV2/us_ia_"))
