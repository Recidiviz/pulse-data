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
"""Tests for the US_IA early discharge MCP callback handling."""
import xml.etree.ElementTree as ET
from typing import Generator
from unittest import TestCase

import pytest
from flask import Flask
from mock import MagicMock

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.writeback.us_ia_early_discharge import (
    _NS_EXCHANGE_NOTIFICATION,
    _NS_WS_RESPONSE,
    EARLY_DISCHARGE_STATUS_DOC_ID,
    McpCallbackData,
    UsIaEarlyDischargeStatusTracker,
)

PERSON_EXTERNAL_ID = "A123456"
CHARGE_IDENTIFICATION = ["CHG789", "CHG790"]
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


@pytest.fixture(autouse=True)
def app_context() -> Generator[None, None, None]:
    test_app = Flask("test_us_ia_writeback")
    with test_app.app_context():
        yield


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
