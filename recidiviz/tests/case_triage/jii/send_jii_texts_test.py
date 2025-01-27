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
"""Implement tests for the send_jii_texts.py file"""

import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch

import freezegun
import pytest
from twilio.rest.api.v2010.account.message import MessageInstance

from recidiviz.case_triage.jii.send_jii_texts import (
    attempt_to_send_text,
    get_initial_and_eligibility_doc_ids,
    get_opt_out_document_ids,
    store_batch_id,
    update_statuses_from_previous_batch,
)
from recidiviz.case_triage.jii.helpers import (
    construct_eligibility_text_body,
    construct_initial_text_body,
)
from recidiviz.case_triage.util import MessageType
from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.tests.case_triage.jii.utils import JIITestObjects


class TestSendJIITexts(TestCase):
    """Implements tests for send_jii_texts.py."""

    def setUp(self) -> None:
        super().setUp()
        self.test_schema_objects = JIITestObjects()

    def tearDown(self) -> None:
        super().tearDown()

    @patch("recidiviz.case_triage.jii.send_jii_texts.FirestoreClientImpl")
    def test_store_batch_id(
        self,
        mock_firestore_impl: MagicMock,
    ) -> None:
        current_batch_id = "01_01_2024_00_00_00"
        message_type = MessageType.INITIAL_TEXT.value.lower()
        redeliver_failed_messages = False
        mock_view_name = "mock_view"
        state_code = "us_test"

        store_batch_id(
            firestore_client=mock_firestore_impl,
            current_batch_id=current_batch_id,
            message_type=message_type,
            redeliver_failed_messages=redeliver_failed_messages,
            bigquery_view=mock_view_name,
            state_code=state_code,
        )
        mock_firestore_impl.set_document.assert_called_once_with(
            document_path=f"batch_ids/{current_batch_id}",
            data={
                "message_type": message_type.upper(),
                "redelivery": redeliver_failed_messages,
                "bigquery_view": mock_view_name,
                "state_code": state_code,
            },
        )

    @patch("recidiviz.case_triage.jii.send_jii_texts.FirestoreClientImpl")
    def test_get_initial_and_eligibility_doc_ids(
        self,
        mock_firestore_impl: MagicMock,
    ) -> None:
        initial_doc_one = self.test_schema_objects.test_text_message_document_A
        eligibility_doc_one = self.test_schema_objects.test_text_message_document_B

        mock_firestore_impl.get_collection_group.return_value.where.return_value.where.return_value.stream.return_value = [
            initial_doc_one,
            eligibility_doc_one,
        ]
        (
            initial_text_document_ids,
            eligibility_text_document_ids_to_text_timestamp,
        ) = get_initial_and_eligibility_doc_ids(
            firestore_client=mock_firestore_impl, state_code="us_test"
        )

        mock_firestore_impl.get_collection_group.assert_called_once()
        self.assertEqual(
            eligibility_text_document_ids_to_text_timestamp,
            {
                "us_test_888888888": datetime.datetime(
                    2024, 6, 11, 19, 13, 40, 403400, tzinfo=datetime.timezone.utc
                )
            },
        )
        self.assertEqual(initial_text_document_ids, {"us_test_999999999"})

    @patch("recidiviz.case_triage.jii.send_jii_texts.FirestoreClientImpl")
    def test_get_opt_out_document_ids(self, mock_firestore_impl: MagicMock) -> None:
        jii_doc_a = self.test_schema_objects.test_jii_document_A

        mock_firestore_impl.get_collection.return_value.where.return_value.where.return_value.stream.return_value = [
            jii_doc_a
        ]

        opt_out_document_ids = get_opt_out_document_ids(
            firestore_client=mock_firestore_impl, state_code="us_test"
        )

        mock_firestore_impl.get_collection.assert_called_once()
        self.assertEqual(opt_out_document_ids, {"us_test_888888888"})

    @freezegun.freeze_time(datetime.datetime(2024, 7, 11, 0, 0, 0, 0))
    def test_attempt_to_send_text(self) -> None:

        external_ids_to_retry = {"us_test_888888888"}
        opt_out_document_ids = {"us_test_808080808", "us_test_201998273"}
        initial_text_document_ids = {"us_test_123456789", "us_test_606228781"}
        eligibility_text_document_ids_to_text_timestamp = {
            "us_test_123456789": datetime.datetime(
                2024, 6, 11, 19, 13, 40, 403400, tzinfo=datetime.timezone.utc
            )
        }
        previous_batch_id = "01_01_2024_12_00_01"

        # We are attempting redelivery of previously undelivered texts, and the previous message was delivered
        send_text = attempt_to_send_text(
            redeliver_failed_messages=True,
            document_id="us_test_123456789",
            external_ids_to_retry=external_ids_to_retry,
            opt_out_document_ids=opt_out_document_ids,
            message_type=MessageType.INITIAL_TEXT.value,
            initial_text_document_ids=initial_text_document_ids,
            eligibility_text_document_ids_to_text_timestamp=eligibility_text_document_ids_to_text_timestamp,
            resend_eligibility_texts=False,
            previous_batch_id=previous_batch_id,
        )
        self.assertFalse(send_text)

        # We are attempting redelivery of previously undelivered texts, and the previous message was undelivered
        send_text = attempt_to_send_text(
            redeliver_failed_messages=True,
            document_id="us_test_888888888",
            external_ids_to_retry=external_ids_to_retry,
            opt_out_document_ids=opt_out_document_ids,
            message_type=MessageType.INITIAL_TEXT.value,
            initial_text_document_ids=initial_text_document_ids,
            eligibility_text_document_ids_to_text_timestamp=eligibility_text_document_ids_to_text_timestamp,
            resend_eligibility_texts=False,
            previous_batch_id=previous_batch_id,
        )
        self.assertTrue(send_text)

        # The user has opted out
        send_text = attempt_to_send_text(
            redeliver_failed_messages=False,
            document_id="us_test_808080808",
            external_ids_to_retry=external_ids_to_retry,
            opt_out_document_ids=opt_out_document_ids,
            message_type=MessageType.ELIGIBILITY_TEXT.value,
            initial_text_document_ids=initial_text_document_ids,
            eligibility_text_document_ids_to_text_timestamp=eligibility_text_document_ids_to_text_timestamp,
            resend_eligibility_texts=False,
        )
        self.assertFalse(send_text)

        # This is an initial/welcome text, and the individual has already received an initial/welcome text in the past
        send_text = attempt_to_send_text(
            redeliver_failed_messages=False,
            document_id="us_test_606228781",
            external_ids_to_retry=external_ids_to_retry,
            opt_out_document_ids=opt_out_document_ids,
            message_type=MessageType.INITIAL_TEXT.value,
            initial_text_document_ids=initial_text_document_ids,
            eligibility_text_document_ids_to_text_timestamp=eligibility_text_document_ids_to_text_timestamp,
            resend_eligibility_texts=False,
        )
        self.assertFalse(send_text)

        # This is an initial/welcome text, and the individual has not received an initial/welcome text in the past
        send_text = attempt_to_send_text(
            redeliver_failed_messages=False,
            document_id="us_test_098765432",
            external_ids_to_retry=external_ids_to_retry,
            opt_out_document_ids=opt_out_document_ids,
            message_type=MessageType.INITIAL_TEXT.value,
            initial_text_document_ids=initial_text_document_ids,
            eligibility_text_document_ids_to_text_timestamp=eligibility_text_document_ids_to_text_timestamp,
            resend_eligibility_texts=False,
        )
        self.assertTrue(send_text)

        # This is an eligibility text, and the individual has not received an initial/welcome text in the past
        send_text = attempt_to_send_text(
            redeliver_failed_messages=False,
            document_id="us_test_098765432",
            external_ids_to_retry=external_ids_to_retry,
            opt_out_document_ids=opt_out_document_ids,
            message_type=MessageType.ELIGIBILITY_TEXT.value,
            initial_text_document_ids=initial_text_document_ids,
            eligibility_text_document_ids_to_text_timestamp=eligibility_text_document_ids_to_text_timestamp,
            resend_eligibility_texts=False,
        )
        self.assertFalse(send_text)

        # This is an eligibility text, and the individual has received an initial/welcome text in the past
        send_text = attempt_to_send_text(
            redeliver_failed_messages=False,
            document_id="us_test_606228781",
            external_ids_to_retry=external_ids_to_retry,
            opt_out_document_ids=opt_out_document_ids,
            message_type=MessageType.ELIGIBILITY_TEXT.value,
            initial_text_document_ids=initial_text_document_ids,
            eligibility_text_document_ids_to_text_timestamp=eligibility_text_document_ids_to_text_timestamp,
            resend_eligibility_texts=False,
        )
        self.assertTrue(send_text)

        # This is an eligibility text, and the individual has received an eligibility text in the past 90 days
        send_text = attempt_to_send_text(
            redeliver_failed_messages=False,
            document_id="us_test_123456789",
            external_ids_to_retry=external_ids_to_retry,
            opt_out_document_ids=opt_out_document_ids,
            message_type=MessageType.ELIGIBILITY_TEXT.value,
            initial_text_document_ids=initial_text_document_ids,
            eligibility_text_document_ids_to_text_timestamp=eligibility_text_document_ids_to_text_timestamp,
            resend_eligibility_texts=False,
        )
        self.assertFalse(send_text)

    @patch("recidiviz.case_triage.jii.send_jii_texts.FirestoreClientImpl")
    @patch("recidiviz.case_triage.jii.send_jii_texts.TwilioClient")
    @freezegun.freeze_time(datetime.datetime(2024, 7, 11, 0, 0, 0, 0))
    def test_update_statuses_from_previous_batch(
        self, mock_firestore_impl: MagicMock, mock_twilio_client: MagicMock
    ) -> None:
        # Let's say we have the message document stored in the database with a status of 'sent'
        message_doc_c = self.test_schema_objects.test_text_message_document_C
        message_doc_c_dict = message_doc_c.to_dict()
        if message_doc_c_dict is not None:
            self.assertEqual(message_doc_c_dict["raw_status"], "sent")

        # However in Twilio, the latest status of the message is 'failed'
        twilio_message = MessageInstance(
            version="2010-04-01",
            payload={
                "status": "failed",
                "error_code": "30007",
                "error_message": "Carrier violation",
            },
            account_sid="FAKE_SID",
            sid="CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
        )

        mock_twilio_client.messages.list.return_value = [twilio_message]
        mock_firestore_impl.get_collection_group.return_value.where.return_value.stream.return_value = [
            message_doc_c
        ]

        _ = update_statuses_from_previous_batch(
            twilio_client=mock_twilio_client,
            firestore_client=mock_firestore_impl,
            previous_batch_id="01_01_2024_12_00_01",
        )

        # Ensure that the status of the document has been updated
        mock_firestore_impl.set_document.assert_called_once_with(
            message_doc_c.reference.path,
            {
                "status": ExternalSystemRequestStatus.FAILURE.value,
                "status_last_updated": datetime.datetime.now(datetime.timezone.utc),
                "raw_status": "failed",
                "error_code": 30007,
                "errors": ["Carrier violation"],
            },
            merge=True,
        )


@pytest.mark.parametrize(
    (
        "fully_eligible",
        "missing_negative_da_within_90_days",
        "missing_income_verified_within_3_months",
        "fines_n_fees_denials",
    ),
    [
        pytest.param(True, False, False, False, id="Fully eligible text"),
        pytest.param(
            True,
            False,
            False,
            True,
            id="Fully eligible except missing fines/fees text",
        ),
        pytest.param(False, True, False, False, id="Missing DA text"),
        pytest.param(False, False, True, False, id="Missing EV text"),
        pytest.param(False, True, True, False, id="Missing DA and EV text"),
    ],
)
def test_text_character_length(
    fully_eligible: bool,
    missing_negative_da_within_90_days: bool,
    missing_income_verified_within_3_months: bool,
    fines_n_fees_denials: bool,
) -> None:
    MAX_CHARACTER_LENGTH = 973
    test_individual = {"person_name": '{"given_names": "Christopher"}'}
    test_po_name = "VeryyLong TestPoName"
    test_district = "district 7"

    # Initial Text
    initial_text_str = construct_initial_text_body(
        given_name="TestGivenName", po_name=test_po_name
    )
    assert len(initial_text_str) < MAX_CHARACTER_LENGTH

    # Eligibility Texts
    eligibility_text_string = construct_eligibility_text_body(
        individual=test_individual,
        fully_eligible=fully_eligible,
        missing_negative_da_within_90_days=missing_negative_da_within_90_days,
        missing_income_verified_within_3_months=missing_income_verified_within_3_months,
        fines_n_fees_denials=fines_n_fees_denials,
        po_name=test_po_name,
        district=test_district,
    )
    assert len(eligibility_text_string) < MAX_CHARACTER_LENGTH
