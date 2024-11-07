# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Utilities used in tests for the send_id_lsu_texts.py file"""
import datetime

from google.cloud.firestore_v1.base_document import DocumentSnapshot
from google.cloud.firestore_v1.document import DocumentReference

from recidiviz.case_triage.util import MessageType
from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus


class JIITestObjects:
    """Class for test DocumentSnapshot objects"""

    def __init__(self) -> None:
        self.test_text_message_document_A = DocumentSnapshot(
            reference=DocumentReference(
                "twilio_messages",
                "us_test_999999999",
                "lsu_eligibility_messages",
                "eligibility_01_2023",
            ),
            data={
                "body": "Hi Michelle, testing testing 123.",
                "message_sid": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "message_type": MessageType.INITIAL_TEXT.value,
                "phone_number": "0123456789",
                "raw_status": "delivered",
                "status": ExternalSystemRequestStatus.SUCCESS.value,
                "status_last_updated": datetime.datetime(
                    2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
                ),
                "timestamp": datetime.datetime(
                    2024, 6, 11, 19, 13, 40, 403400, tzinfo=datetime.timezone.utc
                ),
                "state_code": "us_test",
                "external_id": "999999999",
                "batch_id": "01_2023",
            },
            exists=True,
            read_time=datetime.datetime(
                2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
            ),
            create_time=datetime.datetime(
                2024, 6, 11, 19, 13, 40, 403400, tzinfo=datetime.timezone.utc
            ),
            update_time=datetime.datetime(
                2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
            ),
        )
        self.test_text_message_document_B = DocumentSnapshot(
            reference=DocumentReference(
                "twilio_messages",
                "us_test_888888888",
                "lsu_eligibility_messages",
                "eligibility_01_2023",
            ),
            data={
                "body": "Hi there. This is an eligibility text.",
                "message_sid": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                "message_type": MessageType.ELIGIBILITY_TEXT.value,
                "phone_number": "9876543210",
                "raw_status": "delivered",
                "status": ExternalSystemRequestStatus.SUCCESS.value,
                "status_last_updated": datetime.datetime(
                    2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
                ),
                "timestamp": datetime.datetime(
                    2024, 6, 11, 19, 13, 40, 403400, tzinfo=datetime.timezone.utc
                ),
                "state_code": "us_test",
                "external_id": "888888888",
                "batch_id": "01_2023",
            },
            exists=True,
            read_time=datetime.datetime(
                2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
            ),
            create_time=datetime.datetime(
                2024, 6, 11, 19, 13, 40, 403400, tzinfo=datetime.timezone.utc
            ),
            update_time=datetime.datetime(
                2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
            ),
        )
        self.test_text_message_document_C = DocumentSnapshot(
            reference=DocumentReference(
                "twilio_messages",
                "us_test_222222222",
                "lsu_eligibility_messages",
                "eligibility_01_01_2024_12_00_01",
            ),
            data={
                "body": "You may be eligible. Contact our po.",
                "message_sid": "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
                "message_type": MessageType.INITIAL_TEXT.value,
                "phone_number": "8762947611",
                "raw_status": "sent",
                "status": ExternalSystemRequestStatus.IN_PROGRESS.value,
                "status_last_updated": datetime.datetime(
                    2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
                ),
                "timestamp": datetime.datetime(
                    2024, 6, 11, 19, 13, 40, 403400, tzinfo=datetime.timezone.utc
                ),
                "state_code": "us_test",
                "external_id": "222222222",
                "batch_id": "01_01_2024_12_00_01",
            },
            exists=True,
            read_time=datetime.datetime(
                2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
            ),
            create_time=datetime.datetime(
                2024, 6, 11, 19, 13, 40, 403400, tzinfo=datetime.timezone.utc
            ),
            update_time=datetime.datetime(
                2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
            ),
        )
        self.test_jii_document_A = DocumentSnapshot(
            reference=DocumentReference(
                "twilio_messages",
                "us_test_888888888",
            ),
            data={
                "last_phone_num_update": datetime.datetime(
                    2024, 6, 12, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
                ),
                "phone_numbers": ["9876543210"],
                "last_opt_out_update": datetime.datetime(
                    2024, 6, 12, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
                ),
                "opt_out_type": "STOP",
                "responses": [
                    {
                        "response": "Stop",
                        "response_date": datetime.datetime(
                            2024,
                            6,
                            12,
                            19,
                            13,
                            52,
                            403422,
                            tzinfo=datetime.timezone.utc,
                        ),
                    }
                ],
                "state_code": "us_test",
                "external_id": "888888888",
                "batch_ids": ["01_2023"],
            },
            exists=True,
            read_time=datetime.datetime(
                2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
            ),
            create_time=datetime.datetime(
                2024, 6, 11, 19, 13, 40, 403400, tzinfo=datetime.timezone.utc
            ),
            update_time=datetime.datetime(
                2024, 6, 11, 19, 13, 52, 403422, tzinfo=datetime.timezone.utc
            ),
        )
