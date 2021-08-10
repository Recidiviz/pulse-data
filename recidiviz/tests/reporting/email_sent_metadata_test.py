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

"""Tests for email_sent_metadata.py"""
import datetime
import json
from unittest import TestCase
from unittest.mock import patch

from recidiviz.admin_panel.admin_stores import AdminStores
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.email_reporting_utils import gcsfs_path_for_batch_metadata
from recidiviz.reporting.email_sent_metadata import EmailSentMetadata, EmailSentResult
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class TestEmailSentMetadata(TestCase):
    """Class to test the email sent metadata functions"""

    STATE_CODE_STR = StateCode.US_ID
    BATCH_ID = "20210701202020"
    SENT_DATE = datetime.datetime.now()

    def setUp(self) -> None:
        self.project_id_patcher = patch(
            "recidiviz.admin_panel.admin_stores.metadata.project_id"
        )
        self.project_id_patcher.start().return_value = "recidiviz-staging"
        self.gcs_factory_patcher = patch(
            "recidiviz.admin_panel.admin_stores.GcsfsFactory.build"
        )
        fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = fake_gcs
        self.fs = fake_gcs
        self.admin_store = AdminStores()

        self.email_sent_metadata = EmailSentMetadata(
            batch_id=self.BATCH_ID, send_results=[]
        )

    def tearDown(self) -> None:
        self.gcs_factory_patcher.stop()
        self.project_id_patcher.stop()

    def test_add_new_email_send_result(self) -> None:

        self.email_sent_metadata.add_new_email_send_result(
            total_delivered=4,
            redirect_address="letter@kenny.ca",
            sent_date=self.SENT_DATE,
        )
        self.assertEqual(1, len(self.email_sent_metadata.send_results))

        self.email_sent_metadata.add_new_email_send_result(
            total_delivered=2, sent_date=self.SENT_DATE, redirect_address=None
        )
        self.assertEqual(2, len(self.email_sent_metadata.send_results))

    def test_to_json(self) -> None:
        self.email_sent_metadata.add_new_email_send_result(
            total_delivered=4,
            redirect_address="letter@kenny.ca",
            sent_date=self.SENT_DATE,
        )

        json_dict = self.email_sent_metadata.to_json()
        sent_date_time = self.SENT_DATE.isoformat()
        expected_send_results = [
            {
                "sentDate": sent_date_time,
                "totalDelivered": 4,
                "redirectAddress": "letter@kenny.ca",
            }
        ]
        expected_result = {
            "batchId": "20210701202020",
            "sendResults": json.dumps(expected_send_results),
        }

        self.assertEqual(expected_result, json_dict)

    def test_build_from_gcs(self) -> None:
        email_metadata = EmailSentMetadata.build_from_gcs(
            batch_id="20210701202021", state_code=self.STATE_CODE_STR, gcs_fs=self.fs
        )
        self.assertEqual(
            EmailSentMetadata(batch_id="20210701202021", send_results=[]),
            email_metadata,
        )

    def test_write_to_gcs(self) -> None:
        batch_id = "20210701202022"
        email_metadata = EmailSentMetadata.build_from_gcs(
            state_code=self.STATE_CODE_STR, batch_id=batch_id, gcs_fs=self.fs
        )

        self.assertEqual(
            EmailSentMetadata(batch_id="20210701202022", send_results=[]),
            email_metadata,
        )
        email_metadata.add_new_email_send_result(
            sent_date=self.SENT_DATE,
            total_delivered=1,
            redirect_address="frida@kahlo.gov",
        )
        email_metadata.write_to_gcs(
            state_code=self.STATE_CODE_STR, batch_id=batch_id, gcs_fs=self.fs
        )

        gcs_path = gcsfs_path_for_batch_metadata(batch_id, self.STATE_CODE_STR)
        resulting_metadata = self.fs.get_metadata(gcs_path)

        expected_send_results = [
            {
                "sentDate": self.SENT_DATE.isoformat(),
                "totalDelivered": 1,
                "redirectAddress": "frida@kahlo.gov",
            }
        ]
        expected_result = {
            "batchId": batch_id,
            "sendResults": json.dumps(expected_send_results),
        }

        self.assertEqual(expected_result, resulting_metadata)

    def test_how_it_runs_if_list_batches_refreshed(self) -> None:
        batch_id = "20210701202023"
        email_metadata = EmailSentMetadata.build_from_gcs(
            state_code=self.STATE_CODE_STR, batch_id=batch_id, gcs_fs=self.fs
        )
        email_metadata.add_new_email_send_result(
            sent_date=self.SENT_DATE,
            total_delivered=1,
            redirect_address="frida@kahlo.gov",
        )
        email_metadata.write_to_gcs(
            state_code=self.STATE_CODE_STR, batch_id=batch_id, gcs_fs=self.fs
        )
        self.assertEqual(
            EmailSentMetadata(
                batch_id="20210701202023",
                send_results=[
                    EmailSentResult(
                        sent_date=self.SENT_DATE,
                        total_delivered=1,
                        redirect_address="frida@kahlo.gov",
                    )
                ],
            ),
            email_metadata,
        )

        json_dict = email_metadata.to_json()
        sent_date_time = self.SENT_DATE.isoformat()
        expected_send_results = [
            {
                "sentDate": sent_date_time,
                "totalDelivered": 1,
                "redirectAddress": "frida@kahlo.gov",
            }
        ]
        expected_result = {
            "batchId": "20210701202023",
            "sendResults": json.dumps(expected_send_results),
        }

        self.assertEqual(expected_result, json_dict)

    def test_how_add_new_and_refresh_used_x3(self) -> None:
        batch_id = "20210701202024"
        email_metadata = EmailSentMetadata.build_from_gcs(
            state_code=self.STATE_CODE_STR, batch_id=batch_id, gcs_fs=self.fs
        )
        email_metadata.add_new_email_send_result(
            sent_date=self.SENT_DATE,
            total_delivered=1,
            redirect_address="frida@kahlo.gov",
        )
        email_metadata.write_to_gcs(
            state_code=self.STATE_CODE_STR, batch_id=batch_id, gcs_fs=self.fs
        )

        # refreshing list batches in frontend
        json_dict = email_metadata.to_json()

        # sending same batch for the second time
        # this is the code layout in email_delivery.py when sending batch of emails
        second_sent_date_time = datetime.datetime.now()

        email_metadata = EmailSentMetadata.build_from_gcs(
            state_code=self.STATE_CODE_STR, batch_id=batch_id, gcs_fs=self.fs
        )
        email_metadata.add_new_email_send_result(
            sent_date=second_sent_date_time,
            total_delivered=5,
            redirect_address="letter@kenny.ca",
        )
        email_metadata.write_to_gcs(
            state_code=self.STATE_CODE_STR, batch_id=batch_id, gcs_fs=self.fs
        )

        # refreshing list batches in frontend for the second time
        json_dict = email_metadata.to_json()

        expected_send_results = [
            {
                "sentDate": self.SENT_DATE.isoformat(),
                "totalDelivered": 1,
                "redirectAddress": "frida@kahlo.gov",
            },
            {
                "sentDate": second_sent_date_time.isoformat(),
                "totalDelivered": 5,
                "redirectAddress": "letter@kenny.ca",
            },
        ]
        expected_result = {
            "batchId": "20210701202024",
            "sendResults": json.dumps(expected_send_results),
        }

        self.assertEqual(expected_result, json_dict)

        # sending same batch for the third time
        third_sent_date_time = datetime.datetime.now()

        email_metadata = EmailSentMetadata.build_from_gcs(
            state_code=self.STATE_CODE_STR, batch_id=batch_id, gcs_fs=self.fs
        )
        email_metadata.add_new_email_send_result(
            sent_date=third_sent_date_time,
            total_delivered=100,
            redirect_address=None,
        )
        email_metadata.write_to_gcs(
            state_code=self.STATE_CODE_STR, batch_id=batch_id, gcs_fs=self.fs
        )

        # refreshing list batches in frontend
        json_dict = email_metadata.to_json()

        expected_send_results = [
            {
                "sentDate": self.SENT_DATE.isoformat(),
                "totalDelivered": 1,
                "redirectAddress": "frida@kahlo.gov",
            },
            {
                "sentDate": second_sent_date_time.isoformat(),
                "totalDelivered": 5,
                "redirectAddress": "letter@kenny.ca",
            },
            {
                "sentDate": third_sent_date_time.isoformat(),
                "totalDelivered": 100,
                "redirectAddress": None,
            },
        ]
        expected_result = {
            "batchId": "20210701202024",
            "sendResults": json.dumps(expected_send_results),
        }

        self.assertEqual(expected_result, json_dict)
