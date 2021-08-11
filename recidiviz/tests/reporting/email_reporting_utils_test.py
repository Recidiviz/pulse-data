# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Tests for email reporting utils."""
import datetime
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

import recidiviz.reporting.email_reporting_utils as utils
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.email_reporting_handler import EmailReportingHandler
from recidiviz.reporting.email_sent_metadata import EmailSentMetadata
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

_MOCK_PROJECT_ID = "RECIDIVIZ_TEST"


class EmailReportingUtilsTests(TestCase):
    """Tests for email_reporting_utils.py."""

    def setUp(self) -> None:
        self.eru = utils
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "fake-project"

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    @patch("os.environ.get")
    def test_get_env_var_happy_path(self, mock_environ_get: MagicMock) -> None:
        expected = "ANALYSIS"
        mock_environ_get.return_value = expected

        actual = self.eru.get_env_var("WILDLIFE")
        self.assertEqual(expected, actual)

    @patch("os.environ.get")
    def test_get_env_var_not_found(self, mock_environ_get: MagicMock) -> None:
        mock_environ_get.return_value = None

        with self.assertRaises(KeyError):
            self.eru.get_env_var("SIXTYTEN")

    @patch("recidiviz.utils.metadata.project_id", Mock(return_value=_MOCK_PROJECT_ID))
    def test_get_project_id(self) -> None:
        actual = self.eru.get_project_id()
        self.assertEqual(_MOCK_PROJECT_ID, actual)

    @patch("recidiviz.utils.secrets.get_secret")
    def test_get_cdn_static_ip(self, mock_secret: MagicMock) -> None:
        expected = "123.456.7.8"
        test_secrets = {"po_report_cdn_static_IP": expected}
        mock_secret.side_effect = test_secrets.get

        actual = self.eru.get_cdn_static_ip()
        self.assertEqual(expected, actual)

    @patch("recidiviz.utils.metadata.project_id", Mock(return_value=_MOCK_PROJECT_ID))
    def test_get_data_storage_bucket_name(self) -> None:
        expected = f"{_MOCK_PROJECT_ID}-report-data"
        actual = self.eru.get_data_storage_bucket_name()
        self.assertEqual(expected, actual)

    @patch("recidiviz.utils.metadata.project_id", Mock(return_value=_MOCK_PROJECT_ID))
    def test_get_data_archive_bucket_name(self) -> None:
        expected = f"{_MOCK_PROJECT_ID}-report-data-archive"
        actual = self.eru.get_data_archive_bucket_name()
        self.assertEqual(expected, actual)

    @patch("recidiviz.utils.metadata.project_id", Mock(return_value=_MOCK_PROJECT_ID))
    def test_get_email_content_bucket_name_html(self) -> None:
        expected = f"{_MOCK_PROJECT_ID}-report-html"
        actual = self.eru.get_email_content_bucket_name()
        self.assertEqual(expected, actual)

    @patch("recidiviz.utils.secrets.get_secret")
    def test_get_static_image_path(self, mock_secret: MagicMock) -> None:
        expected = "123.456.7.8"
        test_secrets = {"po_report_cdn_static_IP": expected}
        mock_secret.side_effect = test_secrets.get

        expected = "http://123.456.7.8/US_XX/top_opportunities/static"
        actual = self.eru.get_static_image_path(
            StateCode.US_XX, ReportType.TopOpportunities
        )
        self.assertEqual(expected, actual)

    def test_get_data_filename(self) -> None:
        expected = "po_monthly_report/US_XX/po_monthly_report_data.json"
        actual = self.eru.get_data_filename(StateCode.US_XX, ReportType.POMonthlyReport)
        self.assertEqual(expected, actual)

    def test_get_data_archive_filename(self) -> None:
        expected = "US_XX/batch-1.json"
        actual = self.eru.get_data_archive_filename("batch-1", StateCode.US_XX)
        self.assertEqual(expected, actual)

    def test_get_email_html_filename(self) -> None:
        expected = "US_XX/batch-1/html/boards@canada.ca.html"

        actual = self.eru.get_html_filename(
            "batch-1", "boards@canada.ca", StateCode.US_XX
        )
        self.assertEqual(expected, actual)

    def test_get_email_attachment_filename(self) -> None:
        expected = "US_XX/batch-1/attachments/boards@canada.ca.txt"
        actual = self.eru.get_attachment_filename(
            "batch-1", "boards@canada.ca", StateCode.US_XX
        )
        self.assertEqual(expected, actual)

    def test_format_test_address_valid(self) -> None:
        """Given a valid test_address and recipient_address, it returns the test address with the recipient
        email address name appended."""
        expected = "test+recipient@domain.com"
        actual = utils.format_test_address("test@domain.com", "recipient@id.us.gov")
        self.assertEqual(expected, actual)

    def test_format_test_address_invalid(self) -> None:
        """Given an invalid test address, it raises a ValueError."""
        with self.assertRaises(ValueError):
            utils.format_test_address("random string", "recipient@domain.com")

    def test_format_test_address_invalid_recipient(self) -> None:
        """Given an invalid recipient address, it raises a ValueError."""
        with self.assertRaises(ValueError):
            utils.format_test_address("test@domain.com", "invalid recipient address")

    def test_validate_email_address_valid(self) -> None:
        """Given a valid email address, it does not raise a ValueError."""
        utils.validate_email_address("dev@recidiviz.subdomain.org")
        utils.validate_email_address("testing-1234+@recidiviz.subdomain.org")

    def test_validate_email_address_invalid(self) -> None:
        """Given an invalid email address, it does raise a ValueError."""
        with self.assertRaises(ValueError):
            utils.validate_email_address("some random string @ fake domain")

    def test_validate_email_address_none_provided(self) -> None:
        """Given an empty argument, it does not raise a ValueError."""
        utils.validate_email_address(None)

    def test_validate_email_address_empty_string(self) -> None:
        """Given an empty string, it does raise a ValueError."""
        with self.assertRaises(ValueError):
            utils.validate_email_address("")


class TestGCSEmails(TestCase):
    """Class to test GCS actions for monthly report emails"""

    STATE_CODE_STR = "US_ID"
    STAGE_PROJECT_ID = "recidiviz-staging"
    PROD_PROJECT_ID = "recidiviz-123"
    BUCKET_NAME = f"{STAGE_PROJECT_ID}-report-html"

    def setUp(self) -> None:
        self.project_id_patcher = patch(
            "recidiviz.reporting.email_reporting_handler.metadata.project_id"
        )
        self.project_id_patcher.start().return_value = "recidiviz-staging"
        self.gcs_factory_patcher = patch(
            "recidiviz.reporting.email_reporting_handler.GcsfsFactory.build"
        )
        fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = fake_gcs
        self.fs = fake_gcs
        self.email_handler = EmailReportingHandler()

    def tearDown(self) -> None:
        self.gcs_factory_patcher.stop()
        self.project_id_patcher.stop()

    def _upload_fake_email_buckets(self) -> None:
        """
        Creates fake email buckets in the ID bucket.
        """
        # staging, US_ID buckets
        for x in range(3):
            batch_id = f"2021070120202{x}"
            path = GcsfsFilePath.from_absolute_path(
                f"gs://{self.BUCKET_NAME}/{self.STATE_CODE_STR}/{batch_id}/metadata.json"
            )
            self.fs.update_metadata(path=path, new_metadata={"batchId": batch_id})
            self.fs.upload_from_string(
                path=path,
                contents="this is an email",
                content_type="text/text",
            )

        # staging, US_PA bucket
        batch_id = "20210701202027"
        path = GcsfsFilePath.from_absolute_path(
            f"gs://{self.BUCKET_NAME}/US_PA/{batch_id}/metadata.json"
        )
        self.fs.update_metadata(path=path, new_metadata={"batchId": batch_id})
        self.fs.upload_from_string(
            path=path,
            contents="this is an email",
            content_type="text/text",
        )

        # production, US_PA bucket, decoy to make sure they are pulling from proper bucket
        bucket_name = f"{self.PROD_PROJECT_ID}-report-html"
        batch_id = "20210701202028"
        path = GcsfsFilePath.from_absolute_path(
            f"gs://{bucket_name}/US_PA/{batch_id}/metadata.json"
        )
        self.fs.update_metadata(path=path, new_metadata={"batchId": batch_id})
        self.fs.upload_from_string(
            path=path,
            contents="this is an email",
            content_type="text/text",
        )

    def test_get_batch_ids_valid_arguments(self) -> None:
        """Given all valid arguments, should have a list of batch ids, ordered in descending order,
        since we want the most recent batch to be at the top of the list"""
        self._upload_fake_email_buckets()
        batch_info = self.email_handler.get_batch_info(
            state_code=StateCode(self.STATE_CODE_STR)
        )
        batch_ids = [batch["batchId"] for batch in batch_info]
        self.assertEqual(
            ["20210701202022", "20210701202021", "20210701202020"],
            batch_ids,
        )

    def test_get_batch_ids_invalid_state(self) -> None:
        """Given an invalid state code, should have an empty list"""
        self._upload_fake_email_buckets()
        batch_info = self.email_handler.get_batch_info(state_code=StateCode.US_XX)
        self.assertEqual(0, len(batch_info))

    def test_get_batch_ids_state_with_single(self) -> None:
        """Given valid arguments, should pick correct state and given a list of only one batch id"""
        self._upload_fake_email_buckets()
        batch_info = self.email_handler.get_batch_info(state_code=StateCode.US_PA)
        batch_ids = [batch["batchId"] for batch in batch_info]
        self.assertEqual(["20210701202027"], batch_ids)

    def test_set_custom_email_metadata(self) -> None:
        """Tests what the start/deliver functions will be calling to set the is_sent custom metadata
        to False and then to True"""
        # get batch ids to be able to add custom metadata
        self._upload_fake_email_buckets()
        batch_info = self.email_handler.get_batch_info(
            state_code=StateCode(self.STATE_CODE_STR)
        )

        first_sent_timestamp = datetime.datetime.now()
        for metadata in batch_info:
            batch_id = metadata["batchId"]
            # initializing email_sent_metadata here since it is called like this when calling
            # add_new_email_send_result()
            email_sent_metadata = EmailSentMetadata.build_from_gcs(
                StateCode(self.STATE_CODE_STR), batch_id, self.fs
            )
            email_sent_metadata.add_new_email_send_result(
                total_delivered=4,
                redirect_address="letter@kenny.ca",
                sent_date=first_sent_timestamp,
            )
            email_sent_metadata.write_to_gcs(
                StateCode(self.STATE_CODE_STR), batch_id, self.fs
            )

        # get batch_info again to test
        batch_info = self.email_handler.get_batch_info(
            state_code=StateCode(self.STATE_CODE_STR)
        )
        self.assertEqual(
            [
                {
                    "batchId": "20210701202022",
                    "sendResults": [
                        {
                            "sentDate": first_sent_timestamp.isoformat(),
                            "totalDelivered": 4,
                            "redirectAddress": "letter@kenny.ca",
                        }
                    ],
                },
                {
                    "batchId": "20210701202021",
                    "sendResults": [
                        {
                            "sentDate": first_sent_timestamp.isoformat(),
                            "totalDelivered": 4,
                            "redirectAddress": "letter@kenny.ca",
                        }
                    ],
                },
                {
                    "batchId": "20210701202020",
                    "sendResults": [
                        {
                            "sentDate": first_sent_timestamp.isoformat(),
                            "totalDelivered": 4,
                            "redirectAddress": "letter@kenny.ca",
                        }
                    ],
                },
            ],
            batch_info,
        )

        # sending a batch for the second time
        second_sent_timestamp = datetime.datetime.now()
        email_sent_metadata = EmailSentMetadata.build_from_gcs(
            state_code=StateCode(self.STATE_CODE_STR),
            batch_id="20210701202022",
            gcs_fs=self.fs,
        )
        email_sent_metadata.add_new_email_send_result(
            total_delivered=1,
            redirect_address="frida@kahlo.gov",
            sent_date=second_sent_timestamp,
        )
        email_sent_metadata.write_to_gcs(
            state_code=StateCode(self.STATE_CODE_STR),
            batch_id="20210701202022",
            gcs_fs=self.fs,
        )
        # get batch_info again to test
        batch_info = self.email_handler.get_batch_info(
            state_code=StateCode(self.STATE_CODE_STR)
        )
        self.assertEqual(
            [
                {
                    "batchId": "20210701202022",
                    "sendResults": [
                        {
                            "sentDate": first_sent_timestamp.isoformat(),
                            "totalDelivered": 4,
                            "redirectAddress": "letter@kenny.ca",
                        },
                        {
                            "sentDate": second_sent_timestamp.isoformat(),
                            "totalDelivered": 1,
                            "redirectAddress": "frida@kahlo.gov",
                        },
                    ],
                },
                {
                    "batchId": "20210701202021",
                    "sendResults": [
                        {
                            "sentDate": first_sent_timestamp.isoformat(),
                            "totalDelivered": 4,
                            "redirectAddress": "letter@kenny.ca",
                        }
                    ],
                },
                {
                    "batchId": "20210701202020",
                    "sendResults": [
                        {
                            "sentDate": first_sent_timestamp.isoformat(),
                            "totalDelivered": 4,
                            "redirectAddress": "letter@kenny.ca",
                        }
                    ],
                },
            ],
            batch_info,
        )
