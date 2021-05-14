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

"""Tests for reporting/email_delivery.py."""
from typing import Dict
from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock, call

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.reporting import email_delivery
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class EmailDeliveryTest(TestCase):
    """Tests for reporting/email_delivery.py."""

    def setUp(self) -> None:
        self.batch_id = "20201113101030"
        self.bucket_name = "my-bucket"
        self.to_address = "tester_123@one.domain.org"
        self.redirect_address = "redirect@test.org"
        self.mock_files = {f"{self.to_address}": "<html><body></html>"}
        self.state_code = "US_XX"

        self.sendgrid_client_patcher = patch(
            "recidiviz.reporting.email_delivery.SendGridClientWrapper"
        )
        self.mock_sendgrid_client = self.sendgrid_client_patcher.start().return_value

        self.utils_patcher = patch("recidiviz.reporting.email_delivery.utils")
        self.mock_utils = self.utils_patcher.start()
        self.mock_utils.get_email_content_bucket_name.return_value = self.bucket_name
        self.mock_utils.get_html_folder.return_value = "my-html-folder"
        self.mock_utils.get_attachments_folder.return_value = "my-attachments-folder"

        self.mock_env_vars = {
            "FROM_EMAIL_ADDRESS": "dev@recidiviz.org",
            "FROM_EMAIL_NAME": "Recidiviz",
        }
        self.mock_utils.get_env_var.side_effect = self.mock_env_vars.get

    def tearDown(self) -> None:
        self.sendgrid_client_patcher.stop()
        self.utils_patcher.stop()

    def test_email_from_file_name_html(self) -> None:
        file_name = f"{self.to_address}.html"
        self.assertEqual(
            self.to_address, email_delivery.email_from_file_name(file_name)
        )

    def test_email_from_file_name_txt(self) -> None:
        file_name = f"{self.to_address}.txt"
        self.assertEqual(
            self.to_address, email_delivery.email_from_file_name(file_name)
        )

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_returns_success_count(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id, test that the deliver returns the success_count and fail_count values"""
        mock_load_files_from_storage.return_value = self.mock_files
        self.mock_sendgrid_client.send_message.return_value = True
        with self.assertLogs(level="INFO"):
            result = email_delivery.deliver(self.batch_id, self.state_code)

        self.assertEqual(len(result.successes), 1)
        self.assertEqual(len(result.failures), 0)

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_returns_fail_count(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id, test that the deliver returns the fail_count value when it fails"""
        mock_load_files_from_storage.return_value = self.mock_files
        self.mock_sendgrid_client.send_message.return_value = False
        result = email_delivery.deliver(self.batch_id, self.state_code)

        self.assertEqual(len(result.successes), 0)
        self.assertEqual(len(result.failures), 1)

    @patch("recidiviz.utils.metadata.project_id", Mock(return_value="test-project"))
    @patch(
        "recidiviz.reporting.email_delivery.load_files_from_storage",
        Mock(return_value={}),
    )
    def test_deliver_with_no_files_fails(self) -> None:
        """Test that load_files_from_storage raises an IndexError when there are no files to retrieve"""
        bucket_name = "bucket-name"
        self.mock_utils.get_email_content_bucket_name.return_value = bucket_name

        with self.assertRaises(IndexError):
            email_delivery.deliver(self.batch_id, self.state_code)

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_no_attachments(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """ When emails exist, but no attachments exist, an attachment is not delivered to the recipients """

        def fake_load_files(_bucket: str, prefix: str) -> Dict[str, str]:
            if "attachments" not in prefix:
                return self.mock_files

            return {}

        mock_load_files_from_storage.side_effect = fake_load_files

        with self.assertLogs(level="INFO"):
            email_delivery.deliver(self.batch_id, self.state_code)

        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.mock_env_vars["FROM_EMAIL_ADDRESS"],
            from_email_name=self.mock_env_vars["FROM_EMAIL_NAME"],
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=None,
        )

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_calls_send_message(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id, test that the SendGridClientWrapper send_message is called with
        the data it needs to send an email.
        """
        mock_load_files_from_storage.return_value = self.mock_files
        with self.assertLogs(level="INFO"):
            email_delivery.deliver(self.batch_id, self.state_code)

        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.mock_env_vars["FROM_EMAIL_ADDRESS"],
            from_email_name=self.mock_env_vars["FROM_EMAIL_NAME"],
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
        )

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_with_redirect_address(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id and a redirect_address, test that the SendGridClientWrapper send_message is called with
        the redirect_address."""
        mock_load_files_from_storage.return_value = self.mock_files

        with self.assertLogs(level="INFO"):
            email_delivery.deliver(
                batch_id=self.batch_id,
                state_code=self.state_code,
                redirect_address=self.redirect_address,
            )

        mock_load_files_from_storage.assert_has_calls(
            [
                call(self.bucket_name, "my-html-folder"),
                call(self.bucket_name, "my-attachments-folder"),
            ]
        )

        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.mock_env_vars["FROM_EMAIL_ADDRESS"],
            from_email_name=self.mock_env_vars["FROM_EMAIL_NAME"],
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=self.redirect_address,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
        )

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_with_redirect_address_fails(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id and a redirect_address, test the fail count increases if redirect_address fails to send."""
        mock_load_files_from_storage.return_value = self.mock_files
        self.mock_sendgrid_client.send_message.return_value = False
        result = email_delivery.deliver(self.batch_id, self.redirect_address)
        self.assertEqual(len(result.failures), 1)
        self.assertEqual(len(result.successes), 0)

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_fails_missing_env_vars(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id and a redirect_address, test that the SendGridClientWrapper send_message is called with
        the redirect_address, and the to address is included in the subject."""
        self.mock_utils.get_env_var.side_effect = KeyError
        with self.assertRaises(KeyError), self.assertLogs(level="ERROR"):
            email_delivery.deliver(self.batch_id, self.state_code)

        mock_load_files_from_storage.assert_not_called()
        self.mock_sendgrid_client.send_message.assert_not_called()

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_correctly_with_email_allowlist(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id and an email allowlist, test that the SendGridClientWrapper send_message is only called with
        allowlisted to address."""
        self.mock_files.update({"non_sending@one.domain.org": "<html><body></html>"})
        mock_load_files_from_storage.return_value = self.mock_files
        self.mock_sendgrid_client.send_message.return_value = True
        with self.assertLogs(level="INFO"):
            result = email_delivery.deliver(
                batch_id=self.batch_id,
                state_code=self.state_code,
                email_allowlist=[self.to_address],
            )
        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.mock_env_vars["FROM_EMAIL_ADDRESS"],
            from_email_name=self.mock_env_vars["FROM_EMAIL_NAME"],
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
        )
        self.assertEqual(len(result.successes), 1)
        self.assertEqual(len(result.failures), 0)
        self.assertNotIn("non_sending@one.domain.org", result.successes)

    @patch("recidiviz.utils.metadata.project_id", Mock(return_value="test-project"))
    @patch("recidiviz.reporting.email_delivery.GcsfsFactory.build")
    def test_load_files_from_storage(self, mock_gcs_factory: MagicMock) -> None:
        """Test that load_files_from_storage returns files for the current batch and bucket name"""
        bucket_name = "bucket-name"
        self.mock_utils.get_email_content_bucket_name.return_value = bucket_name

        email_path = GcsfsFilePath.from_absolute_path(
            f"gs://{bucket_name}/{self.state_code}/{self.batch_id}/{self.to_address}.html"
        )
        other_path = GcsfsFilePath.from_absolute_path(
            f"gs://{bucket_name}/excluded/exclude.json"
        )

        fake_gcs_file_system = FakeGCSFileSystem()
        fake_gcs_file_system.upload_from_string(
            path=email_path, contents="<html>", content_type="text/html"
        )
        fake_gcs_file_system.upload_from_string(
            path=other_path, contents="{}", content_type="text/json"
        )

        mock_gcs_factory.return_value = fake_gcs_file_system

        files = email_delivery.load_files_from_storage(
            bucket_name, f"{self.state_code}/{self.batch_id}"
        )

        self.assertEqual(files, {f"{self.to_address}": "<html>"})
