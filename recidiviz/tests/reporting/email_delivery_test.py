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
from unittest.mock import MagicMock, Mock, call, patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.reporting import email_delivery
from recidiviz.reporting.constants import ReportType
from recidiviz.reporting.context.outliers_supervision_officer_supervisor.constants import (
    ADDITIONAL_EMAIL_ADDRESSES_KEY,
    SUBJECT_LINE_KEY,
)
from recidiviz.reporting.email_reporting_utils import Batch
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class EmailDeliveryTest(TestCase):
    """Tests for reporting/email_delivery.py."""

    def setUp(self) -> None:
        self.batch_id = "20201113101030"
        self.bucket_name = "my-bucket"
        self.to_address = "tester_123@one.domain.org"
        self.redirect_address = "redirect@test.org"
        self.mock_files = {f"{self.to_address}": "<html><body></html>"}
        self.state_code = StateCode.US_XX
        self.attachment_title = "2021-05 Recidiviz Monthly Report - Client Details.txt"
        self.batch = Batch(
            state_code=self.state_code,
            batch_id=self.batch_id,
            report_type=ReportType.OutliersSupervisionOfficerSupervisor,
        )

        self.utils_patcher = patch("recidiviz.reporting.email_delivery.utils")
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.gcs_factory_patcher = patch(
            "recidiviz.reporting.email_reporting_handler.GcsfsFactory.build"
        )
        self.mock_utils = self.utils_patcher.start()
        self.project_id_patcher.start().return_value = "recidiviz-test"
        fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = fake_gcs
        self.mock_utils.get_email_content_bucket_name.return_value = self.bucket_name
        self.mock_utils.get_html_folder.return_value = "my-html-folder"
        self.mock_utils.get_attachments_folder.return_value = "my-attachments-folder"

        self.default_sender_email = "reports@recidiviz.org"
        self.default_sender_name = "Recidiviz Alerts"
        self.default_reply_to_email = "feedback@recidiviz.org"
        self.default_reply_to_name = "Recidiviz Support"

    def tearDown(self) -> None:
        self.utils_patcher.stop()
        self.project_id_patcher.stop()
        self.gcs_factory_patcher.stop()


class EmailDeliveryTestSendgridAutospec(EmailDeliveryTest):
    """Tests for reporting/email_delivery.py."""

    @patch("recidiviz.reporting.email_delivery.SendGridClientWrapper", autospec=True)
    @patch("recidiviz.reporting.email_delivery.get_enforced_tls_only_state_codes")
    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_unsupported_tls(
        self,
        mock_load_files_from_storage: MagicMock,
        mock_enforced_tls_only_states: MagicMock,
        mock_sendgrid_client_wrapper: MagicMock,
    ) -> None:
        mock_load_files_from_storage.return_value = self.mock_files
        mock_enforced_tls_only_states.return_value = [StateCode.US_XX]
        mock_sendgrid_client_wrapper.send_message.return_value = True

        result = email_delivery.deliver(
            self.batch,
        )

        self.assertEqual(len(result.successes), 1)
        self.assertEqual(len(result.failures), 0)
        mock_sendgrid_client_wrapper.assert_called_once_with("enforced_tls_only")

    @patch("recidiviz.reporting.email_delivery.SendGridClientWrapper", autospec=True)
    @patch("recidiviz.reporting.email_delivery.get_enforced_tls_only_state_codes")
    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_enforced_tls(
        self,
        mock_load_files_from_storage: MagicMock,
        mock_enforced_tls_only_states: MagicMock,
        mock_sendgrid_client_wrapper: MagicMock,
    ) -> None:
        mock_load_files_from_storage.return_value = self.mock_files
        mock_enforced_tls_only_states.return_value = []
        mock_sendgrid_client_wrapper.send_message.return_value = True
        with self.assertLogs(level="INFO"):
            result = email_delivery.deliver(
                self.batch,
            )

        self.assertEqual(len(result.successes), 1)
        self.assertEqual(len(result.failures), 0)
        mock_sendgrid_client_wrapper.assert_called_once_with()


class EmailDeliveryTestWithSendgridSetup(EmailDeliveryTest):
    """Tests for reporting/email_delivery.py."""

    def setUp(self) -> None:
        self.sendgrid_client_patcher = patch(
            "recidiviz.reporting.email_delivery.SendGridClientWrapper"
        )
        self.mock_sendgrid_client = self.sendgrid_client_patcher.start().return_value

        super().setUp()

    def tearDown(self) -> None:
        self.sendgrid_client_patcher.stop()
        super().tearDown()

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
            result = email_delivery.deliver(
                self.batch,
            )

        self.assertEqual(len(result.successes), 1)
        self.assertEqual(len(result.failures), 0)

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_returns_fail_count(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id, test that the deliver returns the fail_count value when it fails"""
        mock_load_files_from_storage.return_value = self.mock_files
        self.mock_sendgrid_client.send_message.return_value = False
        result = email_delivery.deliver(
            self.batch,
        )

        self.assertEqual(len(result.successes), 0)
        self.assertEqual(len(result.failures), 1)

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_returns_fail_count_for_invalid_email(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id, test that the deliver returns the fail_count value when it fails for invalid email"""
        self.utils_patcher.stop()  # Don't mock validate_email_adddress()
        mock_load_files_from_storage.return_value = {"''": "<html><body></html>"}
        result = email_delivery.deliver(
            self.batch,
        )

        self.assertEqual(len(result.successes), 0)
        self.assertEqual(len(result.failures), 1)

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_returns_fail_count_for_send_exception(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id, test that the deliver returns the fail_count value when it fails for Exception on send"""
        mock_load_files_from_storage.return_value = {"''": "<html><body></html>"}
        self.mock_sendgrid_client.send_message.side_effect = Exception
        result = email_delivery.deliver(
            self.batch,
        )

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
            email_delivery.deliver(
                self.batch,
            )

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_no_attachments(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """When emails exist, but no attachments exist, an attachment is not delivered to the recipients"""

        def fake_load_files(_bucket: str, prefix: str) -> Dict[str, str]:
            if "attachments" not in prefix:
                return self.mock_files

            return {}

        mock_load_files_from_storage.side_effect = fake_load_files

        with self.assertLogs(level="INFO"):
            email_delivery.deliver(
                self.batch,
            )

        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.default_sender_email,
            from_email_name=self.default_sender_name,
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=None,
            disable_unsubscribe=True,
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
            email_delivery.deliver(
                self.batch,
            )

        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.default_sender_email,
            from_email_name=self.default_sender_name,
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
            disable_unsubscribe=True,
        )

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_invalid_email_does_not_call_send(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id, test that the SendGridClientWrapper send_message is not called because
        the email from the loaded files is an empty string, which is invalid.
        """
        self.utils_patcher.stop()

        mock_load_files_from_storage.return_value = {"''": "<html><body></html>"}
        with self.assertLogs(level="INFO"):
            email_delivery.deliver(
                self.batch,
            )

        self.mock_sendgrid_client.send_message.assert_not_called()

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    @patch(
        "recidiviz.reporting.email_delivery.get_file_metadata",
        Mock(
            return_value={
                ADDITIONAL_EMAIL_ADDRESSES_KEY: '["additional@recidiviz.org"]'
            }
        ),
    )
    def test_deliver_calls_send_message_with_additional_addresses(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id, test that the SendGridClientWrapper send_message is called with
        the data it needs to send an email to the recipient with the additional address from metadata CC'd
        """
        mock_load_files_from_storage.return_value = self.mock_files

        with self.assertLogs(level="INFO"):
            email_delivery.deliver(
                self.batch,
            )

        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.default_sender_email,
            from_email_name=self.default_sender_name,
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=["additional@recidiviz.org"],
            text_attachment_content=self.mock_files[self.to_address],
            disable_unsubscribe=True,
        )

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    @patch(
        "recidiviz.reporting.email_delivery.get_file_metadata",
        Mock(
            return_value={
                ADDITIONAL_EMAIL_ADDRESSES_KEY: '["additional@recidiviz.org"]'
            }
        ),
    )
    def test_deliver_calls_send_message_with_additional_addresses_no_cc(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id, test that the SendGridClientWrapper send_message is called with
        the data it needs to send an email to the recipient but do not CC because there is a redirect address.
        """
        mock_load_files_from_storage.return_value = self.mock_files

        with self.assertLogs(level="INFO"):
            email_delivery.deliver(
                self.batch, redirect_address="redirect@recidiviz.org"
            )

        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.default_sender_email,
            from_email_name=self.default_sender_name,
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address="redirect@recidiviz.org",
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
            disable_unsubscribe=True,
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
                batch=self.batch,
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
            from_email=self.default_sender_email,
            from_email_name=self.default_sender_name,
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=self.redirect_address,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
            disable_unsubscribe=True,
        )

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_with_redirect_address_fails(
        self, mock_load_files_from_storage: MagicMock
    ) -> None:
        """Given a batch_id and a redirect_address, test the fail count increases if redirect_address fails to send."""
        mock_load_files_from_storage.return_value = self.mock_files
        self.mock_sendgrid_client.send_message.return_value = False
        result = email_delivery.deliver(
            self.batch,
            self.redirect_address,
        )
        self.assertEqual(len(result.failures), 1)
        self.assertEqual(len(result.successes), 0)

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
                batch=self.batch,
                email_allowlist=[self.to_address],
            )
        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.default_sender_email,
            from_email_name=self.default_sender_name,
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
            disable_unsubscribe=True,
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

    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_sender_config(self, mock_load_files_from_storage: MagicMock) -> None:
        mock_load_files_from_storage.return_value = self.mock_files

        with self.assertLogs(level="INFO"):
            email_delivery.deliver(
                self.batch,
            )

        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.default_sender_email,
            from_email_name="Recidiviz Alerts",
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            disable_unsubscribe=True,
        )

    @patch(
        "recidiviz.reporting.email_delivery.get_file_metadata",
    )
    @patch("recidiviz.reporting.email_delivery.load_files_from_storage")
    def test_deliver_with_subject(
        self, mock_load_files_from_storage: MagicMock, mock_get_file_metadata: MagicMock
    ) -> None:
        mock_load_files_from_storage.return_value = self.mock_files

        with self.assertLogs(level="INFO"):
            email_delivery.deliver(self.batch, subject_override="Test subject override")

        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.default_sender_email,
            from_email_name="Recidiviz Alerts",
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            subject="Test subject override",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
            disable_unsubscribe=True,
        )

        # subject stored with file
        mock_get_file_metadata.return_value = {
            SUBJECT_LINE_KEY: "Subject from metadata"
        }
        with self.assertLogs(level="INFO"):
            email_delivery.deliver(self.batch)

        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.default_sender_email,
            from_email_name="Recidiviz Alerts",
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            subject="Subject from metadata",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
            disable_unsubscribe=True,
        )

        # default
        mock_get_file_metadata.return_value = {
            "some other key": "Subject from metadata"
        }
        with self.assertLogs(level="INFO"):
            email_delivery.deliver(self.batch)
        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.default_sender_email,
            from_email_name="Recidiviz Alerts",
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
            disable_unsubscribe=True,
        )

        mock_get_file_metadata.return_value = None
        with self.assertLogs(level="INFO"):
            email_delivery.deliver(self.batch)
        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.default_sender_email,
            from_email_name="Recidiviz Alerts",
            reply_to_email=self.default_reply_to_email,
            reply_to_name=self.default_reply_to_name,
            subject="Your monthly Recidiviz report",
            html_content=self.mock_files[self.to_address],
            redirect_address=None,
            cc_addresses=None,
            text_attachment_content=self.mock_files[self.to_address],
            disable_unsubscribe=True,
        )
