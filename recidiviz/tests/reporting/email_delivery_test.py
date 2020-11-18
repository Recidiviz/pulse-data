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
import collections
from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock

from recidiviz.reporting import email_delivery


class EmailDeliveryTest(TestCase):
    """Tests for reporting/email_delivery.py."""

    def setUp(self) -> None:
        self.batch_id = '20201113101030'
        self.to_address = 'tester_123@one.domain.org'
        self.redirect_address = 'redirect@test.org'
        self.mock_html_files = {
            f'{self.to_address}': '<html><body></html>'
        }

        self.sendgrid_client_patcher = patch('recidiviz.reporting.email_delivery.SendGridClientWrapper')
        self.mock_sendgrid_client = self.sendgrid_client_patcher.start().return_value

        self.utils_patcher = patch('recidiviz.reporting.email_delivery.utils')
        self.mock_utils = self.utils_patcher.start()
        self.mock_env_vars = {
            'FROM_EMAIL_ADDRESS': 'dev@recidiviz.org',
            'FROM_EMAIL_NAME': 'Recidiviz'
        }
        self.mock_utils.get_env_var.side_effect = self.mock_env_vars.get

    def tearDown(self) -> None:
        self.sendgrid_client_patcher.stop()
        self.utils_patcher.stop()

    def test_email_from_blob_name(self) -> None:
        blob_name = f"20201113143523/{self.to_address}.html"
        self.assertEqual(self.to_address, email_delivery.email_from_blob_name(blob_name))

    @patch('recidiviz.reporting.email_delivery.retrieve_html_files')
    def test_deliver_returns_success_count(self, mock_retrieve_html_files: MagicMock) -> None:
        """Given a batch_id, test that the deliver returns the success_count and fail_count values"""
        mock_retrieve_html_files.return_value = self.mock_html_files
        self.mock_sendgrid_client.send_message.return_value = True
        with self.assertLogs(level='INFO'):
            [success_count, fail_count] = email_delivery.deliver(self.batch_id)

        self.assertEqual(success_count, 1)
        self.assertEqual(fail_count, 0)

    @patch('recidiviz.reporting.email_delivery.retrieve_html_files')
    def test_deliver_returns_fail_count(self, mock_retrieve_html_files: MagicMock) -> None:
        """Given a batch_id, test that the deliver returns the fail_count value when it fails"""
        mock_retrieve_html_files.return_value = self.mock_html_files
        self.mock_sendgrid_client.send_message.return_value = False
        [success_count, fail_count] = email_delivery.deliver(self.batch_id)

        self.assertEqual(success_count, 0)
        self.assertEqual(fail_count, 1)

    @patch('recidiviz.reporting.email_delivery.retrieve_html_files')
    def test_deliver_calls_send_message(self, mock_retrieve_html_files: MagicMock) -> None:
        """Given a batch_id, test that the SendGridClientWrapper send_message is called with
        the data it needs to send an email.
        """
        mock_retrieve_html_files.return_value = self.mock_html_files
        with self.assertLogs(level='INFO'):
            email_delivery.deliver(self.batch_id)

        mock_retrieve_html_files.assert_called_with(self.batch_id)
        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.mock_env_vars['FROM_EMAIL_ADDRESS'],
            from_email_name=self.mock_env_vars['FROM_EMAIL_NAME'],
            subject='Your monthly Recidiviz report',
            html_content=self.mock_html_files[self.to_address],
            redirect_address=None
        )

    @patch('recidiviz.reporting.email_delivery.retrieve_html_files')
    def test_deliver_with_redirect_address(self, mock_retrieve_html_files: MagicMock) -> None:
        """Given a batch_id and a redirect_address, test that the SendGridClientWrapper send_message is called with
        the redirect_address."""
        mock_retrieve_html_files.return_value = self.mock_html_files
        with self.assertLogs(level='INFO'):
            email_delivery.deliver(self.batch_id, self.redirect_address)

        mock_retrieve_html_files.assert_called_with(self.batch_id)
        self.mock_sendgrid_client.send_message.assert_called_with(
            to_email=self.to_address,
            from_email=self.mock_env_vars['FROM_EMAIL_ADDRESS'],
            from_email_name=self.mock_env_vars['FROM_EMAIL_NAME'],
            subject='Your monthly Recidiviz report',
            html_content=self.mock_html_files[self.to_address],
            redirect_address=self.redirect_address
        )

    @patch('recidiviz.reporting.email_delivery.retrieve_html_files')
    def test_deliver_with_redirect_address_fails(self, mock_retrieve_html_files: MagicMock) -> None:
        """Given a batch_id and a redirect_address, test the fail count increases if redirect_address fails to send."""
        mock_retrieve_html_files.return_value = self.mock_html_files
        self.mock_sendgrid_client.send_message.return_value = False
        [success_count, fail_count] = email_delivery.deliver(self.batch_id, self.redirect_address)
        self.assertEqual(fail_count, 1)
        self.assertEqual(success_count, 0)

    @patch('recidiviz.reporting.email_delivery.retrieve_html_files')
    def test_deliver_fails_missing_env_vars(self, mock_retrieve_html_files: MagicMock) -> None:
        """Given a batch_id and a redirect_address, test that the SendGridClientWrapper send_message is called with
        the redirect_address, and the to address is included in the subject."""
        self.mock_utils.get_env_var.side_effect = KeyError
        with self.assertRaises(KeyError), self.assertLogs(level='ERROR'):
            email_delivery.deliver(self.batch_id)

        mock_retrieve_html_files.assert_not_called()
        self.mock_sendgrid_client.send_message.assert_not_called()

    @patch('recidiviz.utils.metadata.project_id', Mock(return_value='test-project'))
    @patch('recidiviz.reporting.email_delivery.storage')
    def test_retrieve_html_files(self, mock_cloud_storage: MagicMock) -> None:
        """Test that retrieve_html_files calls the Google storage client with the correct arguments.
        TODO(#4456): Update this test when updated to use DirectIngestGCSFileSystem
        """
        Blob = collections.namedtuple('Blob', ['name'])
        blob_name = f'{self.batch_id}/{self.to_address}.html'
        mock_cloud_storage.Client.return_value = mock_cloud_storage
        mock_cloud_storage.list_blobs.return_value = [Blob(blob_name)]
        self.mock_utils.get_html_bucket_name.return_value = 'bucket-name'
        self.mock_utils.load_string_from_storage.return_value = '<html>'

        files = email_delivery.retrieve_html_files(self.batch_id)

        mock_cloud_storage.list_blobs.assert_called_with('bucket-name', prefix=self.batch_id)
        self.mock_utils.load_string_from_storage.assert_called_once_with('bucket-name', blob_name)
        self.assertEqual(files, {f'{self.to_address}': '<html>'})

    @patch('recidiviz.utils.metadata.project_id', Mock(return_value='test-project'))
    @patch('recidiviz.reporting.email_delivery.storage')
    def test_retrieve_html_files_fails(self, mock_cloud_storage: MagicMock) -> None:
        """Test that retrieve_html_files calls the Google storage client with the correct arguments.
        TODO(#4456): Update this test when updated to use DirectIngestGCSFileSystem
        """
        bucket_name = 'bucket-name'
        self.mock_utils.get_html_bucket_name.return_value = bucket_name
        mock_cloud_storage.Client.return_value = mock_cloud_storage
        mock_cloud_storage.list_blobs.return_value = []

        with self.assertRaises(IndexError):
            email_delivery.retrieve_html_files(self.batch_id)
