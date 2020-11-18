# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for the wrapper class SendGridClientWrapper"""
import collections
from unittest import TestCase

from unittest.mock import patch

from recidiviz.reporting.sendgrid_client_wrapper import SendGridClientWrapper


class SendGridClientWrapperTest(TestCase):
    """Tests for the wrapper class SendGridClientWrapper"""

    def setUp(self) -> None:
        HttpResponse = collections.namedtuple('Response', ['status_code'])
        self.error_response = HttpResponse(404)
        self.success_response = HttpResponse(202)
        self.client_patcher = patch('recidiviz.reporting.sendgrid_client_wrapper.SendGridAPIClient')
        self.secrets_patcher = patch('recidiviz.reporting.sendgrid_client_wrapper.secrets').start()
        self.mail_patcher = patch('recidiviz.reporting.sendgrid_client_wrapper.Mail')
        self.email_patcher = patch('recidiviz.reporting.sendgrid_client_wrapper.Email')
        self.mock_client = self.client_patcher.start().return_value
        self.mock_mail = self.mail_patcher.start()
        self.mock_email = self.email_patcher.start()
        self.secrets_patcher.get_secret.return_value = 'secret'
        self.wrapper = SendGridClientWrapper()

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.secrets_patcher.stop()
        self.mail_patcher.stop()
        self.email_patcher.stop()


    def test_send_message(self) -> None:
        """Test that send_message sends the return from create_message client's send method and that the Mail helper
        is called with the right arguments. Test that it returns True on success."""
        to_email = 'test@test.org'
        from_email = '<Recidiviz> dev@recidiviz.org'
        mail_message = 'message'
        subject = 'Your monthly Recidiviz Report'
        html_content = '<html></html>'
        self.mock_email.return_value = from_email
        self.mock_mail.return_value = mail_message
        self.mock_client.send.return_value = self.success_response
        args = {
            'to_email': to_email,
            'from_email': 'dev@recidiviz.org',
            'subject': subject,
            'from_email_name': 'Recidiviz',
            'html_content': html_content
        }

        with self.assertLogs(level='INFO'):
            response = self.wrapper.send_message(**args)
            self.assertTrue(response)

        self.mock_email.assert_called_with('dev@recidiviz.org', 'Recidiviz')
        self.mock_mail.assert_called_with(to_emails=to_email,
                                          from_email=from_email,
                                          subject=subject,
                                          html_content=html_content)
        self.mock_client.send.assert_called_with(mail_message)

    def test_send_message_exception(self) -> None:
        """Test that an error is logged when an exception is raised and it returns False"""
        self.mock_client.send.side_effect = Exception
        args = {
            'to_email': 'test@test.org',
            'from_email': 'dev@recidiviz.org',
            'subject': 'Your monthly Recidiviz Report',
            'from_email_name': 'Recidiviz',
            'html_content': '<html></html>'
        }
        with self.assertLogs(level='ERROR'):
            response = self.wrapper.send_message(**args)
            self.assertFalse(response)

    def test_send_message_with_redirect_address(self) -> None:
        """Given a redirect_address, test that _create_message is called with the correct to_email and subject line."""
        self.mock_email.return_value = '<Recidiviz> dev@recidiviz.org'
        self.mock_client.send.return_value = self.success_response
        redirect_address = 'redirect@email.org'
        to_email = 'test@test.org'
        subject = 'Your monthly Recidiviz Report'
        args = {
            'to_email': to_email,
            'from_email': 'dev@recidiviz.org',
            'subject': subject,
            'from_email_name': 'Recidiviz',
            'html_content': '<html></html>',
            'redirect_address': redirect_address
        }
        with self.assertLogs(level='INFO'):
            self.wrapper.send_message(**args)

        self.mock_mail.assert_called_with(to_emails=redirect_address,
                                          from_email='<Recidiviz> dev@recidiviz.org',
                                          subject=f'[{to_email}] {subject}',
                                          html_content='<html></html>')
