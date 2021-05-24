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
from unittest.mock import call, Mock, MagicMock
from unittest.mock import patch

from recidiviz.reporting.sendgrid_client_wrapper import SendGridClientWrapper


class SendGridClientWrapperTest(TestCase):
    """Tests for the wrapper class SendGridClientWrapper"""

    def setUp(self) -> None:
        HttpResponse = collections.namedtuple("HttpResponse", ["status_code"])
        self.error_response = HttpResponse(404)
        self.success_response = HttpResponse(202)
        self.client_patcher = patch(
            "recidiviz.reporting.sendgrid_client_wrapper.SendGridAPIClient"
        )
        self.secrets_patcher = patch(
            "recidiviz.reporting.sendgrid_client_wrapper.secrets"
        ).start()
        self.mail_helpers_patcher = patch(
            "recidiviz.reporting.sendgrid_client_wrapper.mail_helpers"
        )
        self.mock_mail_helpers = self.mail_helpers_patcher.start()
        self.mock_client = self.client_patcher.start().return_value
        self.secrets_patcher.get_secret.return_value = "secret"
        self.wrapper = SendGridClientWrapper()
        self.attachment_title = "2021-05 Recidiviz Monthly Report - Client Details.txt"

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.secrets_patcher.stop()
        self.mail_helpers_patcher.stop()

    def test_send_message(self) -> None:
        """Test that send_message sends the return from create_message client's send method and that the Mail helper
        is called with the right arguments. Test that it returns True on success."""
        to_email = "test@test.org"
        from_email = "<Recidiviz> dev@recidiviz.org"
        mail_message = "message"
        subject = "Your monthly Recidiviz Report"
        html_content = "<html></html>"
        self.mock_mail_helpers.Email.return_value = from_email
        self.mock_mail_helpers.Mail.return_value = mail_message
        self.mock_client.send.return_value = self.success_response
        with self.assertLogs(level="INFO"):
            response = self.wrapper.send_message(
                to_email=to_email,
                from_email="dev@recidiviz.org",
                subject=subject,
                html_content=html_content,
                attachment_title=self.attachment_title,
                from_email_name="Recidiviz",
            )
            self.assertTrue(response)

        self.mock_mail_helpers.CC.assert_not_called()
        self.mock_mail_helpers.Email.assert_called_with(
            "dev@recidiviz.org", "Recidiviz"
        )
        self.mock_mail_helpers.Mail.assert_called_with(
            to_emails=to_email,
            from_email=from_email,
            subject=subject,
            html_content=html_content,
        )
        self.mock_client.send.assert_called_with(mail_message)

    def test_send_message_with_cc(self) -> None:
        """Given cc_addresses, test that _create_message creates a list of Cc objects for the Mail message."""
        self.mock_client.send.return_value = self.success_response

        Mail = collections.namedtuple("Mail", [])
        mock_message = MagicMock(return_value=Mail())
        self.mock_mail_helpers.Mail.return_value = mock_message

        mock_parent = Mock()
        mock_parent.attach_mock(self.mock_mail_helpers.Cc, "Cc")
        cc_addresses = ["cc_address@one.org", "cc_address@two.org"]
        expected_calls = [call.Cc(email=cc_address) for cc_address in cc_addresses]

        self.wrapper.send_message(
            to_email="test@test.org",
            from_email="dev@recidiviz.org",
            subject="Your monthly Recidiviz Report",
            html_content="<html></html>",
            attachment_title=self.attachment_title,
            from_email_name="Recidiviz",
            cc_addresses=cc_addresses,
        )
        self.assertTrue(hasattr(mock_message, "cc"))
        self.assertEqual(len(mock_message.cc), 2)
        mock_parent.assert_has_calls(expected_calls)

    def test_send_message_exception(self) -> None:
        """Test that an error is logged when an exception is raised and it returns False"""
        self.mock_client.send.side_effect = Exception
        with self.assertLogs(level="ERROR"):
            response = self.wrapper.send_message(
                to_email="test@test.org",
                from_email="dev@recidiviz.org",
                subject="Your monthly Recidiviz Report",
                html_content="<html></html>",
                attachment_title=self.attachment_title,
                from_email_name="Recidiviz",
            )
            self.assertFalse(response)

    def test_send_message_with_redirect_address(self) -> None:
        """Given a redirect_address, test that _create_message is called with the correct to_email and subject line."""
        self.mock_mail_helpers.Email.return_value = "<Recidiviz> dev@recidiviz.org"
        self.mock_client.send.return_value = self.success_response
        redirect_address = "redirect@email.org"
        to_email = "test@test.org"
        subject = "Your monthly Recidiviz Report"
        with self.assertLogs(level="INFO"):
            self.wrapper.send_message(
                to_email=to_email,
                from_email="dev@recidiviz.org",
                subject=subject,
                html_content="<html></html>",
                attachment_title=self.attachment_title,
                from_email_name="Recidiviz",
                redirect_address=redirect_address,
            )

        self.mock_mail_helpers.Mail.assert_called_with(
            to_emails=redirect_address,
            from_email="<Recidiviz> dev@recidiviz.org",
            subject=f"[{to_email}] {subject}",
            html_content="<html></html>",
        )

    def test_send_message_with_text_attachment_content(self) -> None:
        """Given text_attachment_content, test that an attachment is created and attached to the outgoing message."""
        file_content = "Fake email attachment content"
        self.wrapper.send_message(
            to_email="test@test.org",
            from_email="dev@recidiviz.org",
            subject="Your monthly Recidiviz Report",
            html_content="<html></html>",
            attachment_title=self.attachment_title,
            from_email_name="Recidiviz",
            text_attachment_content=file_content,
        )
        self.mock_mail_helpers.Attachment.assert_called_with(
            file_content=self.mock_mail_helpers.FileContent(
                "Fake email attachment content"
            ),
            file_name=self.mock_mail_helpers.FileName(
                "2021-05 Recidiviz Monthly Report - Client Details.txt"
            ),
            file_type=self.mock_mail_helpers.FileType("text/plain"),
            disposition=self.mock_mail_helpers.Disposition("attachment"),
        )

    def test_send_message_with_text_attachment_content_none(self) -> None:
        """Given no text_attachment_content, assert that an attachment is not created."""
        self.wrapper.send_message(
            to_email="test@test.org",
            from_email="dev@recidiviz.org",
            subject="Your monthly Recidiviz Report",
            html_content="<html></html>",
            attachment_title=self.attachment_title,
            from_email_name="Recidiviz",
            text_attachment_content=None,
        )
        self.mock_mail_helpers.Attachment.assert_not_called()
