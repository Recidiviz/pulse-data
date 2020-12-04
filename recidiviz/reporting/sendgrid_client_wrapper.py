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
"""An wrapper class for accessing the SendGridAPIClient"""

import logging
from typing import Optional, List

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Cc

from recidiviz.utils import secrets

_SENDGRID_API_KEY = 'sendgrid_api_key'


class SendGridClientWrapper:
    """Wrapper class for SendGridAPIClient"""

    def __init__(self) -> None:
        self.client = SendGridAPIClient(api_key=self._get_sendgrid_api_key())

    def _get_sendgrid_api_key(self) -> str:
        """Returns the value for the SendGrid API key from the secrets manager."""
        sendgrid_api_value = secrets.get_secret(_SENDGRID_API_KEY)
        if not sendgrid_api_value:
            raise ValueError(f"Could not get secret value for `Sendgrid API Key`. Provided with "
                             f"key={_SENDGRID_API_KEY}")
        return sendgrid_api_value

    def _create_message(self,
                        to_email: str,
                        from_email: str,
                        from_email_name: str,
                        subject: str,
                        html_content: str,
                        cc_addresses: Optional[List[str]] = None) -> Mail:
        """Creates the request body for the email that will be sent. Includes all required data to send a single email.

        If there are cc_addresses, it adds those to the request body for the emails.
        """
        message = Mail(to_emails=to_email,
                       from_email=self._create_email_address(from_email, from_email_name),
                       subject=subject,
                       html_content=html_content)
        if cc_addresses:
            message.cc = [Cc(email=cc_email_address) for cc_email_address in cc_addresses]

        return message

    @staticmethod
    def _create_email_address(from_email_address: str, from_email_name: str) -> Email:
        """Create an email address with a name to personalize emails."""
        return Email(from_email_address, from_email_name)

    def send_message(self,
                     to_email: str,
                     from_email: str,
                     from_email_name: str,
                     subject: str,
                     html_content: str,
                     redirect_address: Optional[str] = None,
                     cc_addresses: Optional[List[str]] = None) -> bool:
        """Sends the email to the provided address by making a Twilio SendGrid API request.

        If there is a redirect_address, it creates the message for the redirect address instead and updates the
        subject line to include to original recipient's email address.

        Args:
            to_email: The recipient's email address
            from_email: The sender's email address
            from_email_name: A personalized name for the sender to display in the email client
            subject: The email subject line
            html_content: An string with HTML content for the email body
            redirect_address: (Optional) An email address to which all emails will be sent
            instead of the to_email address.
            cc_addresses: (Optional) A list of email addresses to CC

        Returns:
            True if the message is sent successfully
            False if the response is not OK or an exception is thrown
        """
        if redirect_address:
            subject = f"[{to_email}] {subject}"
            to_email = redirect_address

        message = self._create_message(to_email=to_email,
                                       from_email=from_email,
                                       from_email_name=from_email_name,
                                       subject=subject,
                                       html_content=html_content,
                                       cc_addresses=cc_addresses)
        try:
            response = self.client.send(message)
        except Exception:
            logging.exception("Error sending the file created for %s", to_email)
            return False

        logging.info("Sent email to %s. Status code = %s", to_email, response.status_code)
        return True
