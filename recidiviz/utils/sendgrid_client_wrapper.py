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
"""An wrapper class for accessing the SendGridAPIClient for sending emails to external
(state user) email addresses."""
import base64
import logging
from typing import List, Optional

from sendgrid import SendGridAPIClient
from sendgrid.helpers import mail as mail_helpers

from recidiviz.common.constants.states import StateCode
from recidiviz.utils import secrets

_JC_SENDGRID_API_KEY = "justice_counts_sendgrid_api_key"
_SENDGRID_ENFORCED_TLS_AND_CERT_API_KEY = "sendgrid_enforced_tls_and_cert_api_key"
_SENDGRID_ENFORCED_TLS_API_KEY = "sendgrid_enforced_tls_api_key"

_STATE_CODES_WITH_ENFORCED_TLS_ONLY: List[StateCode] = [
    StateCode.US_ID,
    StateCode.US_IX,
]


def get_enforced_tls_only_state_codes() -> List[StateCode]:
    """Returns a list of states where only TLS is required, but not a valid certificate"""
    return _STATE_CODES_WITH_ENFORCED_TLS_ONLY


class SendGridClientWrapper:
    """Wrapper class for SendGridAPIClient"""

    def __init__(self, key_type: Optional[str] = None) -> None:
        if key_type == "justice_counts":
            api_key = self._get_sendgrid_api_key(secret_id=_JC_SENDGRID_API_KEY)
        elif key_type == "enforced_tls_only":
            api_key = self._get_sendgrid_api_key(
                secret_id=_SENDGRID_ENFORCED_TLS_API_KEY
            )
        else:
            api_key = self._get_sendgrid_api_key(
                secret_id=_SENDGRID_ENFORCED_TLS_AND_CERT_API_KEY
            )

        self.client = SendGridAPIClient(api_key=api_key)

    def _get_sendgrid_api_key(self, secret_id: str) -> str:
        """Returns the value for the SendGrid API key from the secrets manager."""
        sendgrid_api_value = secrets.get_secret(secret_id)
        if not sendgrid_api_value:
            raise ValueError(
                f"Could not get secret value for `Sendgrid API Key`. Provided with "
                f"key={secret_id}"
            )
        return sendgrid_api_value

    def _create_text_attachment(
        self,
        file_content: str,
        attachment_title: Optional[str] = None,
    ) -> mail_helpers.Attachment:
        encoded_content = base64.b64encode(file_content.encode("ascii")).decode()
        return mail_helpers.Attachment(
            file_content=mail_helpers.FileContent(encoded_content),
            file_name=mail_helpers.FileName(attachment_title),
            file_type=mail_helpers.FileType("text/plain"),
            disposition=mail_helpers.Disposition("attachment"),
        )

    def _create_message(
        self,
        *,
        to_email: str,
        from_email: str,
        from_email_name: str,
        subject: str,
        html_content: str,
        cc_addresses: Optional[List[str]] = None,
        text_attachment_content: Optional[str] = None,
        attachment_title: Optional[str] = None,
        disable_link_click: Optional[bool] = False,
        disable_unsubscribe: Optional[bool] = False,
        reply_to_email: Optional[str] = None,
        reply_to_name: Optional[str] = None,
        unsubscribe_group_id: Optional[int] = None,
    ) -> mail_helpers.Mail:
        """Creates the request body for the email that will be sent. Includes all required data to send a single email.

        If there are cc_addresses, it adds those to the request body for the emails.

        If there is text_attachment_content, it creates a 'text/plain' txt file attachment with the content.
        """
        message = mail_helpers.Mail(
            to_emails=to_email,
            from_email=self._create_email_address(from_email, from_email_name),
            subject=subject,
            html_content=html_content,
        )
        if reply_to_email is not None:
            message.reply_to = mail_helpers.ReplyTo(reply_to_email, reply_to_name)

        if cc_addresses is not None:
            message.cc = [
                mail_helpers.Cc(email=cc_email_address)
                for cc_email_address in cc_addresses
            ]

        if text_attachment_content is not None and attachment_title is not None:
            message.attachment = self._create_text_attachment(
                text_attachment_content,
                attachment_title,
            )

        if disable_link_click is True or disable_unsubscribe is True:
            args = {}
            if disable_link_click is True:
                args["click_tracking"] = mail_helpers.ClickTracking(  # type: ignore[attr-defined]
                    enable=False,
                    enable_text=False,
                )
            if disable_unsubscribe is True:
                args["subscription_tracking"] = mail_helpers.SubscriptionTracking(enable=False)  # type: ignore[attr-defined]
            message.tracking_settings = mail_helpers.TrackingSettings(**args)  # type: ignore[attr-defined]

        if unsubscribe_group_id is not None:
            message.asm = mail_helpers.Asm(  # type: ignore[attr-defined]
                group_id=mail_helpers.GroupId(unsubscribe_group_id),  # type: ignore[attr-defined]
            )

        return message

    @staticmethod
    def _create_email_address(
        from_email_address: str, from_email_name: str
    ) -> mail_helpers.Email:
        """Create an email address with a name to personalize emails."""
        return mail_helpers.Email(from_email_address, from_email_name)

    def send_message(
        self,
        *,
        to_email: str,
        from_email: str,
        from_email_name: str,
        subject: str,
        html_content: str,
        redirect_address: Optional[str] = None,
        cc_addresses: Optional[List[str]] = None,
        text_attachment_content: Optional[str] = None,
        attachment_title: Optional[str] = None,
        disable_link_click: Optional[bool] = False,
        disable_unsubscribe: Optional[bool] = False,
        reply_to_email: Optional[str] = None,
        reply_to_name: Optional[str] = None,
        unsubscribe_group_id: Optional[int] = None,
    ) -> bool:
        """Sends the email to the provided address by making a Twilio SendGrid API request.

        If there is a redirect_address, it creates the message for the redirect address instead and updates the
        subject line to include to original recipient's email address.

        Args:
            to_email: The recipient's email address
            from_email: The sender's email address
            from_email_name: A personalized name for the sender to display in the email client
            subject: The email subject line
            html_content: An string with HTML content for the email body
            attachment_title: The title of the attachment file
            redirect_address: (Optional) An email address to which all emails will be sent
            instead of the to_email address.
            cc_addresses: (Optional) A list of email addresses to CC
            text_attachment_content: (Optional) Content for the plain text attachment file
            disable_link_click: (Optional) Boolean to indicate if we want to disable link click tracking
            reply_to_email: (Optional) An alternate email address for recipients to reply to
            reply_to_name: (Optional) Name associated with the reply-to email
            unsubscribe_group_id: (Optional) Integer representing a SendGrid Unsubscribe Group

        Returns:
            True if the message is sent successfully
            False if the response is not OK or an exception is thrown
        """
        if redirect_address is not None:
            subject = f"[{to_email}] {subject}"
            to_email = redirect_address

        message = self._create_message(
            to_email=to_email,
            from_email=from_email,
            from_email_name=from_email_name,
            subject=subject,
            html_content=html_content,
            attachment_title=attachment_title,
            cc_addresses=cc_addresses,
            text_attachment_content=text_attachment_content,
            disable_link_click=disable_link_click,
            disable_unsubscribe=disable_unsubscribe,
            reply_to_email=reply_to_email,
            reply_to_name=reply_to_name,
            unsubscribe_group_id=unsubscribe_group_id,
        )

        try:
            response = self.client.send(message)
        except Exception:
            logging.exception("Error sending the file created for %s", to_email)
            return False

        logging.info(
            "Sent email to %s. Status code = %s", to_email, response.status_code
        )
        return True
