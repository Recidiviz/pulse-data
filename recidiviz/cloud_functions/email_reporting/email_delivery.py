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

"""Functionality for delivery of email reports.

This module implements delivery of emails. The expectation is that the emails are in the appropriate Storage bucket
and conform to the following:

1) The bucket contains a folder whose name is the batch id
2) Inside the folder is a single html file for each recipient
3) Each file name is [email address].html
4) The contents of the file are ready to be sent with no further modification
"""

import logging
from typing import Dict, Tuple

from google.cloud import storage
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email

import email_reporting_utils as utils  # type: ignore[import]

EMAIL_SUBJECT = "Your monthly Recidiviz report"


def deliver(batch_id: str, test_address: str = None) -> Tuple[int, int]:
    """Delivers emails for the given batch.

    Delivers emails to either the desired recipients for the batch or to a test address. Emails delivered to the test
    address are identical to a production send except that the customer email address is appended to the subject.

    Args:
        batch_id: The identifier for the batch
        test_address: If provided, all emails will be sent to this email address

    Returns:
        A tuple with counts of successful deliveries and failures (successes, failures)

    Raises:
        Raises errors related to external services like Google Storage but continues attempting to send subsequent
        emails if it receives an exception while attempting to send.  In that case the error is logged.
    """
    if test_address:
        logging.info("Delivering test emails for batch %s to %s", batch_id, test_address)
    else:
        logging.info("Delivering emails for batch %s", batch_id)

    try:
        sendgrid_api_key = utils.get_env_var('SENDGRID_API_KEY')
        from_email_address = utils.get_env_var('FROM_EMAIL_ADDRESS')
        from_email_name = utils.get_env_var('FROM_EMAIL_NAME')
    except KeyError:
        logging.error("Unable to get a required environment variable. Exiting.")
        raise

    files = retrieve_html_files(batch_id)
    success_count = 0
    fail_count = 0

    for email_address in files:
        if test_address:
            subject = f"[{email_address}] {EMAIL_SUBJECT}"
            to_address = test_address
        else:
            subject = EMAIL_SUBJECT
            to_address = email_address

        try:
            send_email(to_address, subject, files[email_address], sendgrid_api_key, from_email_address, from_email_name)
        except Exception as e:
            logging.error("Error sending the file created for %s to %s", email_address, to_address)
            logging.error(e)
            fail_count = fail_count + 1
        else:
            logging.info("Email for %s sent to %s", email_address, to_address)
            success_count = success_count + 1

    logging.info("Sent %s emails. %s emails failed to send", success_count, fail_count)
    return success_count, fail_count


def send_email(email_address: str,
               subject: str,
               body: str,
               sendgrid_api_key: str,
               from_email_address: str,
               from_email_name: str) -> None:
    """Send an email via SendGrid.

    Args:
        email_address: The address to deliver to
        subject: Text for the subject line
        body: The body of the email
        sendgrid_api_key: The SendGrid API key needed to send emails
        from_email_address: The address that the delivered emails should be from
        from_email_name: The name of the person sending emails

    Raises:
        All errors so that calling functions can handle appropriately for their use case.
    """
    sg = SendGridAPIClient(sendgrid_api_key)

    message = Mail(to_emails=email_address,
                   from_email=Email(from_email_address, from_email_name),
                   subject=subject,
                   html_content=body)

    response = sg.send(message)
    logging.info("Sent email. Status code = %s", response.status_code)
    logging.info("Email response body = %s", response.body)


def email_from_blob_name(blob_name: str) -> str:
    """Extract the email address from a blob name.

    Args:
        blob_name: Assumes that the blob name is of the form [folder names]/[email address].html
    """
    a = blob_name.rsplit("/", 1)
    email_address = a[1].replace(".html", "")
    return email_address


def retrieve_html_files(batch_id: str) -> Dict[str, str]:
    """Loads the HTML files for this batch from Storage.

    This function is guaranteed to either return a dictionary with 1 or more results or throw an exception if there is
    a problem loading any of the files.

    Args:
        batch_id: The identifier for this batch

    Returns:
        A dict whose keys are the email addresses of recipients and values are strings of the email body to send.

    Raises:
        Passes through exceptions from Storage and raises its own if there are no results in this batch.
    """
    storage_client = storage.Client()

    html_bucket = utils.get_html_bucket_name()
    try:
        blobs = storage_client.list_blobs(html_bucket, prefix=batch_id)
    except Exception:
        logging.error("Unable to list files in html folder. Bucket = %s, folder = %s", html_bucket, batch_id)
        raise

    files = {}
    for blob in blobs:
        try:
            body = utils.load_string_from_storage(html_bucket, blob.name)
        except Exception:
            logging.error("Unable to load html file %s from bucket %s", blob.name, html_bucket)
            raise
        else:
            email_address = email_from_blob_name(blob.name)
            files[email_address] = body

    if len(files) == 0:
        msg = f"No html files found for batch {batch_id} in the bucket {html_bucket}"
        logging.error(msg)
        raise IndexError(msg)

    return files
