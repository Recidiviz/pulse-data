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

"""A single module containing all Python Cloud Functions related to Email Reporting.

This file contains all the *Python* Cloud Functions for Email Reporting. (There are also Node/JavaScript Cloud Functions
used for rendering charts that can be injected into generated emails. Each Cloud Function is a wrapper around a
function that appears in a different module. It is here that we deal with Cloud Function-related errors and missing
data.
"""

import base64
import json
import logging

import data_retrieval
import email_delivery
import email_generation


def report_start_new_batch(request):
    """Start a new batch of email generation for the indicated state.

    This function is the entry point for generating a new batch. The process starts with data retrieval and thus that
    is what is called from here. The caller should provide valid JSON with a "state_code" and "report_type" keys.

    Args:
        request: The HTTP request.  Must contain JSON with "state_code" and
        "report_type" keys

    Returns:
        An HTTP status based on the outcome of the data retrieval process.

    Raises:
        Errors will be caught in the called function and logged to StackDriver.
    """
    request_json = request.get_json()

    if request_json and 'state_code' in request_json and 'report_type' in request_json:
        state_code = request_json.get('state_code')
        report_type = request_json.get('report_type')
        batch_id = data_retrieval.start(state_code, report_type)
        return (f"New batch started for {state_code} and {report_type}.  Batch "
                f"id = {batch_id}"), 200

    msg = "Request does not include JSON with 'state_code' and 'report_type' keys"
    logging.error(msg)
    return msg, 400


def report_generate_email_for_recipient(event, _context):
    """Generate an email for the indicated recipient.

    This function is triggered by a Pub/Sub message containing the complete data for a recipient. It assumes that all
    data is available, including static assets. It composes a final email in HTML form and stores it in a configured
    storage bucket to be retrieved later for delivery.

    Args:
        event: The Pub/Sub event payload, including the recipient data
        _context: Metadata for the event. Not used.
    """
    json_str = base64.b64decode(event['data']).decode("utf-8")
    try:
        json_data = json.loads(json_str)
    except Exception:
        logging.error("Unable to parse JSON received in Pub/Sub message. Received: %s", json_str)
        raise

    email_generation.generate(json_data)


def report_deliver_emails_for_batch(request):
    """Cloud function to deliver a batch of generated emails.

    This is an HTTP function that delivers emails. It requires a JSON input containing the following keys:
        batch_id: (required) Identifier for this batch
        test_address: (optional) An email address to which all emails should
        be sent instead of to their actual recipients.

    Args:
        request: HTTP request payload containing JSON with keys as described above

    Returns:
        An HTTP response code and informative text
    """
    request_json = request.get_json()
    batch_id_param = "batch_id"

    if request_json and batch_id_param in request_json:
        batch_id = request_json.get(batch_id_param)
    else:
        msg = f"Query parameter '{batch_id_param}' not received"
        logging.error(msg)
        return msg, 400

    if "test_address" in request_json:
        test_address = request_json.get("test_address")
        results = email_delivery.deliver(batch_id, test_address=test_address)
        return (f"Sent {results[0]} emails to the test address {test_address}. "
                f"{results[1]} emails failed to send"), 200

    results = email_delivery.deliver(batch_id)
    return f"Sent {results[0]} emails. {results[1]} emails failed to send", 200
