#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Utilities for api calls."""
import datetime
import enum
import logging
from http import HTTPStatus
from typing import Optional

from flask import Response, jsonify, make_response

from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.firestore.firestore_client import FirestoreClientImpl


def jsonify_response(message: str, response_code: HTTPStatus) -> Response:
    return make_response(jsonify(message=message), response_code)


def allowed_twilio_dev_recipient(recipient_phone_number: str) -> bool:
    """
    Checks the supplied number against a configuration stored in firestore and returns
    True if we are allowed to message this recipient from dev/staging
    """
    firestore_client = FirestoreClientImpl()
    path = "configs/dev"

    try:
        config = firestore_client.get_document(path).get().to_dict()
        if not config:
            return False

        return config.get("allowedTwilioRecipients", {}).get(
            recipient_phone_number, False
        )
    except Exception as e:
        logging.error(e)
        return False


def get_sms_request_firestore_path(state: str, recipient_external_id: str) -> str:
    month = datetime.date.today().strftime("%m_%Y")
    month_code = f"milestones_{month}"
    client_firestore_id = f"{state.lower()}_{recipient_external_id}"
    return f"clientUpdatesV2/{client_firestore_id}/milestonesMessages/{month_code}"


def get_consolidated_status(status: Optional[str]) -> str:
    if status and status.lower() in ["undelivered", "failed", "canceled"]:
        return ExternalSystemRequestStatus.FAILURE.value
    if status and status.lower() in ["delivered", "read"]:
        return ExternalSystemRequestStatus.SUCCESS.value
    # The IN_PROGRESS statuses are ["accepted", "scheduled", "queued", "sending", "sent"]
    return ExternalSystemRequestStatus.IN_PROGRESS.value


# These are the error codes from Twilio that are sent to the status webhook
# if there is an error with our account (vs a receiver or carrier error)
# https://docs.google.com/spreadsheets/d/1xGgbc86Lmnk0uL3e7n2XZAnF4U-vTD3Z6-pfKZCM2q8/edit#gid=0
TWILIO_CRITICAL_ERROR_CODES = {"30001", "30002", "30007", "30010", "30032"}


def get_workflows_texting_error_message(error_code: Optional[str]) -> Optional[str]:
    # Most common Twillio error code descriptions:
    # https://www.twilio.com/docs/sms/troubleshooting/debugging-tools#error-codes
    if error_code is None:
        return None
    if error_code in ["30002"]:
        return "The message could not be delivered at this time. Consider congratulating the client in person or through some other way."
    if error_code in ["30003"]:
        return "The message could not be delivered at this time. The recipient's device is switched off or otherwise unavailable. Consider congratulating the client in person or through some other way."
    if error_code in ["30004"]:
        return "The message could not be delivered. This recipient can't receive messages. Consider congratulating the client in person or through some other way."
    if error_code in ["30005"]:
        return "The message could not be delivered. The mobile number entered is unknown or may no longer exist. Please update to a valid number and re-send."
    if error_code in ["30006"]:
        return "The message could not be delivered. The number entered is not a valid mobile number or may be a landline and is unable to receive the message. Please update to a valid number and re-send."
    if error_code in ["30007"]:
        return "The message could not be delivered. It has been flagged as spam by the carrier. Please update the body of the message and re-send."
    if error_code in ["21610"]:
        return "The message could not be delivered because the recipient has opted out of receiving messages from this number. Consider congratulating the client in person or through some other way."
    return "The message could not be delivered at this time. Consider congratulating the client in person or through some other way."


def get_jii_texting_error_message(error_code: Optional[str]) -> Optional[str]:
    # Most common Twillio error code descriptions:
    # https://www.twilio.com/docs/sms/troubleshooting/debugging-tools#error-codes
    if error_code is None:
        return None
    error_code_to_message = {
        "30001": "Queue overflow",
        "30002": "Account suspended",
        "30003": "Unreachable destination handset",
        "30004": "Message blocked",
        "30005": "Unknown destination handset",
        "30006": "Landline or unreachable carrier",
        "30007": "Carrier violation",
        "30008": "Unknown error",
        "30009": "Missing segment",
        "30010": "Message price exceeds max price.",
    }

    return error_code_to_message.get(error_code)


class TwilioStatus(enum.Enum):
    """Enum for possible Twilio Message Statuses"""

    ACCEPTED = "accepted"
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    SENDING = "sending"
    SENT = "sent"
    DELIVERY_UNKNOWN = "delivery_unknown"
    DELIVERED = "delivered"
    UNDELIVERED = "undelivered"
    FAILED = "failed"
    RECEIVED = "received"
