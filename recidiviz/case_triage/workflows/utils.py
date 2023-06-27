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
import logging
from http import HTTPStatus

from flask import Response, jsonify, make_response

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
        return config.get("allowedTwilioRecipients", {}).get(
            recipient_phone_number, False
        )
    except Exception as e:
        logging.error(e)
        return False
