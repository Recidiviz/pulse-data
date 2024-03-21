# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Implements routes to support the Idaho LSU JII Pilot."""

import datetime
import logging
from http import HTTPStatus
from typing import Optional

import werkzeug.wrappers
from flask import Blueprint, Response, jsonify, make_response, request
from google.cloud.firestore_v1 import ArrayUnion, FieldFilter

from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.case_triage.helpers import (
    add_cors_headers_helper,
    validate_cors_helper,
    validate_request_helper,
)
from recidiviz.case_triage.jii.helpers import update_status_helper
from recidiviz.case_triage.workflows.utils import TWILIO_CRITICAL_ERROR_CODES
from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
    on_successful_authorization_recidiviz_only,
)
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.metadata import CloudRunMetadata
from recidiviz.utils.params import get_str_param_value

if in_gcp():
    cloud_run_metadata = CloudRunMetadata.build_from_metadata_server("case-triage-web")
else:
    cloud_run_metadata = CloudRunMetadata(
        project_id="123",
        region="us-central1",
        url="http://localhost:5000",
        service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
    )


def create_jii_api_blueprint() -> Blueprint:
    """Creates the API blueprint for ID LSU JII Pilot"""
    jii_api = Blueprint("jii", __name__)

    handle_authorization = build_authorization_handler(
        on_successful_authorization, "dashboard_auth0"
    )
    handle_recidiviz_only_authorization = build_authorization_handler(
        on_successful_authorization_recidiviz_only, "dashboard_auth0"
    )

    @jii_api.before_request
    def validate_request() -> None:
        validate_request_helper(
            handle_authorization=handle_authorization,
            handle_recidiviz_only_authorization=handle_recidiviz_only_authorization,
        )

    @jii_api.before_request
    def validate_cors() -> Optional[Response]:
        return validate_cors_helper()

    @jii_api.after_request
    def add_cors_headers(
        response: werkzeug.wrappers.Response,
    ) -> werkzeug.wrappers.Response:
        return add_cors_headers_helper(response=response)

    @jii_api.post("/webhook/twilio_status")
    def handle_twilio_status() -> Response:
        """Given incoming Twilio statuses, update the previously saved documents/messages
        in Firestore accordingly.
        """
        logging.info("ENDPOINT HIT")
        message_status = get_str_param_value(
            "MessageStatus", request.values, preserve_case=True
        )
        message_sid = get_str_param_value(
            "MessageSid", request.values, preserve_case=True
        )
        error_code = get_str_param_value(
            "ErrorCode", request.values, preserve_case=True
        )

        firestore_client = FirestoreClientImpl(project_id="jii-pilots")

        # Get the previously stored message
        jii_messages_ref = firestore_client.get_collection_group(
            collection_path="lsu_eligibility_messages"
        )
        query = jii_messages_ref.where(
            filter=FieldFilter("message_sid", "==", message_sid)
        )
        jii_updates_docs = query.stream()

        update_status_helper(
            message_status=message_status,
            firestore_client=firestore_client,
            jii_updates_docs=jii_updates_docs,
            error_code=error_code,
        )

        try:
            # Raise an exception if the error code from Twilio is an
            # error with our account (vs a receiver or carrier error)
            # https://docs.google.com/spreadsheets/d/1xGgbc86Lmnk0uL3e7n2XZAnF4U-vTD3Z6-pfKZCM2q8/edit#gid=0
            if error_code and error_code in TWILIO_CRITICAL_ERROR_CODES:
                message = f"Critical Twilio account error [{error_code}] for message_sid [{message_sid}]"
                logging.error(message)
                raise FlaskException(
                    code="twilio_account_error",
                    description=message,
                    status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
        except FlaskException as error:
            return make_response(str(error.description), error.status_code)

        return make_response(jsonify(), HTTPStatus.NO_CONTENT)

    @jii_api.post("/webhook/twilio_incoming_message")
    def handle_twilio_incoming_message() -> Response:
        opt_out_type = get_str_param_value(
            "OptOutType", request.values, preserve_case=True
        )
        phone_number = get_str_param_value("From", request.values, preserve_case=True)

        if not phone_number:
            return make_response(jsonify(), HTTPStatus.NO_CONTENT)

        # recipient phone numbers are prefixed with +1 in the request, but do not contain that prefix in Firestore
        phone_number = phone_number[2:]

        firestore_client = FirestoreClientImpl(project_id="jii-pilots")
        twilio_message_ref = firestore_client.get_collection(
            collection_path="twilio_messages"
        )
        query = twilio_message_ref.where(
            filter=FieldFilter("phone_numbers", "array_contains", phone_number)
        )
        jii_update_docs = query.stream()

        known_phone_number = False

        jii_response = get_str_param_value("Body", request.values, preserve_case=True)
        response_update = {
            "responses": ArrayUnion(
                [
                    {
                        "response": jii_response,
                        "response_date": datetime.datetime.now(datetime.timezone.utc),
                    },
                ]
            ),
        }

        for jii_doc_snapshot in jii_update_docs:
            jii_doc = jii_doc_snapshot.to_dict()

            if jii_doc is None:
                continue

            # Store the individual's response and the datetime of their response
            logging.info(
                "Storing jii response for doc: [%s]",
                jii_doc_snapshot.reference.path,
            )
            firestore_client.update_document(
                jii_doc_snapshot.reference.path,
                response_update,
            )

            known_phone_number = True

            # This endpoint may be hit multiple times per recipient, so check here if this is a new
            # opt out type from what we already have in Firestore.
            # OptOutType key words include: (cancel, end, quit, stop, stopall, unsubscribe, start,
            # unstop, yes, help, info)
            if opt_out_type and opt_out_type != jii_doc.get("opt_out_type"):
                logging.info(
                    "Updating Twilio opt-out type for doc: [%s] with type: [%s]",
                    jii_doc_snapshot.reference.path,
                    opt_out_type,
                )
                doc_update = {
                    "last_opt_out_update": datetime.datetime.now(datetime.timezone.utc),
                    "opt_out_type": opt_out_type,
                }
                firestore_client.set_document(
                    jii_doc_snapshot.reference.path,
                    doc_update,
                    merge=True,
                )

        if known_phone_number is False:
            # A phone number that we do not have stored / associated with a jii document in the Firebase db
            # has responded to our Twilio Campaign phone number. Let's store their response
            # in our Firebase db in a separate collection

            # Store the unknown individual's response and the datetime of their response
            logging.info(
                "Storing unknown jii response in unknown_phone_number_replies collection"
            )
            firestore_client.set_document(
                f"unknown_phone_number_replies/{phone_number}",
                response_update,
                merge=True,
            )

        return make_response(jsonify(), HTTPStatus.NO_CONTENT)

    return jii_api
