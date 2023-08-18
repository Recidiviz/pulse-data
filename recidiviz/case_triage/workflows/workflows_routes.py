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
"""Implements routes to support external requests from Workflows."""
import datetime
import logging
import re
import uuid
from http import HTTPStatus
from typing import Optional

import requests
import werkzeug.wrappers
from flask import Blueprint, Response, current_app, g, jsonify, make_response, request
from flask_wtf.csrf import generate_csrf
from twilio.rest import Client as TwilioClient
from werkzeug.http import parse_set_header

from recidiviz.case_triage.api_schemas_utils import load_api_schema, requires_api_schema
from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.case_triage.workflows.api_schemas import (
    ProxySchema,
    WorkflowsEnqueueSmsRequestSchema,
    WorkflowsSendSmsRequestSchema,
    WorkflowsUsTnInsertTEPEContactNoteSchema,
)
from recidiviz.case_triage.workflows.constants import (
    WORKFLOWS_SMS_ENABLED_STATES,
    ExternalSystemRequestStatus,
)
from recidiviz.case_triage.workflows.interface import (
    WorkflowsUsTnExternalRequestInterface,
)
from recidiviz.case_triage.workflows.twilio_validation import WorkflowsTwilioValidator
from recidiviz.case_triage.workflows.utils import (
    TWILIO_CRITICAL_ERROR_CODES,
    allowed_twilio_dev_recipient,
    get_sms_request_firestore_path,
    get_workflows_consolidated_status,
    get_workflows_texting_error_message,
    jsonify_response,
)
from recidiviz.case_triage.workflows.workflows_analytics import WorkflowsSegmentClient
from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
    on_successful_authorization_recidiviz_only,
)
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    SingleCloudTaskQueueManager,
    get_cloud_task_json_body,
)
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import get_gcp_environment, in_gcp, in_gcp_production
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.metadata import CloudRunMetadata
from recidiviz.utils.params import get_str_param_value
from recidiviz.utils.secrets import get_secret

if in_gcp():
    cloud_run_metadata = CloudRunMetadata.build_from_metadata_server("case-triage-web")
else:
    cloud_run_metadata = CloudRunMetadata(
        project_id="123",
        region="us-central1",
        url="http://localhost:5000",
        service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
    )

WORKFLOWS_EXTERNAL_SYSTEM_REQUESTS_QUEUE = "workflows-external-system-requests-queue"

WORKFLOWS_ALLOWED_ORIGINS = [
    r"http\://localhost:3000",
    r"http\://localhost:5000",
    r"https\://dashboard-staging\.recidiviz\.org$",
    r"https\://dashboard-demo\.recidiviz\.org$",
    r"https\://dashboard\.recidiviz\.org$",
    r"https\://recidiviz-dashboard-stag-e1108--[^.]+?\.web\.app$",
    r"https\://app-staging\.recidiviz\.org$",
    cloud_run_metadata.url,
]

LOCALHOST_URL = "http://localhost:5000"
STAGING_URL = "https://app-staging.recidiviz.org"
PRODUCTION_URL = "https://app.recidiviz.org"

OPT_OUT_MESSAGE = "To stop receiving these texts, reply: STOP."


def create_workflows_api_blueprint() -> Blueprint:
    """Creates the API blueprint for Workflows"""
    workflows_api = Blueprint("workflows", __name__)
    proxy_endpoint = "workflows.proxy"
    webhook_endpoints = [
        "workflows.handle_twilio_status",
        "workflows.handle_twilio_incoming_message",
    ]

    handle_authorization = build_authorization_handler(
        on_successful_authorization, "dashboard_auth0"
    )
    handle_recidiviz_only_authorization = build_authorization_handler(
        on_successful_authorization_recidiviz_only, "dashboard_auth0"
    )
    twilio_validator = WorkflowsTwilioValidator()

    @workflows_api.before_request
    def validate_request() -> None:
        if request.method == "OPTIONS":
            return
        if request.endpoint in webhook_endpoints:
            logging.info("Twilio webhook endpoint request origin: [%s]", request.origin)

            signature = request.headers["X-Twilio-Signature"]
            params = request.values.to_dict()
            twilio_validator.validate(
                url=request.url, params=params, signature=signature
            )
            return
        if request.endpoint == proxy_endpoint:
            handle_recidiviz_only_authorization()
            return
        handle_authorization()

    @workflows_api.before_request
    def validate_cors() -> Optional[Response]:
        if request.endpoint in [proxy_endpoint] + webhook_endpoints:
            # Proxy or webhook requests will generally be sent from a developer's machine or Twilio server
            # and not a browser, so there is no origin to check against.
            return None

        is_allowed = any(
            re.match(allowed_origin, request.origin)
            for allowed_origin in WORKFLOWS_ALLOWED_ORIGINS
        )

        if not is_allowed:
            response = make_response()
            response.status_code = HTTPStatus.FORBIDDEN
            return response

        return None

    @workflows_api.after_request
    def add_cors_headers(
        response: werkzeug.wrappers.Response,
    ) -> werkzeug.wrappers.Response:
        # Don't cache access control headers across origins
        response.vary = "Origin"
        response.access_control_allow_origin = request.origin
        response.access_control_allow_headers = parse_set_header(
            "authorization, sentry-trace, x-csrf-token, content-type"
        )
        response.access_control_allow_credentials = True
        # Cache preflight responses for 2 hours
        response.access_control_max_age = 2 * 60 * 60
        return response

    @workflows_api.route("/<state>/init")
    def init(state: str) -> Response:  # pylint: disable=unused-argument
        return jsonify({"csrf": generate_csrf(current_app.secret_key)})

    @workflows_api.post("/proxy")
    @requires_api_schema(ProxySchema)
    def proxy() -> Response:
        url_secret = g.api_data["url_secret"]
        if (url := get_secret(url_secret)) is None:
            return make_response(f"Secret {url_secret} not found", HTTPStatus.NOT_FOUND)

        method = g.api_data.get("method")
        logging.info(
            "Workflows proxy: [%s] is sending a [%s] request to url_secret [%s] with value [%s]",
            g.authenticated_user_email,
            method,
            url_secret,
            url,
        )

        response = requests.request(
            url=url,
            method=method,
            headers=g.api_data.get("headers"),
            json=g.api_data.get("json"),
            timeout=g.api_data.get("timeout"),
        )
        return make_response(response.text, response.status_code)

    @workflows_api.post("/external_request/<state>/insert_tepe_contact_note")
    @requires_api_schema(WorkflowsUsTnInsertTEPEContactNoteSchema)
    def insert_tepe_contact_note(
        state: str,  # pylint: disable=unused-argument
    ) -> Response:
        person_external_id = g.api_data["person_external_id"]

        interface = WorkflowsUsTnExternalRequestInterface()
        firestore_client = FirestoreClientImpl()
        doc_path = interface.get_contact_note_updates_firestore_path(person_external_id)

        if not g.api_data["should_queue_task"]:
            try:
                # set_document will create new firestore document if it doesn't exist
                firestore_client.update_document(
                    doc_path,
                    {
                        "contactNote.status": ExternalSystemRequestStatus.IN_PROGRESS.value,
                        f"contactNote.{firestore_client.timestamp_key}": datetime.datetime.now(
                            datetime.timezone.utc
                        ),
                    },
                )

                data = WorkflowsUsTnInsertTEPEContactNoteSchema().dump(g.api_data)
                interface.insert_tepe_contact_note(**data)
            except Exception:
                make_response(
                    jsonify("Error in inserting contact note without queueing task"),
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )

            return make_response(
                jsonify("Complete note inserted without queueing task"), HTTPStatus.OK
            )

        try:
            cloud_task_manager = SingleCloudTaskQueueManager(
                queue_info_cls=CloudTaskQueueInfo,
                queue_name=WORKFLOWS_EXTERNAL_SYSTEM_REQUESTS_QUEUE,
            )

            headers_copy = dict(request.headers)
            headers_copy["Referer"] = cloud_run_metadata.url

            cloud_task_manager.create_task(
                absolute_uri=f"{cloud_run_metadata.url}"
                f"/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
                body=WorkflowsUsTnInsertTEPEContactNoteSchema().dump(g.api_data),  # type: ignore
                headers=headers_copy,
            )
        except Exception as e:
            logging.error(e)
            firestore_client.update_document(
                doc_path,
                {
                    "contactNote.status": ExternalSystemRequestStatus.FAILURE.value,
                    f"contactNote.{firestore_client.timestamp_key}": datetime.datetime.now(
                        datetime.timezone.utc
                    ),
                },
            )
            return make_response(
                jsonify(
                    message=f"An unknown error occurred while queueing the handle_insert_tepe_contact_note task: {e}",
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        logging.info("Enqueued handle_insert_tepe_contact_note task")

        firestore_client.update_document(
            doc_path,
            {
                "contactNote.status": ExternalSystemRequestStatus.IN_PROGRESS.value,
                f"contactNote.{firestore_client.timestamp_key}": datetime.datetime.now(
                    datetime.timezone.utc
                ),
            },
        )

        return make_response(jsonify(), HTTPStatus.OK)

    @workflows_api.post("/external_request/<state>/handle_insert_tepe_contact_note")
    def handle_insert_tepe_contact_note(
        state: str,  # pylint: disable=unused-argument
    ) -> Response:
        cloud_task_body = get_cloud_task_json_body()
        person_external_id = cloud_task_body.get("person_external_id", None)

        if person_external_id is None:
            logging.error("No person_external_id provided")
            return make_response(
                jsonify(
                    message=(
                        "Person_external_id missing. Requests must have a person_external_id "
                        "in order to make the TOMIS request and update firestore. "
                    )
                ),
                HTTPStatus.BAD_REQUEST,
            )

        try:
            # Validate schema
            data = load_api_schema(
                WorkflowsUsTnInsertTEPEContactNoteSchema, cloud_task_body
            )
            # Dump to remove load_only fields from body
            data = WorkflowsUsTnInsertTEPEContactNoteSchema().dump(data)
            WorkflowsUsTnExternalRequestInterface().insert_tepe_contact_note(**data)
            logging.info("Handling contact note: %s", data)
        except Exception as e:
            logging.error("Write to TOMIS failed due to error: %s", e)
            firestore_client = FirestoreClientImpl()
            firestore_doc_path = WorkflowsUsTnExternalRequestInterface().get_contact_note_updates_firestore_path(
                person_external_id
            )
            firestore_client.update_document(
                firestore_doc_path,
                {
                    "contactNote.status": ExternalSystemRequestStatus.FAILURE.value,
                    f"contactNote.{firestore_client.timestamp_key}": datetime.datetime.now(
                        datetime.timezone.utc
                    ),
                },
            )

            return make_response(
                jsonify(
                    message=(
                        f"Complete contact note was not successfully inserted into TOMIS. Error: {e}"
                    )
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        return make_response(
            jsonify(
                message="Complete contact note was successfully inserted into TOMIS"
            ),
            HTTPStatus.OK,
        )

    @workflows_api.post("/external_request/<state>/enqueue_sms_request")
    @requires_api_schema(WorkflowsEnqueueSmsRequestSchema)
    def handle_enqueue_sms_request(
        state: str,
    ) -> Response:
        state_code = state.upper()

        if state_code not in WORKFLOWS_SMS_ENABLED_STATES:
            return jsonify_response(
                f"Unsupported sender state: {state_code}",
                HTTPStatus.UNAUTHORIZED,
            )

        recipient_external_id = g.api_data["recipient_external_id"]
        recipient_phone_number = g.api_data["recipient_phone_number"]
        sender_id = g.api_data["sender_id"]
        message = g.api_data["message"]

        if sender_id != g.authenticated_user_email:
            return jsonify_response(
                "sender_id does not match authenticated user",
                HTTPStatus.UNAUTHORIZED,
            )

        if not in_gcp_production() and not allowed_twilio_dev_recipient(
            recipient_phone_number
        ):
            return jsonify_response(
                f"{recipient_phone_number} is not an allowed recipient in f{get_gcp_environment()}",
                HTTPStatus.UNAUTHORIZED,
            )

        mid = str(uuid.uuid4())
        firestore_client = FirestoreClientImpl()
        firestore_path = get_sms_request_firestore_path(state, recipient_external_id)
        client_firestore_id = f"{state.lower()}_{recipient_external_id}"

        try:
            cloud_task_manager = SingleCloudTaskQueueManager(
                queue_info_cls=CloudTaskQueueInfo,
                queue_name=WORKFLOWS_EXTERNAL_SYSTEM_REQUESTS_QUEUE,
            )

            headers_copy = dict(request.headers)
            headers_copy["Referer"] = cloud_run_metadata.url

            cloud_task_manager.create_task(
                absolute_uri=f"{cloud_run_metadata.url}"
                f"/workflows/external_request/{state_code}/send_sms_request",
                body={
                    "message": message,
                    "recipient": f"+1{recipient_phone_number}",
                    "client_firestore_id": client_firestore_id,
                    "recipient_external_id": recipient_external_id,
                },
                headers=headers_copy,
            )
        except Exception as e:
            logging.error(e)
            firestore_client.set_document(
                firestore_path,
                {
                    "status": ExternalSystemRequestStatus.FAILURE.value,
                    "updated": {
                        "date": datetime.datetime.now(datetime.timezone.utc),
                        "by": sender_id,
                    },
                },
                merge=True,
            )
            return make_response(
                jsonify(
                    message=f"An unknown error occurred while queueing the send_sms_request task: {e}",
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        logging.info("Enqueued send_sms_request task")

        firestore_client.set_document(
            firestore_path,
            {
                "updated": {
                    "date": datetime.datetime.now(datetime.timezone.utc),
                    "by": sender_id,
                },
                "status": ExternalSystemRequestStatus.IN_PROGRESS.value,
                "mid": mid,
                "sentBy": sender_id,
            },
            merge=True,
        )

        return make_response(jsonify(), HTTPStatus.OK)

    @workflows_api.post("/external_request/<state>/send_sms_request")
    def handle_send_sms_request(state: str) -> Response:
        cloud_task_body = get_cloud_task_json_body()

        # Validate schema
        data = load_api_schema(WorkflowsSendSmsRequestSchema, cloud_task_body)

        message = data["message"]
        recipient = data["recipient"]
        recipient_external_id = data["recipient_external_id"]
        state_code = state.upper()

        if state_code != "US_CA":
            return jsonify_response(
                f"Unsupported sender state: {state_code}", HTTPStatus.UNAUTHORIZED
            )

        if not in_gcp_production() and not allowed_twilio_dev_recipient(
            recipient[2:]  # Strip off the +1 at the beginning
        ):
            return jsonify_response(
                f"{recipient} is not an allowed recipient in GCP environment: {get_gcp_environment()}",
                HTTPStatus.UNAUTHORIZED,
            )

        account_sid = get_secret("twilio_sid")
        auth_token = get_secret("twilio_auth_token")
        messaging_service_sid = get_secret("twilio_us_ca_messaging_service_sid")

        if not account_sid or not auth_token or not messaging_service_sid:
            return jsonify_response(
                "Server missing API credentials",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        firestore_client = FirestoreClientImpl()
        firestore_path = get_sms_request_firestore_path(state, recipient_external_id)

        logging.info("Twilio send SMS gcp environment: [%s]", get_gcp_environment())
        url = LOCALHOST_URL
        if get_gcp_environment() == "staging":
            url = STAGING_URL
        if get_gcp_environment() == "production":
            url = PRODUCTION_URL

        try:
            client = TwilioClient(account_sid, auth_token)

            response_message = client.messages.create(
                body=f"{message}\n\n{OPT_OUT_MESSAGE}",
                messaging_service_sid=messaging_service_sid,
                to=recipient,
                status_callback=f"{url}/workflows/webhook/twilio_status",
            )

            firestore_client.set_document(
                firestore_path,
                {
                    "lastUpdated": datetime.datetime.now(datetime.timezone.utc),
                    "message_sid": response_message.sid,
                },
                merge=True,
            )

        except Exception as e:
            logging.error("Error sending sms request to Twilio. Error: %s", e)
            firestore_client.set_document(
                firestore_path,
                {
                    "status": ExternalSystemRequestStatus.FAILURE.value,
                    "updated": {
                        "date": datetime.datetime.now(datetime.timezone.utc),
                        "by": "RECIDIVIZ",
                    },
                    "errors": [str(e)],
                },
                merge=True,
            )
            return jsonify_response(
                "Error sending sms request to Twilio", HTTPStatus.INTERNAL_SERVER_ERROR
            )

        return jsonify_response(
            "Successfully sent sms request to twilio", HTTPStatus.OK
        )

    @workflows_api.post("/webhook/twilio_status")
    def handle_twilio_status() -> Response:
        # Segment setup
        segment_client = WorkflowsSegmentClient()

        status = get_str_param_value(
            "MessageStatus", request.values, preserve_case=True
        )
        message_sid = get_str_param_value(
            "MessageSid", request.values, preserve_case=True
        )
        error_code = get_str_param_value(
            "ErrorCode", request.values, preserve_case=True
        )
        error_message = get_workflows_texting_error_message(error_code)

        firestore_client = FirestoreClientImpl()
        milestones_messages_ref = firestore_client.get_collection_group(
            collection_path="milestonesMessages"
        )
        query = milestones_messages_ref.where("message_sid", "==", message_sid)
        client_updates_docs = query.get()

        for doc in client_updates_docs:
            milestonesMessage = doc.to_dict()

            if not milestonesMessage:
                continue

            # This endpoint will be hit multiple times per message, so check here if this is a new status change from
            # what we already have in Firestore.
            if milestonesMessage.get("rawStatus", "") != status:
                logging.info(
                    "Updating Segment logs with message status for doc: [%s]",
                    doc.reference.path,
                )
                segment_client.track_milestones_message_status(
                    user_hash=milestonesMessage.get("userHash", ""),
                    twilioRawStatus=status,
                    status=get_workflows_consolidated_status(status),
                    error_code=error_code,
                    error_message=error_message,
                )

            logging.info(
                "Updating Twilio message status for doc: [%s]", doc.reference.path
            )

            doc_update = {
                "status": get_workflows_consolidated_status(status),
                "lastUpdated": datetime.datetime.now(datetime.timezone.utc),
                "rawStatus": status,
            }

            if error_code:
                doc_update["errors"] = [error_message]
                doc_update["errorCode"] = error_code

            firestore_client.set_document(
                doc.reference.path,
                doc_update,
                merge=True,
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

    @workflows_api.post("/webhook/twilio_incoming_message")
    def handle_twilio_incoming_message() -> Response:
        opt_out_type = get_str_param_value(
            "OptOutType", request.values, preserve_case=True
        )
        recipient = get_str_param_value("From", request.values, preserve_case=True)

        if opt_out_type and recipient:
            segment_client = WorkflowsSegmentClient()
            firestore_client = FirestoreClientImpl()
            milestones_messages_ref = firestore_client.get_collection_group(
                collection_path="milestonesMessages"
            )
            # recipient phone numbers are prefixed with +1 in the request, but do not contain that prefix in Firestore
            query = milestones_messages_ref.where("recipient", "==", recipient[2:])
            client_updates_docs = query.get()

            for doc in client_updates_docs:
                milestonesMessage = doc.to_dict()

                if not milestonesMessage:
                    continue

                # This endpoint may be hit multiple times per recipient, so check here if this is a new
                # opt out type from what we already have in Firestore.
                # Opt out types are: STOP, START, and HELP
                if opt_out_type and opt_out_type != milestonesMessage.get("optOutType"):
                    logging.info(
                        "Updating Segment logs with opt out type for doc: [%s]",
                        doc.reference.path,
                    )
                    segment_client.track_milestones_message_opt_out(
                        user_hash=milestonesMessage.get("userHash", ""),
                        opt_out_type=opt_out_type,
                    )

                doc_update = {
                    "lastUpdated": datetime.datetime.now(datetime.timezone.utc),
                    "optOutType": opt_out_type,
                }
                firestore_client.set_document(
                    doc.reference.path,
                    doc_update,
                    merge=True,
                )

        return make_response(jsonify(), HTTPStatus.NO_CONTENT)

    @workflows_api.get("/ip")
    def ip() -> Response:
        ip_response = requests.get("http://curlmyip.org", timeout=10)
        return jsonify({"ip": ip_response.text})

    return workflows_api
