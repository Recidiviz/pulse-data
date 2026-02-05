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
import uuid
from http import HTTPStatus
from typing import List, Optional

import requests
import werkzeug.wrappers
from flask import Response, current_app, g, jsonify, make_response, request
from flask_smorest import Blueprint
from flask_wtf.csrf import generate_csrf
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from twilio.rest import Client as TwilioClient

from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.case_triage.api_schemas_utils import load_api_schema, requires_api_schema
from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.case_triage.helpers import (
    add_cors_headers_helper,
    validate_cors_helper,
    validate_request_helper,
)
from recidiviz.case_triage.workflows.api_schemas import (
    ProxySchema,
    WorkflowsConfigurationsResponseSchema,
    WorkflowsEmailUserSchema,
    WorkflowsEnqueueSmsRequestSchema,
    WorkflowsOptimizeRouteSchema,
    WorkflowsSendSmsRequestSchema,
    WorkflowsUsNdUpdateDocstarsEarlyTerminationDateSchema,
    WorkflowsUsTnInsertTEPEContactNoteSchema,
)
from recidiviz.case_triage.workflows.constants import (
    WORKFLOWS_SMS_ENABLED_STATES,
    ExternalSystemRequestStatus,
)
from recidiviz.case_triage.workflows.interface import (
    WorkflowsUsNdExternalRequestInterface,
    WorkflowsUsTnExternalRequestInterface,
)
from recidiviz.case_triage.workflows.utils import (
    TWILIO_CRITICAL_ERROR_CODES,
    allowed_twilio_dev_recipient,
    get_consolidated_status,
    get_sms_request_firestore_path,
    get_workflows_texting_error_message,
    jsonify_response,
)
from recidiviz.case_triage.workflows.workflows_analytics import WorkflowsSegmentClient
from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
    on_successful_authorization_recidiviz_only,
)
from recidiviz.common.constants.states import StateCode
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
from recidiviz.utils.sendgrid_client_wrapper import (
    SendGridClientWrapper,
    get_enforced_tls_only_state_codes,
)
from recidiviz.workflows.querier.querier import WorkflowsQuerier
from recidiviz.workflows.types import OpportunityConfigResponse, WorkflowsSystemType

if in_gcp():
    cloud_run_metadata = CloudRunMetadata.build_from_metadata_server(
        CloudRunMetadata.Service.CASE_TRIAGE
    )
else:
    cloud_run_metadata = CloudRunMetadata(
        project_id="123",
        region="us-central1",
        url="http://localhost:5000",
        service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
    )

WORKFLOWS_EXTERNAL_SYSTEM_REQUESTS_QUEUE = "workflows-external-system-requests-queue"

LOCALHOST_URL = "http://localhost:5000"
STAGING_URL = "https://app-staging.recidiviz.org"
PRODUCTION_URL = "https://app.recidiviz.org"

OPT_OUT_MESSAGE = "To stop receiving these texts, reply: STOP"


def create_workflows_api_blueprint() -> Blueprint:
    """Creates the API blueprint for Workflows"""
    workflows_api = Blueprint("workflows", __name__)

    handle_authorization = build_authorization_handler(
        on_successful_authorization, "dashboard_auth0"
    )
    handle_recidiviz_only_authorization = build_authorization_handler(
        on_successful_authorization_recidiviz_only, "dashboard_auth0"
    )

    @workflows_api.before_request
    def validate_request() -> None:
        validate_request_helper(
            handle_authorization=handle_authorization,
            handle_recidiviz_only_authorization=handle_recidiviz_only_authorization,
        )

    @workflows_api.before_request
    def validate_cors() -> Optional[Response]:
        return validate_cors_helper()

    @workflows_api.after_request
    def add_cors_headers(
        response: werkzeug.wrappers.Response,
    ) -> werkzeug.wrappers.Response:
        return add_cors_headers_helper(response=response)

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

    @workflows_api.get("/ip")
    def ip() -> Response:
        ip_response = requests.get("http://curlmyip.org", timeout=10)
        return jsonify({"ip": ip_response.text})

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
                return make_response(
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
        if state.upper() != "US_TN":
            return jsonify_response(
                f"Not supported in {state.upper()}", HTTPStatus.BAD_REQUEST
            )

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
                    status=get_consolidated_status(status),
                    error_code=error_code,
                    error_message=error_message,
                )

            logging.info(
                "Updating Twilio message status for doc: [%s]", doc.reference.path
            )

            doc_update = {
                "status": get_consolidated_status(status),
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

    @workflows_api.post(
        "/external_request/<state>/update_docstars_early_termination_date"
    )
    @requires_api_schema(WorkflowsUsNdUpdateDocstarsEarlyTerminationDateSchema)
    def update_docstars_early_termination_date(state: str) -> Response:
        if state.upper() != StateCode.US_ND.value:
            return jsonify_response(
                f"Unsupported state: {state}",
                HTTPStatus.UNAUTHORIZED,
            )

        if g.api_data["user_email"] != g.authenticated_user_email:
            return jsonify_response(
                "user_email does not match authenticated user",
                HTTPStatus.UNAUTHORIZED,
            )

        person_external_id = g.api_data["person_external_id"]
        interface = WorkflowsUsNdExternalRequestInterface(person_external_id)

        if not g.api_data["should_queue_task"]:
            try:
                interface.set_firestore_early_termination_status(
                    ExternalSystemRequestStatus.IN_PROGRESS,
                )
                data = WorkflowsUsNdUpdateDocstarsEarlyTerminationDateSchema().dump(
                    g.api_data
                )
                interface.update_early_termination_date(
                    user_email=data["user_email"],
                    early_termination_date=data["early_termination_date"],
                    justification_reasons=data["justification_reasons"],
                )
            except Exception:
                interface.set_firestore_early_termination_status(
                    ExternalSystemRequestStatus.FAILURE,
                )
                return jsonify_response(
                    "Error in updating early termination date without queueing task",
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            interface.set_firestore_early_termination_status(
                ExternalSystemRequestStatus.SUCCESS,
            )
            return jsonify_response(
                "Early termination date updated without queueing task",
                HTTPStatus.OK,
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
                f"/workflows/external_request/US_ND/handle_update_docstars_early_termination_date",
                body=WorkflowsUsNdUpdateDocstarsEarlyTerminationDateSchema().dump(g.api_data),  # type: ignore
                headers=headers_copy,
            )
        except Exception as e:
            logging.error(e)
            interface.set_firestore_early_termination_status(
                ExternalSystemRequestStatus.FAILURE,
            )
            return jsonify_response(
                f"An unknown error occurred while queueing the handle_update_docstars_early_termination_date task: {e}",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        logging.info("Enqueued handle_update_docstars_early_termination_date task")

        interface.set_firestore_early_termination_status(
            ExternalSystemRequestStatus.IN_PROGRESS,
        )

        return make_response(jsonify(), HTTPStatus.OK)

    @workflows_api.post(
        "/external_request/<state>/handle_update_docstars_early_termination_date"
    )
    def handle_update_docstars_early_termination_date(
        state: str,  # pylint: disable=unused-argument
    ) -> Response:
        cloud_task_body = get_cloud_task_json_body()
        person_external_id = cloud_task_body.get("person_external_id", None)

        if person_external_id is None:
            logging.error("No person_external_id provided")
            return jsonify_response(
                "Person_external_id missing. Requests must have a person_external_id "
                "in order to make the DOCSTARS request and update firestore.",
                HTTPStatus.BAD_REQUEST,
            )

        interface = WorkflowsUsNdExternalRequestInterface(person_external_id)

        try:
            # Validate schema
            data = load_api_schema(
                WorkflowsUsNdUpdateDocstarsEarlyTerminationDateSchema, cloud_task_body
            )
            data = WorkflowsUsNdUpdateDocstarsEarlyTerminationDateSchema().dump(data)
            interface.update_early_termination_date(
                user_email=data["user_email"],
                early_termination_date=data["early_termination_date"],
                justification_reasons=data["justification_reasons"],
            )
        except Exception as e:
            logging.error("Write to DOCSTARS failed due to error: %s", e)
            interface.set_firestore_early_termination_status(
                ExternalSystemRequestStatus.FAILURE,
            )

            return jsonify_response(
                f"Early termination date was not updated in DOCSTARS. Error: {e}",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        interface.set_firestore_early_termination_status(
            ExternalSystemRequestStatus.SUCCESS,
        )
        return jsonify_response(
            "Early termination date successfully updated in DOCSTARS.",
            HTTPStatus.OK,
        )

    @workflows_api.post("/external_request/<state>/email_user")
    @requires_api_schema(WorkflowsEmailUserSchema)
    def email_user(
        state: str,
    ) -> Response:
        # Validate the email from the request matches the signed-in user
        # This is currently only used in Texas for emailing a google maps route link
        if state.upper() != StateCode.US_TX.value:
            return jsonify_response(
                f"Unsupported state for user emails: {state}",
                HTTPStatus.UNAUTHORIZED,
            )
        recipient_email = g.api_data["user_email"]
        if recipient_email != g.authenticated_user_email:
            return jsonify_response(
                "user_email in request body does not match authenticated user",
                HTTPStatus.UNAUTHORIZED,
            )

        sendgrid = (
            SendGridClientWrapper(key_type="enforced_tls_only")
            if state in get_enforced_tls_only_state_codes() and not g.is_recidiviz_user
            else SendGridClientWrapper()
        )

        subject = g.api_data["email_subject"]
        html_content = g.api_data["email_body"]
        reply_to_address = "feedback@recidiviz.org"
        reply_to_name = "Recidiviz Support"

        try:
            sendgrid.send_message(
                to_email=recipient_email,
                from_email="no-reply@recidiviz.org",
                from_email_name="Recidiviz",
                subject=subject,
                html_content=html_content,
                reply_to_email=reply_to_address,
                reply_to_name=reply_to_name,
                disable_unsubscribe=True,
            )
            logging.info('Emailed %s with the subject "%s"', recipient_email, subject)
            return jsonify_response(
                "Email successfully sent.",
                HTTPStatus.OK,
            )
        except Exception as e:
            logging.error(
                "Error while trying to send email to %s: %s", recipient_email, e
            )
            return jsonify_response(
                f"Error while trying to send email. Error: {e}",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def build_routes_api_request(
        origin: str,
        intermediate_place_ids: List[str],
        destination_place_id: Optional[str] = None,
        destination_address: Optional[str] = None,
    ) -> dict:
        """
        Builds a type-safe request payload for the Google Routes API computeRoutes endpoint.

        Args:
            origin: The starting address as a string
            intermediate_place_ids: List of Google Maps Place IDs for waypoints to optimize
            destination_place_id: Google Maps Place ID for the destination (mutually exclusive with destination_address)
            destination_address: Address string for the destination (mutually exclusive with destination_place_id)

        Returns:
            Dictionary formatted for the Google Routes API with waypoint optimization enabled
        """
        # destination_address: used when the user provides a custom ending address (typed in the UI).
        # destination_place_id: used when no custom ending address is provided and the last
        # waypoint's place ID serves as the destination.
        if destination_address:
            destination = {"address": destination_address}
        elif destination_place_id:
            destination = {"placeId": destination_place_id}
        else:
            raise ValueError(
                "Either destination_place_id or destination_address must be provided"
            )

        return {
            "origin": {"address": origin},
            "destination": destination,
            "intermediates": [{"placeId": pid} for pid in intermediate_place_ids],
            "travelMode": "DRIVE",
            "optimizeWaypointOrder": True,
        }

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=8),
        reraise=True,
    )
    def _call_google_routes_api(
        request_body: dict,
        api_key: str,
    ) -> requests.Response:
        response = requests.post(
            "https://routes.googleapis.com/directions/v2:computeRoutes",
            json=request_body,
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": api_key,
                "X-Goog-FieldMask": "routes.optimizedIntermediateWaypointIndex",
            },
            timeout=10,
        )
        response.raise_for_status()
        return response

    @workflows_api.post("/external_request/<state>/optimize_route")
    @requires_api_schema(WorkflowsOptimizeRouteSchema)
    def optimize_route(state: str) -> Response:
        """
        Optimizes the order of waypoints for a route using Google Routes API.
        Currently only supported for Texas (US_TX).

        If a destination is provided, it will be used as the ending point and all
        waypoints will be treated as intermediate stops. Otherwise, the last
        waypoint will be treated as the destination.
        """
        if state.upper() != StateCode.US_TX.value:
            return jsonify_response(
                f"Unsupported state for route optimization: {state}",
                HTTPStatus.UNAUTHORIZED,
            )

        origin = g.api_data["origin"]
        waypoints = g.api_data["waypoints"]
        # Optional ending address - if provided, all waypoints become intermediates
        destination = g.api_data.get("destination")

        if len(waypoints) < 2:
            return jsonify_response(
                "At least 2 waypoints required for optimization",
                HTTPStatus.BAD_REQUEST,
            )

        google_api_key = get_secret("google_routes_api_key")
        if not google_api_key:
            logging.error("Google Routes API key not found in secrets")
            return jsonify_response(
                "Route optimization service unavailable",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        # If a destination address is provided, all waypoints become intermediates.
        # Otherwise, the last waypoint becomes the destination.
        if destination:
            intermediate_place_ids = [wp["place_id"] for wp in waypoints]
            dest_kwargs = {"destination_address": destination}
        else:
            intermediate_place_ids = [wp["place_id"] for wp in waypoints[:-1]]
            dest_kwargs = {"destination_place_id": waypoints[-1]["place_id"]}

        google_request = build_routes_api_request(
            origin=origin,
            intermediate_place_ids=intermediate_place_ids,
            **dest_kwargs,
        )

        try:
            response = _call_google_routes_api(google_request, google_api_key)
            result = response.json()

            if "routes" not in result or len(result["routes"]) == 0:
                return jsonify_response(
                    "No route found",
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )

            optimized_indices = result["routes"][0].get(
                "optimizedIntermediateWaypointIndex", []
            )

            original_order = [wp["pseudonymized_id"] for wp in waypoints]

            if optimized_indices:
                optimized_order = [
                    waypoints[i]["pseudonymized_id"] for i in optimized_indices
                ]
                # If no destination was provided, the last waypoint was used as
                # destination (not in intermediates), so add it at the end
                if not destination:
                    optimized_order.append(waypoints[-1]["pseudonymized_id"])
            else:
                # No optimization returned, use original order
                optimized_order = original_order

            # Google returns optimizedIntermediateWaypointIndex even when the
            # order hasn't changed, so we compare against original_order explicitly.
            is_changed = optimized_order != original_order

            logging.info(
                "Route optimized for %d waypoints. Order changed: %s",
                len(waypoints),
                is_changed,
            )

            return jsonify({"optimizedOrder": optimized_order, "isChanged": is_changed})

        except requests.RequestException as e:
            logging.error("Error calling Google Routes API: %s", e)
            return jsonify_response(
                "Route optimization service unavailable",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        except Exception as e:
            logging.error("Unexpected error during route optimization: %s", e)
            return jsonify_response(
                "Route optimization failed", HTTPStatus.INTERNAL_SERVER_ERROR
            )

    @workflows_api.get("/<state>/opportunities")
    @workflows_api.response(HTTPStatus.OK, WorkflowsConfigurationsResponseSchema)
    def get_opportunities(state: str) -> dict:
        state_code = state.upper()
        if state_code not in get_workflows_enabled_states():
            return {"enabled_configs": {}}

        feature_variants: List[str] = list(g.get("feature_variants", {}).keys())
        if g.is_recidiviz_user and "featureVariants" in request.args:
            feature_variants = request.args["featureVariants"].split(",")

        querier = WorkflowsQuerier(StateCode(state_code))

        opps = querier.get_enabled_opportunities(
            [WorkflowsSystemType.INCARCERATION, WorkflowsSystemType.SUPERVISION],
            feature_variants,
        )

        opp_types = [o.opportunity_type for o in opps]

        enabled_configs = querier.get_top_config_for_opportunity_types(
            opp_types, feature_variants
        )

        config_response = {
            opp.opportunity_type: OpportunityConfigResponse.from_opportunity_and_config(
                opp, enabled_configs[opp.opportunity_type]
            ).to_dict()
            for opp in opps
            if opp.opportunity_type in enabled_configs
        }

        return {"enabled_configs": config_response}

    return workflows_api
