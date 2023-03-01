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
from http import HTTPStatus
from typing import Optional

import requests
import werkzeug.wrappers
from flask import Blueprint, Response, current_app, g, jsonify, make_response, request
from flask_wtf.csrf import generate_csrf
from werkzeug.http import parse_set_header

from recidiviz.case_triage.api_schemas_utils import load_api_schema, requires_api_schema
from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.case_triage.workflows.api_schemas import (
    ProxySchema,
    WorkflowsUsTnInsertTEPEContactNoteSchema,
)
from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.interface import (
    WorkflowsUsTnExternalRequestInterface,
)
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
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.metadata import CloudRunMetadata
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


def create_workflows_api_blueprint() -> Blueprint:
    """Creates the API blueprint for Workflows"""
    workflows_api = Blueprint("workflows", __name__)
    proxy_endpoint = "workflows.proxy"

    handle_authorization = build_authorization_handler(
        on_successful_authorization, "dashboard_auth0"
    )
    handle_recidiviz_only_authorization = build_authorization_handler(
        on_successful_authorization_recidiviz_only, "dashboard_auth0"
    )

    @workflows_api.before_request
    def validate_request() -> None:
        if request.method == "OPTIONS":
            return
        if request.endpoint == proxy_endpoint:
            handle_recidiviz_only_authorization()
            return
        handle_authorization()

    @workflows_api.before_request
    def validate_cors() -> Optional[Response]:
        if request.endpoint == proxy_endpoint:
            # Proxy requests will generally be sent from a developer's machine and not a browser,
            # so there is no origin to check against.
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

        response = requests.request(
            url=url,
            method=g.api_data.get("method"),
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

    @workflows_api.get("/ip")
    def ip() -> Response:
        ip_response = requests.get("http://curlmyip.org", timeout=10)
        return jsonify({"ip": ip_response.text})

    return workflows_api
