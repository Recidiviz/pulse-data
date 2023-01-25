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
from http import HTTPStatus

import requests
from flask import Blueprint, Response, current_app, g, jsonify, make_response, request
from flask_wtf.csrf import generate_csrf

from recidiviz.case_triage.api_schemas_utils import load_api_schema, requires_api_schema
from recidiviz.case_triage.authorization_utils import build_authorization_handler
from recidiviz.case_triage.workflows.api_schemas import (
    WorkflowsUsTnHandleInsertTEPEContactNoteSchema,
    WorkflowsUsTnInsertTEPEContactNoteSchema,
)
from recidiviz.case_triage.workflows.constants import ExternalSystemRequestStatus
from recidiviz.case_triage.workflows.interface import (
    WorkflowsUsTnExternalRequestInterface,
)
from recidiviz.case_triage.workflows.workflows_authorization import (
    on_successful_authorization,
)
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    SingleCloudTaskQueueManager,
    get_cloud_task_json_body,
)
from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.metadata import CloudRunMetadata

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


def create_workflows_api_blueprint() -> Blueprint:
    """Creates the API blueprint for Workflows"""
    workflows_api = Blueprint("workflows", __name__)

    handle_authorization = build_authorization_handler(
        on_successful_authorization, "dashboard_auth0"
    )

    # TODO(#18008): Enable CORS such that requests from dashboard are allowed

    @workflows_api.before_request
    def validate_request() -> None:
        if request.method != "OPTIONS":
            handle_authorization()

    @workflows_api.route("/<state>/init")
    def init(state: str) -> Response:  # pylint: disable=unused-argument
        return jsonify({"csrf": generate_csrf(current_app.secret_key)})

    @workflows_api.post("/external_request/<state>/insert_tepe_contact_note")
    @requires_api_schema(WorkflowsUsTnInsertTEPEContactNoteSchema)
    def insert_tepe_contact_note(
        state: str,  # pylint: disable=unused-argument
    ) -> Response:
        person_external_id = g.api_data["person_external_id"]

        interface = WorkflowsUsTnExternalRequestInterface()
        firestore_client = FirestoreClientImpl()
        doc_path = interface.get_contact_note_updates_firestore_path(person_external_id)

        if not g.api_data.get("should_queue_task", True):
            try:
                # set_document will create new firestore document if it doesn't exist
                firestore_client.set_document(
                    doc_path,
                    {
                        "status": ExternalSystemRequestStatus.IN_PROGRESS.value,
                        firestore_client.timestamp_key: datetime.datetime.now(
                            datetime.timezone.utc
                        ),
                    },
                    merge=True,
                )

                interface.insert_tepe_contact_note(
                    person_external_id,
                    g.api_data["user_id"],
                    g.api_data["contact_note_date_time"],
                    g.api_data["contact_note"],
                    g.api_data.get("voters_rights_code", None),
                )
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
            cloud_task_manager.create_task(
                absolute_uri=f"{cloud_run_metadata.url}"
                f"/workflows/external_request/US_TN/handle_insert_tepe_contact_note",
                body={
                    "person_external_id": person_external_id,
                    "user_id": g.api_data["user_id"],
                    "contact_note_date_time": str(g.api_data["contact_note_date_time"]),
                    "contact_note": g.api_data["contact_note"],
                    "voters_rights_code": g.api_data.get("voters_rights_code", None),
                },
                headers=request.headers,
            )
        except Exception as e:
            logging.error(e)
            firestore_client.set_document(
                doc_path,
                {
                    "status": ExternalSystemRequestStatus.FAILURE.value,
                    firestore_client.timestamp_key: datetime.datetime.now(
                        datetime.timezone.utc
                    ),
                },
                merge=True,
            )
            return make_response(
                jsonify(
                    message=f"An unknown error occurred while queueing the handle_insert_tepe_contact_note task: {e}",
                ),
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        logging.info("Enqueued handle_insert_tepe_contact_note task")

        firestore_client.set_document(
            doc_path,
            {
                "status": ExternalSystemRequestStatus.IN_PROGRESS.value,
                firestore_client.timestamp_key: datetime.datetime.now(
                    datetime.timezone.utc
                ),
            },
            merge=True,
        )

        return make_response(jsonify(), HTTPStatus.OK)

    @workflows_api.post("/external_request/<state>/handle_insert_tepe_contact_note")
    def handle_insert_tepe_contact_note(
        state: str,  # pylint: disable=unused-argument
    ) -> Response:
        cloud_task_body = get_cloud_task_json_body()
        person_external_id = cloud_task_body.get("person_external_id", None)

        if person_external_id is None:
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
            data = load_api_schema(
                WorkflowsUsTnHandleInsertTEPEContactNoteSchema, cloud_task_body
            )
            logging.info("Handling contact note: %s", data)

            # The load_api_schema call will validate the schema request, thus the defaults below exist for mypy
            WorkflowsUsTnExternalRequestInterface().insert_tepe_contact_note(
                data.get("person_external_id", ""),
                data.get("user_id", ""),
                data.get("contact_note_date_time", datetime.datetime.now()),
                data.get("contact_note", {}),
                data.get("voters_rights_code", None),
            )
        except Exception as e:
            firestore_client = FirestoreClientImpl()
            firestore_doc_path = WorkflowsUsTnExternalRequestInterface().get_contact_note_updates_firestore_path(
                person_external_id
            )
            firestore_client.update_document(
                firestore_doc_path,
                {"status": ExternalSystemRequestStatus.FAILURE.value},
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
