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
"""Endpoints for Workflows ETL"""
import logging
import os
from http import HTTPStatus
from typing import List, Tuple

from flask import Blueprint, request

from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
    get_cloud_task_json_body,
)
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.pubsub_helper import OBJECT_ID, extract_pubsub_message_from_json
from recidiviz.workflows.etl.archive import archive_etl_file
from recidiviz.workflows.etl.regions.us_nd.client_record_etl_delegate import (
    ClientRecordETLDelegate as UsNdClientRecordETLDelegate,
)
from recidiviz.workflows.etl.regions.us_tn.client_record_etl_delegate import (
    ClientRecordETLDelegate as UsTnClientRecordETLDelegate,
)
from recidiviz.workflows.etl.regions.us_tn.compliant_reporting_referral_record_etl_delegate import (
    CompliantReportingReferralRecordETLDelegate,
)
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsETLDelegate
from recidiviz.workflows.etl.workflows_opportunity_etl_delegate import (
    WorkflowsOpportunityETLDelegate,
)
from recidiviz.workflows.etl.workflows_staff_etl_delegate import (
    WorkflowsStaffETLDelegate,
)

WORKFLOWS_ETL_OPERATIONS_QUEUE = "workflows-etl-operations-queue"


def get_workflows_delegates() -> List[WorkflowsETLDelegate]:
    return [
        UsTnClientRecordETLDelegate(),
        UsNdClientRecordETLDelegate(),
        CompliantReportingReferralRecordETLDelegate(),
        WorkflowsOpportunityETLDelegate(),
        WorkflowsStaffETLDelegate(),
    ]


def get_workflows_etl_blueprint() -> Blueprint:
    """Creates a Flask Blueprint for Workflows ETL routes."""
    workflows_etl_blueprint = Blueprint("practices-etl", __name__)

    @workflows_etl_blueprint.route("/handle_workflows_firestore_etl", methods=["POST"])
    @requires_gae_auth
    def _handle_workflows_firestore_etl() -> Tuple[str, HTTPStatus]:
        """Called from a Cloud Storage Notification when a new file is exported to the practices-etl-data bucket
        It enqueues a task to ETL the data into Firestore."""
        try:
            message = extract_pubsub_message_from_json(request.get_json())
        except Exception as e:
            return str(e), HTTPStatus.BAD_REQUEST

        if not message.attributes:
            return "Invalid Pub/Sub message", HTTPStatus.BAD_REQUEST

        attributes = message.attributes
        region_code, filename = os.path.split(attributes[OBJECT_ID])

        if not region_code:
            logging.info("Missing region, ignoring")
            return "Missing region, ignoring", HTTPStatus.OK

        # Ignore staged files
        if "staging" in region_code:
            return "", HTTPStatus.OK

        cloud_task_manager = CloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo, queue_name=WORKFLOWS_ETL_OPERATIONS_QUEUE
        )
        cloud_task_manager.create_task(
            relative_uri="/practices-etl/_run_firestore_etl",
            body={"filename": filename, "state_code": region_code},
        )
        logging.info(
            "Enqueued _run_firestore_etl task to %s", WORKFLOWS_ETL_OPERATIONS_QUEUE
        )
        return "", HTTPStatus.OK

    @workflows_etl_blueprint.route("/_run_firestore_etl", methods=["POST"])
    @requires_gae_auth
    def _run_firestore_etl() -> Tuple[str, HTTPStatus]:
        """This endpoint is triggered by a CloudTask created by _handle_workflows_firestore_etl"""
        body = get_cloud_task_json_body()
        filename = body.get("filename")
        state_code = body.get("state_code")

        if not filename or not state_code:
            return (
                "Must include filename and state_code in the request body",
                HTTPStatus.BAD_REQUEST,
            )

        for delegate in get_workflows_delegates():
            try:
                if delegate.supports_file(state_code, filename):
                    delegate.run_etl(state_code, filename)
            except ValueError as e:
                logging.error(str(e))
                logging.info(
                    "Error running Firestore ETL for file %s for state_code %s",
                    filename,
                    state_code,
                )
                return "", HTTPStatus.OK

        return "", HTTPStatus.OK

    # This endpoint is triggered by a pub/sub subscription on the GCS bucket.
    # To trigger it manually, run (substituting PROJECT_ID and FILENAME):
    # `gcloud pubsub topics publish storage-notification-$PROJECT_ID-practices-etl-data --attribute=objectId=$FILENAME`
    @workflows_etl_blueprint.route("/archive-file", methods=["POST"])
    @requires_gae_auth
    def _archive_file() -> Tuple[str, HTTPStatus]:
        try:
            message = extract_pubsub_message_from_json(request.get_json())
        except Exception as e:
            return str(e), HTTPStatus.BAD_REQUEST

        if not message.attributes:
            return "Invalid Pub/Sub message", HTTPStatus.BAD_REQUEST

        attributes = message.attributes

        filename = attributes[OBJECT_ID]
        if filename is None:
            return "Missing filename", HTTPStatus.BAD_REQUEST

        # ignore temp files generated by export
        if not filename.startswith("staging"):
            archive_etl_file(filename)

        return "", HTTPStatus.OK

    return workflows_etl_blueprint
