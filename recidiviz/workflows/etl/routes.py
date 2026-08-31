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

from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    SingleCloudTaskQueueManager,
    get_cloud_task_json_body,
)
from recidiviz.metrics.export.export_config import WORKFLOWS_VIEWS_OUTPUT_DIRECTORY_URI
from recidiviz.tools.archive import archive_etl_file
from recidiviz.utils.metadata import CloudRunMetadata
from recidiviz.utils.pubsub_helper import OBJECT_ID, extract_pubsub_message_from_json
from recidiviz.workflows.etl.typesense_backfill_client import TypesenseBackfillClient
from recidiviz.workflows.etl.workflows_client_etl_delegate import (
    WorkflowsClientETLDelegate,
)
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsETLDelegate
from recidiviz.workflows.etl.workflows_incarceration_staff_etl_delegate import (
    WorkflowsIncarcerationStaffETLDelegate,
)
from recidiviz.workflows.etl.workflows_location_etl_delegate import (
    WorkflowsLocationETLDelegate,
)
from recidiviz.workflows.etl.workflows_opportunity_etl_delegate import (
    WorkflowsOpportunityETLDelegate,
)
from recidiviz.workflows.etl.workflows_resident_etl_delegate import (
    WorkflowsResidentETLDelegate,
)
from recidiviz.workflows.etl.workflows_supervision_staff_etl_delegate import (
    WorkflowsSupervisionStaffETLDelegate,
)
from recidiviz.workflows.etl.workflows_tasks_etl_delegate import (
    WorkflowsTasksETLDelegate,
)

WORKFLOWS_ETL_OPERATIONS_QUEUE = "workflows-etl-operations-queue"

# Queue that drives Typesense backfill triggers. Separate from the ETL queue because a
# backfill runs for minutes and the backfill service refuses triggers it cannot serve, so
# the trigger needs retry timing the ETL task's deadline cannot accommodate.
WORKFLOWS_TYPESENSE_BACKFILL_QUEUE = "workflows-typesense-backfill-queue"

# Endpoint that the Typesense backfill task targets, relative to this blueprint.
TRIGGER_TYPESENSE_BACKFILL_ROUTE = "_trigger_typesense_backfill"

# Keys in the body of a Typesense backfill task. Exactly one of COLLECTION_KEY and
# SOURCE_COLLECTION_KEY is set: the latter for opportunities, whose many Firestore
# collections all feed one Typesense collection.
STATE_CODE_KEY = "state_code"
COLLECTION_KEY = "collection"
SOURCE_COLLECTION_KEY = "source_collection"

# Delegates whose completion should trigger a Typesense backfill in the separate
# search-indexing project. The tasks delegate is intentionally excluded — its collection
# is not indexed in Typesense. Opportunities are handled separately below, since their
# many Firestore collections all feed one Typesense collection.
DELEGATES_TRIGGERING_TYPESENSE_BACKFILL = (
    WorkflowsSupervisionStaffETLDelegate,
    WorkflowsIncarcerationStaffETLDelegate,
    WorkflowsClientETLDelegate,
    WorkflowsResidentETLDelegate,
    WorkflowsLocationETLDelegate,
)


def get_workflows_delegates(state_code: StateCode) -> List[WorkflowsETLDelegate]:
    return [
        WorkflowsOpportunityETLDelegate(state_code),
        WorkflowsSupervisionStaffETLDelegate(state_code),
        WorkflowsIncarcerationStaffETLDelegate(state_code),
        WorkflowsClientETLDelegate(state_code),
        WorkflowsResidentETLDelegate(state_code),
        WorkflowsTasksETLDelegate(state_code),
        WorkflowsLocationETLDelegate(state_code),
    ]


def _typesense_backfill_task_body(
    delegate: WorkflowsETLDelegate, filename: str
) -> dict[str, str] | None:
    """Returns the body of the Typesense backfill task for the collection this ETL just
    wrote, or None when the delegate's collection is not indexed in Typesense.

    Opportunity tasks name the Firestore source collection instead of a Typesense one,
    since every per-state, per-opportunity Firestore collection feeds the single Typesense
    `opportunities` collection."""
    if isinstance(delegate, WorkflowsOpportunityETLDelegate):
        return {
            STATE_CODE_KEY: delegate.state_code.value,
            SOURCE_COLLECTION_KEY: delegate.COLLECTION_BY_FILENAME[filename],
        }

    if not isinstance(delegate, DELEGATES_TRIGGERING_TYPESENSE_BACKFILL):
        return None

    return {
        STATE_CODE_KEY: delegate.state_code.value,
        COLLECTION_KEY: delegate.COLLECTION_BY_FILENAME[filename],
    }


def _maybe_enqueue_typesense_backfill(
    *,
    delegate: WorkflowsETLDelegate,
    filename: str,
    cloud_run_metadata: CloudRunMetadata,
) -> None:
    """Enqueues a task that refreshes the search index for the collection this ETL just
    wrote, for the delegates whose collections are indexed in Typesense.

    The trigger is enqueued rather than sent inline because a backfill takes minutes and
    the backfill service refuses triggers it has no capacity for: on its own queue, a
    refused trigger can wait out the run that refused it and be retried without re-running
    this ETL. Failing to enqueue is logged and does not fail the ETL, since the Firestore
    write has already succeeded and retrying this task would rewrite all of it."""
    body = _typesense_backfill_task_body(delegate, filename)
    if body is None:
        return

    try:
        SingleCloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo,
            queue_name=WORKFLOWS_TYPESENSE_BACKFILL_QUEUE,
        ).create_task(
            absolute_uri=(
                f"{cloud_run_metadata.url}/practices-etl/"
                f"{TRIGGER_TYPESENSE_BACKFILL_ROUTE}"
            ),
            body=body,
            service_account_email=cloud_run_metadata.service_account_email,
        )
    except Exception:
        logging.exception(
            "Failed to enqueue Typesense backfill task with body [%s]", body
        )


def get_workflows_etl_blueprint(cloud_run_metadata: CloudRunMetadata) -> Blueprint:
    """Creates a Flask Blueprint for Workflows ETL routes."""
    workflows_etl_blueprint = Blueprint("practices-etl", __name__)

    @workflows_etl_blueprint.route("/handle_workflows_firestore_etl", methods=["POST"])
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
        if region_code.startswith("staging"):
            return "", HTTPStatus.OK

        # Ignore files that have been exported to the bucket as part of a sandbox export
        if region_code.startswith("sandbox"):
            return "", HTTPStatus.OK

        cloud_task_manager = SingleCloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo, queue_name=WORKFLOWS_ETL_OPERATIONS_QUEUE
        )
        cloud_task_manager.create_task(
            absolute_uri=f"{cloud_run_metadata.url}/practices-etl/_run_firestore_etl",
            body={"filename": filename, "state_code": region_code},
            service_account_email=cloud_run_metadata.service_account_email,
        )
        logging.info(
            "Enqueued _run_firestore_etl task to %s", WORKFLOWS_ETL_OPERATIONS_QUEUE
        )
        return "", HTTPStatus.OK

    @workflows_etl_blueprint.route("/_run_firestore_etl", methods=["POST"])
    async def _run_firestore_etl() -> Tuple[str, HTTPStatus]:
        """This endpoint is triggered by a CloudTask created by _handle_workflows_firestore_etl"""
        body = get_cloud_task_json_body()
        filename = body.get("filename")
        state_code = body.get("state_code")

        if not filename or not state_code:
            return (
                "Must include filename and state_code in the request body",
                HTTPStatus.BAD_REQUEST,
            )

        for delegate in get_workflows_delegates(StateCode(state_code)):
            try:
                if delegate.supports_file(filename):
                    await delegate.run_etl(filename)
                    _maybe_enqueue_typesense_backfill(
                        delegate=delegate,
                        filename=filename,
                        cloud_run_metadata=cloud_run_metadata,
                    )
            except ValueError as e:
                logging.error(str(e))
                logging.info(
                    "Error running Firestore ETL for file %s for state_code %s",
                    filename,
                    state_code,
                )
                return "", HTTPStatus.OK

        return "", HTTPStatus.OK

    @workflows_etl_blueprint.route(
        f"/{TRIGGER_TYPESENSE_BACKFILL_ROUTE}", methods=["POST"]
    )
    def _trigger_typesense_backfill() -> Tuple[str, HTTPStatus]:
        """Re-indexes one collection in Typesense. Triggered by a CloudTask created by
        _maybe_enqueue_typesense_backfill once that collection's Firestore ETL finished.

        Unlike the ETL endpoint, this one lets a failed backfill surface as an error
        response so its queue retries the trigger. Nothing here writes to Firestore, and
        the backfill's prune re-confirms its delete candidates against Firestore, so a
        retry that overlaps a still-running backfill costs duplicated work rather than
        dropped documents — while dropping the trigger leaves the search index stale until
        the next ETL run."""
        body = get_cloud_task_json_body()
        state_code = body.get(STATE_CODE_KEY)
        collection = body.get(COLLECTION_KEY)
        source_collection = body.get(SOURCE_COLLECTION_KEY)

        if not state_code:
            return (
                f"Must include {STATE_CODE_KEY} in the request body",
                HTTPStatus.BAD_REQUEST,
            )

        if source_collection:
            TypesenseBackfillClient().trigger_opportunities_backfill(
                state_code=StateCode(state_code),
                source_collection=source_collection,
            )
            return "", HTTPStatus.OK

        if collection:
            TypesenseBackfillClient().trigger_backfill(
                state_code=StateCode(state_code), collection=collection
            )
            return "", HTTPStatus.OK

        return (
            f"Must include either {COLLECTION_KEY} or {SOURCE_COLLECTION_KEY} in the "
            f"request body",
            HTTPStatus.BAD_REQUEST,
        )

    # This endpoint is triggered by a pub/sub subscription on the GCS bucket.
    # To trigger it manually, run (substituting PROJECT_ID and FILENAME):
    # `gcloud pubsub topics publish storage-notification-$PROJECT_ID-practices-etl-data --attribute=objectId=$FILENAME`
    @workflows_etl_blueprint.route("/archive-file", methods=["POST"])
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
            archive_etl_file(filename, WORKFLOWS_VIEWS_OUTPUT_DIRECTORY_URI)

        return "", HTTPStatus.OK

    return workflows_etl_blueprint
