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
"""Entry point for Application Data Import service."""
import logging
import os
from http import HTTPStatus
from typing import Tuple

from flask import Flask, request
from google.cloud import pubsub
from google.protobuf import json_format
from google.protobuf.json_format import ParseError

from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_views import (
    PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
)
from recidiviz.case_triage.ops_routes import CASE_TRIAGE_DB_OPERATIONS_QUEUE
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
)
from recidiviz.metrics.export.export_config import (
    DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.persistence.database.schema.pathways import schema as pathways_schema
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    get_database_entity_by_table_name,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.metadata import CloudRunMetadata
from recidiviz.utils.string import StrictStringFormatter

app = Flask(__name__)


if in_gcp():
    cloud_run_metadata = CloudRunMetadata.build_from_metadata_server(
        "application-data-import"
    )
else:
    cloud_run_metadata = CloudRunMetadata(
        project_id="123",
        region="us-central1",
        url="http://localhost:5000",
        service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
    )


@app.route("/import/pathways/<state_code>/<filename>", methods=["POST"])
def _import_pathways(state_code: str, filename: str) -> Tuple[str, HTTPStatus]:
    """Imports a CSV file from GCS into the Pathways Cloud SQL database"""
    if not StateCode.is_state_code(state_code.upper()):
        return (
            f"Unknown state_code [{state_code}] received, must be a valid state code.",
            HTTPStatus.BAD_REQUEST,
        )

    view_builder = None
    for builder in PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS:
        if f"{builder.view_id}.csv" == filename:
            view_builder = builder
    if not view_builder:
        return (
            f"Invalid filename {filename}, must match a Pathways event-level view",
            HTTPStatus.BAD_REQUEST,
        )

    try:
        db_entity = get_database_entity_by_table_name(
            pathways_schema, view_builder.view_id
        )
    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    csv_path = GcsfsFilePath.from_absolute_path(
        os.path.join(
            _dashboard_event_level_bucket(),
            state_code + "/" + filename,
        )
    )
    import_gcs_csv_to_cloud_sql(
        SchemaType.PATHWAYS,
        db_entity,
        csv_path,
        view_builder.columns,
        db_name=state_code.lower(),
    )
    logging.info("View (%s) successfully imported", view_builder.view_id)

    return "", HTTPStatus.OK


@app.route("/import/trigger_pathways", methods=["POST"])
def _import_trigger_pathways() -> Tuple[str, HTTPStatus]:
    """Exposes an endpoint to trigger standard GCS imports."""
    data = request.get_json()
    if not isinstance(data, dict) or "message" not in data:
        return "Invalid Pub/Sub message", HTTPStatus.BAD_REQUEST

    try:
        message = json_format.ParseDict(data["message"], pubsub.types.PubsubMessage())
    except ParseError:
        return "Invalid Pub/Sub message", HTTPStatus.BAD_REQUEST
    else:
        if not message.attributes:
            return "Invalid Pub/Sub message", HTTPStatus.BAD_REQUEST

    attributes = message.attributes

    bucket_id = attributes["bucketId"]
    object_id = attributes["objectId"]
    if "gs://" + bucket_id != _dashboard_event_level_bucket():
        return (
            f"/trigger_pathways is only configured for the dashboard-event-level-data bucket, saw {bucket_id}",
            HTTPStatus.BAD_REQUEST,
        )

    cloud_task_manager = CloudTaskQueueManager(
        queue_info_cls=CloudTaskQueueInfo, queue_name=CASE_TRIAGE_DB_OPERATIONS_QUEUE
    )
    cloud_task_manager.create_task(
        absolute_uri=f"{cloud_run_metadata.url}/import/pathways/{object_id}",
        service_account_email=cloud_run_metadata.service_account_email,
    )
    logging.info("Enqueued gcs_import task to %s", CASE_TRIAGE_DB_OPERATIONS_QUEUE)
    return "", HTTPStatus.OK


def _dashboard_event_level_bucket() -> str:
    return StrictStringFormatter().format(
        DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
        project_id=metadata.project_id(),
    )
