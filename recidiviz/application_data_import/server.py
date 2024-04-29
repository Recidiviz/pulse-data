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
import re
from http import HTTPStatus
from typing import Tuple

import google.cloud.pubsub_v1 as pubsub
import pandas as pd
from flask import Flask, request
from google.api_core.exceptions import AlreadyExists
from sqlalchemy import delete

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_views import (
    PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.impact.impact_dashboard_views import (
    IMPACT_DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.calculator.query.state.views.outliers.outliers_views import (
    OUTLIERS_ARCHIVE_VIEW_BUILDERS,
    OUTLIERS_VIEW_BUILDERS,
)
from recidiviz.case_triage.pathways.enabled_metrics import get_metrics_for_entity
from recidiviz.case_triage.pathways.metric_cache import PathwaysMetricCache
from recidiviz.case_triage.pathways.pathways_database_manager import (
    PathwaysDatabaseManager,
)
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    SingleCloudTaskQueueManager,
)
from recidiviz.metrics.export.export_config import (
    DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
    OUTLIERS_VIEWS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.outliers import schema as outliers_schema
from recidiviz.persistence.database.schema.pathways import schema as pathways_schema
from recidiviz.persistence.database.schema.pathways.schema import MetricMetadata
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_database_entity_by_table_name,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.utils import metadata, structured_logging
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.metadata import CloudRunMetadata
from recidiviz.utils.pubsub_helper import (
    BUCKET_ID,
    OBJECT_ID,
    extract_pubsub_message_from_json,
)
from recidiviz.utils.string import StrictStringFormatter

app = Flask(__name__)


if in_gcp():
    structured_logging.setup()
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

PATHWAYS_DB_IMPORT_QUEUE = "pathways-db-import"
OUTLIERS_DB_IMPORT_QUEUE = "outliers-db-import"


def _dashboard_event_level_bucket() -> str:
    return StrictStringFormatter().format(
        DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
        project_id=metadata.project_id(),
    )


def _outliers_bucket() -> str:
    return StrictStringFormatter().format(
        OUTLIERS_VIEWS_OUTPUT_DIRECTORY_URI,
        project_id=metadata.project_id(),
    )


@app.route("/import/pathways/<state_code>/<filename>", methods=["POST"])
def _import_pathways(state_code: str, filename: str) -> Tuple[str, HTTPStatus]:
    """Imports a CSV file from GCS into the Pathways Cloud SQL database"""
    # TODO(#20600): Create a helper for the shared logic between this endpoint and /import/outliers
    if not StateCode.is_state_code(state_code.upper()):
        return (
            f"Unknown state_code [{state_code}] received, must be a valid state code.",
            HTTPStatus.BAD_REQUEST,
        )

    view_builder = None
    view_id = ""
    for pathways_view_builder in PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS:
        if f"{pathways_view_builder.view_id}.csv" == filename:
            view_builder = pathways_view_builder.delegate
            view_id = pathways_view_builder.view_id
    for impact_view_builder in IMPACT_DASHBOARD_VIEW_BUILDERS:
        if f"{impact_view_builder.view_id}.csv" == filename:
            view_builder = impact_view_builder
            view_id = impact_view_builder.view_id
    if not view_builder and not view_id:
        return (
            f"Invalid filename {filename}, must match a Pathways event-level view or impact dashboard view",
            HTTPStatus.BAD_REQUEST,
        )
    if not isinstance(view_builder, SelectedColumnsBigQueryViewBuilder):
        return (
            f"Unexpected view builder type found when importing {filename}",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    try:
        db_entity = get_database_entity_by_table_name(pathways_schema, view_id)

    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    csv_path = GcsfsFilePath.from_absolute_path(
        os.path.join(
            _dashboard_event_level_bucket(),
            state_code + "/" + filename,
        )
    )

    database_key = PathwaysDatabaseManager.database_key_for_state(state_code)
    import_gcs_csv_to_cloud_sql(
        database_key=database_key,
        model=db_entity,
        gcs_uri=csv_path,
        columns=view_builder.columns,
    )
    logging.info("View (%s) successfully imported", view_id)

    gcsfs = GcsfsFactory.build()
    object_metadata = gcsfs.get_metadata(csv_path) or {}
    last_updated = object_metadata.get("last_updated", None)
    if last_updated:
        with SessionFactory.using_database(database_key=database_key) as session:
            # Replace any existing entries with this state code + metric with the new one
            session.execute(
                delete(MetricMetadata).where(
                    MetricMetadata.metric == db_entity.__name__
                )
            )
            session.add(
                MetricMetadata(
                    metric=db_entity.__name__,
                    last_updated=last_updated,
                )
            )

    metric_cache = PathwaysMetricCache.build(StateCode(state_code))
    for metric in get_metrics_for_entity(db_entity):
        metric_cache.reset_cache(metric)

    return "", HTTPStatus.OK


@app.route("/import/trigger_pathways", methods=["POST"])
def _import_trigger_pathways() -> Tuple[str, HTTPStatus]:
    """Exposes an endpoint to trigger standard GCS imports."""

    try:
        message = extract_pubsub_message_from_json(request.get_json())
    except Exception as e:
        return str(e), HTTPStatus.BAD_REQUEST

    try:
        create_import_task_helper(
            request_path=request.path,
            message=message,
            gcs_bucket=_dashboard_event_level_bucket(),
            import_queue_name=PATHWAYS_DB_IMPORT_QUEUE,
            task_prefix="import-pathways",
            task_url="/import/pathways",
        )
    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    return "", HTTPStatus.OK


@app.route("/import/trigger_outliers", methods=["POST"])
def _import_trigger_outliers() -> Tuple[str, HTTPStatus]:
    """Exposes an endpoint to trigger standard GCS imports for outliers."""

    try:
        message = extract_pubsub_message_from_json(request.get_json())
    except Exception as e:
        return str(e), HTTPStatus.BAD_REQUEST

    try:
        request_path = request.path
        gcs_bucket = _outliers_bucket()

        _, object_id = _validate_bucket_and_object_id(
            request_path=request_path, gcs_bucket=gcs_bucket, message=message
        )

        _, file = object_id.split("/")
        _, file_type = file.split(".")

        if file_type == "csv":
            create_import_task_helper(
                request_path=request_path,
                message=message,
                gcs_bucket=gcs_bucket,
                import_queue_name=OUTLIERS_DB_IMPORT_QUEUE,
                task_prefix="import-outliers",
                task_url="/import/outliers",
            )
        elif file_type == "json":
            create_import_task_helper(
                request_path=request_path,
                message=message,
                gcs_bucket=gcs_bucket,
                import_queue_name=OUTLIERS_DB_IMPORT_QUEUE,
                task_prefix="import-outliers-utils",
                task_url="/import/outliers/utils",
            )
        else:
            error_msg = f"Unexpected handling of file type .{file_type} for file {file}"
            logging.error(error_msg)
            return error_msg, HTTPStatus.BAD_REQUEST

    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    return "", HTTPStatus.OK


def create_import_task_helper(
    request_path: str,
    message: pubsub.types.PubsubMessage,
    gcs_bucket: str,
    import_queue_name: str,
    task_prefix: str,
    task_url: str,
) -> None:
    """
    Creates a CloudTask to import a file from a bucket via an import endpoint.

    :param message: The Pub/Sub message that triggered this endpoint
    :param gcs_bucket: The bucket that is expected to have sent the Pub/Sub message
    :param import_queue_name: The name of the TaskQueue that the import task should be queued to
    :param task_prefix: A more general prefix to use for the task id
    :param task_url: The endpoint that will handle the task
    :rtype: None
    """

    _, object_id = _validate_bucket_and_object_id(
        request_path=request_path, gcs_bucket=gcs_bucket, message=message
    )

    cloud_task_manager = SingleCloudTaskQueueManager(
        queue_info_cls=CloudTaskQueueInfo, queue_name=import_queue_name
    )

    task_id = re.sub(r"[^a-zA-Z0-9_-]", "-", f"{task_prefix}-{object_id}")

    try:
        cloud_task_manager.create_task(
            absolute_uri=f"{cloud_run_metadata.url}{task_url}/{object_id}",
            service_account_email=cloud_run_metadata.service_account_email,
            task_id=task_id,  # deduplicate import requests for the same file
        )
        logging.info("Enqueued gcs_import task to %s", import_queue_name)
    except AlreadyExists:
        logging.info(
            "Skipping enqueueing of %s because it is already being imported",
            task_id,
        )


def _validate_bucket_and_object_id(
    request_path: str, gcs_bucket: str, message: pubsub.types.PubsubMessage
) -> Tuple[str, str]:
    """
    Validates the bucket id and object id from the request and returns both if valid.
    """
    if not message.attributes:
        logging.error("Invalid Pub/Sub message")
        raise ValueError("Invalid Pub/Sub message")

    attributes = message.attributes

    bucket_id = attributes[BUCKET_ID]
    object_id = attributes[OBJECT_ID]

    if "gs://" + bucket_id != gcs_bucket:
        logging.error(
            "%s is only configured for the %s bucket, saw %s",
            request_path,
            gcs_bucket,
            bucket_id,
        )
        raise ValueError(
            f"{request_path} is only configured for the {gcs_bucket} bucket, saw {bucket_id}"
        )

    obj_id_parts = object_id.split("/")
    if len(obj_id_parts) != 2:
        logging.error(
            "Invalid object ID %s, must be of format <state_code>/<filename>", object_id
        )
        raise ValueError(
            f"Invalid object ID {object_id}, must be of format <state_code>/<filename>"
        )

    return bucket_id, object_id


@app.route("/import/outliers/<state_code>/<filename>", methods=["POST"])
def _import_outliers(state_code: str, filename: str) -> Tuple[str, HTTPStatus]:
    """Imports a CSV file from GCS into the Outliers Cloud SQL database"""
    # TODO(#20600): Create a helper for the shared logic between this endpoint and /import/outliers
    if not StateCode.is_state_code(state_code.upper()):
        return (
            f"Unknown state_code [{state_code}] received, must be a valid state code.",
            HTTPStatus.BAD_REQUEST,
        )

    view_builder = None
    for builder in OUTLIERS_VIEW_BUILDERS:
        if f"{builder.view_id}.csv" == filename:
            view_builder = builder
    if not view_builder:
        return (
            f"Invalid filename {filename}, must match a Outliers view",
            HTTPStatus.BAD_REQUEST,
        )

    if view_builder in OUTLIERS_ARCHIVE_VIEW_BUILDERS:
        return (
            "",
            HTTPStatus.OK,
        )

    if not isinstance(view_builder, SelectedColumnsBigQueryViewBuilder):
        return (
            f"Unexpected view builder delegate found when importing {filename}",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    try:
        db_entity = get_database_entity_by_table_name(
            outliers_schema, view_builder.view_id
        )

    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    csv_path = GcsfsFilePath.from_absolute_path(
        os.path.join(
            _outliers_bucket(),
            state_code + "/" + filename,
        )
    )

    outliers_db_manager = StateSegmentedDatabaseManager(
        get_outliers_enabled_states(), SchemaType.OUTLIERS
    )

    database_key = outliers_db_manager.database_key_for_state(state_code)
    import_gcs_csv_to_cloud_sql(
        database_key,
        db_entity,
        csv_path,
        view_builder.columns,
    )
    logging.info("View (%s) successfully imported", view_builder.view_id)

    return "", HTTPStatus.OK


@app.route("/import/outliers/utils/<state_code>/<filename>", methods=["POST"])
def _import_outliers_convert_json_to_csv(
    state_code: str, filename: str
) -> Tuple[str, HTTPStatus]:
    """Exports a JSON file from the Outlies GCS bucket into a CSV file in the same bucket"""
    # TODO(#20600): Create a helper for the shared logic between this endpoint and /import/outliers
    if not StateCode.is_state_code(state_code.upper()):
        return (
            f"Unknown state_code [{state_code}] received, must be a valid state code.",
            HTTPStatus.BAD_REQUEST,
        )

    file_name, _ = filename.split(".")

    view_builder = None
    for builder in OUTLIERS_VIEW_BUILDERS:
        if f"{builder.view_id}.json" == filename:
            view_builder = builder
    if not view_builder:
        return (
            f"Invalid filename {filename}, must match a Outliers view",
            HTTPStatus.BAD_REQUEST,
        )

    if view_builder in OUTLIERS_ARCHIVE_VIEW_BUILDERS:
        return (
            "",
            HTTPStatus.OK,
        )

    if not isinstance(view_builder, SelectedColumnsBigQueryViewBuilder):
        return (
            f"Unexpected view builder delegate found when importing {filename}",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    try:
        db_entity = get_database_entity_by_table_name(
            outliers_schema, view_builder.view_id
        )

    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    json_path = GcsfsFilePath.from_absolute_path(
        os.path.join(
            _outliers_bucket(),
            state_code + "/" + filename,
        )
    )

    gcsfs = GcsfsFactory.build()
    with gcsfs.open(json_path) as fp:
        df = pd.read_json(
            fp,
            # Read the file as a JSON object per line
            lines=True,
            orient="records",
            # Read all columns as strings into the DataFrame
            dtype={c.name: "object" for c in db_entity.__table__.columns},
        )

        destination_path = GcsfsFilePath.from_absolute_path(
            f"gs://{_outliers_bucket()}/{state_code.upper()}/{file_name}.csv"
        )

        gcsfs.upload_from_string(
            path=destination_path,
            contents=df.to_csv(
                # Don't include an index column
                index=False
            ),
            content_type="application/octet-stream",
        )

    logging.info("JSON of view (%s) successfully written as CSV", view_builder.view_id)

    return "", HTTPStatus.OK
