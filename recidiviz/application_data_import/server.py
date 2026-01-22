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

import google.cloud.pubsub_v1 as pubsub
from flask import Flask, request
from google.cloud import datastore

from recidiviz.auth.auth_endpoint import get_auth_endpoint_blueprint
from recidiviz.backup.backup_manager import backup_manager_blueprint
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_views import (
    PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.calculator.query.state.views.outliers.outliers_views import (
    OUTLIERS_ARCHIVE_VIEW_BUILDERS,
    OUTLIERS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.outliers.supervision_district_managers import (
    SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER,
)
from recidiviz.case_triage.pathways.enabled_metrics import get_metrics_for_entity
from recidiviz.case_triage.pathways.metric_cache import PathwaysMetricCache
from recidiviz.case_triage.shared_pathways.pathways_database_manager import (
    PathwaysDatabaseManager,
)
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import (
    import_gcs_csv_to_cloud_sql,
    import_gcs_file_to_cloud_sql,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    SingleCloudTaskQueueManager,
)
from recidiviz.metrics.export.export_config import (
    DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
    INSIGHTS_VIEWS_DEMO_OUTPUT_DIRECTORY_URI,
    INSIGHTS_VIEWS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.outliers.utils.routes import get_outliers_utils_blueprint
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.insights import schema as insights_schema
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
from recidiviz.workflows.etl.routes import get_workflows_etl_blueprint

app = Flask(__name__)


if in_gcp():
    structured_logging.setup()
    cloud_run_metadata = CloudRunMetadata.build_from_metadata_server(
        CloudRunMetadata.Service.APPLICATION_DATA_IMPORT
    )
else:
    cloud_run_metadata = CloudRunMetadata(
        project_id="123",
        region="us-central1",
        url="http://localhost:5000",
        service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
    )

APPLICATION_DATA_IMPORT_BLUEPRINTS = [
    (backup_manager_blueprint, "/backup_manager"),
    (
        get_workflows_etl_blueprint(cloud_run_metadata=cloud_run_metadata),
        "/practices-etl",
    ),
    (
        get_outliers_utils_blueprint(),
        "/outliers-utils",
    ),
    (
        get_auth_endpoint_blueprint(
            cloud_run_metadata=cloud_run_metadata,
            authentication_middleware=None,
        ),
        "/auth",
    ),
]

for blueprint, url_prefix in APPLICATION_DATA_IMPORT_BLUEPRINTS:
    app.register_blueprint(blueprint, url_prefix=url_prefix)

PATHWAYS_DB_IMPORT_QUEUE = "pathways-db-import-v2"
OUTLIERS_DB_IMPORT_QUEUE = "outliers-db-import-v2"


def _dashboard_event_level_bucket() -> str:
    return StrictStringFormatter().format(
        DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
        project_id=metadata.project_id(),
    )


def _insights_bucket(demo: bool = False) -> str:
    output_dir = (
        INSIGHTS_VIEWS_DEMO_OUTPUT_DIRECTORY_URI
        if demo is True
        else INSIGHTS_VIEWS_OUTPUT_DIRECTORY_URI
    )
    return StrictStringFormatter().format(
        output_dir,
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
    if not view_builder and not view_id:
        return (
            f"Invalid filename {filename}, must match a Pathways event-level view",
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

    database_key = PathwaysDatabaseManager(
        get_pathways_enabled_states_for_cloud_sql(), SchemaType.PATHWAYS
    ).database_key_for_state(state_code)
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
    facility_id_name_map = object_metadata.get("facility_id_name_map", None)
    gender_id_name_map = object_metadata.get("gender_id_name_map", None)
    race_id_name_map = object_metadata.get("race_id_name_map", None)
    dynamic_filter_options = object_metadata.get("dynamic_filter_options", None)

    # Updating/merging the metadata fields separately allows us to maintain any
    # previously set fields that are not included in the current import
    if last_updated:
        with SessionFactory.using_database(database_key=database_key) as session:
            # Replace any existing entries with this state code + metric with the new one
            session.merge(
                MetricMetadata(
                    metric=db_entity.__name__,
                    last_updated=last_updated,
                )
            )

    if facility_id_name_map:
        with SessionFactory.using_database(database_key=database_key) as session:
            # Replace any existing entries with this state code + metric with the new one
            session.merge(
                MetricMetadata(
                    metric=db_entity.__name__,
                    facility_id_name_map=facility_id_name_map,
                )
            )

    if gender_id_name_map:
        with SessionFactory.using_database(database_key=database_key) as session:
            # Replace any existing entries with this state code + metric with the new one
            session.merge(
                MetricMetadata(
                    metric=db_entity.__name__,
                    gender_id_name_map=gender_id_name_map,
                )
            )

    if race_id_name_map:
        with SessionFactory.using_database(database_key=database_key) as session:
            # Replace any existing entries with this state code + metric with the new one
            session.merge(
                MetricMetadata(
                    metric=db_entity.__name__,
                    race_id_name_map=race_id_name_map,
                )
            )

    if dynamic_filter_options:
        with SessionFactory.using_database(database_key=database_key) as session:
            # Replace any existing entries with this state code + metric with the new one
            session.merge(
                MetricMetadata(
                    metric=db_entity.__name__,
                    dynamic_filter_options=dynamic_filter_options,
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
            task_url="/import/pathways",
        )
    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    return "", HTTPStatus.OK


def _should_load_demo_data_into_insights() -> bool:
    """Queries the Datastore Entity to determine if we should load Insights data into demo (versus staging)"""
    datastore_client = datastore.Client(project=metadata.project_id())
    entities = list(datastore_client.query(kind="ProductConfiguration").fetch())
    entity = entities[0]
    load_demo_data_into_insights = entity["loadDemoDataIntoInsights"]
    return load_demo_data_into_insights


@app.route("/import/trigger_insights", methods=["POST"])
def _import_trigger_insights() -> Tuple[str, HTTPStatus]:
    """Exposes an endpoint to trigger standard GCS imports for insights."""

    try:
        message = extract_pubsub_message_from_json(request.get_json())
    except Exception as e:
        return str(e), HTTPStatus.BAD_REQUEST

    if not message.attributes:
        error_str = "Invalid Pub/Sub message"
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    actual_bucket_id = message.attributes[BUCKET_ID]
    load_demo_data_into_insights = _should_load_demo_data_into_insights()
    expected_insights_bucket = _insights_bucket(demo=load_demo_data_into_insights)

    # If loadDemoDataIntoInsights is True and triggering notification is from regular bucket: exit early
    # If loadDemoDataIntoInsights is False and triggering notification is from demo bucket: exit early
    if expected_insights_bucket != f"gs://{actual_bucket_id}":
        error_str = f"loadDemoDataIntoInsights is {load_demo_data_into_insights} but triggering notification is from bucket {actual_bucket_id}"
        logging.error(error_str)
        if load_demo_data_into_insights:
            # If we're planning on loading demo data in, then we expect that the regular exports
            # would still get triggered daily and reach this point- return OK so the pub/sub
            # message is acked.
            return error_str, HTTPStatus.OK
        # If we weren't planning on loading demo data in and we got data in that bucket, return an
        # error so we find out about it.
        return error_str, HTTPStatus.BAD_REQUEST

    try:
        create_import_task_helper(
            request_path=request.path,
            message=message,
            gcs_bucket=expected_insights_bucket,
            import_queue_name=OUTLIERS_DB_IMPORT_QUEUE,
            task_url="/import/insights",
        )
    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    return "", HTTPStatus.OK


def create_import_task_helper(
    request_path: str,
    message: pubsub.types.PubsubMessage,
    gcs_bucket: str,
    import_queue_name: str,
    task_url: str,
) -> None:
    """
    Creates a CloudTask to import a file from a bucket via an import endpoint.

    :param message: The Pub/Sub message that triggered this endpoint
    :param gcs_bucket: The bucket that is expected to have sent the Pub/Sub message
    :param import_queue_name: The name of the TaskQueue that the import task should be queued to
    :param task_url: The endpoint that will handle the task
    :rtype: None
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

    cloud_task_manager = SingleCloudTaskQueueManager(
        queue_info_cls=CloudTaskQueueInfo, queue_name=import_queue_name
    )

    cloud_task_manager.create_task(
        absolute_uri=f"{cloud_run_metadata.url}{task_url}/{object_id}",
        service_account_email=cloud_run_metadata.service_account_email,
    )
    logging.info("Enqueued gcs_import task to %s", import_queue_name)


@app.route("/import/insights/<state_code>/<filename>", methods=["POST"])
def _import_insights(state_code: str, filename: str) -> Tuple[str, HTTPStatus]:
    """Imports a JSON file from GCS into the Insights Cloud SQL database"""
    # TODO(#20600): Create a helper for the shared logic between this endpoint and /import/outliers
    if not StateCode.is_state_code(state_code.upper()):
        return (
            f"Unknown state_code [{state_code}] received, must be a valid state code.",
            HTTPStatus.BAD_REQUEST,
        )

    view_builder = None
    for builder in OUTLIERS_VIEW_BUILDERS:
        if f"{builder.view_id}.json" == filename:
            view_builder = builder
    if not view_builder:
        return (
            f"Invalid filename {filename}, must match a Outliers view",
            HTTPStatus.BAD_REQUEST,
        )

    # Skip importing archive files and the district managers file for non-email states
    if view_builder in OUTLIERS_ARCHIVE_VIEW_BUILDERS or (
        view_builder.view_id == SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER.view_id
        and StateCode(state_code.upper()) != "US_PA"
    ):
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
            insights_schema, view_builder.view_id
        )

    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    # If loadDemoDataIntoInsights is True and triggering notification is from demo bucket: read data from demo bucket
    # If loadDemoDataIntoInsights is False and triggering notification is from regular bucket: read data from regular bucket
    load_demo_data_into_insights = _should_load_demo_data_into_insights()
    insights_bucket = _insights_bucket(demo=load_demo_data_into_insights)

    json_path = GcsfsFilePath.from_absolute_path(
        os.path.join(
            insights_bucket,
            state_code + "/" + filename,
        )
    )

    insights_db_manager = StateSegmentedDatabaseManager(
        get_outliers_enabled_states(), SchemaType.INSIGHTS
    )

    database_key = insights_db_manager.database_key_for_state(state_code)
    import_gcs_file_to_cloud_sql(
        database_key=database_key,
        model=db_entity,
        gcs_uri=json_path,
        columns=view_builder.columns,
    )
    logging.info("View (%s) successfully imported", view_builder.view_id)

    return "", HTTPStatus.OK
