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
from types import ModuleType
from typing import Callable, List, Tuple

import google.cloud.pubsub_v1 as pubsub
from flask import Flask, request
from google.cloud import datastore

from recidiviz.auth.auth_endpoint import get_auth_endpoint_blueprint
from recidiviz.backup.backup_manager import backup_manager_blueprint
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
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
from recidiviz.calculator.query.state.views.public_pathways.public_pathways_enabled_states import (
    get_public_pathways_enabled_states_for_cloud_sql,
)
from recidiviz.calculator.query.state.views.public_pathways.public_pathways_views import (
    PUBLIC_PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS,
)
from recidiviz.case_triage.pathways.enabled_metrics import get_metrics_for_entity
from recidiviz.case_triage.shared_pathways.metric_cache import PathwaysMetricCache
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
    PUBLIC_PATHWAYS_VIEWS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.outliers.utils.routes import get_outliers_utils_blueprint
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.insights import schema as insights_schema
from recidiviz.persistence.database.schema.pathways import schema as pathways_schema
from recidiviz.persistence.database.schema.pathways.schema import MetricMetadata
from recidiviz.persistence.database.schema.public_pathways import (
    schema as public_pathways_schema,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_database_entity_by_table_name,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
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
PUBLIC_PATHWAYS_DB_IMPORT_QUEUE = "public-pathways-db-import-v2"
OUTLIERS_DB_IMPORT_QUEUE = "outliers-db-import-v2"


def _dashboard_event_level_bucket() -> str:
    return StrictStringFormatter().format(
        DASHBOARD_EVENT_LEVEL_VIEWS_OUTPUT_DIRECTORY_URI,
        project_id=metadata.project_id(),
    )


def _public_pathways_data_bucket() -> str:
    return StrictStringFormatter().format(
        PUBLIC_PATHWAYS_VIEWS_OUTPUT_DIRECTORY_URI,
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


def _get_view_builder(
    state_code: str,
    filename: str,
    view_builders: List[SelectedColumnsBigQueryViewBuilder] | List[BigQueryViewBuilder],
) -> SelectedColumnsBigQueryViewBuilder | BigQueryViewBuilder:
    if not StateCode.is_state_code(state_code.upper()):
        raise ValueError(
            f"Unknown state_code [{state_code}] received, must be a valid state code."
        )
    view_builder = None
    view_id = None
    for builder in view_builders:
        if filename in (f"{builder.view_id}.csv", f"{builder.view_id}.json"):
            view_builder = builder
            view_id = builder.view_id
    if not view_builder and not view_id:
        view_ids = ", ".join(builder.view_id for builder in view_builders)
        raise ValueError(f"Invalid filename {filename}, must match one of: {view_ids}")
    if not view_builder:
        raise ValueError(f"View builder not found for {filename}")

    return view_builder


@app.route("/import/pathways/<state_code>/<filename>", methods=["POST"])
def _import_pathways(state_code: str, filename: str) -> Tuple[str, HTTPStatus]:
    """Imports a CSV file from GCS into the Pathways Cloud SQL database"""
    return _import_pathways_helper(
        state_code=state_code,
        filename=filename,
        view_builders=[
            builder.delegate for builder in PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS
        ],
        schema_module=pathways_schema,
        bucket_path=_dashboard_event_level_bucket(),
        enabled_states_getter=get_pathways_enabled_states_for_cloud_sql,
        schema_type=SchemaType.PATHWAYS,
        reset_cache=True,
    )


@app.route("/import/public_pathways/<state_code>/<filename>", methods=["POST"])
def _import_public_pathways(state_code: str, filename: str) -> Tuple[str, HTTPStatus]:
    """Imports a CSV file from GCS into the Public Pathways Cloud SQL database"""
    return _import_pathways_helper(
        state_code=state_code,
        filename=filename,
        view_builders=[
            builder.delegate for builder in PUBLIC_PATHWAYS_EVENT_LEVEL_VIEW_BUILDERS
        ],
        schema_module=public_pathways_schema,
        bucket_path=_public_pathways_data_bucket(),
        enabled_states_getter=get_public_pathways_enabled_states_for_cloud_sql,
        schema_type=SchemaType.PUBLIC_PATHWAYS,
        reset_cache=False,
    )


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


@app.route("/import/trigger_public_pathways", methods=["POST"])
def _import_trigger_public_pathways() -> Tuple[str, HTTPStatus]:
    """Exposes an endpoint to trigger standard GCS imports."""

    try:
        message = extract_pubsub_message_from_json(request.get_json())
    except Exception as e:
        return str(e), HTTPStatus.BAD_REQUEST

    try:
        create_import_task_helper(
            request_path=request.path,
            message=message,
            gcs_bucket=_public_pathways_data_bucket(),
            import_queue_name=PUBLIC_PATHWAYS_DB_IMPORT_QUEUE,
            task_url="/import/public_pathways",
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


def skip_import_for_insights_file(state_code: str, filename: str) -> bool:
    """Determines whether to skip importing a given file based on state and filename."""
    # Skip importing archive files and the district managers file for non-email states
    if filename in OUTLIERS_ARCHIVE_VIEW_BUILDERS or (
        filename == SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER.view_id
        and StateCode(state_code.upper()) != "US_PA"
    ):
        return True
    return False


def _validate_selected_columns_view_builder(
    filename: str, view_builder: BigQueryViewBuilder
) -> SelectedColumnsBigQueryViewBuilder:
    """Validates that the given view_builder is of the correct type and returns it cast to SelectedColumnsBigQueryViewBuilder."""
    if not isinstance(view_builder, SelectedColumnsBigQueryViewBuilder):
        raise TypeError(
            f"Unexpected view builder found when importing {filename}",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    return view_builder


def _merge_metric_metadata(
    database_key: SQLAlchemyDatabaseKey,
    db_entity_name: str,
    last_updated: str | None = None,
    facility_id_name_map: str | None = None,
    gender_id_name_map: str | None = None,
    race_id_name_map: str | None = None,
    dynamic_filter_options: str | None = None,
) -> None:
    """Merges metadata fields into the database separately to maintain previously set fields."""
    if last_updated:
        with SessionFactory.using_database(database_key=database_key) as session:
            session.merge(
                MetricMetadata(
                    metric=db_entity_name,
                    last_updated=last_updated,
                )
            )

    if facility_id_name_map:
        with SessionFactory.using_database(database_key=database_key) as session:
            session.merge(
                MetricMetadata(
                    metric=db_entity_name,
                    facility_id_name_map=facility_id_name_map,
                )
            )

    if gender_id_name_map:
        with SessionFactory.using_database(database_key=database_key) as session:
            session.merge(
                MetricMetadata(
                    metric=db_entity_name,
                    gender_id_name_map=gender_id_name_map,
                )
            )

    if race_id_name_map:
        with SessionFactory.using_database(database_key=database_key) as session:
            session.merge(
                MetricMetadata(
                    metric=db_entity_name,
                    race_id_name_map=race_id_name_map,
                )
            )

    if dynamic_filter_options:
        with SessionFactory.using_database(database_key=database_key) as session:
            session.merge(
                MetricMetadata(
                    metric=db_entity_name,
                    dynamic_filter_options=dynamic_filter_options,
                )
            )


def _import_pathways_helper(
    state_code: str,
    filename: str,
    view_builders: List[SelectedColumnsBigQueryViewBuilder],
    schema_module: ModuleType,
    bucket_path: str,
    enabled_states_getter: Callable[[], list[str]],
    schema_type: SchemaType,
    reset_cache: bool = True,
) -> Tuple[str, HTTPStatus]:
    """Helper function to import CSV files from GCS into a pathways-related Cloud SQL database."""
    try:
        view_builder = _get_view_builder(state_code, filename, view_builders)
    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    try:
        view_builder = _validate_selected_columns_view_builder(filename, view_builder)
        db_entity = get_database_entity_by_table_name(
            schema_module, view_builder.view_id
        )
    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST
    except TypeError as e:
        return str(e), HTTPStatus.INTERNAL_SERVER_ERROR

    csv_path = GcsfsFilePath.from_absolute_path(
        os.path.join(bucket_path, state_code + "/" + filename)
    )

    database_key = PathwaysDatabaseManager(
        enabled_states_getter(), schema_type
    ).database_key_for_state(state_code)

    import_gcs_csv_to_cloud_sql(
        database_key=database_key,
        model=db_entity,
        gcs_uri=csv_path,
        columns=view_builder.columns,
    )
    logging.info("View (%s) successfully imported", view_builder.view_id)

    gcsfs = GcsfsFactory.build()
    object_metadata = gcsfs.get_metadata(csv_path) or {}

    _merge_metric_metadata(
        database_key=database_key,
        db_entity_name=db_entity.__name__,
        last_updated=object_metadata.get("last_updated", None),
        facility_id_name_map=object_metadata.get("facility_id_name_map", None),
        gender_id_name_map=object_metadata.get("gender_id_name_map", None),
        race_id_name_map=object_metadata.get("race_id_name_map", None),
        dynamic_filter_options=object_metadata.get("dynamic_filter_options", None),
    )

    # TODO(#57285) Remove the schema_type conditional once we have PublicPathwaysMetricCache
    if reset_cache and schema_type == SchemaType.PATHWAYS:
        metric_cache = PathwaysMetricCache.build(
            StateCode(state_code), schema_type=SchemaType.PATHWAYS
        )
        for metric in get_metrics_for_entity(db_entity):
            metric_cache.reset_cache(metric)

    return "", HTTPStatus.OK


@app.route("/import/insights/<state_code>/<filename>", methods=["POST"])
def _import_insights(state_code: str, filename: str) -> Tuple[str, HTTPStatus]:
    """Imports a JSON file from GCS into the Insights Cloud SQL database"""
    try:
        view_builder = _get_view_builder(state_code, filename, OUTLIERS_VIEW_BUILDERS)
    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST

    # Skip importing archive files and the district managers file for non-email states
    if view_builder in OUTLIERS_ARCHIVE_VIEW_BUILDERS or (
        view_builder.view_id == SUPERVISION_DISTRICT_MANAGERS_VIEW_BUILDER.view_id
        and StateCode(state_code.upper()) != "US_PA"
    ):
        return (
            "",
            HTTPStatus.OK,
        )

    try:
        view_builder = _validate_selected_columns_view_builder(filename, view_builder)
        db_entity = get_database_entity_by_table_name(
            insights_schema, view_builder.view_id
        )

    except ValueError as e:
        return str(e), HTTPStatus.BAD_REQUEST
    except TypeError as e:
        return str(e), HTTPStatus.INTERNAL_SERVER_ERROR

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
