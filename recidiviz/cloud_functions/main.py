# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""This file contains all of the relevant cloud functions"""
import json
import os
from base64 import b64decode
from http import HTTPStatus
from typing import Any, Dict, Optional, Tuple, TypeVar
from urllib.parse import urlencode

# Mypy errors "Cannot find implementation or library stub for module named 'xxxx'"
# ignored here because cloud functions require that imports are declared relative to
# the cloud functions package itself. In general, we should avoid shipping complex
# code in cloud functions. The function itself should call an API endpoint that can
# live in an external package with proper import resolution.
from cloud_function_utils import (  # type: ignore[import]
    GCP_PROJECT_ID_KEY,
    IAP_CLIENT_ID,
    cloud_functions_log,
    make_iap_request,
    trigger_dag,
)
from cloudsql_to_bq_refresh_utils import (  # type: ignore[import]
    PIPELINE_RUN_TYPE_HISTORICAL_VALUE,
    PIPELINE_RUN_TYPE_NONE_VALUE,
    PIPELINE_RUN_TYPE_REQUEST_ARG,
    TRIGGER_HISTORICAL_DAG_FLAG,
    UPDATE_MANAGED_VIEWS_REQUEST_ARG,
)
from direct_ingest_bucket_name_utils import (  # type: ignore[import]
    get_region_code_from_direct_ingest_bucket,
)

# A stand-in type for google.cloud.functions.Context for which no apparent type is available
ContextType = TypeVar("ContextType", bound=Any)

_STATE_AGGREGATE_PATH = "/aggregate/persist_file"
_DIRECT_INGEST_PATH = "/direct/handle_direct_ingest_file"
_DIRECT_INGEST_NORMALIZE_RAW_PATH_PATH = "/direct/normalize_raw_file_path"

_METRIC_VIEW_EXPORT_PATH = "/export/create_metric_view_data_export_tasks"
_APP_ENGINE_IMPORT_USER_RESTRICTIONS_CSV_TO_SQL_PATH = (
    "/auth/handle_import_user_restrictions_csv_to_sql"
)
_APP_ENGINE_IMPORT_CASE_TRIAGE_ETL_CSV_TO_SQL_PATH = (
    "/case_triage_ops/handle_gcs_imports"
)
_APP_ENGINE_PRACTICES_ETL_ARCHIVE_PATH = "/practices-etl/archive-file"


def _build_url(
    project_id: str,
    path: str,
    params: Optional[Dict[str, Any]],
) -> str:
    url = f"https://{project_id}.appspot.com{path}"
    if params is not None:
        url += f"?{urlencode(params)}"
    return url


def parse_state_aggregate(
    data: Dict[str, Any], _: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered when a file is dropped into the state
    aggregate bucket and makes a request to parse and write the data to the
    aggregate table database.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.
    """
    bucket = data["bucket"]
    state, filename = data["name"].split("/")
    project_id = os.environ[GCP_PROJECT_ID_KEY]
    cloud_functions_log(
        severity="INFO",
        message=f"Running cloud function for bucket [{bucket}], state [{state}], filename"
        f" [{filename}]",
    )
    url = _build_url(
        project_id,
        _STATE_AGGREGATE_PATH,
        {"bucket": bucket, "state": state, "filename": filename},
    )
    # Hit the cloud function backend, which persists the table data to our
    # database.
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    cloud_functions_log(
        severity="INFO", message=f"The response status is {response.status_code}"
    )
    return "", HTTPStatus(response.status_code)


def normalize_raw_file_path(
    data: Dict[str, Any], _: ContextType
) -> Tuple[str, HTTPStatus]:
    """Cloud functions can be configured to trigger this function on any bucket that is being used as a test bed for
    automatic uploads. This will just rename the incoming files to have a normalized path with a timestamp so
    subsequent uploads do not have naming conflicts."""
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        error_str = (
            "No project id set for call to direct ingest cloud function, returning."
        )
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    bucket = data["bucket"]
    relative_file_path = data["name"]

    url = _build_url(
        project_id,
        _DIRECT_INGEST_NORMALIZE_RAW_PATH_PATH,
        {
            "bucket": bucket,
            "relative_file_path": relative_file_path,
        },
    )

    cloud_functions_log(severity="INFO", message=f"Calling URL: {url}")

    # Hit the cloud function backend, which will schedule jobs to parse
    # data for unprocessed files in this bucket and persist to our database.
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    cloud_functions_log(
        severity="INFO", message=f"The response status is " f"{response.status_code}."
    )
    return "", HTTPStatus(response.status_code)


def handle_state_direct_ingest_file(
    data: Dict[str, Any], _: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered when a file is dropped into any of the state
    direct ingest buckets and makes a request to parse and write the data to
    the database.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.

    """
    return _handle_state_direct_ingest_file(data, start_ingest=True)


def handle_state_direct_ingest_file_rename_only(
    data: Dict[str, Any], _: ContextType
) -> Tuple[str, HTTPStatus]:
    """Cloud functions can be configured to trigger this function instead of
    handle_state_direct_ingest_file when a region has turned on nightly/weekly
    automatic data transfer before we are ready to schedule and process ingest
    jobs for that region (e.g. before ingest is "launched"). This will just
    rename the incoming files to have a normalized path with a timestamp
    so subsequent nightly uploads do not have naming conflicts.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.

    """
    return _handle_state_direct_ingest_file(data, start_ingest=False)


def handle_new_case_triage_etl(
    data: Dict[str, Any], _: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered when a file is dropped in the
    `{project_id}-case-triage-data` bucket. If the file matches `etl_*.csv`,
    then it makes a request to import the CSV to Cloud SQL.
    """
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        cloud_functions_log(
            severity="ERROR",
            message="No project id set for call to update auth0 users, returning.",
        )
        return "", HTTPStatus.BAD_REQUEST

    filename = data["name"]
    if not filename.startswith("etl_") or not filename.endswith(".csv"):
        cloud_functions_log(severity="INFO", message=f"Ignoring file {filename}")
        return "", HTTPStatus.OK

    import_url = _build_url(
        project_id,
        _APP_ENGINE_IMPORT_CASE_TRIAGE_ETL_CSV_TO_SQL_PATH,
        {"filename": filename},
    )
    import_response = make_iap_request(import_url, IAP_CLIENT_ID[project_id])
    return "", HTTPStatus(import_response.status_code)


def handle_state_dashboard_user_restrictions_file(
    data: Dict[str, Any], _: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered when a file is dropped in a
    `recidiviz-{project_id}-dashboard-user-restrictions/US_XX` bucket.

    If the file matches `dashboard_user_restrictions.csv`, then it makes a request to import the CSV
    to the Cloud SQL `dashboard_user_restrictions` table in the Case Triage schema.

    Once the CSV import finishes, it makes a request to update the Auth0 users with the user restrictions.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.

    """
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        cloud_functions_log(
            severity="ERROR",
            message="No project id set for call to update auth0 users, returning.",
        )
        return "", HTTPStatus.BAD_REQUEST

    filepath = data["name"].split("/")

    # Expected file path structure is US_XX/dashboard_user_restrictions.csv
    if len(filepath) != 2:
        cloud_functions_log(
            severity="INFO",
            message=f"Skipping filepath, incorrect "
            f"number of nested directories: {filepath}",
        )
        return "", HTTPStatus.OK

    region_code, filename = filepath
    csv_file = "dashboard_user_restrictions.csv"

    if filename == csv_file:
        import_user_restrictions_url = _build_url(
            project_id,
            _APP_ENGINE_IMPORT_USER_RESTRICTIONS_CSV_TO_SQL_PATH,
            {"region_code": region_code},
        )
        cloud_functions_log(
            severity="INFO", message=f"Calling URL: {import_user_restrictions_url}"
        )

        # Hit the App Engine endpoint `auth/import_user_restrictions_csv_to_sql`.
        response = make_iap_request(
            import_user_restrictions_url, IAP_CLIENT_ID[project_id]
        )
        cloud_functions_log(
            severity="INFO",
            message=f"The {import_user_restrictions_url} response status is {response.status_code}",
        )

    return "", HTTPStatus.OK


def _handle_state_direct_ingest_file(
    data: Dict[str, Any], start_ingest: bool
) -> Tuple[str, HTTPStatus]:
    """Calls direct ingest cloud function when a new file is dropped into a
    bucket."""
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        error_str = (
            "No project id set for call to direct ingest cloud function, returning."
        )
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    bucket = data["bucket"]
    relative_file_path = data["name"]
    region_code = get_region_code_from_direct_ingest_bucket(bucket)
    if not region_code:
        error_str = f"Cannot parse region code from bucket {bucket}, returning."
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    url = _build_url(
        project_id,
        _DIRECT_INGEST_PATH,
        {
            "region": region_code,
            "bucket": bucket,
            "relative_file_path": relative_file_path,
            "start_ingest": str(start_ingest),
        },
    )

    cloud_functions_log(severity="INFO", message=f"Calling URL: {url}")

    # Hit the cloud function backend, which will schedule jobs to parse
    # data for unprocessed files in this bucket and persist to our database.
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    cloud_functions_log(
        severity="INFO", message=f"The response status is {response.status_code}"
    )
    return "", HTTPStatus(response.status_code)


def export_metric_view_data(
    event: Dict[str, Any], _context: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered by a Pub/Sub event to begin the export of data contained in BigQuery metric views to
    files in cloud storage buckets.
    """
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        error_str = "No project id set for call to export view data, returning."
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    if "data" in event:
        cloud_functions_log(severity="INFO", message="data found")
        url = _build_url(
            project_id,
            _METRIC_VIEW_EXPORT_PATH,
            {"export_job_filter": b64decode(event["data"]).decode("utf-8")},
        )
    else:
        error_str = "Missing required export_job_filter in data of the Pub/Sub message."
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    cloud_functions_log(severity="INFO", message=f"project_id: {project_id}")
    cloud_functions_log(severity="INFO", message=f"Calling URL: {url}")

    # Hit the cloud function backend, which exports view data to their assigned cloud storage bucket
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    cloud_functions_log(
        severity="INFO", message=f"The response status is {response.status_code}"
    )
    return "", HTTPStatus(response.status_code)


def trigger_calculation_pipeline_dag(
    data: Dict[str, Any], _context: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered by a Pub/Sub event, triggers an Airflow DAG where all
    the calculation pipelines (either daily or historical) run simultaneously.
    """
    project_id = os.environ.get(GCP_PROJECT_ID_KEY, "")
    if not project_id:
        error_str = (
            "No project id set for call to run the calculation pipelines, returning."
        )
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    airflow_uri = os.environ.get("AIRFLOW_URI")
    if not airflow_uri:
        error_str = "The environment variable 'AIRFLOW_URI' is not set"
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    pipeline_dag_type = os.environ.get("PIPELINE_DAG_TYPE")
    if not pipeline_dag_type:
        error_str = "The environment variable 'PIPELINE_DAG_TYPE' is not set"
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    # The name of the DAG you wish to trigger
    dag_name = f"{project_id}_{pipeline_dag_type}_calculation_pipeline_dag"

    monitor_response = trigger_dag(airflow_uri, dag_name, data)
    cloud_functions_log(
        severity="INFO",
        message=f"The monitoring Airflow response is {monitor_response}",
    )
    return "", HTTPStatus(monitor_response.status_code)


def trigger_post_deploy_cloudsql_to_bq_refresh(
    event: Dict[str, Any], _context: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered by a Pub/Sub event to begin the refresh of BigQuery
    data for a given schema, pulling data from the appropriate CloudSQL Postgres
    instance, and to trigger the historical pipelines once the refresh is complete.
    """
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        error_str = "No project id set for call to refresh BigQuery data, returning."
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    schema = os.environ.get("SCHEMA")
    if not schema:
        error_str = "The schema variable 'SCHEMA' is not set."
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    url = _build_url(
        project_id,
        f"/cloud_sql_to_bq/create_refresh_bq_schema_task/{schema}",
        params=None,
    )

    trigger_historical_dag: bool = False

    if "data" in event:
        if b64decode(event["data"]).decode("utf-8") == TRIGGER_HISTORICAL_DAG_FLAG:
            trigger_historical_dag = True

    data = {}

    if schema.upper() == "STATE":
        cloud_functions_log(
            severity="INFO",
            message="Managed views will be deployed after refresh.",
        )
        # Always update managed views when refreshing the state schema after a deploy
        data[UPDATE_MANAGED_VIEWS_REQUEST_ARG] = "true"

        if trigger_historical_dag:
            cloud_functions_log(
                severity="INFO",
                message="Historical DAG will be triggered after refresh.",
            )
            data[PIPELINE_RUN_TYPE_REQUEST_ARG] = PIPELINE_RUN_TYPE_HISTORICAL_VALUE
        else:
            data[PIPELINE_RUN_TYPE_REQUEST_ARG] = PIPELINE_RUN_TYPE_NONE_VALUE

    cloud_functions_log(
        severity="INFO",
        message=f"Data sent to request: {data}.",
    )

    cloud_functions_log(severity="INFO", message=f"project_id: {project_id}")
    cloud_functions_log(severity="INFO", message=f"Calling URL: {url}")

    # Hit the cloud function backend, which starts the post-deploy refresh of the
    # given schema
    response = make_iap_request(
        url, IAP_CLIENT_ID[project_id], method="POST", data=json.dumps(data).encode()
    )
    cloud_functions_log(
        severity="INFO", message=f"The response status is {response.status_code}"
    )
    return "", HTTPStatus(response.status_code)


def archive_practices_etl_data(
    data: Dict[str, Any], _: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered when a file is dropped in the
    `{project_id}-practices-etl-data` bucket and calls the endpoint that will archive it.
    """
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        cloud_functions_log(
            severity="ERROR",
            message="No project id set for call to archive Practices ETL data, returning.",
        )
        return "", HTTPStatus.BAD_REQUEST

    filename = data["name"]

    # ignore temp files generated by export
    if not filename.startswith("staging"):
        endpoint_url = _build_url(
            project_id, _APP_ENGINE_PRACTICES_ETL_ARCHIVE_PATH, None
        )
        endpoint_response = make_iap_request(
            endpoint_url,
            IAP_CLIENT_ID[project_id],
            method="POST",
            json={"filename": filename},
        )
        return "", HTTPStatus(endpoint_response.status_code)
    return "", HTTPStatus.OK
