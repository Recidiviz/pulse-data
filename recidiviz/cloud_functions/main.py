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

# A stand-in type for google.cloud.functions.Context for which no apparent type is available
ContextType = TypeVar("ContextType", bound=Any)

_METRIC_VIEW_EXPORT_PATH = "/export/create_metric_view_data_export_tasks"


def _build_url(
    project_id: str,
    path: str,
    params: Optional[Dict[str, Any]],
) -> str:
    url = f"https://{project_id}.appspot.com{path}"
    if params is not None:
        url += f"?{urlencode(params)}"
    return url


# TODO(#4593): We might be able to get rid of this function entirely once we run the
#  metric export endpoints directly in Airflow, rather than just triggering the tasks
#  with Pub/Sub topics.
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
