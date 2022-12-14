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
import base64
import os
from http import HTTPStatus
from typing import Any, Dict, Optional, Tuple, TypeVar
from urllib.parse import urlencode

# Mypy errors "Cannot find implementation or library stub for module named 'xxxx'"
# ignored here because cloud functions require that imports are declared relative to
# the cloud functions package itself. In general, we should avoid shipping complex
# code in cloud functions. The function itself should call an API endpoint that can
# live in an external package with proper import resolution.
# pylint: disable=unused-import
from cloud_function_utils import (  # type: ignore[import]
    GCP_PROJECT_ID_KEY,
    IAP_CLIENT_ID,
    cloud_functions_log,
    make_iap_request,
    trigger_dag,
)
from cloudsql_to_bq_refresh_utils import (  # type: ignore[import]
    PIPELINE_RUN_TYPE_HISTORICAL_VALUE,
    PIPELINE_RUN_TYPE_REQUEST_ARG,
)

# A stand-in type for google.cloud.functions.Context for which no apparent type is available
ContextType = TypeVar("ContextType", bound=Any)

TRIGGER_SOURCE_POST_DEPLOY = "POST_DEPLOY"
TRIGGER_SOURCE_DAILY = "DAILY"
TRIGGER_SOURCES = [TRIGGER_SOURCE_POST_DEPLOY, TRIGGER_SOURCE_DAILY]


def _build_url(
    project_id: str,
    path: str,
    params: Optional[Dict[str, Any]],
) -> str:
    url = f"https://{project_id}.appspot.com{path}"
    if params is not None:
        url += f"?{urlencode(params)}"
    return url


def trigger_calculation_pipeline_dag(
    event: Dict[str, Any], _context: ContextType
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

    if "data" in event:
        trigger_source = base64.b64decode(event["data"]).decode("utf-8")
    else:
        error_str = f"Could not find data needs in event parameter: {event}"
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    if trigger_source not in TRIGGER_SOURCES:
        error_str = (
            f"data needs to pass in the trigger source, but received: {trigger_source}"
        )
        cloud_functions_log(severity="ERROR", message=error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    # The name of the DAG you wish to trigger
    dag_name = f"{project_id}_{pipeline_dag_type}_calculation_pipeline_dag"

    monitor_response = trigger_dag(
        airflow_uri, dag_name, {"TRIGGER_SOURCE": trigger_source}
    )
    cloud_functions_log(
        severity="INFO",
        message=f"The monitoring Airflow response is {monitor_response}",
    )
    return "", HTTPStatus(monitor_response.status_code)
