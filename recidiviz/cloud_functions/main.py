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
import json
import os
from typing import Any, Dict

import functions_framework

# Mypy errors "Cannot find implementation or library stub for module named 'xxxx'"
# ignored here because cloud functions require that imports are declared relative to
# the cloud functions package itself. In general, we should avoid shipping complex
# code in cloud functions. The function itself should call an API endpoint that can
# live in an external package with proper import resolution.
# pylint: disable=unused-import
from cloud_function_utils import (  # type: ignore[import]
    GCP_PROJECT_ID_KEY,
    cloud_functions_log,
    trigger_dag,
)
from cloudevents.http import CloudEvent


def _get_json_body(cloud_event: CloudEvent) -> Dict[str, Any]:
    if "message" in cloud_event.data and "data" in cloud_event.data["message"]:
        data_str = base64.b64decode(cloud_event.data["message"]["data"]).decode()
        json_body = json.loads(data_str)
    else:
        raise ValueError(f"Could not find data needs in event parameter: {cloud_event}")
    return json_body


@functions_framework.cloud_event
def trigger_calculation_dag(cloud_event: CloudEvent) -> None:
    """This function is triggered by a Pub/Sub event, triggers an Airflow DAG where
    the calculation pipeline runs.
    """
    project_id = os.environ[GCP_PROJECT_ID_KEY]
    airflow_uri = os.environ["AIRFLOW_URI"]

    json_body = _get_json_body(cloud_event)

    # The name of the DAG you wish to trigger
    dag_name = f"{project_id}_calculation_dag"

    monitor_response = trigger_dag(
        airflow_uri,
        dag_name,
        {
            "state_code_filter": json_body.get("state_code_filter"),
            "sandbox_prefix": json_body.get("sandbox_prefix"),
            "ingest_instance": json_body["ingest_instance"],
        },
    )
    cloud_functions_log(
        severity="INFO",
        message=f"The monitoring Airflow response is {monitor_response}",
    )
    if monitor_response.status_code != 200:
        raise RuntimeError(
            f"Airflow monitoring DAG failed with status code {monitor_response.status_code}"
        )


@functions_framework.cloud_event
def trigger_hourly_monitoring_dag(_cloud_event: CloudEvent) -> None:
    """This function is triggered by a Pub/Sub event, triggers an Airflow hourly monitoring DAG"""
    project_id = os.environ[GCP_PROJECT_ID_KEY]
    airflow_uri = os.environ["AIRFLOW_URI"]

    # The name of the DAG you wish to trigger
    dag_name = f"{project_id}_hourly_monitoring_dag"

    monitor_response = trigger_dag(airflow_uri, dag_name, data={})
    cloud_functions_log(
        severity="INFO",
        message=f"The monitoring Airflow response is {monitor_response}",
    )
    if monitor_response.status_code != 200:
        raise RuntimeError(
            f"Airflow monitoring DAG failed with status code {monitor_response.status_code}"
        )


@functions_framework.cloud_event
def trigger_sftp_dag(_cloud_event: CloudEvent) -> None:
    """This function is triggered by a Pub/Sub event, triggers an Airflow DAG where all
    the SFTP downloads for all states run simultaneously."""
    project_id = os.environ[GCP_PROJECT_ID_KEY]
    airflow_uri = os.environ["AIRFLOW_URI"]

    # The name of the DAG you wish to trigger
    dag_name = f"{project_id}_sftp_dag"

    monitor_response = trigger_dag(airflow_uri, dag_name, data={})
    cloud_functions_log(
        severity="INFO",
        message=f"The monitoring Airflow response is {monitor_response}",
    )
    if monitor_response.status_code != 200:
        raise RuntimeError(
            f"Airflow monitoring DAG failed with status code {monitor_response.status_code}"
        )


@functions_framework.cloud_event
def trigger_raw_data_import_dag(cloud_event: CloudEvent) -> None:
    """This function is triggered by a Pub/Sub event and in turn triggers the raw data
    import DAG.
    """
    project_id = os.environ[GCP_PROJECT_ID_KEY]
    airflow_uri = os.environ["AIRFLOW_URI"]

    json_body = _get_json_body(cloud_event)

    monitor_response = trigger_dag(
        airflow_uri,
        dag_id=f"{project_id}_raw_data_import_dag",
        data={
            "state_code_filter": json_body.get("state_code_filter"),
            "ingest_instance": json_body.get("ingest_instance"),
        },
    )
    cloud_functions_log(
        severity="INFO",
        message=f"The monitoring Airflow response is {monitor_response}",
    )
    if monitor_response.status_code != 200:
        raise RuntimeError(
            f"Airflow monitoring DAG failed with status code {monitor_response.status_code}"
        )
