# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Helper functions for triggering DAGs."""
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from google.cloud.orchestration.airflow.service_v1 import EnvironmentsClient

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.metadata import project_id

_AIRFLOW_LOCATION = "us-central1"
_AIRFLOW_ENVIRONMENT = "orchestration-v2"

# Timeout for a single CLI command to finish executing
_POLL_TIMEOUT_SECONDS = 120
_POLL_INTERVAL_SECONDS = 2

_TASK_WAIT_INTERVAL_SECONDS = 30


def _get_environment_path(client: EnvironmentsClient) -> str:
    return client.environment_path(
        project=project_id(),
        location=_AIRFLOW_LOCATION,
        environment=_AIRFLOW_ENVIRONMENT,
    )


def _execute_and_poll(
    client: EnvironmentsClient,
    environment: str,
    command: str,
    subcommand: str,
    parameters: list[str],
) -> list[str]:
    """Executes an Airflow CLI command via the Cloud Composer API and polls until
    complete. Returns the list of strings in the poll response.
    """

    # The execute_airflow_command is async. It returns immediately with execution_id,
    # pod, and pod_namespace. To determine if it was actually successful--i.e. accepted
    # by Airflow--we need to call poll_airflow_command repeatedly until output_end is
    # True and then check exit_code.
    response = client.execute_airflow_command(
        request={
            "environment": environment,
            "command": command,
            "subcommand": subcommand,
            "parameters": parameters,
        }
    )

    if response.error:
        raise ValueError(
            f"Failed to execute `airflow {command} {subcommand}`: {response.error}"
        )

    response_output_lines: list[str] = []
    next_line_number = 1
    start_time = time.time()

    while True:
        if time.time() - start_time > _POLL_TIMEOUT_SECONDS:
            raise TimeoutError(
                f"Timed out after {_POLL_TIMEOUT_SECONDS}s waiting for "
                f"`airflow {command} {subcommand}` to complete"
            )

        poll_response = client.poll_airflow_command(
            request={
                "environment": environment,
                "execution_id": response.execution_id,
                "pod": response.pod,
                "pod_namespace": response.pod_namespace,
                "next_line_number": next_line_number,
            }
        )

        for line in poll_response.output:
            logging.info("Airflow command output: %s", line.content)
            response_output_lines.append(line.content)
            next_line_number = line.line_number + 1

        if poll_response.output_end:
            if poll_response.exit_info.exit_code != 0:
                raise ValueError(
                    f"`airflow {command} {subcommand}` failed with exit code "
                    f"{poll_response.exit_info.exit_code}: "
                    f"{poll_response.exit_info.error}"
                )
            return response_output_lines

        time.sleep(_POLL_INTERVAL_SECONDS)


def trigger_dag_run(dag_id: str, conf: dict) -> str:
    """Triggers a DAG run and confirms the trigger command actually completed.
    Returns the run ID of the new DAG run.
    """
    run_id = f"manual__{datetime.now(timezone.utc).isoformat()}"
    client = EnvironmentsClient()
    environment = _get_environment_path(client)

    _execute_and_poll(
        client=client,
        environment=environment,
        command="dags",
        subcommand="trigger",
        parameters=[dag_id, "--conf", json.dumps(conf), "--run-id", run_id],
    )

    return run_id


def wait_for_dag_task_success(
    dag_id: str,
    run_id: str,
    task_id: str,
    *,
    timeout_seconds: int,
) -> None:
    """Polls until the specified task in the given DAG run has succeeded.
    Raises ValueError if the task enters a failed state, or TimeoutError if it
    has not succeeded within timeout_seconds.

    Note: This provides deeper confirmation than trigger_dag_run alone. It waits, not
    just to be sure that the trigger command succeeded, but that the DAG has actually
    progressed to a specific task (which may take minutes to hours). Callers should
    not use this if they need a fast response.
    """
    client = EnvironmentsClient()
    environment = _get_environment_path(client)

    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(
                f"Timed out after {timeout_seconds}s waiting for task {task_id!r} "
                f"to succeed in DAG {dag_id!r} run {run_id!r}"
            )

        output_lines = _execute_and_poll(
            client=client,
            environment=environment,
            command="tasks",
            subcommand="state",
            parameters=[dag_id, task_id, run_id],
        )

        # `airflow tasks state` outputs the state as the last line, potentially
        # preceded by Airflow log lines. For example:
        #   [2026-03-02 14:32:07 UTC] INFO - Some log message from Airflow
        #   [2026-03-02 14:32:07 UTC] INFO - Another log line
        #   success
        # We take [-1] to skip any preceding log output and get just the state.
        state = output_lines[-1].strip() if output_lines else ""
        logging.info(
            "Task %s in DAG %s run %s is in state: %s", task_id, dag_id, run_id, state
        )

        if state == "success":
            return
        if state in ("failed", "upstream_failed", "skipped"):
            raise ValueError(
                f"Task {task_id!r} in DAG {dag_id!r} run {run_id!r} ended with "
                f"state: {state!r}"
            )

        time.sleep(_TASK_WAIT_INTERVAL_SECONDS)


# Task ID defined in recidiviz/airflow/dags/calculation_dag.py. Cannot be
# imported directly because we don't want airflow dependencies bleeding into
# the rest of the codebase.
_CALCULATION_DAG_UPDATE_SCHEMAS_TASK_ID = "update_big_query_table_schemata"


def trigger_calculation_dag() -> None:
    """Triggers the calculation DAG and waits for the update_big_query_table_schemata
    task to succeed, confirming the DAG has actually started running."""

    logging.info("Triggering the calc DAG.")
    dag_id = f"{project_id()}_calculation_dag"
    run_id = trigger_dag_run(dag_id, conf={})
    wait_for_dag_task_success(
        dag_id, run_id, _CALCULATION_DAG_UPDATE_SCHEMAS_TASK_ID, timeout_seconds=1200
    )


def trigger_raw_data_import_dag(
    *,
    raw_data_instance: DirectIngestInstance,
    state_code_filter: Optional[StateCode],
) -> None:
    """Triggers the raw data import DAG via the Cloud Composer API."""

    logging.info(
        "Triggering the raw data import DAG with instance: [%s], and state code filter: [%s]",
        raw_data_instance.value,
        state_code_filter.value if state_code_filter else None,
    )

    if raw_data_instance == DirectIngestInstance.SECONDARY and not state_code_filter:
        raise ValueError(
            "Cannot trigger a state-agnostic SECONDARY dag run; please provide a state_code_filter if you want to trigger a raw data import DAG run in SECONDARY"
        )

    trigger_dag_run(
        f"{project_id()}_raw_data_import_dag",
        conf={
            "state_code_filter": (
                state_code_filter.value if state_code_filter else None
            ),
            "ingest_instance": raw_data_instance.value,
        },
    )
