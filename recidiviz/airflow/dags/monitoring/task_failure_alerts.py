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
"""Functionality for reporting consecutive failures of tasks as incidents to PagerDuty"""
import json
import logging
import os
from datetime import timedelta
from enum import Enum
from pprint import pprint
from typing import Dict, List, Set

import pandas
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Connection, DagRun, TaskInstance
from airflow.providers.sendgrid.utils.emailer import send_email
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State
from sqlalchemy import case, func, text
from sqlalchemy.orm import Query, Session

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.alerting_trigger_predicates import (
    should_trigger_airflow_alerting_incident,
)
from recidiviz.airflow.dags.utils.email import can_send_mail
from recidiviz.airflow.dags.utils.environment import (
    get_composer_environment,
    is_experiment_environment,
)

_project_id = os.getenv("GCP_PROJECT")

# If a new parameter is being added, add it here and review the DISCRETE_CONFIGURATION_PARAMETERS list
KNOWN_CONFIGURATION_PARAMETERS: Dict[str, Set[str]] = {
    f"{_project_id}_calculation_dag": {
        "ingest_instance",
        "state_code_filter",
        "sandbox_prefix",
    },
    f"{_project_id}_hourly_monitoring_dag": set(),
    f"{_project_id}_sftp_dag": set(),
}

# The list of parameters that cause DAG Run history to partition into discrete sets of runs
# For example, we track incidents for primary instance runs and secondary instance runs separately
DISCRETE_CONFIGURATION_PARAMETERS: Dict[str, List[str]] = {
    f"{_project_id}_calculation_dag": ["ingest_instance", "state_code"],
    f"{_project_id}_hourly_monitoring_dag": [],
    f"{_project_id}_sftp_dag": [],
}

DAGS_TO_IGNORE_IN_ALERTING = ["airflow_monitoring"]

INCIDENT_START_DATE_LOOKBACK = timedelta(days=30)


@provide_session
def build_incident_history(
    dag_ids: List[str],
    session: Session = NEW_SESSION,
) -> Dict[str, AirflowAlertingIncident]:
    """Builds a dictionary of incidents to report"""
    dataframe = _build_task_instance_state_dataframe(dag_ids=dag_ids, session=session)

    dataframe = dataframe[
        (dataframe.state == TaskInstanceState.failed)
        | (dataframe.state == TaskInstanceState.success)
    ]

    # Map of unique incident IDs to incidents
    incidents: Dict[str, AirflowAlertingIncident] = {}

    # Loop over discrete dag members (dag_id, conf, task_id)
    for discrete_dag in dataframe.index:
        # Select all task runs for this discrete dag and copy slice into a new DataFrame
        task_runs = dataframe.loc[discrete_dag].copy()
        dag_id, conf, task_id = discrete_dag

        task_state_sessions = task_runs.groupby(["session_id"]).agg(
            start_date=("execution_date", "min"),
            end_date=("execution_date", "max"),
            state=("state", "first"),
        )

        # Generate a hash map of failures, grouped by the last successful execution
        for _index, row in task_state_sessions.iterrows():
            if row.state != TaskInstanceState.failed:
                continue

            previous_success = task_runs[
                (task_runs.state == TaskInstanceState.success)
                & (task_runs.execution_date < row.start_date)
            ].execution_date.max()

            next_success = task_runs[
                (task_runs.state == TaskInstanceState.success)
                & (task_runs.execution_date > row.end_date)
            ].execution_date.min()

            execution_dates = [
                execution_date.to_pydatetime()
                for execution_date in task_runs[
                    task_runs.execution_date.between(
                        row.start_date, row.end_date, inclusive="both"
                    )
                ].execution_date.to_list()
            ]

            incident = AirflowAlertingIncident(
                dag_id=dag_id,
                conf=conf,
                task_id=task_id,
                failed_execution_dates=execution_dates,
                previous_success_date=previous_success.to_pydatetime()
                if not pandas.isna(previous_success)
                else None,
                next_success_date=next_success.to_pydatetime()
                if not pandas.isna(next_success)
                else None,
            )

            incidents[incident.unique_incident_id] = incident

    return incidents


class TaskInstanceState(Enum):
    """
    Indicates the status of a task instance. For task instances with multiple dynamically mapped tasks
    (e.g. download_sftp_files in the SFTP DAG), the value for a task instance is assigned using
    this logic:
    - If any member failed, the group is considered failed
    - If any member has not completed its run, the group will be pending
    - If any member is unknown, the group's state is unknown
    - A group is only considered successful if all members are successful.
    """

    success = 0
    unknown = 1
    pending = 2
    failed = 3


@provide_session
def _query_task_instance_state(
    dag_ids: List[str], session: Session = NEW_SESSION
) -> Query:
    """Returns the alerting state of each task instance, aggregating across all dynamically mapped tasks
    for that task instance."""

    # Fetch alerting state for each TaskInstance
    latest_tasks = (
        session.query(
            TaskInstance.dag_id,
            DagRun.execution_date,
            DagRun.conf,
            TaskInstance.task_id,
            TaskInstance.map_index,
            case(
                [
                    (
                        TaskInstance.state.in_(State.unfinished),
                        TaskInstanceState.pending.value,
                    ),
                    (
                        TaskInstance.state.in_([State.FAILED.value]),
                        TaskInstanceState.failed.value,
                    ),
                    (
                        TaskInstance.state.in_(State.success_states),
                        TaskInstanceState.success.value,
                    ),
                ],
                else_=TaskInstanceState.unknown.value,
            ).label("state"),
        )
        .join(TaskInstance.dag_run)
        .filter(
            func.age(DagRun.execution_date)
            < text(f"interval '{INCIDENT_START_DATE_LOOKBACK.total_seconds()} seconds'")
        )
        .filter(DagRun.dag_id.in_(dag_ids))
    ).cte("latest_tasks")

    # Group mapped tasks into a single row per parent task
    return session.query(
        latest_tasks.c.dag_id,
        latest_tasks.c.execution_date,
        latest_tasks.c.conf,
        latest_tasks.c.task_id,
        func.max(latest_tasks.c.state).label("state"),
    ).group_by(
        latest_tasks.c.dag_id,
        latest_tasks.c.execution_date,
        latest_tasks.c.conf,
        latest_tasks.c.task_id,
    )


@provide_session
def _build_task_instance_state_dataframe(
    dag_ids: List[str],
    session: Session = NEW_SESSION,
) -> pandas.DataFrame:
    """Builds a DataFrame representation of our task instance state"""
    df = pandas.DataFrame(
        columns=["dag_id", "execution_date", "conf", "task_id", "state"],
        data=list(_query_task_instance_state(dag_ids=dag_ids, session=session).all()),
    )
    # Slice the conf parameters down to the discrete conf params
    df.conf = df.apply(
        lambda row: {
            key: value
            for key, value in row["conf"].items()
            if key in DISCRETE_CONFIGURATION_PARAMETERS.get(row["dag_id"], [])
        },
        axis=1,
    )
    # Convert the conf to a string for use in the dataframe index
    df.conf = df.conf.apply(json.dumps)
    df.state = df.state.apply(TaskInstanceState)
    df.execution_date = df.execution_date.dt.to_pydatetime()
    df = df.set_index(["dag_id", "conf", "task_id"]).sort_index()
    df["state_change"] = df.state != df.state.shift(1)
    df["session_id"] = df.groupby(level=df.index.names)["state_change"].cumsum()

    return df


def get_configured_pagerduty_integrations() -> Dict[str, str]:
    """Returns a dictionary of configured DAG IDs -> PagerDuty Integration email.
    The PagerDuty Integrations are configured on a service-by-service basis inside PagerDuty."""
    try:
        connection = Connection.get_connection_from_secrets("pagerduty_integration")
        return connection.extra_dejson
    except AirflowNotFoundException as e:
        logging.warning("Could not find connection with id %s", "pagerduty_integration")
        raise e


@provide_session
def report_failed_tasks(
    # Defaults to NEW_SESSION so mypy types session as Session rather than Session | None
    session: Session = NEW_SESSION,
) -> None:
    """Reports unique task failure incidents to PagerDuty.
    If the task has succeeded since the incident was opened, we send with the subject `Task success: `
    which resolves the open incident in PagerDuty.
    """
    pagerduty_integrations = get_configured_pagerduty_integrations()

    incident_history = build_incident_history(
        dag_ids=pagerduty_integrations.keys(),
        session=session,
    )

    # Print the incident history for use when reviewing task logs
    pprint(incident_history)

    if is_experiment_environment() and not can_send_mail():
        logging.info(
            "Cannot report incidents to PagerDuty in %s as Sendgrid is not configured",
            get_composer_environment(),
        )
        return

    for incident in incident_history.values():
        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)

        for message in messages:
            logging.info(
                "Skipping reporting of incident: %s, reason: %s",
                incident.unique_incident_id,
                message,
            )

        if not should_trigger:
            continue

        event = (
            "Task failure:" if incident.next_success_date is None else "Task success:"
        )

        send_email(
            to=pagerduty_integrations[incident.dag_id],
            subject=f"{event} {incident.unique_incident_id}",
            html_content=f"{incident}",
        )
