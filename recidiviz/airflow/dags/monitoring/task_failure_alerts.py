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
from datetime import datetime, timedelta, timezone
from enum import Enum
from pprint import pprint
from typing import Dict, List, Optional, Set

import attr
import pandas
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Connection, DagRun, TaskInstance
from airflow.providers.sendgrid.utils.emailer import send_email
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State
from sqlalchemy import case, func, text
from sqlalchemy.orm import Query, Session

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


@attr.s(auto_attribs=True)
class AirflowAlertingIncident:
    """Representation of an alerting incident"""

    dag_id: str
    conf: str
    task_id: str
    most_recent_failure_date: datetime
    # This value will never be more than INCIDENT_START_DATE_LOOKBACK ago, even if it started failing before then.
    previous_success_date: Optional[datetime]
    next_success_date: Optional[datetime]

    @property
    def unique_incident_id(self) -> str:
        """
        The PagerDuty email integration is configured to group incidents by their subject.
        Incidents can only be resolved once. Afterward, any new alerts will not re-open the incident.
        The unique incident id includes the last successful task run in order to group incidents by distinct sets of
        consecutive failures.
        """
        conf_string = f"{self.conf} " if self.conf != "{}" else ""
        success_string = (
            self.previous_success_date.strftime("%Y-%m-%d %H:%M %Z")
            if self.previous_success_date is not None
            else "never"
        )
        return f"{conf_string}{self.dag_id}.{self.task_id}, last succeeded: {success_string}"

    @property
    def should_report_to_pagerduty(self) -> bool:
        """Incidents are only reported to PagerDuty if they are "active" (opened, still occurring, or resolved) within
        the last two days.
        This has no functional difference and is only done to save on SendGrid email quota.
        edge case: if the monitoring DAG is down for more than 2 days, some auto-resolve emails will be missed
        """
        now_utc = datetime.now(tz=timezone.utc)
        most_recent_update = max(
            self.previous_success_date or datetime.fromtimestamp(0, tz=timezone.utc),
            self.most_recent_failure_date,
            self.next_success_date or datetime.fromtimestamp(0, tz=timezone.utc),
        )

        return timedelta(days=2) > now_utc - most_recent_update

    @staticmethod
    @provide_session
    def build_incident_history(
        dag_ids: List[str],
        session: Session = NEW_SESSION,
    ) -> Dict[str, "AirflowAlertingIncident"]:
        """Builds a dictionary of incidents to report"""
        dataframe = _build_task_instance_state_dataframe(
            dag_ids=dag_ids, session=session
        )

        dataframe = dataframe[
            (dataframe.state == TaskInstanceState.failed)
            | (dataframe.state == TaskInstanceState.success)
        ]

        # Map of unique incident IDs to incidents
        incidents: Dict[str, AirflowAlertingIncident] = {}

        # Loop over discrete dag members (dag_id, conf, task_id)
        for discrete_dag in dataframe.index:
            # Select all task runs for this discrete dag
            task_runs = dataframe.loc[discrete_dag]
            # Collapse the dataframe into one row per state transition (i.e. success -> failed edges)
            collapsed_runs = task_runs[task_runs.state != task_runs.state.shift(-1)]
            failed_runs = collapsed_runs[
                collapsed_runs.state == TaskInstanceState.failed
            ]

            # Generate a hash map of failures, grouped by the last successful execution
            for index, row in failed_runs.iterrows():
                dag_id, conf, task_id = index

                previous_success = task_runs[
                    (task_runs.state == TaskInstanceState.success)
                    & (task_runs.execution_date < row.execution_date)
                ].execution_date.max()

                next_success = task_runs[
                    (task_runs.state == TaskInstanceState.success)
                    & (task_runs.execution_date > row.execution_date)
                ].execution_date.min()

                incident = AirflowAlertingIncident(
                    dag_id=dag_id,
                    conf=conf,
                    task_id=task_id,
                    most_recent_failure_date=row.execution_date.to_pydatetime(),
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
        columns=[
            "dag_id",
            "execution_date",
            "conf",
            "task_id",
            "state",
        ],
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

    incident_history = AirflowAlertingIncident.build_incident_history(
        dag_ids=pagerduty_integrations.keys(), session=session
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
        if not incident.should_report_to_pagerduty:
            logging.info(
                "Skipping reporting of incident: %s", incident.unique_incident_id
            )
            continue

        event = (
            "Task failure:" if incident.next_success_date is None else "Task success:"
        )

        send_email(
            to=pagerduty_integrations[incident.dag_id],
            subject=f"{event} {incident.unique_incident_id}",
            html_content=f"{incident}",
        )
