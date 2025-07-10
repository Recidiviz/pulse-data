# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Builds incident history for an Airflow DAG."""
import datetime
import logging
from collections import defaultdict
from typing import Any

import attr
import pandas as pd

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.job_run import JobRun, JobRunState
from recidiviz.airflow.dags.monitoring.job_run_history_delegate_factory import (
    JobRunHistoryDelegateFactory,
)


class IncidentHistoryBuilder:
    """Builds incident history for errors seen for a particular Airflow DAG."""

    def __init__(self, dag_id: str) -> None:
        self._dag_id = dag_id
        self.job_run_history_delegates = JobRunHistoryDelegateFactory.build(
            dag_id=dag_id
        )

    def build(
        self, *, lookback: datetime.timedelta
    ) -> dict[str, AirflowAlertingIncident]:
        """Builds a mapping of incident id to incident for the past |lookback|, only
        building incidents for JobRuns that have had a recent "state change" (i.e. from
        failing to succeeding or vice versa).
        """

        job_history = []
        for delegate in self.job_run_history_delegates:
            job_history.extend(delegate.fetch_job_runs(lookback=lookback))

        if not job_history:
            logging.error("No recent runs found for [%s]", self._dag_id)
            return {}

        data_by_distinct_dag: dict[
            tuple[str, str, str, str], list[dict[str, Any]]
        ] = defaultdict(list)
        for job_run in job_history:
            data_by_distinct_dag[job_run.unique_key].append(attr.asdict(job_run))

        # Map of unique incident IDs to incidents
        incidents: dict[str, AirflowAlertingIncident] = {}
        for distinct_dag_key, dag_data in data_by_distinct_dag.items():
            df = (
                pd.DataFrame(data=dag_data)
                .drop(columns=JobRun.unique_keys())
                .sort_values(JobRun.order_by_keys())
            )
            # Filter down to job runs in a failed or success state.
            df = df[df.state.isin([JobRunState.FAILED, JobRunState.SUCCESS])]

            # Skip if we have no runs or they are all SUCCESS.
            if df.empty or df.state.unique().tolist() == [JobRunState.SUCCESS]:
                continue

            # Add session_id column which is the same for all contiguous rows of the same state
            df["session_id"] = (df.state != df.state.shift(1)).cumsum()

            # Hold on to sessions as groups to aggregate most recent error and critical dates.
            sessions = df.groupby("session_id")

            # This is a DataFrame with the most recent error for each session
            last_error_per_session = sessions.tail(1)[
                ["session_id", "state", "error_message"]
            ]
            # Creates a dataframe with aggregated dates for each session,
            # then we can shift critical dates forwards and backwards as needed.
            session_dates = sessions.execution_date.agg((list, "min", "max")).rename(
                columns={
                    "list": "all_execution_dates",
                    "min": "earliest_execution_date",
                    "max": "most_recent_execution_date",
                }
            )
            session_dates[
                "previous_session_critical_date"
            ] = session_dates.most_recent_execution_date.shift(1)
            session_dates[
                "next_session_critical_date"
            ] = session_dates.earliest_execution_date.shift(-1)

            failed_sessions = session_dates.merge(
                last_error_per_session[
                    last_error_per_session.state == JobRunState.FAILED
                ],
                on="session_id",
                how="inner",
            )
            for session_record in failed_sessions.to_dict("records"):
                dag_id, dag_run_config, job_type_value, job_id = distinct_dag_key
                incident = AirflowAlertingIncident.build(
                    dag_id=dag_id,
                    conf=dag_run_config,
                    job_id=job_id,
                    incident_type=job_type_value,
                    failed_execution_dates=session_record["all_execution_dates"],
                    previous_success=session_record["previous_session_critical_date"],
                    next_success=session_record["next_session_critical_date"],
                    error_message=session_record["error_message"],
                )

                incidents[incident.unique_incident_id] = incident
        return incidents
