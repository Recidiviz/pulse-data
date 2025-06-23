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

        df = pd.DataFrame(data=[attr.asdict(job_run) for job_run in job_history])

        # partition by JobRun's unique keys (dag_id, dag_run_conf, job_id)
        df = df.set_index(JobRun.unique_keys())
        # ensure that we sort by unique keys, and that the rows within each unique key
        # are in ascending order by sorted by JobRun's date_key
        df = df.sort_values(by=JobRun.date_key()).sort_index()

        # Filter down to job runs in a failed or success state.
        df = df[(df.state == JobRunState.FAILED) | (df.state == JobRunState.SUCCESS)]

        if df.empty:
            return {}

        # Add a new column that is True every time the state changes as compared to the
        # previous row each partition of (dag_id, dag_run_config, job_id)
        df["state_change"] = df.state != df.state.shift(1)

        # Add session_id column which is the same for all contiguous rows of the same state
        df["session_id"] = df.groupby(level=df.index.names)["state_change"].cumsum()

        # Map of unique incident IDs to incidents
        incidents: dict[str, AirflowAlertingIncident] = {}

        # Loop over discrete DAG members (dag_id, dag_run_config, job_id)
        for discrete_dag in df.index.unique():
            # Select all job runs for this discrete DAG and copy slice into a new df
            job_runs = df.loc[
                # We must pass the discrete_dag index cols as a list here so that loc always
                # returns a df. Otherwise, it will return a Series if there is only
                # one row in job_runs.
                [discrete_dag]
            ].copy()

            dag_id, conf, job_type, job_id = discrete_dag

            job_state_sessions = job_runs.groupby(["session_id"]).agg(
                start_date=("execution_date", "min"),
                end_date=("execution_date", "max"),
                state=("state", "first"),
            )

            # Generate a hash map of failures, grouped by the last successful execution
            for _index, row in job_state_sessions.iterrows():
                if row.state != JobRunState.FAILED:
                    continue

                previous_success = job_runs[
                    (job_runs.state == JobRunState.SUCCESS)
                    & (job_runs.execution_date < row.start_date)
                ].execution_date.max()

                next_success = job_runs[
                    (job_runs.state == JobRunState.SUCCESS)
                    & (job_runs.execution_date > row.end_date)
                ].execution_date.min()

                failed_execution_dates = [
                    execution_date.to_pydatetime()
                    for execution_date in job_runs[
                        job_runs.execution_date.between(
                            row.start_date, row.end_date, inclusive="both"
                        )
                    ].execution_date.to_list()
                ]

                most_recent_error_message = job_runs[
                    job_runs.execution_date == row.end_date
                ].error_message.iloc[0]

                incident = AirflowAlertingIncident.build(
                    dag_id=dag_id,
                    conf=conf,
                    job_id=job_id,
                    failed_execution_dates=failed_execution_dates,
                    previous_success=previous_success,
                    next_success=next_success,
                    error_message=most_recent_error_message,
                    incident_type=job_type.value,
                )

                incidents[incident.unique_incident_id] = incident

        return incidents
