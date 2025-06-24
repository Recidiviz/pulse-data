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
"""Metadata about a job run"""
import datetime
import json
from enum import Enum, StrEnum
from typing import Any

import attr
from airflow.utils.timezone import utc

from recidiviz.airflow.dags.monitoring.utils import filter_params_to_discrete
from recidiviz.common import attr_validators


class JobRunState(Enum):
    """Indicates the status of a JobRun.

    - SUCCESS: succeeded or did not need to run.
    - UNKNOWN: an indeterminate state.
    - PENDING: has not yet reached its terminal state (i.e. is waiting to start, is
        already in progress, is being retried, etc).
    - FAILED: failed itself or failed to start due to a related failure.
    """

    SUCCESS = 0
    UNKNOWN = 1
    PENDING = 2
    FAILED = 3


class JobRunType(StrEnum):
    """Enum of known job run types; is used as a unique key in grouping job runs."""

    AIRFLOW_TASK_RUN = "Task Run"
    RAW_DATA_IMPORT = "Raw Data Import"


@attr.define(kw_only=True, frozen=True)
class JobRun:
    """Metadata about a JobRun, or some unit of work done in an Airflow DAG that usually,
    but does not always, map onto an individual Airflow task.

    Attributes:
        - dag_id (str): the dag_id of the Airflow DAG this job is apart of
        - execution_date (datetime.datetime): the execution date of the DAG. for more
            context on the specific meaning in Airflow, see https://airflow.apache.org/docs/apache-airflow/stable/faq.html#faq-what-does-execution-date-mean
        - dag_run_config (str): the run config associated with the DAG run. This is
            serialized json, stored as a string so it can be used as a dataframe index.
        - job_id (str): the unique job_id of the job run. If a job run is representing a
            an Airflow task, this will be the `task_id`.
        - state (JobRunState): the state of the this job.
        - error_message (str | None): error message associated with this job.
        - job_type (str): the type of run associated with this job.
    """

    dag_id: str = attr.field(validator=attr_validators.is_str)
    execution_date: datetime.datetime = attr.field(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    # n.b. make sure this is SORTED before you convert it to a string; otherwise, it might
    # break deduplication logic
    dag_run_config: str = attr.field(validator=attr_validators.is_str)
    job_id: str = attr.field(validator=attr_validators.is_str)
    state: JobRunState = attr.field(validator=attr.validators.in_(JobRunState))
    error_message: str | None = attr.field(validator=attr_validators.is_opt_str)
    job_type: JobRunType = attr.field(validator=attr.validators.in_(JobRunType))

    @classmethod
    def unique_keys(cls) -> list[str]:
        return ["dag_id", "dag_run_config", "job_type", "job_id"]

    @classmethod
    def date_key(cls) -> list[str]:
        return ["execution_date"]

    @classmethod
    def from_airflow_task_instance(
        cls,
        dag_id: str,
        execution_date: datetime.datetime,
        conf: dict[str, Any],
        task_id: str,
        state: int,
        *,
        job_type: JobRunType,
    ) -> "JobRun":
        # sort dag run config to make sure that two different parameter orderings
        # doesn't break incident de-duplication
        sorted_dag_run_config = dict(
            sorted(filter_params_to_discrete(conf, dag_id).items())
        )
        # Airflow uses pendulum as it's timezone library; let's convert it to native UTC
        # so our validators understand it
        if execution_date.tzinfo == utc:
            execution_date = execution_date.replace(tzinfo=datetime.UTC)
        return JobRun(
            dag_id=dag_id,
            execution_date=execution_date,
            dag_run_config=json.dumps(sorted_dag_run_config),
            job_id=task_id,
            state=JobRunState(state),
            job_type=job_type,
            error_message=None,
        )
