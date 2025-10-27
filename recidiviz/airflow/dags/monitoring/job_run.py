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
    """Enum of known job run types; is used as a unique key in grouping job runs.

    n.b.: the values of this enum are used to generate an AirflowAlertingIncident's
    unique id which is used to de-dupe incidents in PagerDuty; if you plan on updating
    any of these values, please close the newly created duplicates.
    """

    AIRFLOW_TASK_RUN = "Task Run"
    RAW_DATA_IMPORT = "Raw Data Import"
    RUNTIME_MONITORING = "Runtime Monitoring"


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
        - job_type (JobRunType): the type of run associated with this job.
        - job_run_num (int): the run / attempt number of this job run. Used to represent
            and properly order cases with multiple runs with the same JobRun.unique_keys
            and the same |execution_date|.
        - max_tries (int | None): the maximum number of retry attempts configured for this
            task. A task has exhausted retries when job_run_num >= max_tries.
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
    job_run_num: int = attr.field(validator=attr_validators.is_int)
    max_tries: int = attr.field(default=-1, validator=attr_validators.is_int)

    @classmethod
    def unique_keys(cls) -> list[str]:
        return ["dag_id", "dag_run_config", "job_type", "job_id"]

    @property
    def unique_key(self) -> tuple[str, str, str, str]:
        """Returns the unique key for this job run, which is a tuple of the
        JobRun.unique_keys.
        """
        return tuple(getattr(self, key) for key in self.unique_keys())

    @classmethod
    def order_by_keys(cls) -> list[str]:
        """List of keys to order by in a SQL-order by fashion (order by the first, only
        use the second if the first two are equal, etc).
        """
        return ["execution_date", "job_run_num"]

    @classmethod
    def from_airflow_task_instance(
        cls,
        *,
        dag_id: str,
        execution_date: datetime.datetime,
        conf: dict[str, Any],
        task_id: str,
        state: int,
        job_type: JobRunType,
        try_number: int | None,
        max_tries: int | None,
        error_message: str | None
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
            job_run_num=try_number or 0,
            job_type=job_type,
            error_message=error_message,
            max_tries=max_tries if max_tries is not None else -1,
        )
