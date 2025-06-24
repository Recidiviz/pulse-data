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
"""JobRunHistoryDelegate that builds JobRunHistory object based on airflow task
runtimes
"""
import abc
import datetime

import attr
import sqlalchemy as sa
from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState
from more_itertools import one
from sqlalchemy import and_, case, func, or_, text, true
from sqlalchemy.sql.elements import BooleanClauseList, ColumnElement

from recidiviz.airflow.dags.monitoring.job_run import JobRun, JobRunState, JobRunType
from recidiviz.airflow.dags.monitoring.job_run_history_delegate import (
    JobRunHistoryDelegate,
)
from recidiviz.common import attr_validators


@attr.define
class AirflowTaskRuntimeAlertingConfig:
    """Represents an alerting config that monitors the runtime of tasks for a particular
    airflow DAG.

    Attributes:
        dag_id (str): the id of the DAG that this config applies to
        runtime_minutes (int): the runtime duration ceiling, in minutes, before the config
            will begin to trigger an alert
    """

    dag_id: str = attr.field(validator=attr_validators.is_str)
    runtime_minutes: int = attr.field(validator=attr_validators.is_int)

    @abc.abstractmethod
    def build_task_filter_clause(self) -> ColumnElement:
        """Clause used to filter down to the set of tasks that this config applies to.
        Also used to build the success/failure case clauses.
        """


@attr.define
class AirflowExplicitTaskRuntimeAlertingConfig(AirflowTaskRuntimeAlertingConfig):
    """An airflow runtime alerting config for an explicit set of tasks.

    Attributes:
        dag_id (str): the id of the DAG that this config applies to
        runtime_minutes (int): the runtime duration ceiling, in minutes, before the config
            will begin to trigger an alert
        task_names: (list[str]): a list of task names to alert on
    """

    task_names: list[str] = attr.field(validator=attr_validators.is_list_of(str))

    def build_task_filter_clause(self) -> ColumnElement:
        return sa.column("task_id").in_(self.task_names)


@attr.define
class AirflowTaskNameRegexRuntimeAlertingConfig(AirflowTaskRuntimeAlertingConfig):
    """An airflow runtime alerting config that uses postgres's flavor of regex to do a
    case-sensitive match of task names.

    Attributes:
        dag_id (str): the id of the DAG that this config applies to
        runtime_minutes (int): the runtime duration ceiling, in minutes, before the config
            will begin to trigger an alert
        task_name_regex (str): a postgres-flavored regex pattern to be used in case-sensitive
            matching of airflow task names.
    """

    task_name_regex: str = attr.field(validator=attr_validators.is_str)

    def build_task_filter_clause(self) -> ColumnElement:
        return sa.column("task_id").op("~")(self.task_name_regex)


@attr.define
class AirflowAllTaskRuntimeConfig(AirflowTaskRuntimeAlertingConfig):
    """An airflow runtime alerting config for all tasks within an airflow DAG. This will
    only apply to individual tasks and will not alert on overall task group runtimes.

    Attributes:
        dag_id (str): the id of the DAG that this config applies to
        runtime_minutes (int): the runtime duration ceiling, in minutes, before the config
            will begin to trigger an alert
    """

    def build_task_filter_clause(self) -> ColumnElement:
        return true()


STARTED_TASK_STATES = frozenset(
    [
        # all failed and success states already have a start and end date
        TaskInstanceState.SUCCESS,
        TaskInstanceState.FAILED,
        # running has a start date but no end date which we can infer
        TaskInstanceState.RUNNING,
    ]
)


class AirflowTaskRuntimeDelegate(JobRunHistoryDelegate):
    """Delegate that builds JobRunHistory based on the historical runtime of specific
    airflow tasks.

    """

    def __init__(
        self, *, dag_id: str, configs: list[AirflowTaskRuntimeAlertingConfig]
    ) -> None:
        self.dag_id = dag_id
        self._validate_configs(configs)
        self.configs = configs

    def _validate_configs(
        self, configs: list[AirflowTaskRuntimeAlertingConfig]
    ) -> None:
        if not self.dag_id == (configs_dag_id := one({c.dag_id for c in configs})):
            raise ValueError(
                f"Expected DAG referenced in configs [{configs_dag_id}] to match the provided DAG [{self.dag_id}]"
            )

    def task_runtime_error_message(
        self,
        *,
        max_duration: float,
        threshold_exceeded: int | None,
        task_id: str,
        state: int,
    ) -> str | None:
        """Builds the error message that will be passed to the JobRun object from the
        results of the task runtime details.
        """
        if JobRunState(state) != JobRunState.FAILED:
            return None

        return (
            f"Task [{task_id}] had a runtime of [{round(max_duration, 2)}] minutes which is more "
            f"than the configured [{threshold_exceeded}] minutes."
        )

    @staticmethod
    def _build_failure_case_clause(
        config: AirflowTaskRuntimeAlertingConfig, then_value: int
    ) -> tuple[BooleanClauseList, int]:
        return (
            and_(
                config.build_task_filter_clause(),
                sa.column("task_duration") > config.runtime_minutes,
            ),
            then_value,
        )

    @staticmethod
    def _build_success_case_clause(
        config: AirflowTaskRuntimeAlertingConfig, then_value: int
    ) -> tuple[BooleanClauseList, int]:
        return (
            and_(
                config.build_task_filter_clause(),
                sa.column("task_duration") <= config.runtime_minutes,
            ),
            then_value,
        )

    def _build_state_case_clauses(self) -> list[tuple[BooleanClauseList, int]]:
        """Builds a SQL case statement used to classify task runs as either 'successes'
        if they complete faster than relevant config's runtime, or 'failures' if they
        exceed the relevant runtimes.
        """
        failure_clauses = [
            self._build_failure_case_clause(config, JobRunState.FAILED.value)
            for config in self.configs
        ]

        success_clauses = [
            self._build_success_case_clause(config, JobRunState.SUCCESS.value)
            for config in self.configs
        ]

        return [*failure_clauses, *success_clauses]

    def _build_threshold_case_clauses(self) -> list[tuple[BooleanClauseList, int]]:
        """Builds a SQL case statement that returns the relevant runtime threshold in
        minutes that is exceeded; this value will be used when building the failure
        message.
        """
        failure_clauses = [
            self._build_failure_case_clause(config, config.runtime_minutes)
            for config in self.configs
        ]

        return [*failure_clauses]

    def fetch_job_runs(self, *, lookback: datetime.timedelta) -> list[JobRun]:
        with create_session() as session:

            latest_tasks = (
                session.query(
                    TaskInstance.dag_id,
                    TaskInstance.task_id,
                    DagRun.conf,
                    DagRun.execution_date,
                    # map_index will be -1 for all non-mapped tasks
                    TaskInstance.map_index,
                    (
                        sa.extract(
                            "epoch",
                            func.coalesce(
                                TaskInstance.end_date,
                                DagRun.end_date,
                                datetime.datetime.now(tz=datetime.UTC).isoformat(),
                            )
                            - TaskInstance.start_date,
                        )
                        / 60
                    ).label("task_duration"),
                )
                .join(TaskInstance.dag_run)
                # filter to correct dag
                .filter(TaskInstance.dag_id.in_([self.dag_id]))
                # filter to set of tasks that we want to alert on
                .filter(
                    or_(*[config.build_task_filter_clause() for config in self.configs])
                )
                # filter to the set of task states we know how to reason about
                .filter(TaskInstance.state.in_(STARTED_TASK_STATES))
                # all of the above task states should have a started time, but just
                # make sure it does
                .filter(TaskInstance.start_date.isnot(None))
                .filter(
                    func.age(TaskInstance.start_date)
                    < text(f"interval '{lookback.total_seconds()} seconds'")
                )
            ).cte("latest_tasks")

            latest_tasks_with_states = (
                session.query(
                    latest_tasks,
                    case(
                        self._build_state_case_clauses(),
                        else_=JobRunState.UNKNOWN.value,
                    ).label("state"),
                    case(
                        self._build_threshold_case_clauses(),
                        else_=None,
                    ).label("threshold_exceeded"),
                )
            ).cte("latest_tasks_with_states")

            # Every non-mapped task is already a single row.
            # For mapped tasks, groups each group of mapped tasks into a single row per
            # "parent task", the value for a task instance is assigned like:
            #   - If any member failed, the group is considered failed
            #   - If any member has not completed its run, the group will be pending
            #   - If any member is unknown, the group's state is unknown
            #   - A group is only considered successful if all members are successful

            query = session.query(
                latest_tasks_with_states.c.dag_id,
                latest_tasks_with_states.c.task_id,
                latest_tasks_with_states.c.conf,
                latest_tasks_with_states.c.execution_date,
                func.max(latest_tasks_with_states.c.task_duration).label(
                    "max_duration"
                ),
                func.min(latest_tasks_with_states.c.threshold_exceeded).label(
                    "threshold_exceeded"
                ),
                func.max(latest_tasks_with_states.c.state).label("state"),
            ).group_by(
                latest_tasks_with_states.c.dag_id,
                latest_tasks_with_states.c.task_id,
                latest_tasks_with_states.c.conf,
                latest_tasks_with_states.c.execution_date,
            )

            return [
                JobRun.from_airflow_task_instance(
                    dag_id=task_instance_run.dag_id,
                    execution_date=task_instance_run.execution_date,
                    conf=task_instance_run.conf,
                    task_id=task_instance_run.task_id,
                    state=task_instance_run.state,
                    error_message=self.task_runtime_error_message(
                        max_duration=task_instance_run.max_duration,
                        threshold_exceeded=task_instance_run.threshold_exceeded,
                        task_id=task_instance_run.task_id,
                        state=task_instance_run.state,
                    ),
                    job_type=JobRunType.RUNTIME_MONITORING,
                )
                for task_instance_run in query.all()
            ]
