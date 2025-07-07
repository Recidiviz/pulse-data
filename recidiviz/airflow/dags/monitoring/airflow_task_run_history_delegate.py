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
"""JobRunHistoryDelegate for Airflow's TaskInstance"""
import datetime

import sqlalchemy as sa
from airflow.utils.session import create_session
from airflow.utils.state import State
from sqlalchemy import case, func

from recidiviz.airflow.dags.monitoring.airflow_task_history_builder_mixin import (
    AirflowTaskHistoryBuilderMixin,
)
from recidiviz.airflow.dags.monitoring.job_run import JobRun, JobRunState, JobRunType
from recidiviz.airflow.dags.monitoring.job_run_history_delegate import (
    JobRunHistoryDelegate,
)


class AirflowTaskRunHistoryDelegate(
    JobRunHistoryDelegate, AirflowTaskHistoryBuilderMixin
):
    """Builds JobRun objects from Airflow's TaskInstance model."""

    def __init__(self, *, dag_id: str) -> None:
        self.dag_id = dag_id

    def fetch_job_runs(self, *, lookback: datetime.timedelta) -> list[JobRun]:
        with create_session() as session:

            # Airflow stores the most recent run of each task in the TaskInstance table,
            # and all previous runs in TaskInstanceHistory. We want to fetch all previous
            # runs of tasks
            all_tasks = self.build_task_history(
                dag_id=self.dag_id, lookback=lookback
            ).cte("all_tasks")

            filtered_tasks_with_state = session.query(
                all_tasks,
                case(
                    [
                        (
                            sa.column("task_state").in_(State.unfinished),
                            JobRunState.PENDING.value,
                        ),
                        (
                            sa.column("task_state").in_([State.FAILED]),
                            JobRunState.FAILED.value,
                        ),
                        (
                            sa.column("task_state").in_([State.SUCCESS]),
                            JobRunState.SUCCESS.value,
                        ),
                    ],
                    else_=JobRunState.UNKNOWN.value,
                ).label("state"),
            ).cte("filtered_tasks_with_state")

            # Group mapped tasks into a single row per parent task, the value for a task
            # instance is assigned like:
            #   - If any member failed, the group is considered failed
            #   - If any member has not completed its run, the group will be pending
            #   - If any member is unknown, the group's state is unknown
            #   - A group is only considered successful if all members are successful

            query = session.query(
                filtered_tasks_with_state.c.dag_id,
                filtered_tasks_with_state.c.execution_date,
                filtered_tasks_with_state.c.conf,
                filtered_tasks_with_state.c.task_id,
                filtered_tasks_with_state.c.try_number,
                func.max(filtered_tasks_with_state.c.state).label("state"),
            ).group_by(
                filtered_tasks_with_state.c.dag_id,
                filtered_tasks_with_state.c.execution_date,
                filtered_tasks_with_state.c.conf,
                filtered_tasks_with_state.c.task_id,
                filtered_tasks_with_state.c.try_number,
            )
            return [
                JobRun.from_airflow_task_instance(
                    dag_id=task_instance_run.dag_id,
                    execution_date=task_instance_run.execution_date,
                    conf=task_instance_run.conf,
                    task_id=task_instance_run.task_id,
                    state=task_instance_run.state,
                    try_number=task_instance_run.try_number,
                    job_type=JobRunType.AIRFLOW_TASK_RUN,
                    error_message=None,
                )
                for task_instance_run in query.all()
            ]
