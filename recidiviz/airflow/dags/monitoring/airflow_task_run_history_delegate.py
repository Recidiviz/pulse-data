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

from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import State
from sqlalchemy import case, func, text

from recidiviz.airflow.dags.monitoring.job_run import JobRun, JobRunState, JobRunType
from recidiviz.airflow.dags.monitoring.job_run_history_delegate import (
    JobRunHistoryDelegate,
)


class AirflowTaskRunHistoryDelegate(JobRunHistoryDelegate):
    """Builds JobRun objects from Airflow's TaskInstance model."""

    def __init__(self, *, dag_id: str) -> None:
        self.dag_id = dag_id

    def fetch_job_runs(self, *, lookback: datetime.timedelta) -> list[JobRun]:
        with create_session() as session:

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
                                JobRunState.PENDING.value,
                            ),
                            (
                                TaskInstance.state.in_([State.FAILED.value]),
                                JobRunState.FAILED.value,
                            ),
                            (
                                TaskInstance.state.in_([State.SUCCESS]),
                                JobRunState.SUCCESS.value,
                            ),
                        ],
                        else_=JobRunState.UNKNOWN.value,
                    ).label("state"),
                )
                .join(TaskInstance.dag_run)
                .filter(
                    func.age(DagRun.execution_date)
                    < text(f"interval '{lookback.total_seconds()} seconds'")
                )
                .filter(DagRun.dag_id.in_([self.dag_id]))
            ).cte("latest_tasks")

            # Group mapped tasks into a single row per parent task, the value for a task
            # instance is assigned like:
            #   - If any member failed, the group is considered failed
            #   - If any member has not completed its run, the group will be pending
            #   - If any member is unknown, the group's state is unknown
            #   - A group is only considered successful if all members are successful

            query = session.query(
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

            return [
                JobRun.from_airflow_task_instance(
                    **task_instance_run,
                    job_type=JobRunType.AIRFLOW_TASK_RUN,
                    error_message=None,
                )
                for task_instance_run in query.all()
            ]
