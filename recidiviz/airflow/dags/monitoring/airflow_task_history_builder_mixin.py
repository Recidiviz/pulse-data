# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Mixin class for building a full lookback at all attempts of a TaskInstance"""
import datetime

from airflow.models import DagRun, TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from sqlalchemy import and_, func, select, text, union_all
from sqlalchemy.sql import Select
from sqlalchemy.sql.selectable import GenerativeSelect


class AirflowTaskHistoryBuilderMixin:
    """Mixin abstraction for building a full look at Airflow task history, which
    combines the most recent run of tasks from the TaskInstance table with previous
    task attempts from the TaskInstanceHistory table.

    If you need to access additional attributes, from TaskInstance[History] or DagRun
    add them to the select statement in build_task_history_for_model.
    """

    @classmethod
    def build_task_history_for_model(
        cls,
        *,
        task_model: type[TaskInstance] | type[TaskInstanceHistory],
        dag_id: str,
        lookback: datetime.timedelta,
    ) -> Select:
        return (
            select(
                task_model.dag_id,
                DagRun.execution_date,
                DagRun.conf,
                DagRun.end_date.label("dag_end_date"),
                task_model.task_id,
                # map_index will be -1 for all non-mapped tasks
                task_model.map_index,
                task_model.try_number,
                task_model.state.label("task_state"),
                task_model.start_date,
                task_model.end_date,
            )
            .join(
                DagRun,
                and_(
                    DagRun.dag_id == task_model.dag_id,
                    DagRun.run_id == task_model.run_id,
                ),
            )
            .filter(
                func.age(datetime.datetime.now(tz=datetime.UTC), DagRun.execution_date)
                < text(f"interval '{lookback.total_seconds()} seconds'")
            )
            .filter(DagRun.dag_id == dag_id)
        )

    @classmethod
    def build_task_history(
        cls,
        *,
        dag_id: str,
        lookback: datetime.timedelta,
    ) -> GenerativeSelect:
        return union_all(
            cls.build_task_history_for_model(
                task_model=TaskInstance, lookback=lookback, dag_id=dag_id
            ),
            cls.build_task_history_for_model(
                task_model=TaskInstanceHistory, lookback=lookback, dag_id=dag_id
            ),
        )
