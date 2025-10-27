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
"""Tests for the AirflowTaskHistoryBuilderMixin."""

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement
import contextlib
import datetime
from typing import Any, Generator
from unittest.mock import patch

import attr
from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.operators.python import PythonOperator
from freezegun import freeze_time
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.airflow_task_history_builder_mixin import (
    AirflowTaskHistoryBuilderMixin,
)
from recidiviz.airflow.dags.raw_data.utils import partition_as_list
from recidiviz.airflow.tests.fixtures import monitoring as monitoring_fixtures
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest

_PROJECT_ID = "recidiviz-testing"
_TEST_DAG_ID = "test_dag"
_TEST_DAG_ID_2 = "test_dag_2"


@attr.define(frozen=True, kw_only=True)
class MixinResult:
    dag_id: str = attr.field()
    execution_date: datetime.datetime = attr.field()
    conf: str = attr.field(converter=str)
    dag_end_date: datetime.datetime | None = attr.field()
    task_id: str = attr.field()
    map_index: int = attr.field()
    try_number: int = attr.field()
    max_tries: int = attr.field()
    task_state: str = attr.field()
    start_date: datetime.datetime | None = attr.field()
    end_date: datetime.datetime | None = attr.field()

    @classmethod
    def from_fixture_row(cls, row: dict) -> "MixinResult":
        return MixinResult(
            dag_id=row["dag_id"],
            execution_date=datetime.datetime.fromisoformat(row["execution_date"]),
            conf=row["conf"],
            dag_end_date=row["dag_end_date"] or None,
            task_id=row["task_id"],
            task_state=row["task_state"],
            try_number=int(row["try_number"]),
            max_tries=int(row["max_tries"]),
            map_index=int(row["map_index"]),
            start_date=(
                datetime.datetime.fromisoformat(row["start_date"])
                if row["start_date"]
                else None
            ),
            end_date=(
                datetime.datetime.fromisoformat(row["start_date"])
                if row["start_date"]
                else None
            ),
        )


def read_csv_fixture_for_delegate(file: str) -> list[MixinResult]:
    return [
        MixinResult.from_fixture_row(row)
        for row in monitoring_fixtures.read_csv_fixture(file)
    ]


def dummy_dag_run(dag: DAG, date: str, **kwargs: Any) -> DagRun:
    try:
        execution_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        execution_date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M")

    execution_date = execution_date.replace(tzinfo=datetime.timezone.utc)

    dr = DagRun(
        dag_id=dag.dag_id,
        run_id=execution_date.strftime("%Y-%m-%d-%H:%M"),
        run_type="manual",
        start_date=execution_date,
        execution_date=execution_date,
    )

    if kwargs:
        for k, v in kwargs.items():
            setattr(dr, k, v)

    return dr


def dummy_ti_retry(
    ti: TaskInstance, retry_state: str, **retry_kwargs: Any
) -> tuple[TaskInstance, TaskInstanceHistory]:
    ti_history = TaskInstanceHistory(ti, state=ti.state)
    ti.try_number = ti.next_try_number
    ti.state = retry_state

    if retry_kwargs:
        for k, v in retry_kwargs.items():
            setattr(ti, k, v)

    return ti, ti_history


def dummy_ti(
    task: PythonOperator,
    dag_run: DagRun,
    state: str,
    try_number: int,
    max_tries: int,
    map_index: int = -1,
    **kwargs: Any,
) -> TaskInstance:
    ti = TaskInstance(
        task=task,
        run_id=dag_run.run_id,
        execution_date=dag_run.execution_date,
        state=state,
        map_index=map_index,
    )
    ti.try_number = try_number
    ti.max_tries = max_tries

    if kwargs:
        for k, v in kwargs.items():
            setattr(ti, k, v)

    return ti


def dummy_task(dag: DAG, name: str, retries: int) -> PythonOperator:
    return PythonOperator(
        dag=dag, task_id=name, python_callable=lambda: None, retries=retries
    )


test_dag = DAG(
    dag_id=_TEST_DAG_ID,
    start_date=datetime.datetime(year=2023, month=6, day=21),
    schedule=None,
)

parent_task = dummy_task(test_dag, "parent_task", retries=2)
child_task = dummy_task(test_dag, "child_task", retries=2)
parent_task >> child_task

test_dag_2 = DAG(
    dag_id=_TEST_DAG_ID_2,
    start_date=datetime.datetime(year=2023, month=6, day=21),
    schedule=None,
)

parent_task_2 = dummy_task(test_dag_2, "parent_task", retries=2)
child_task_2 = dummy_task(test_dag_2, "child_task", retries=2)
parent_task_2 >> child_task_2


TEST_START_DATE_LOOKBACK = datetime.timedelta(days=20 * 365)


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class AirflowTaskHistoryBuilderMixinTest(AirflowIntegrationTest):
    """Tests for AirflowTaskHistoryBuilderMixin"""

    @contextlib.contextmanager
    def _get_session(self) -> Generator[Session, None, None]:
        session = Session(bind=self.engine)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def test_no_tasks(self) -> None:

        with self._get_session() as session:
            builder = AirflowTaskHistoryBuilderMixin.build_task_history(
                dag_id=_TEST_DAG_ID, lookback=TEST_START_DATE_LOOKBACK
            )
            all_tasks = list(session.execute(builder).all())

            self.assertEqual([], all_tasks)

    def test_mixin_filters_by_dag_id(self) -> None:

        results = read_csv_fixture_for_delegate("test_mixin_filters_by_dag_id.csv")
        dag_two_expected, dag_one_expected = partition_as_list(
            lambda x: x.dag_id == "test_dag", results
        )

        with self._get_session() as session:
            july_sixth = dummy_dag_run(test_dag, "2023-07-06")
            july_sixth_parent = dummy_ti(
                parent_task, july_sixth, "success", try_number=0, max_tries=2
            )
            july_sixth_parent, july_sixth_parent_first_try = dummy_ti_retry(
                july_sixth_parent, "running"
            )

            july_sixth_two = dummy_dag_run(test_dag_2, "2023-07-06")
            july_sixth_two_parent = dummy_ti(
                parent_task_2, july_sixth_two, "success", try_number=0, max_tries=2
            )
            july_sixth_two_parent, july_sixth_two_parent_first_try = dummy_ti_retry(
                july_sixth_two_parent, "running"
            )

            session.add_all(
                [july_sixth, july_sixth_parent, july_sixth_two, july_sixth_two_parent]
            )

            session.commit()

            session.add_all(
                [july_sixth_parent_first_try, july_sixth_two_parent_first_try]
            )
            session.commit()

            first_dag_builder = AirflowTaskHistoryBuilderMixin.build_task_history(
                dag_id=_TEST_DAG_ID, lookback=TEST_START_DATE_LOOKBACK
            )
            first_dag_tasks = [
                MixinResult(**row) for row in session.execute(first_dag_builder).all()
            ]

            self.assertEqual(
                set(dag_one_expected),
                set(first_dag_tasks),
            )

            second_dag_builder = AirflowTaskHistoryBuilderMixin.build_task_history(
                dag_id=_TEST_DAG_ID_2, lookback=TEST_START_DATE_LOOKBACK
            )
            second_dag_tasks = [
                MixinResult(**row) for row in session.execute(second_dag_builder).all()
            ]

            self.assertEqual(
                set(dag_two_expected),
                set(second_dag_tasks),
            )

    def test_mixin_with_no_retries(self) -> None:
        """Test tasks with retries=0 (max_tries=1) are handled correctly"""
        # Create a test DAG with a task that has no retries
        test_dag_no_retries = DAG(
            dag_id="test_dag_no_retries",
            start_date=datetime.datetime(year=2023, month=6, day=21),
            schedule=None,
        )
        no_retry_task = dummy_task(test_dag_no_retries, "no_retry_task", retries=0)

        results = read_csv_fixture_for_delegate("test_mixin_with_no_retries.csv")

        with self._get_session() as session:
            july_sixth = dummy_dag_run(test_dag_no_retries, "2023-07-06")
            july_sixth_task = dummy_ti(
                no_retry_task, july_sixth, "failed", try_number=0, max_tries=-1
            )

            session.add_all([july_sixth, july_sixth_task])
            session.commit()

            builder = AirflowTaskHistoryBuilderMixin.build_task_history(
                dag_id="test_dag_no_retries", lookback=TEST_START_DATE_LOOKBACK
            )
            tasks = [MixinResult(**row) for row in session.execute(builder).all()]

            self.assertEqual(
                set(results),
                set(tasks),
            )

    def test_mixin_applies_lookback(self) -> None:

        with self._get_session() as session:
            for day in range(6, 10):
                july_day = dummy_dag_run(test_dag, f"2023-07-0{day}")
                july_day_parent = dummy_ti(
                    parent_task, july_day, "success", try_number=0, max_tries=2
                )
                july_day_parent, july_day_parent_first_try = dummy_ti_retry(
                    july_day_parent, "running"
                )

                session.add_all(
                    [
                        july_day,
                        july_day_parent,
                    ]
                )

                session.commit()

                session.add_all([july_day_parent_first_try])
                session.commit()

            with freeze_time(datetime.datetime(2023, 7, 9, 1)):
                no_lookback = AirflowTaskHistoryBuilderMixin.build_task_history(
                    dag_id=_TEST_DAG_ID, lookback=datetime.timedelta(seconds=1)
                )
                tasks = [
                    MixinResult(**row) for row in session.execute(no_lookback).all()
                ]

                self.assertEqual(
                    set(),
                    set(tasks),
                )

                some_lookback = AirflowTaskHistoryBuilderMixin.build_task_history(
                    dag_id=_TEST_DAG_ID, lookback=datetime.timedelta(days=3)
                )
                tasks = [
                    MixinResult(**row) for row in session.execute(some_lookback).all()
                ]

                self.assertEqual(
                    set(
                        read_csv_fixture_for_delegate(
                            "test_mixin_applies_some_lookback.csv"
                        )
                    ),
                    set(tasks),
                )

                full_lookback = AirflowTaskHistoryBuilderMixin.build_task_history(
                    dag_id=_TEST_DAG_ID, lookback=datetime.timedelta(days=4)
                )
                tasks = [
                    MixinResult(**row) for row in session.execute(full_lookback).all()
                ]

                self.assertEqual(
                    set(
                        read_csv_fixture_for_delegate(
                            "test_mixin_applies_full_lookback.csv"
                        )
                    ),
                    set(tasks),
                )
