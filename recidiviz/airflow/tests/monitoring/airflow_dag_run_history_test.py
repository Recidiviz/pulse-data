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
"""Tests for the airflow dag metrics"""

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement
import contextlib
import datetime
import os
from typing import Any, Generator, List
from unittest import TestCase
from unittest.mock import call, patch

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils.state import DagRunState
from airflow.utils.timezone import utc
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.airflow_dag_run_history import (
    DAG_RUN_METADATA_TABLE_ADDRESS,
    DAG_RUN_METADATA_TABLE_SCHEMA,
    build_dag_run_history,
    generate_airflow_dag_run_history,
)
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest
from recidiviz.source_tables import yaml_managed
from recidiviz.source_tables.source_table_config import SourceTableConfig

_PROJECT_ID = "recidiviz-testing"
_TEST_DAG_ID = "test_dag"


AIRFLOW_OPERATIONS_DIRECTORY = f"{os.path.dirname(yaml_managed.__file__)}/{DAG_RUN_METADATA_TABLE_ADDRESS.dataset_id}/"
DAG_RUN_METADATA_TABLE_PATH = os.path.join(
    AIRFLOW_OPERATIONS_DIRECTORY, f"{DAG_RUN_METADATA_TABLE_ADDRESS.table_id}.yaml"
)


def dummy_dag_run(dag: DAG, date: str, *, state: DagRunState | None = None) -> DagRun:
    try:
        execution_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        execution_date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M")

    execution_date = execution_date.replace(tzinfo=datetime.timezone.utc)

    return DagRun(
        dag_id=dag.dag_id,
        run_id=execution_date.strftime("%Y-%m-%d-%H:%M"),
        run_type="manual",
        start_date=execution_date,
        execution_date=execution_date,
        state=state,
    )


def dummy_ti(task: PythonOperator, dag_run: DagRun, **kwargs: Any) -> TaskInstance:
    return TaskInstance(
        task=task,
        run_id=dag_run.run_id,
        execution_date=dag_run.execution_date,
        **kwargs,
    )


def dummy_mapped_tis(
    task: PythonOperator, dag_run: DagRun, states: List[str]
) -> List[TaskInstance]:
    return [
        dummy_ti(task=task, dag_run=dag_run, state=state, map_index=index)
        for index, state in enumerate(states)
    ]


def dummy_task(dag: DAG, name: str) -> PythonOperator:
    return PythonOperator(
        dag=dag, task_id=name, python_callable=lambda: None, retries=3
    )


test_dag = DAG(
    dag_id=_TEST_DAG_ID,
    start_date=datetime.datetime(year=2023, month=6, day=21),
    schedule=None,
)

parent_task = dummy_task(test_dag, "parent_task")
child_task = dummy_task(test_dag, "child_task")
parent_task >> child_task

TEST_START_DATE_LOOKBACK = datetime.timedelta(days=20 * 365)


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class TestGenerateAirflowDAGMetrics(AirflowIntegrationTest):
    """Tests for generate_airflow_dag_metrics"""

    maxDiff = None

    def setUp(self) -> None:
        self.bq_patch = patch(
            "recidiviz.airflow.dags.monitoring.airflow_dag_run_history.BigQueryClientImpl"
        )
        self.bq_mock = self.bq_patch.start()

        # compatibility issues w/ freezegun, see https://github.com/apache/airflow/pull/25511#issuecomment-1204297524
        self.frozen_time = datetime.datetime(2024, 1, 26, 3, 4, 6, tzinfo=datetime.UTC)
        self.write_datetime_patcher = patch(
            "recidiviz.airflow.dags.monitoring.airflow_dag_run_history.datetime",
        )
        self.write_datetime_patcher.start().datetime.now.return_value = self.frozen_time
        return super().setUp()

    def tearDown(self) -> None:
        self.bq_patch.stop()
        self.write_datetime_patcher.stop()
        return super().tearDown()

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

    def test_no_history(self) -> None:
        job_history = build_dag_run_history(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertEqual([], job_history)

    def test_no_terminated_tasks(self) -> None:
        with self._get_session() as session:
            july_sixth = dummy_dag_run(test_dag, "2023-07-06")
            july_sixth_parent = dummy_ti(parent_task, july_sixth, state="running")

            session.add_all([july_sixth, july_sixth_parent])

        job_history = build_dag_run_history(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertEqual([], job_history)

    def test_multiple_runs_same_dag(self) -> None:
        with self._get_session() as session:
            july_sixth = dummy_dag_run(test_dag, "2023-07-06", state=DagRunState.FAILED)
            july_sixth_parent = dummy_ti(parent_task, july_sixth, state="success")

            july_sixth_children = dummy_mapped_tis(
                child_task, july_sixth, ["success", "success"]
            )
            july_sixth.end_date = datetime.datetime(2023, 7, 6, 1, tzinfo=utc)

            july_seventh = dummy_dag_run(
                test_dag, "2023-07-07", state=DagRunState.SUCCESS
            )
            july_seventh_parent = dummy_ti(parent_task, july_seventh, state="success")
            july_seventh_children = dummy_mapped_tis(
                child_task, july_seventh, ["success", "failed"]
            )
            july_seventh.end_date = datetime.datetime(2023, 7, 7, 1, tzinfo=utc)

            july_eighth = dummy_dag_run(test_dag, "2023-07-08")
            july_eighth_parent = dummy_ti(parent_task, july_eighth)
            july_eighth_children = dummy_ti(child_task, july_eighth)

            session.add_all(
                [
                    july_sixth,
                    july_sixth_parent,
                    *july_sixth_children,
                    july_seventh,
                    july_seventh_parent,
                    *july_seventh_children,
                    july_eighth,
                    july_eighth_parent,
                    july_eighth_children,
                ]
            )

        job_history = build_dag_run_history(
            lookback=TEST_START_DATE_LOOKBACK,
        )

        expected_results = [
            {
                "write_time": self.frozen_time,
                "project_id": _PROJECT_ID,
                "dag_id": _TEST_DAG_ID,
                "dag_run_id": "2023-07-06-00:00",
                "execution_time": datetime.datetime(2023, 7, 6, tzinfo=utc),
                "start_time": datetime.datetime(2023, 7, 6, tzinfo=utc),
                "end_time": datetime.datetime(2023, 7, 6, 1, tzinfo=utc),
                "dag_run_config": {},
                "terminal_state": "failed",
            },
            {
                "write_time": self.frozen_time,
                "project_id": _PROJECT_ID,
                "dag_id": _TEST_DAG_ID,
                "dag_run_id": "2023-07-07-00:00",
                "execution_time": datetime.datetime(2023, 7, 7, tzinfo=utc),
                "start_time": datetime.datetime(2023, 7, 7, tzinfo=utc),
                "end_time": datetime.datetime(2023, 7, 7, 1, tzinfo=utc),
                "dag_run_config": {},
                "terminal_state": "success",
            },
            # the third run is still in progress
        ]

        self.assertEqual(expected_results, job_history)

        generate_airflow_dag_run_history(lookback=TEST_START_DATE_LOOKBACK)
        self.bq_mock().stream_into_table.has_calls(
            [
                call(DAG_RUN_METADATA_TABLE_ADDRESS, result)
                for result in expected_results
            ]
        )


class DagRunMetadataSchemaTest(TestCase):
    def test_schema_matches_source_table(self) -> None:
        results_table = SourceTableConfig.from_file(DAG_RUN_METADATA_TABLE_PATH)

        assert results_table.address == DAG_RUN_METADATA_TABLE_ADDRESS
        assert {
            (field.field_type, field.name) for field in results_table.schema_fields
        } == {(field.field_type, field.name) for field in DAG_RUN_METADATA_TABLE_SCHEMA}
