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
"""Tests for the AirflowTaskRunHistoryDelegate."""

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement
import contextlib
import datetime
from typing import Any, Generator, List
from unittest.mock import MagicMock, patch

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.operators.python import PythonOperator
from freezegun import freeze_time
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.airflow_task_runtime_delegate import (
    AirflowAllTaskRuntimeConfig,
    AirflowExplicitTaskRuntimeAlertingConfig,
    AirflowTaskNameRegexRuntimeAlertingConfig,
    AirflowTaskRuntimeDelegate,
)
from recidiviz.airflow.dags.monitoring.job_run import JobRun, JobRunState, JobRunType
from recidiviz.airflow.tests.fixtures import monitoring as monitoring_fixtures
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest

_PROJECT_ID = "recidiviz-testing"
_TEST_DAG_ID = "test_dag"


def read_csv_fixture_for_delegate(file: str) -> set[JobRun]:
    return {
        JobRun(
            dag_id=row["dag_id"],
            execution_date=datetime.datetime.fromisoformat(row["execution_date"]),
            dag_run_config=row["conf"],
            job_id=row["job_id"],
            state=JobRunState(int(row["state"])),
            error_message=row["error_message"] or None,
            job_type=JobRunType.RUNTIME_MONITORING,
        )
        for row in monitoring_fixtures.read_csv_fixture(file)
    }


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


def dummy_ti(
    task: PythonOperator,
    dag_run: DagRun,
    state: str,
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

    if kwargs:
        for k, v in kwargs.items():
            setattr(ti, k, v)

    return ti


def dummy_mapped_tis(
    task: PythonOperator, dag_run: DagRun, states: List[str], **kwargs: Any
) -> List[TaskInstance]:
    return [
        dummy_ti(task=task, dag_run=dag_run, state=state, map_index=index, **kwargs)
        for index, state in enumerate(states)
    ]


def dummy_task(dag: DAG, name: str) -> PythonOperator:
    return PythonOperator(
        dag=dag, task_id=name, python_callable=lambda: None, retries=-1
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
class AirflowTaskRuntimeDelegateTest(AirflowIntegrationTest):
    """Tests for AirflowTaskRuntimeDelegate"""

    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
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

    def test_no_tasks(self) -> None:
        all_config = AirflowAllTaskRuntimeConfig(dag_id=_TEST_DAG_ID, runtime_minutes=0)

        job_history = AirflowTaskRuntimeDelegate(
            dag_id=test_dag.dag_id, configs=[all_config]
        ).fetch_job_runs(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertEqual([], job_history)

    def test_running_too_long(self) -> None:
        with self._get_session() as session:
            jan_one = dummy_dag_run(test_dag, "2024-01-01")
            jan_one_parent = dummy_ti(
                parent_task,
                jan_one,
                "success",
                start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2024, 1, 1, 1, tzinfo=datetime.UTC),
            )
            jan_one_child = dummy_ti(
                child_task,
                jan_one,
                "running",
                start_date=datetime.datetime(2024, 1, 1, 1, tzinfo=datetime.UTC),
            )

            session.add_all([jan_one, jan_one_parent, jan_one_child])

        current_time = datetime.datetime(2024, 1, 1, 3, 10, tzinfo=datetime.UTC)

        with freeze_time(current_time):
            explicit_config = AirflowExplicitTaskRuntimeAlertingConfig(
                dag_id=_TEST_DAG_ID,
                task_names=["parent_task", "child_task"],
                runtime_minutes=120,
            )

            job_history = AirflowTaskRuntimeDelegate(
                dag_id=test_dag.dag_id, configs=[explicit_config]
            ).fetch_job_runs(
                lookback=TEST_START_DATE_LOOKBACK,
            )
            self.assertSetEqual(
                set(job_history),
                read_csv_fixture_for_delegate("test_mixed_status.csv"),
            )

            task_pattern_config = AirflowTaskNameRegexRuntimeAlertingConfig(
                dag_id=_TEST_DAG_ID,
                task_name_regex=".*task",
                runtime_minutes=120,
            )

            pattern_history = AirflowTaskRuntimeDelegate(
                dag_id=test_dag.dag_id, configs=[task_pattern_config]
            ).fetch_job_runs(
                lookback=TEST_START_DATE_LOOKBACK,
            )
            self.assertSetEqual(
                set(pattern_history),
                read_csv_fixture_for_delegate("test_mixed_status.csv"),
            )

            child_pattern_config = AirflowTaskNameRegexRuntimeAlertingConfig(
                dag_id=_TEST_DAG_ID,
                task_name_regex="child.*",
                runtime_minutes=120,
            )

            pattern_pattern_config = AirflowTaskNameRegexRuntimeAlertingConfig(
                dag_id=_TEST_DAG_ID,
                task_name_regex="parent.*",
                runtime_minutes=120,
            )

            pattern_history = AirflowTaskRuntimeDelegate(
                dag_id=test_dag.dag_id,
                configs=[child_pattern_config, pattern_pattern_config],
            ).fetch_job_runs(
                lookback=TEST_START_DATE_LOOKBACK,
            )
            self.assertSetEqual(
                set(pattern_history),
                read_csv_fixture_for_delegate("test_mixed_status.csv"),
            )

    def test_graph_map_index_deadline(self) -> None:
        """
        Given a DAG grid view that looks like this:

                     2023-07-06 2023-07-07  2023-07-08
        parent_task  🟩         🟩          🟩
        child_task   🟩         🟥          🟥
            idx=0    🟩         🟥          🟥
            idx=1    🟩         🟥          🟩

        Assert that the last successful run of `child_task` was 2023-07-06
        2023-07-08 is not considered fully successful as one of the mapped tasks failed
        """
        with self._get_session() as session:
            july_sixth = dummy_dag_run(test_dag, "2023-07-06")
            july_sixth_parent = dummy_ti(
                parent_task,
                july_sixth,
                "success",
                start_date=datetime.datetime(2023, 7, 6, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2023, 7, 6, 1, tzinfo=datetime.UTC),
            )

            july_sixth_children = dummy_mapped_tis(
                child_task,
                july_sixth,
                ["success", "success"],
                start_date=datetime.datetime(2023, 7, 6, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2023, 7, 6, 1, tzinfo=datetime.UTC),
            )

            july_seventh = dummy_dag_run(test_dag, "2023-07-07")
            july_seventh_parent = dummy_ti(
                parent_task,
                july_seventh,
                state="success",
                start_date=datetime.datetime(2023, 7, 7, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2023, 7, 7, 0, 1, tzinfo=datetime.UTC),
            )
            july_seventh_children = [
                # 120 mins, trips the task-specific
                dummy_ti(
                    child_task,
                    july_seventh,
                    "failed",
                    map_index=0,
                    start_date=datetime.datetime(2023, 7, 7, tzinfo=datetime.UTC),
                    end_date=datetime.datetime(2023, 7, 7, 1, tzinfo=datetime.UTC),
                ),
                # 180 mins, trips the dag wide
                dummy_ti(
                    child_task,
                    july_seventh,
                    "failed",
                    map_index=1,
                    start_date=datetime.datetime(2023, 7, 7, tzinfo=datetime.UTC),
                    end_date=datetime.datetime(2023, 7, 7, 2, tzinfo=datetime.UTC),
                ),
            ]

            july_eighth = dummy_dag_run(test_dag, "2023-07-08")
            july_eighth_parent = dummy_ti(
                parent_task,
                july_eighth,
                state="success",
                start_date=datetime.datetime(2023, 7, 8, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2023, 7, 8, 0, 1, tzinfo=datetime.UTC),
            )
            july_eighth_children = [
                dummy_ti(
                    child_task,
                    july_eighth,
                    "failed",
                    map_index=0,
                    start_date=datetime.datetime(2023, 7, 8, tzinfo=datetime.UTC),
                    end_date=datetime.datetime(2023, 7, 8, 1, tzinfo=datetime.UTC),
                ),
                # 180 mins
                dummy_ti(
                    child_task,
                    july_eighth,
                    "success",
                    map_index=1,
                    start_date=datetime.datetime(2023, 7, 8, tzinfo=datetime.UTC),
                    end_date=datetime.datetime(2023, 7, 8, 3, tzinfo=datetime.UTC),
                ),
            ]

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
                    *july_eighth_children,
                ]
            )

        config = AirflowExplicitTaskRuntimeAlertingConfig(
            dag_id=_TEST_DAG_ID,
            task_names=["child_task"],
            runtime_minutes=119,
        )

        all_config = AirflowAllTaskRuntimeConfig(
            dag_id=_TEST_DAG_ID, runtime_minutes=179
        )

        job_history = AirflowTaskRuntimeDelegate(
            dag_id=test_dag.dag_id, configs=[config, all_config]
        ).fetch_job_runs(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertSetEqual(
            set(job_history),
            read_csv_fixture_for_delegate("test_graph_map_index_deadline.csv"),
        )

    def test_graph_task_up_for_retry(self) -> None:
        """
        Given a DAG that has a task which is retryable and a task that has failed its first attempt

                    2023-07-10
        parent_task  🟨

        Assert that it does not cause an incident as it is pending retry -- it's not in the
        set of terminal task states.
        """
        with self._get_session() as session:
            july_sixth_primary = dummy_dag_run(test_dag, "2023-07-10 12:00")
            july_sixth_parent_primary = dummy_ti(
                parent_task,
                july_sixth_primary,
                "up_for_retry",
            )

            session.add_all(
                [
                    july_sixth_primary,
                    july_sixth_parent_primary,
                ]
            )

        all_config = AirflowAllTaskRuntimeConfig(dag_id=_TEST_DAG_ID, runtime_minutes=0)

        job_history = AirflowTaskRuntimeDelegate(
            dag_id=test_dag.dag_id, configs=[all_config]
        ).fetch_job_runs(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertSetEqual(set(job_history), set())

    @patch(
        "recidiviz.airflow.dags.monitoring.utils.get_discrete_configuration_parameters"
    )
    def test_graph_config_idempotency(
        self, mock_get_discrete_parameters: MagicMock
    ) -> None:
        """
        Given a DAG which utilizes configuration parameters that should be treated as distinct sets of runs
        for example, a successful parent_task in PRIMARY instance won't resolve an open incident in SECONDARY

                                               2023-07-06 2023-07-07 2023-07-08
        {"instance": "PRIMARY"}   parent_task  🟩         🟩         🟩
        {"instance": "SECONDARY"} parent_task  🟩         🟥         🟥
        {"instance": "TERTIARY"}  parent_task  🟩         🟥         🟩

        Assert that the last successful run of `parent_task` for the primary instance was 2023-07-07
        Assert that the last successful run of `parent_task` for the secondary instance was 2023-07-06
        Assert that the tertiary incident from 2023-07-07 is resolved
        """

        def _fake_get_discrete_params(project_id: str, dag_id: str) -> str:
            if project_id == _PROJECT_ID and dag_id == _TEST_DAG_ID:
                return "instance"
            raise ValueError(f"Unexpected dag_id [{dag_id}]")

        mock_get_discrete_parameters.side_effect = _fake_get_discrete_params

        with self._get_session() as session:
            july_sixth_primary = dummy_dag_run(
                test_dag,
                "2023-07-06 12:00",
                conf={"instance": "PRIMARY", "extraneous_param": 1},
            )
            july_sixth_parent_primary = dummy_ti(
                parent_task,
                july_sixth_primary,
                "success",
                start_date=datetime.datetime(2023, 7, 6, 12, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2023, 7, 6, 13, tzinfo=datetime.UTC),
            )

            july_seventh_primary = dummy_dag_run(
                test_dag,
                "2023-07-07 12:00",
                conf={"instance": "PRIMARY", "extraneous_param": 2},
            )
            july_seventh_primary_parent = dummy_ti(
                parent_task,
                july_seventh_primary,
                state="success",
                start_date=datetime.datetime(2023, 7, 7, 12, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2023, 7, 7, 13, tzinfo=datetime.UTC),
            )

            july_eighth_primary = dummy_dag_run(
                test_dag, "2023-07-08 12:00", conf={"instance": "PRIMARY"}
            )

            july_eighth_primary_parent = dummy_ti(
                parent_task,
                july_eighth_primary,
                state="success",
                start_date=datetime.datetime(2023, 7, 8, 12, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2023, 7, 8, 13, tzinfo=datetime.UTC),
            )

            july_sixth_secondary = dummy_dag_run(
                test_dag,
                "2023-07-06 12:01",
                conf={"instance": "SECONDARY", "extraneous_param": 3},
            )
            july_sixth_parent_secondary = dummy_ti(
                parent_task,
                july_sixth_secondary,
                state="success",
                start_date=datetime.datetime(2023, 7, 6, 12, 1, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2023, 7, 6, 13, 1, tzinfo=datetime.UTC),
            )

            july_seventh_secondary = dummy_dag_run(
                test_dag,
                "2023-07-07 12:01",
                conf={"instance": "SECONDARY", "extraneous_param": 4},
            )
            july_seventh_secondary_parent = dummy_ti(
                parent_task,
                july_seventh_secondary,
                state="failed",
                start_date=datetime.datetime(2023, 7, 7, 12, 1, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2023, 7, 7, 14, 1, tzinfo=datetime.UTC),
            )

            july_eighth_secondary = dummy_dag_run(
                test_dag, "2023-07-08 12:01", conf={"instance": "SECONDARY"}
            )

            july_eighth_secondary_parent = dummy_ti(
                parent_task,
                july_eighth_secondary,
                state="failed",
                start_date=datetime.datetime(2023, 7, 8, 12, 1, tzinfo=datetime.UTC),
                # no end date, will use the current date as the end date
            )

            july_sixth_tertiary = dummy_dag_run(
                test_dag,
                "2023-07-06 12:02",
                conf={"instance": "TERTIARY", "extraneous_param": 3},
            )
            july_sixth_parent_tertiary = dummy_ti(
                parent_task,
                july_sixth_tertiary,
                state="success",
                start_date=datetime.datetime(2023, 7, 6, 12, 2, tzinfo=datetime.UTC),
                end_date=datetime.datetime(2023, 7, 6, 13, 1, tzinfo=datetime.UTC),
            )

            july_seventh_tertiary = dummy_dag_run(
                test_dag,
                "2023-07-07 12:02",
                conf={"instance": "TERTIARY", "extraneous_param": 4},
                end_date=datetime.datetime(2023, 7, 7, 14, 1, tzinfo=datetime.UTC),
            )
            july_seventh_tertiary_parent = dummy_ti(
                parent_task,
                july_seventh_tertiary,
                state="failed",
                start_date=datetime.datetime(2023, 7, 7, 12, 1, tzinfo=datetime.UTC),
                # no end date, will use dag run end date (still failure)
            )

            july_eighth_tertiary = dummy_dag_run(
                test_dag,
                "2023-07-08 12:02",
                conf={"instance": "TERTIARY"},
                end_date=datetime.datetime(2023, 7, 8, 12, 2, tzinfo=datetime.UTC),
            )

            july_eighth_tertiary_parent = dummy_ti(
                parent_task,
                july_eighth_tertiary,
                state="success",
                start_date=datetime.datetime(2023, 7, 8, 13, 2, tzinfo=datetime.UTC),
                # no end date, will duse the dag run end date (success)
            )

            session.add_all(
                [
                    july_sixth_primary,
                    july_sixth_parent_primary,
                    july_seventh_primary,
                    july_seventh_primary_parent,
                    july_eighth_primary,
                    july_eighth_primary_parent,
                    july_sixth_secondary,
                    july_sixth_parent_secondary,
                    july_seventh_secondary,
                    july_seventh_secondary_parent,
                    july_eighth_secondary,
                    july_eighth_secondary_parent,
                    july_sixth_tertiary,
                    july_sixth_parent_tertiary,
                    july_seventh_tertiary,
                    july_seventh_tertiary_parent,
                    july_eighth_tertiary,
                    july_eighth_tertiary_parent,
                ]
            )
            session.commit()

        with freeze_time(datetime.datetime(2023, 7, 9, 12, tzinfo=datetime.UTC)):
            explicit_config = AirflowExplicitTaskRuntimeAlertingConfig(
                dag_id=_TEST_DAG_ID,
                task_names=["parent_task"],
                runtime_minutes=60,
            )

            job_history = AirflowTaskRuntimeDelegate(
                dag_id=test_dag.dag_id, configs=[explicit_config]
            ).fetch_job_runs(
                lookback=TEST_START_DATE_LOOKBACK,
            )
            self.assertSetEqual(
                set(job_history),
                read_csv_fixture_for_delegate("test_graph_config_idempotency.csv"),
            )
