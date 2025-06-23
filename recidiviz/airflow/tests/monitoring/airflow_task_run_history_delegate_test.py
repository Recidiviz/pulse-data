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
from typing import Any, Dict, Generator, List
from unittest.mock import MagicMock, patch

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.operators.python import PythonOperator
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.monitoring.airflow_task_run_history_delegate import (
    AirflowTaskRunHistoryDelegate,
)
from recidiviz.airflow.dags.monitoring.job_run import JobRun, JobRunState, JobRunType
from recidiviz.airflow.dags.monitoring.job_run_history_delegate import (
    JobRunHistoryDelegate,
)
from recidiviz.airflow.dags.monitoring.job_run_history_delegate_factory import (
    JobRunHistoryDelegateFactory,
)
from recidiviz.airflow.tests.fixtures import monitoring as monitoring_fixtures
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest

_PROJECT_ID = "recidiviz-testing"
_TEST_DAG_ID = "test_dag"


class FakeJobRunHistoryDelegateFactory(JobRunHistoryDelegateFactory):
    @classmethod
    def build(cls, *, dag_id: str) -> list[JobRunHistoryDelegate]:
        if dag_id == _TEST_DAG_ID:
            return [AirflowTaskRunHistoryDelegate(dag_id=dag_id)]

        raise ValueError(f"Unrecognized DAG :{dag_id}")


def read_csv_fixture_for_delegate(file: str) -> set[JobRun]:
    return {
        JobRun(
            dag_id=row["dag_id"],
            execution_date=datetime.datetime.fromisoformat(row["execution_date"]),
            dag_run_config=row["conf"],
            job_id=row["job_id"],
            state=JobRunState(int(row["state"])),
            error_message=None,
            job_type=JobRunType.AIRFLOW_TASK_RUN,
        )
        for row in monitoring_fixtures.read_csv_fixture(file)
    }


def dummy_dag_run(dag: DAG, date: str, **kwargs: Dict[str, Any]) -> DagRun:
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
        **kwargs,
    )


def dummy_ti(
    task: PythonOperator, dag_run: DagRun, state: str, **kwargs: Any
) -> TaskInstance:
    return TaskInstance(
        task=task,
        run_id=dag_run.run_id,
        execution_date=dag_run.execution_date,
        state=state,
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
class AirflowTaskRunHistoryDelegateTest(AirflowIntegrationTest):
    """Tests for AirflowTaskRunHistoryDelegate"""

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
        job_history = AirflowTaskRunHistoryDelegate(
            dag_id=test_dag.dag_id
        ).fetch_job_runs(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertEqual([], job_history)

    def test_no_terminated_tasks(self) -> None:
        with self._get_session() as session:
            july_sixth = dummy_dag_run(test_dag, "2023-07-06")
            july_sixth_parent = dummy_ti(parent_task, july_sixth, "running")

            session.add_all([july_sixth, july_sixth_parent])

        job_history = AirflowTaskRunHistoryDelegate(
            dag_id=test_dag.dag_id
        ).fetch_job_runs(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertSetEqual(
            set(job_history),
            read_csv_fixture_for_delegate("test_no_terminated_tasks.csv"),
        )

    def test_exactly_one_success(self) -> None:
        with self._get_session() as session:
            july_sixth = dummy_dag_run(test_dag, "2023-07-06")
            july_sixth_parent = dummy_ti(parent_task, july_sixth, "success")

            session.add_all([july_sixth, july_sixth_parent])

        job_history = AirflowTaskRunHistoryDelegate(
            dag_id=test_dag.dag_id
        ).fetch_job_runs(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertSetEqual(
            set(job_history),
            read_csv_fixture_for_delegate("test_exactly_one_success.csv"),
        )

    def test_graph_map_index(self) -> None:
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
            july_sixth_parent = dummy_ti(parent_task, july_sixth, "success")

            july_sixth_children = dummy_mapped_tis(
                child_task, july_sixth, ["success", "success"]
            )

            july_seventh = dummy_dag_run(test_dag, "2023-07-07")
            july_seventh_parent = dummy_ti(parent_task, july_seventh, "success")
            july_seventh_children = dummy_mapped_tis(
                child_task, july_seventh, ["success", "failed"]
            )

            july_eighth = dummy_dag_run(test_dag, "2023-07-08")
            july_eighth_parent = dummy_ti(parent_task, july_eighth, "success")
            july_eighth_children = dummy_mapped_tis(
                child_task, july_eighth, ["failed", "success"]
            )

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

        job_history = AirflowTaskRunHistoryDelegate(
            dag_id=test_dag.dag_id
        ).fetch_job_runs(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertSetEqual(
            set(job_history),
            read_csv_fixture_for_delegate("test_graph_map_index.csv"),
        )

    def test_graph_task_up_for_retry(self) -> None:
        """
        Given a DAG that has a task which is retryable and a task that has failed its first attempt

                    2023-07-10
        parent_task  🟨

        Assert that an incident is not reported until a task has failed all of its retries
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

        job_history = AirflowTaskRunHistoryDelegate(
            dag_id=test_dag.dag_id
        ).fetch_job_runs(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertSetEqual(
            set(job_history),
            read_csv_fixture_for_delegate("test_graph_task_up_for_retry.csv"),
        )

    def test_graph_task_upstream_failed(self) -> None:
        """
        Given a task has consecutively failed, but its upstream task failed in between

                    2023-07-09 2023-07-10 2023-07-11
        parent_task 🟥         🟧         🟥

        Assert that an incident is only reported once
        """
        with self._get_session() as session:
            july_ninth = dummy_dag_run(test_dag, "2023-07-09 12:00")
            july_ninth_ti = dummy_ti(parent_task, july_ninth, "failed")

            july_tenth = dummy_dag_run(test_dag, "2023-07-10 12:00")
            july_tenth_ti = dummy_ti(parent_task, july_tenth, "upstream_failed")

            july_eleventh = dummy_dag_run(test_dag, "2023-07-11 12:00")
            july_eleventh_ti = dummy_ti(parent_task, july_eleventh, "failed")

            session.add_all(
                [
                    july_ninth,
                    july_ninth_ti,
                    july_tenth,
                    july_tenth_ti,
                    july_eleventh,
                    july_eleventh_ti,
                ]
            )
            session.commit()

            # validate job run history
            job_run_history = AirflowTaskRunHistoryDelegate(
                dag_id=test_dag.dag_id
            ).fetch_job_runs(
                lookback=TEST_START_DATE_LOOKBACK,
            )

            self.assertSetEqual(
                set(job_run_history),
                read_csv_fixture_for_delegate("test_graph_task_upstream_failed.csv"),
            )

    def test_graph_task_skipped(self) -> None:
        """
        Given a task has consecutively failed, but its upstream task failed in between

                    2023-07-09 2023-07-10 2023-07-11
        parent_task 🟥         🟪         🟥

        Assert that an incident is only reported once
        """
        with self._get_session() as session:
            july_ninth = dummy_dag_run(test_dag, "2023-07-09 12:00")
            july_ninth_ti = dummy_ti(parent_task, july_ninth, "failed")

            july_tenth = dummy_dag_run(test_dag, "2023-07-10 12:00")
            july_tenth_ti = dummy_ti(parent_task, july_tenth, "skipped")

            july_eleventh = dummy_dag_run(test_dag, "2023-07-11 12:00")
            july_eleventh_ti = dummy_ti(parent_task, july_eleventh, "failed")

            session.add_all(
                [
                    july_ninth,
                    july_ninth_ti,
                    july_tenth,
                    july_tenth_ti,
                    july_eleventh,
                    july_eleventh_ti,
                ]
            )
            session.commit()

            # validate job run history
            job_run_history = AirflowTaskRunHistoryDelegate(
                dag_id=test_dag.dag_id
            ).fetch_job_runs(
                lookback=TEST_START_DATE_LOOKBACK,
            )

            self.assertSetEqual(
                set(job_run_history),
                read_csv_fixture_for_delegate("test_graph_task_skipped.csv"),
            )

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
                parent_task, july_sixth_primary, "success"
            )

            july_seventh_primary = dummy_dag_run(
                test_dag,
                "2023-07-07 12:00",
                conf={"instance": "PRIMARY", "extraneous_param": 2},
            )
            july_seventh_primary_parent = dummy_ti(
                parent_task, july_seventh_primary, state="success"
            )

            july_eighth_primary = dummy_dag_run(
                test_dag, "2023-07-08 12:00", conf={"instance": "PRIMARY"}
            )

            july_eighth_primary_parent = dummy_ti(
                parent_task, july_eighth_primary, state="success"
            )

            july_sixth_secondary = dummy_dag_run(
                test_dag,
                "2023-07-06 12:01",
                conf={"instance": "SECONDARY", "extraneous_param": 3},
            )
            july_sixth_parent_secondary = dummy_ti(
                parent_task, july_sixth_secondary, state="success"
            )

            july_seventh_secondary = dummy_dag_run(
                test_dag,
                "2023-07-07 12:01",
                conf={"instance": "SECONDARY", "extraneous_param": 4},
            )
            july_seventh_secondary_parent = dummy_ti(
                parent_task, july_seventh_secondary, state="failed"
            )

            july_eighth_secondary = dummy_dag_run(
                test_dag, "2023-07-08 12:01", conf={"instance": "SECONDARY"}
            )

            july_eighth_secondary_parent = dummy_ti(
                parent_task, july_eighth_secondary, state="failed"
            )

            july_sixth_tertiary = dummy_dag_run(
                test_dag,
                "2023-07-06 12:02",
                conf={"instance": "TERTIARY", "extraneous_param": 3},
            )
            july_sixth_parent_tertiary = dummy_ti(
                parent_task, july_sixth_tertiary, state="success"
            )

            july_seventh_tertiary = dummy_dag_run(
                test_dag,
                "2023-07-07 12:02",
                conf={"instance": "TERTIARY", "extraneous_param": 4},
            )
            july_seventh_tertiary_parent = dummy_ti(
                parent_task, july_seventh_tertiary, state="failed"
            )

            july_eighth_tertiary = dummy_dag_run(
                test_dag, "2023-07-08 12:02", conf={"instance": "TERTIARY"}
            )

            july_eighth_tertiary_parent = dummy_ti(
                parent_task, july_eighth_tertiary, state="success"
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

            # validate job run history
            job_run_history = AirflowTaskRunHistoryDelegate(
                dag_id=test_dag.dag_id
            ).fetch_job_runs(
                lookback=TEST_START_DATE_LOOKBACK,
            )

            self.assertSetEqual(
                set(job_run_history),
                read_csv_fixture_for_delegate("test_graph_config_idempotency.csv"),
            )

    def test_graph_never_succeeded_most_recent_success(self) -> None:
        """
        Given a DAG where a task failed once introduced

        2023-07-06 2023-07-07 2023-07-08
        🟥         🟩         🟩

        Assert that the last successful run of `parent_task` was `never`
        Assert that the next successful run of `parent_task` was `2023-07-07`
        """

        with self._get_session() as session:
            july_sixth_primary = dummy_dag_run(test_dag, "2023-07-06 12:00")
            july_sixth_parent_primary = dummy_ti(
                parent_task, july_sixth_primary, "failed"
            )

            july_seventh_primary = dummy_dag_run(test_dag, "2023-07-07 12:00")
            july_seventh_primary_parent = dummy_ti(
                parent_task, july_seventh_primary, state="success"
            )

            july_eighth_primary = dummy_dag_run(test_dag, "2023-07-08 12:00")

            july_eighth_primary_parent = dummy_ti(
                parent_task, july_eighth_primary, state="success"
            )

            session.add_all(
                [
                    july_sixth_primary,
                    july_sixth_parent_primary,
                    july_seventh_primary,
                    july_seventh_primary_parent,
                    july_eighth_primary,
                    july_eighth_primary_parent,
                ]
            )

            session.commit()

            # validate job run history
            job_run_history = AirflowTaskRunHistoryDelegate(
                dag_id=test_dag.dag_id
            ).fetch_job_runs(
                lookback=TEST_START_DATE_LOOKBACK,
            )

            self.assertSetEqual(
                set(job_run_history),
                read_csv_fixture_for_delegate(
                    "test_graph_never_succeeded_most_recent_success.csv"
                ),
            )
