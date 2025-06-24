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
"""Tests for IncidentHistoryBuilder"""
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
from recidiviz.airflow.dags.monitoring.file_tag_import_run_summary import (
    BigQueryFailedFileImportRunSummary,
    FileTagImportRunSummary,
)
from recidiviz.airflow.dags.monitoring.incident_history_builder import (
    IncidentHistoryBuilder,
)
from recidiviz.airflow.dags.monitoring.job_run import JobRun, JobRunState, JobRunType
from recidiviz.airflow.dags.monitoring.job_run_history_delegate import (
    JobRunHistoryDelegate,
)
from recidiviz.airflow.dags.monitoring.job_run_history_delegate_factory import (
    JobRunHistoryDelegateFactory,
)
from recidiviz.airflow.dags.monitoring.raw_data_file_tag_task_run_history_delegate import (
    RawDataFileTagTaskRunHistoryDelegate,
)
from recidiviz.airflow.tests.fixtures import monitoring as monitoring_fixtures
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

_PROJECT_ID = "recidiviz-testing"
_TEST_DAG_ID = "test_dag"
_MULTIPLE_DELEGATE_DAG = "multiple_delegate_dag"
_RAW_DATA_DAG = "raw_data_dag"


class FakeJobRunHistoryDelegateFactory(JobRunHistoryDelegateFactory):
    @classmethod
    def build(cls, *, dag_id: str) -> list[JobRunHistoryDelegate]:
        if dag_id == _TEST_DAG_ID:
            return [AirflowTaskRunHistoryDelegate(dag_id=dag_id)]

        if dag_id == _MULTIPLE_DELEGATE_DAG:
            return [
                AirflowTaskRunHistoryDelegate(dag_id=dag_id),
                AirflowTaskRunHistoryDelegate(dag_id=dag_id),
                AirflowTaskRunHistoryDelegate(dag_id=dag_id),
            ]

        if dag_id == _RAW_DATA_DAG:
            return [
                AirflowTaskRunHistoryDelegate(dag_id=dag_id),
                RawDataFileTagTaskRunHistoryDelegate(dag_id=dag_id),
            ]

        raise ValueError(f"Unrecognized DAG :{dag_id}")


def read_csv_fixture_for_delegate(file: str) -> set[JobRun]:
    return {
        JobRun(
            dag_id=row["dag_id"],
            execution_date=datetime.datetime.fromisoformat(row["execution_date"]),
            dag_run_config=row["conf"],
            job_id=row["job_id"],
            state=JobRunState(int(row["state"])),
            error_message=row.get("error_message"),
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
        dag_id=kwargs.pop("dag_id", None) or dag.dag_id,
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


def build_test_dag_with_id(dag_id: str) -> tuple[DAG, PythonOperator, PythonOperator]:
    """for building a dag with a specific dag name"""
    test_dag_ = DAG(
        dag_id=dag_id,
        start_date=datetime.datetime(year=2023, month=6, day=21),
        schedule=None,
    )

    parent_task_ = dummy_task(test_dag_, "parent_task")
    child_task_ = dummy_task(test_dag_, "child_task")
    parent_task_ >> child_task_
    return test_dag_, parent_task_, child_task_


test_dag, parent_task, child_task = build_test_dag_with_id(_TEST_DAG_ID)

TEST_START_DATE_LOOKBACK = datetime.timedelta(days=20 * 365)


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class IncidentHistoryBuilderTest(AirflowIntegrationTest):
    """Tests for IncidentHistoryBuilder"""

    def setUp(self) -> None:
        self.delegate_patcher = patch(
            "recidiviz.airflow.dags.monitoring.incident_history_builder.JobRunHistoryDelegateFactory",
            FakeJobRunHistoryDelegateFactory,
        )
        self.delegate_patcher.start()
        super().setUp()

    def tearDown(self) -> None:
        self.delegate_patcher.stop()
        super().tearDown()

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
        history = IncidentHistoryBuilder(dag_id=test_dag.dag_id).build(
            lookback=TEST_START_DATE_LOOKBACK
        )

        self.assertEqual({}, history)

    def test_no_terminated_tasks(self) -> None:
        with self._get_session() as session:
            july_sixth = dummy_dag_run(test_dag, "2023-07-06")
            july_sixth_parent = dummy_ti(parent_task, july_sixth, "running")

            session.add_all([july_sixth, july_sixth_parent])

        history = IncidentHistoryBuilder(dag_id=test_dag.dag_id).build(
            lookback=TEST_START_DATE_LOOKBACK,
        )
        self.assertEqual({}, history)

    def test_exactly_one_success(self) -> None:
        with self._get_session() as session:
            july_sixth = dummy_dag_run(test_dag, "2023-07-06")
            july_sixth_parent = dummy_ti(parent_task, july_sixth, "success")

            session.add_all([july_sixth, july_sixth_parent])

        history = IncidentHistoryBuilder(dag_id=test_dag.dag_id).build(
            lookback=TEST_START_DATE_LOOKBACK
        )
        self.assertEqual({}, history)

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

            # validate incident history
            incidents = IncidentHistoryBuilder(dag_id=test_dag.dag_id).build(
                lookback=TEST_START_DATE_LOOKBACK
            )
            incident_key = (
                "Task Run: test_dag.parent_task, started: 2023-07-09 12:00 UTC"
            )
            self.assertListEqual(list(incidents.keys()), [incident_key])
            self.assertEqual(
                incidents[incident_key].failed_execution_dates,
                [july_ninth.execution_date, july_eleventh.execution_date],
            )

    def test_graph_task_upstream_failed_duplicate_delegates(self) -> None:
        """
        Given a task has consecutively failed, but its upstream task failed in between

                    2023-07-09 2023-07-10 2023-07-11
        parent_task 🟥         🟧         🟥

        Assert that an incident is only reported once
        """
        multi_test_dag = DAG(
            dag_id=_MULTIPLE_DELEGATE_DAG,
            start_date=datetime.datetime(year=2023, month=6, day=21),
            schedule=None,
        )

        multi_parent_task = dummy_task(multi_test_dag, "parent_task")
        multi_child_task = dummy_task(multi_test_dag, "child_task")
        multi_parent_task >> multi_child_task

        with self._get_session() as session:
            july_ninth = dummy_dag_run(multi_test_dag, "2023-07-09 12:00")
            july_ninth_ti = dummy_ti(multi_parent_task, july_ninth, "failed")

            july_tenth = dummy_dag_run(multi_test_dag, "2023-07-10 12:00")
            july_tenth_ti = dummy_ti(multi_parent_task, july_tenth, "upstream_failed")

            july_eleventh = dummy_dag_run(multi_test_dag, "2023-07-11 12:00")
            july_eleventh_ti = dummy_ti(multi_parent_task, july_eleventh, "failed")

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
                dag_id=multi_test_dag.dag_id
            ).fetch_job_runs(
                lookback=TEST_START_DATE_LOOKBACK,
            )

            self.assertSetEqual(
                set(job_run_history),
                read_csv_fixture_for_delegate(
                    "test_graph_task_upstream_failed_duplicate_delegates.csv"
                ),
            )

            # validate incident history
            incidents = IncidentHistoryBuilder(dag_id=multi_test_dag.dag_id).build(
                lookback=TEST_START_DATE_LOOKBACK
            )
            incident_key = "Task Run: multiple_delegate_dag.parent_task, started: 2023-07-09 12:00 UTC"
            self.assertListEqual(list(incidents.keys()), [incident_key])
            self.assertEqual(
                set(incidents[incident_key].failed_execution_dates),
                {july_ninth.execution_date, july_eleventh.execution_date},
            )
            self.assertEqual(
                incidents[incident_key].failed_execution_dates,
                [
                    july_ninth.execution_date,
                    july_ninth.execution_date,
                    july_ninth.execution_date,
                    july_eleventh.execution_date,
                    july_eleventh.execution_date,
                    july_eleventh.execution_date,
                ],
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

            # validate incident history
            incident_history = IncidentHistoryBuilder(dag_id=test_dag.dag_id).build(
                lookback=TEST_START_DATE_LOOKBACK
            )
            incident_key = 'Task Run: {"instance": "SECONDARY"} test_dag.parent_task, started: 2023-07-07 12:01 UTC'
            self.assertIn(incident_key, incident_history)
            secondary_incident = incident_history[incident_key]
            # Assert that the task was last successful on July 6th
            self.assertEqual(
                secondary_incident.previous_success_date,
                july_sixth_secondary.execution_date,
            )
            # Assert that the starting date was on July 7th
            self.assertEqual(
                secondary_incident.incident_start_date,
                july_seventh_secondary.execution_date,
            )

            # Assert that the most recent failure was on July 8th
            self.assertEqual(
                secondary_incident.most_recent_failure,
                july_eighth_secondary.execution_date,
            )

            incident_key = 'Task Run: {"instance": "TERTIARY"} test_dag.parent_task, started: 2023-07-07 12:02 UTC'
            self.assertIn(incident_key, incident_history)
            tertiary_incident = incident_history[incident_key]
            # Assert that the task was last successful on July 6th
            self.assertEqual(
                tertiary_incident.previous_success_date,
                july_sixth_tertiary.execution_date,
            )

            # Assert that the starting failure date was on July 7th
            self.assertEqual(
                tertiary_incident.incident_start_date,
                july_seventh_tertiary.execution_date,
            )

            # Assert that the most recent failure was on July 7th
            self.assertEqual(
                tertiary_incident.most_recent_failure,
                july_seventh_tertiary.execution_date,
            )

            # Assert that the most recent success was on July 8th
            self.assertEqual(
                tertiary_incident.next_success_date, july_eighth_tertiary.execution_date
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

            # validate incident history

            history = IncidentHistoryBuilder(dag_id=test_dag.dag_id).build(
                lookback=TEST_START_DATE_LOOKBACK
            )

            incident_key = (
                "Task Run: test_dag.parent_task, started: 2023-07-06 12:00 UTC"
            )
            self.assertIn(incident_key, history)
            incident = history[incident_key]
            # Assert that the task has never succeeded
            self.assertEqual(incident.previous_success_date, None)
            # Assert that the most recent failure was on July 6th
            self.assertEqual(
                incident.most_recent_failure,
                july_sixth_primary.execution_date,
            )
            # Assert that the next success was on July 7th
            self.assertEqual(
                incident.next_success_date,
                july_seventh_primary.execution_date,
            )

    @patch(
        "recidiviz.airflow.dags.monitoring.raw_data_file_tag_task_run_history_delegate.get_current_context"
    )
    def test_error_messages(self, context_patcher: MagicMock) -> None:
        """
        Given a raw data file import history like (where ⬜ is no run)

        tag     id  instance        2024-01-01  2024-01-02  2024-01-03
        tag_a   1   SECONDARY       🟥          ⬜           🟥
                2   SECONDARY       🟥          ⬜           🟥
                3   SECONDARY       🟥          ⬜           🟥
                4   PRIMARY         🟥          🟩           🟩
                5   PRIMARY         🟥          🟥           🟩
                6   PRIMARY         🟥          🟥           🟩


        parent_task                 🟥          🟥           🟩


        """

        rd_test_dag, rd_parent_task, _ = build_test_dag_with_id(_RAW_DATA_DAG)

        date_2024_01_01 = datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)
        date_2024_01_02 = datetime.datetime(2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC)
        date_2024_01_03 = datetime.datetime(2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC)

        primary_2024_01_01 = FileTagImportRunSummary(
            import_run_start=date_2024_01_01,
            state_code=StateCode.US_XX,
            raw_data_instance=DirectIngestInstance.PRIMARY,
            file_tag="tag_a",
            file_tag_import_state=JobRunState.FAILED,
            failed_file_import_runs=[
                BigQueryFailedFileImportRunSummary(
                    file_id=4,
                    update_datetime=datetime.datetime(
                        2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                    error_message="ERROR\nfailed load step, silly\nERROR!",
                ),
                BigQueryFailedFileImportRunSummary(
                    file_id=5,
                    update_datetime=datetime.datetime(
                        2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP,
                    error_message="ERROR\nfailed pre-import norm step\nERROR!",
                ),
                BigQueryFailedFileImportRunSummary(
                    file_id=6,
                    update_datetime=datetime.datetime(
                        2024, 1, 4, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                    error_message="Blocked by 4",
                ),
            ],
        )
        primary_2024_01_02 = FileTagImportRunSummary(
            import_run_start=date_2024_01_02,
            state_code=StateCode.US_XX,
            raw_data_instance=DirectIngestInstance.PRIMARY,
            file_tag="tag_a",
            file_tag_import_state=JobRunState.FAILED,
            failed_file_import_runs=[
                BigQueryFailedFileImportRunSummary(
                    file_id=4,
                    update_datetime=datetime.datetime(
                        2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                    error_message="ERROR\nfailed load step, silly\nERROR!",
                ),
                BigQueryFailedFileImportRunSummary(
                    file_id=5,
                    update_datetime=datetime.datetime(
                        2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                    error_message=None,
                ),
                BigQueryFailedFileImportRunSummary(
                    file_id=6,
                    update_datetime=datetime.datetime(
                        2024, 1, 4, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                    error_message="Blocked by 4",
                ),
            ],
        )
        primary_2024_01_03 = FileTagImportRunSummary(
            import_run_start=date_2024_01_03,
            state_code=StateCode.US_XX,
            raw_data_instance=DirectIngestInstance.PRIMARY,
            file_tag="tag_a",
            file_tag_import_state=JobRunState.SUCCESS,
            failed_file_import_runs=[],
        )
        secondary_2024_01_01 = FileTagImportRunSummary(
            import_run_start=date_2024_01_01,
            state_code=StateCode.US_XX,
            raw_data_instance=DirectIngestInstance.SECONDARY,
            file_tag="tag_a",
            file_tag_import_state=JobRunState.FAILED,
            failed_file_import_runs=[
                BigQueryFailedFileImportRunSummary(
                    file_id=1,
                    update_datetime=datetime.datetime(
                        2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                    error_message="ERROR\nfailed load step, silly\nERROR!",
                ),
                BigQueryFailedFileImportRunSummary(
                    file_id=2,
                    update_datetime=datetime.datetime(
                        2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP,
                    error_message="ERROR\nfailed pre-import norm step\nERROR!",
                ),
                BigQueryFailedFileImportRunSummary(
                    file_id=3,
                    update_datetime=datetime.datetime(
                        2024, 1, 4, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                    error_message="Blocked by 1",
                ),
            ],
        )
        secondary_2024_01_03 = FileTagImportRunSummary(
            import_run_start=date_2024_01_03,
            state_code=StateCode.US_XX,
            raw_data_instance=DirectIngestInstance.SECONDARY,
            file_tag="tag_a",
            file_tag_import_state=JobRunState.FAILED,
            failed_file_import_runs=[
                BigQueryFailedFileImportRunSummary(
                    file_id=1,
                    update_datetime=datetime.datetime(
                        2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
                    error_message="ERROR\nfailed load step, silly\nERROR!",
                ),
                BigQueryFailedFileImportRunSummary(
                    file_id=2,
                    update_datetime=datetime.datetime(
                        2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_PRE_IMPORT_NORMALIZATION_STEP,
                    error_message="ERROR\nfailed pre-import norm step\nERROR!",
                ),
                BigQueryFailedFileImportRunSummary(
                    file_id=3,
                    update_datetime=datetime.datetime(
                        2024, 1, 4, 1, 1, 1, tzinfo=datetime.UTC
                    ),
                    file_import_status=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                    error_message="Blocked by 1",
                ),
            ],
        )
        summaries = [
            # are out of order, will be ordered in .build()
            # if we didn't sort properly, we would see 3 incidents as it history would
            # look like: FAILURE SUCCESS FAILURE instead of FAILURE FAILURE SUCCESS
            primary_2024_01_01,
            primary_2024_01_03,
            primary_2024_01_02,
            # are out of order, will be ordered in .build()
            # if we didn't sort properly, we would see the incident_start_date be
            # the more recent incident (2024-01-03)
            secondary_2024_01_03,
            secondary_2024_01_01,
        ]

        ti = MagicMock()
        ti.xcom_pull.return_value = [s.serialize() for s in summaries]
        context_patcher.return_value = {"ti": ti}

        with self._get_session() as session:
            jan_one_primary = dummy_dag_run(
                rd_test_dag, date_2024_01_01.strftime("%Y-%m-%d %H:%M")
            )
            jan_one_parent_primary = dummy_ti(rd_parent_task, jan_one_primary, "failed")

            jan_two_primary = dummy_dag_run(
                rd_test_dag, date_2024_01_02.strftime("%Y-%m-%d %H:%M")
            )
            jan_two_primary_parent = dummy_ti(
                rd_parent_task, jan_two_primary, state="failed"
            )

            jan_three_primary = dummy_dag_run(
                rd_test_dag, date_2024_01_03.strftime("%Y-%m-%d %H:%M")
            )

            jan_three_primary_parent = dummy_ti(
                rd_parent_task, jan_three_primary, state="success"
            )

            session.add_all(
                [
                    jan_one_primary,
                    jan_one_parent_primary,
                    jan_two_primary,
                    jan_two_primary_parent,
                    jan_three_primary,
                    jan_three_primary_parent,
                ]
            )

            session.commit()

        # validate job run history
        job_run_history = RawDataFileTagTaskRunHistoryDelegate(
            dag_id=_RAW_DATA_DAG
        ).fetch_job_runs(
            lookback=TEST_START_DATE_LOOKBACK,
        )

        assert job_run_history == [s.as_job_run(_RAW_DATA_DAG) for s in summaries]

        # validate incident history

        history = IncidentHistoryBuilder(dag_id=_RAW_DATA_DAG).build(
            lookback=TEST_START_DATE_LOOKBACK
        )

        assert len(history) == 3

        primary_key = (
            "Raw Data Import: raw_data_dag.US_XX.tag_a, started: 2024-01-01 01:01 UTC"
        )
        assert primary_key in history
        primary_incident = history[primary_key]
        primary_incident.next_success_date = date_2024_01_03
        primary_incident.failed_execution_dates = [date_2024_01_01, date_2024_01_02]
        primary_incident.job_id = "raw_data_dag.US_XX.tag_a"
        primary_incident.error_message = primary_2024_01_02.format_error_message()

        secondary_key = 'Raw Data Import: {"ingest_instance": "SECONDARY"} raw_data_dag.US_XX.tag_a, started: 2024-01-01 01:01 UTC'
        assert secondary_key in history
        secondy_incident = history[secondary_key]
        secondy_incident.next_success_date = None
        secondy_incident.failed_execution_dates = [date_2024_01_01, date_2024_01_03]
        secondy_incident.job_id = "raw_data_dag.US_XX.tag_a"
        secondy_incident.error_message = secondary_2024_01_03.format_error_message()

        task_key = "Task Run: raw_data_dag.parent_task, started: 2024-01-01 01:01 UTC"
        assert task_key in history
        task_incident = history[task_key]
        task_incident.next_success_date = date_2024_01_03
        task_incident.failed_execution_dates = [date_2024_01_01, date_2024_01_02]
        task_incident.job_id = "raw_data_dag.parent_task"
        task_incident.error_message = None
