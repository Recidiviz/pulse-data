# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""
Tests for the initialize_calculation_dag_group.py task group.
"""
import time
import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import Mock, patch

from airflow.decorators import task
from airflow.exceptions import TaskDeferred
from airflow.models import DagRun
from airflow.models.dag import DAG, dag
from airflow.utils.state import DagRunState
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.calculation.initialize_calculation_dag_group import (
    CalcDagWaitUntilCanContinueOrCancelDelegate,
    WaitUntilCanContinueOrCancelSensorAsync,
    initialize_calculation_dag_group,
)
from recidiviz.airflow.dags.monitoring.dag_registry import get_calculation_dag_id
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned

_VERIFY_PARAMETERS_TASK_ID = "initialize_dag.verify_parameters"
_HANDLE_QUEUEING_RESULT_TASK_ID = "initialize_dag.handle_queueing_result"
_WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID = "initialize_dag.wait_to_continue_or_cancel"
_WAIT_SECONDS_TASK_ID = "wait_seconds"
_PROJECT_ID = "recidiviz-testing"


@dag(
    dag_id=get_calculation_dag_id(_PROJECT_ID),
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
)
def _create_test_initialize_dag() -> None:
    @task(task_id=_WAIT_SECONDS_TASK_ID)
    def wait_seconds(dag_run: DagRun = None) -> None:
        if not dag_run:
            raise ValueError("Dag run not passed to task")
        time.sleep(dag_run.conf.get("wait_seconds", 0))

    initialize_calculation_dag_group() >> wait_seconds()


test_dag: DAG = _create_test_initialize_dag()


class TestInitializeCalculationDagGroup(unittest.TestCase):
    """
    Tests for the initialize_calculation_dag_group.py task group.
    """

    def test_verify_parameters_upstream_of_handle_params_check(
        self,
    ) -> None:
        verify_parameters_task = test_dag.get_task("initialize_dag.verify_parameters")
        handle_params_check = test_dag.get_task("initialize_dag.handle_params_check")

        self.assertEqual(
            handle_params_check.upstream_task_ids,
            {verify_parameters_task.task_id},
        )

    def test_handle_params_check_upstream_of_wait_to_continue_or_cancel(
        self,
    ) -> None:
        handle_params_check = test_dag.get_task("initialize_dag.handle_params_check")
        wait_to_continue_or_cancel = test_dag.get_task(
            "initialize_dag.wait_to_continue_or_cancel"
        )

        self.assertEqual(
            handle_params_check.downstream_task_ids,
            {wait_to_continue_or_cancel.task_id},
        )
        self.assertEqual(
            wait_to_continue_or_cancel.upstream_task_ids,
            {handle_params_check.task_id},
        )

    def test_wait_to_continue_or_cancel_upstream_of_handle_queueing_result(
        self,
    ) -> None:
        handle_queueing_result = test_dag.get_task(
            "initialize_dag.handle_queueing_result"
        )
        wait_to_continue_or_cancel = test_dag.get_task(
            "initialize_dag.wait_to_continue_or_cancel"
        )

        self.assertEqual(
            wait_to_continue_or_cancel.downstream_task_ids,
            {handle_queueing_result.task_id},
        )
        self.assertEqual(
            handle_queueing_result.upstream_task_ids,
            {wait_to_continue_or_cancel.task_id},
        )


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class TestInitializeCalculationDagGroupIntegration(AirflowIntegrationTest):
    """
    Integration tests for the initialize_calculation_dag_group.py task group.
    """

    def test_successfully_initializes_dag(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
                {
                    "ingest_instance": "PRIMARY",
                    "trigger_ingest_dag_post_bq_refresh": False,
                },
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_successfully_initializes_dag_primary_with_state_code_and_sandbox(
        self,
    ) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
                {
                    "ingest_instance": "PRIMARY",
                    "state_code_filter": "US_XX",
                    "sandbox_prefix": "my_prefix",
                    "trigger_ingest_dag_post_bq_refresh": True,
                },
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_initializes_dag_primary_with_state_code(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
                {
                    "ingest_instance": "PRIMARY",
                    "state_code_filter": "US_XX",
                    "trigger_ingest_dag_post_bq_refresh": True,
                },
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _WAIT_SECONDS_TASK_ID,
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertEqual(
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
                "[sandbox_prefix] must be set in dag_run configuration for PRIMARY ingest_instance when [state_code_filter] is set",
            )

    def test_initializes_dag_missing_trigger_ingest_dag_post_bq_refresh(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
                {
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _WAIT_SECONDS_TASK_ID,
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertEqual(
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
                "[trigger_ingest_dag_post_bq_refresh] must be set in dag_run configuration as boolean",
            )

    def test_successfully_initializes_dag_secondary(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={
                    "ingest_instance": "SECONDARY",
                    "state_code_filter": "US_XX",
                    "sandbox_prefix": "my_prefix",
                    "trigger_ingest_dag_post_bq_refresh": True,
                },
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_secondary_no_state_code(
        self,
    ) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={
                    "ingest_instance": "SECONDARY",
                    "sandbox_prefix": "my_prefix",
                    "trigger_ingest_dag_post_bq_refresh": True,
                },
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _WAIT_SECONDS_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertEqual(
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
                "[state_code_filter] must be set in dag_run configuration for SECONDARY ingest_instance",
            )

    def test_secondary_no_sandbox_prefix(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={
                    "ingest_instance": "SECONDARY",
                    "state_code_filter": "US_XX",
                    "trigger_ingest_dag_post_bq_refresh": True,
                },
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _WAIT_SECONDS_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertEqual(
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
                "[sandbox_prefix] must be set in dag_run configuration for SECONDARY ingest_instance",
            )

    def test_unknown_parameters(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={"unknown_key": "value"},
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _WAIT_SECONDS_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertEqual(
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
                "Unknown configuration parameters supplied: {'unknown_key'}",
            )


class TestCalcDagWaitUntilCanContinueOrCancelDelegate(unittest.TestCase):
    """
    Tests to validate CalcDagWaitUntilCanContinueOrCancelDelegate queueing logic.
    """

    def setUp(self) -> None:
        self.get_all_active_dag_runs_patcher = mock.patch(
            "recidiviz.airflow.dags.operators.wait_until_can_continue_or_cancel_sensor_async._get_all_active_dag_runs"
        )
        self.mock_get_all_active_dag_runs = self.get_all_active_dag_runs_patcher.start()
        self.operator = WaitUntilCanContinueOrCancelSensorAsync(
            task_id="test_task", delegate=CalcDagWaitUntilCanContinueOrCancelDelegate()
        )

    def tearDown(self) -> None:
        self.get_all_active_dag_runs_patcher.stop()

    def test_first_in_queue(self) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"
        dag_run_2.conf = {"ingest_instance": "PRIMARY"}

        dag_run_3 = Mock()
        dag_run_3.run_id = "test_run_3"
        dag_run_3.conf = {"ingest_instance": "SECONDARY"}

        self.mock_get_all_active_dag_runs.return_value = [dag_run, dag_run_2, dag_run_3]

        results = self.operator.execute(context={"dag_run": dag_run})

        self.assertEqual(results, "CONTINUE")

    def test_defers_if_last_in_queue(self) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"
        dag_run_2.conf = {"ingest_instance": "PRIMARY"}

        dag_run_3 = Mock()
        dag_run_3.run_id = "test_run_3"
        dag_run_3.conf = {"ingest_instance": "SECONDARY"}

        self.mock_get_all_active_dag_runs.return_value = [dag_run_2, dag_run, dag_run_3]

        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": dag_run})

    def test_middle_of_queue(self) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"
        dag_run_2.conf = {"ingest_instance": "PRIMARY"}

        dag_run_3 = Mock()
        dag_run_3.run_id = "test_run_3"
        dag_run_3.conf = {"ingest_instance": "PRIMARY"}

        self.mock_get_all_active_dag_runs.return_value = [dag_run_2, dag_run, dag_run_3]

        results = self.operator.execute(context={"dag_run": dag_run})

        self.assertEqual(results, "CANCEL")

    def test_secondary_dag_run(self) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "SECONDARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"
        dag_run_2.conf = {"ingest_instance": "PRIMARY"}

        dag_run_3 = Mock()
        dag_run_3.run_id = "test_run_3"
        dag_run_3.conf = {"ingest_instance": "PRIMARY"}

        dag_run_4 = Mock()
        dag_run_4.run_id = "test_run_4"
        dag_run_4.conf = {"ingest_instance": "SECONDARY"}

        dag_run_5 = Mock()
        dag_run_5.run_id = "test_run_5"
        dag_run_5.conf = {"ingest_instance": "SECONDARY"}

        self.mock_get_all_active_dag_runs.return_value = [
            dag_run_2,
            dag_run_4,
            dag_run,
            dag_run_3,
            dag_run_5,
        ]

        results = self.operator.execute(context={"dag_run": dag_run})

        self.assertEqual(results, "CONTINUE")
