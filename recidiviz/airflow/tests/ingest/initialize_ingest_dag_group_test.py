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
Tests for the initialize_ingest_dag_group.py task group.
"""
import unittest
from datetime import datetime
from typing import Optional
from unittest import mock
from unittest.mock import Mock, patch

from airflow.exceptions import TaskDeferred
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.ingest.initialize_ingest_dag_group import (
    IngestDagWaitUntilCanContinueOrCancelDelegate,
    create_initialize_ingest_dag,
)
from recidiviz.airflow.dags.monitoring.dag_registry import get_ingest_dag_id
from recidiviz.airflow.dags.operators.wait_until_can_continue_or_cancel_sensor_async import (
    WaitUntilCanContinueOrCancelSensorAsync,
)
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned

_PROJECT_ID = "recidiviz-testing"
_TEST_DAG_ID = get_ingest_dag_id(_PROJECT_ID)
_VERIFY_PARAMETERS_TASK_ID = "initialize_ingest_dag.verify_parameters"
_WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID = "initialize_ingest_dag.wait_to_continue_or_cancel"
_HANDLE_QUEUEING_RESULT_TASK_ID = "initialize_ingest_dag.handle_queueing_result"
_DOWNSTREAM_TASK_ID = "downstream_task"


@dag(
    dag_id=_TEST_DAG_ID,
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
)
def _create_test_initialize_ingest_dag() -> None:
    create_initialize_ingest_dag() >> EmptyOperator(task_id=_DOWNSTREAM_TASK_ID)


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

    def setUp(self) -> None:
        super().setUp()
        self.get_all_enabled_state_and_instance_pairs_patcher = patch(
            "recidiviz.airflow.dags.ingest.initialize_ingest_dag_group.get_ingest_pipeline_enabled_state_and_instance_pairs",
            return_value=[
                (StateCode.US_XX, DirectIngestInstance.PRIMARY),
                (StateCode.US_XX, DirectIngestInstance.SECONDARY),
                (StateCode.US_YY, DirectIngestInstance.PRIMARY),
                (StateCode.US_YY, DirectIngestInstance.SECONDARY),
            ],
        )
        self.get_all_enabled_state_and_instance_pairs_patcher.start()

    def tearDown(self) -> None:
        self.get_all_enabled_state_and_instance_pairs_patcher.stop()
        super().tearDown()

    def test_successfully_initializes_dag(self) -> None:
        test_dag = _create_test_initialize_ingest_dag()
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(test_dag, session)
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_successfully_initializes_with_state_and_instance_filter(self) -> None:
        test_dag = _create_test_initialize_ingest_dag()
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
                run_conf={
                    "state_code_filter": "US_XX",
                    "ingest_instance": "PRIMARY",
                },
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

            result = self.run_dag_test(
                test_dag,
                session,
                run_conf={
                    "state_code_filter": "US_YY",
                    "ingest_instance": "SECONDARY",
                },
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_unknown_parameters(self) -> None:
        test_dag = _create_test_initialize_ingest_dag()
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={"unknown_key": "value"},
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _DOWNSTREAM_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertEqual(
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
                "Unknown configuration parameters supplied: {'unknown_key'}",
            )

    def test_only_ingest_instance_filter(self) -> None:
        test_dag = _create_test_initialize_ingest_dag()
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={"ingest_instance": "PRIMARY"},
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _DOWNSTREAM_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertEqual(
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
                "[ingest_instance] and [state_code_filter] must both be set or both be unset.",
            )

    def test_only_state_code_filter(self) -> None:
        test_dag = _create_test_initialize_ingest_dag()
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={"state_code_filter": "US_XX"},
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _DOWNSTREAM_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertEqual(
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
                "[ingest_instance] and [state_code_filter] must both be set or both be unset.",
            )

    def test_only_state_code_and_instance_filter_not_enabled(self) -> None:
        test_dag = _create_test_initialize_ingest_dag()
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={
                    "state_code_filter": "US_LL",
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _DOWNSTREAM_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertIn(
                "US_LL in PRIMARY must be a enabled for ingest.",
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
            )

    def test_invalid_state_code_filter(self) -> None:
        test_dag = _create_test_initialize_ingest_dag()
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={
                    "state_code_filter": "US_ASDF",
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _DOWNSTREAM_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertIn(
                "'US_ASDF' is not a valid",
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
            )

    def test_invalid_ingest_instance_filter(self) -> None:
        test_dag = _create_test_initialize_ingest_dag()
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                dag=test_dag,
                session=session,
                run_conf={
                    "state_code_filter": "US_XX",
                    "ingest_instance": "PRIMARY_ASDF",
                },
                expected_failure_ids=[_VERIFY_PARAMETERS_TASK_ID],
                expected_skipped_ids=[
                    _WAIT_TO_CONTINUE_OR_CANCEL_TASK_ID,
                    _HANDLE_QUEUEING_RESULT_TASK_ID,
                    _DOWNSTREAM_TASK_ID,
                ],
            )

            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
            self.assertIn(
                "'PRIMARY_ASDF' is not a valid",
                result.failure_messages[_VERIFY_PARAMETERS_TASK_ID],
            )


def _create_mock_dag_run(state_code: Optional[str], instance: Optional[str]) -> Mock:
    mock_dag_run = Mock()
    mock_dag_run.conf = {
        "state_code_filter": state_code,
        "ingest_instance": instance,
    }
    mock_dag_run.execution_date = datetime.now()
    mock_dag_run.dag_id = "test_dag"
    mock_dag_run.run_id = f"test_dag_run_{mock_dag_run.execution_date}"
    return mock_dag_run


class TestIngestDagWaitUntilCanContinueOrCancelDelegate(unittest.TestCase):
    """
    Tests to validate IngestDagWaitUntilCanContinueOrCancelDelegate queueing logic.
    """

    def setUp(self) -> None:
        self.get_all_active_dag_runs_patcher = mock.patch(
            "recidiviz.airflow.dags.operators.wait_until_can_continue_or_cancel_sensor_async._get_all_active_dag_runs"
        )
        self.mock_get_all_active_dag_runs = self.get_all_active_dag_runs_patcher.start()
        self.operator = WaitUntilCanContinueOrCancelSensorAsync(
            task_id="test_task",
            delegate=IngestDagWaitUntilCanContinueOrCancelDelegate(),
        )

    def tearDown(self) -> None:
        self.get_all_active_dag_runs_patcher.stop()

    def test_state_agnostic_dag_run_should_continue(self) -> None:
        test_dag_run = _create_mock_dag_run(state_code=None, instance=None)

        self.mock_get_all_active_dag_runs.return_value = [
            test_dag_run,
            _create_mock_dag_run(state_code="US_XX", instance="PRIMARY"),
        ]

        results = self.operator.execute(context={"dag_run": test_dag_run})

        self.assertEqual(results, "CONTINUE")

    def test_state_agnostic_dag_run_defers_if_state_agnostic_dag_before_it(
        self,
    ) -> None:
        mock_state_agnostic_dag_run = _create_mock_dag_run(
            state_code=None, instance=None
        )
        test_dag_run = _create_mock_dag_run(state_code=None, instance=None)
        mock_filtered_dag_run = _create_mock_dag_run(
            state_code="US_XX", instance="PRIMARY"
        )

        self.mock_get_all_active_dag_runs.return_value = [
            mock_state_agnostic_dag_run,
            test_dag_run,
            mock_filtered_dag_run,
        ]

        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": test_dag_run})

    def test_state_agnostic_dag_run_cancel_if_state_agnostic_in_queue_after_it(
        self,
    ) -> None:
        mock_filtered_dag_run = _create_mock_dag_run(
            state_code="US_XX", instance="PRIMARY"
        )
        test_dag_run = _create_mock_dag_run(state_code=None, instance=None)
        mock_state_agnostic_dag_run = _create_mock_dag_run(
            state_code=None, instance=None
        )

        self.mock_get_all_active_dag_runs.return_value = [
            mock_filtered_dag_run,
            test_dag_run,
            mock_state_agnostic_dag_run,
        ]

        results = self.operator.execute(context={"dag_run": test_dag_run})
        self.assertEqual(results, "CANCEL")

        self.mock_get_all_active_dag_runs.return_value = [
            test_dag_run,
            mock_state_agnostic_dag_run,
        ]

        results = self.operator.execute(context={"dag_run": test_dag_run})
        self.assertEqual(results, "CANCEL")

    def test_filtered_dag_run_should_continue(self) -> None:
        test_dag_run = _create_mock_dag_run(state_code="US_XX", instance="PRIMARY")
        mock_us_xx_secondary_dag_run = _create_mock_dag_run(
            state_code="US_XX", instance="SECONDARY"
        )
        mock_us_yy_primary_dag_run = _create_mock_dag_run(
            state_code="US_YY", instance="PRIMARY"
        )

        self.mock_get_all_active_dag_runs.return_value = [
            test_dag_run,
            mock_us_xx_secondary_dag_run,
            mock_us_yy_primary_dag_run,
        ]

        results = self.operator.execute(context={"dag_run": test_dag_run})

        self.assertEqual(results, "CONTINUE")

    def test_primary_filtered_dag_run_continues_if_not_same_filtered_dag_run_before_it(
        self,
    ) -> None:
        mock_us_xx_secondary_dag_run = _create_mock_dag_run(
            state_code="US_XX", instance="SECONDARY"
        )
        mock_us_yy_primary_dag_run = _create_mock_dag_run(
            state_code="US_YY", instance="PRIMARY"
        )
        test_dag_run = _create_mock_dag_run(state_code="US_XX", instance="PRIMARY")

        self.mock_get_all_active_dag_runs.return_value = [
            mock_us_xx_secondary_dag_run,
            mock_us_yy_primary_dag_run,
            test_dag_run,
        ]

        results = self.operator.execute(context={"dag_run": test_dag_run})

        self.assertEqual(results, "CONTINUE")

    def test_filtered_dag_run_defers_if_state_agnostic_dag_before_it(self) -> None:
        mock_state_agnostic_dag_run = _create_mock_dag_run(
            state_code=None, instance=None
        )
        test_dag_run = _create_mock_dag_run(state_code="US_XX", instance="PRIMARY")

        self.mock_get_all_active_dag_runs.return_value = [
            mock_state_agnostic_dag_run,
            test_dag_run,
        ]

        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": test_dag_run})

    def test_filtered_dag_run_defers_if_equivalent_filtered_dag_before_it(self) -> None:
        mock_us_xx_primary_dag_run = _create_mock_dag_run(
            state_code="US_XX", instance="PRIMARY"
        )
        test_dag_run = _create_mock_dag_run(state_code="US_XX", instance="PRIMARY")

        self.mock_get_all_active_dag_runs.return_value = [
            mock_us_xx_primary_dag_run,
            test_dag_run,
        ]

        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": test_dag_run})

    def test_filtered_dag_run_cancel_if_state_agnostic_in_queue_after_it(self) -> None:
        mock_us_xx_primary_dag_run = _create_mock_dag_run(
            state_code="US_XX", instance="PRIMARY"
        )
        test_dag_run = _create_mock_dag_run(state_code="US_XX", instance="PRIMARY")
        mock_state_agnostic_dag_run = _create_mock_dag_run(
            state_code=None, instance=None
        )

        self.mock_get_all_active_dag_runs.return_value = [
            mock_us_xx_primary_dag_run,
            test_dag_run,
            mock_state_agnostic_dag_run,
        ]

        results = self.operator.execute(context={"dag_run": test_dag_run})
        self.assertEqual(results, "CANCEL")

        self.mock_get_all_active_dag_runs.return_value = [
            test_dag_run,
            mock_state_agnostic_dag_run,
        ]

        results = self.operator.execute(context={"dag_run": test_dag_run})
        self.assertEqual(results, "CANCEL")
