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

from recidiviz.airflow.dags.calculation.initialize_calculation_dag_group import (
    WaitUntilCanContinueOrCancelSensorAsync,
    initialize_calculation_dag_group,
)
from recidiviz.airflow.dags.monitoring.task_failure_alerts import (
    KNOWN_CONFIGURATION_PARAMETERS,
)
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


@dag(
    dag_id="test_initialize_dag",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
)
def _create_test_initialize_dag() -> None:
    @task
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

    def test_verify_and_update_parameters_upstream_of_handle_params_check(
        self,
    ) -> None:

        verify_and_update_parameters_task = test_dag.get_task(
            "initialize_dag.verify_and_update_parameters"
        )
        handle_params_check = test_dag.get_task("initialize_dag.handle_params_check")

        self.assertEqual(
            handle_params_check.upstream_task_ids,
            {verify_and_update_parameters_task.task_id},
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


@patch.dict(KNOWN_CONFIGURATION_PARAMETERS, {test_dag.dag_id: ["known_key"]})
class TestInitializeCalculationDagGroupIntegration(AirflowIntegrationTest):
    def test_unknown_parameters(self) -> None:
        with self.assertRaises(
            ValueError, msg="Unknown configuration parameters supplied: unknown_key"
        ):
            test_dag.test(run_conf={"unknown_key": "value"})


class TestWaitUntilCanContinueOrCancelSensorAsync(unittest.TestCase):
    """
    Tests for the WaitUntilCanContinueOrCancelSensorAsync sensor.
    """

    def setUp(self) -> None:
        self.operator = WaitUntilCanContinueOrCancelSensorAsync(task_id="test_task")

    @mock.patch(
        "recidiviz.airflow.dags.calculation.initialize_calculation_dag_group._get_all_active_primary_dag_runs"
    )
    def test_first_in_queue(self, get_all_active_primary_dag_runs: Mock) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"

        get_all_active_primary_dag_runs.return_value = [dag_run, dag_run_2]

        results = self.operator.execute(context={"dag_run": dag_run})

        self.assertEqual(results, "CONTINUE")

    @mock.patch(
        "recidiviz.airflow.dags.calculation.initialize_calculation_dag_group._get_all_active_primary_dag_runs"
    )
    def test_defers_if_last_in_queue(
        self, get_all_active_primary_dag_runs: Mock
    ) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"

        get_all_active_primary_dag_runs.return_value = [dag_run_2, dag_run]

        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": dag_run})

    @mock.patch(
        "recidiviz.airflow.dags.calculation.initialize_calculation_dag_group._get_all_active_primary_dag_runs"
    )
    def test_middle_of_queue(self, get_all_active_primary_dag_runs: Mock) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"

        dag_run_3 = Mock()
        dag_run_3.run_id = "test_run_3"

        get_all_active_primary_dag_runs.return_value = [dag_run_2, dag_run, dag_run_3]

        results = self.operator.execute(context={"dag_run": dag_run})

        self.assertEqual(results, "CANCEL")

    @mock.patch(
        "recidiviz.airflow.dags.calculation.initialize_calculation_dag_group._get_all_active_primary_dag_runs"
    )
    def test_secondary_dag_run(self, get_all_active_primary_dag_runs: Mock) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "SECONDARY"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"

        dag_run_3 = Mock()
        dag_run_3.run_id = "test_run_3"

        get_all_active_primary_dag_runs.return_value = [dag_run_2, dag_run, dag_run_3]

        results = self.operator.execute(context={"dag_run": dag_run})

        self.assertEqual(results, "CONTINUE")
