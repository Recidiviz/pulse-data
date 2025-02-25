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
"""Tests for WaitUntilCanContinueOrCancelDelegates"""

import unittest
from unittest import mock
from unittest.mock import Mock

from airflow.exceptions import TaskDeferred

from recidiviz.airflow.dags.calculation.initialize_calculation_dag_group import (
    WaitUntilCanContinueOrCancelSensorAsync,
)
from recidiviz.airflow.dags.utils.wait_until_can_continue_or_cancel_delegate import (
    StateSpecificNonBlockingDagWaitUntilCanContinueOrCancelDelegate,
)


class TestStateSpecificNonBlockingDagWaitUntilCanContinueOrCancelDelegate(
    unittest.TestCase
):
    """Tests to validate queueing logic of
    StateSpecificNonBlockingDagWaitUntilCanContinueOrCancelDelegate
    """

    def setUp(self) -> None:
        self.get_all_active_dag_runs_patcher = mock.patch(
            "recidiviz.airflow.dags.operators.wait_until_can_continue_or_cancel_sensor_async._get_all_active_dag_runs"
        )
        self.mock_get_all_active_dag_runs = self.get_all_active_dag_runs_patcher.start()
        self.operator = WaitUntilCanContinueOrCancelSensorAsync(
            task_id="test_task",
            delegate=StateSpecificNonBlockingDagWaitUntilCanContinueOrCancelDelegate(),
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
