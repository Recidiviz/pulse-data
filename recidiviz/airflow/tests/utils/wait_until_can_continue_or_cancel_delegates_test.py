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
from recidiviz.airflow.dags.utils.config_utils import QueuingActionType
from recidiviz.airflow.dags.utils.wait_until_can_continue_or_cancel_delegates import (
    NoConcurrentDagsWaitUntilCanContinueOrCancelDelegate,
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

    def test_agnostic_first_in_queue(self) -> None:
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

        # continue since first
        results = self.operator.execute(context={"dag_run": dag_run})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)

        # defer since second
        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": dag_run_2})

        # continue, since only SECONDARY in teh queue
        results = self.operator.execute(context={"dag_run": dag_run_3})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)

    def test_agnostic_middle_of_queue(self) -> None:
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

        self.assertEqual(results, QueuingActionType.CANCEL.value)

    def test_agnostic_cancel_logic(self) -> None:
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
            dag_run,
            dag_run_2,
            dag_run_3,
            dag_run_4,
            dag_run_5,
        ]

        # first in secondary queue, can go
        results = self.operator.execute(context={"dag_run": dag_run})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)

        # first in primary queue, can go
        results = self.operator.execute(context={"dag_run": dag_run_2})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)

        # last in primary queue, will defer
        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": dag_run_3})

        # second in secondary queue, will cancel
        results = self.operator.execute(context={"dag_run": dag_run_4})
        self.assertEqual(results, QueuingActionType.CANCEL.value)

        # last in secondary queue, will defer
        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": dag_run_5})

    def test_specific_and_agnostic_queue(self) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY", "state_code_filter": None}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"
        dag_run_2.conf = {"ingest_instance": "PRIMARY", "state_code_filter": "US_OZ"}

        dag_run_3 = Mock()
        dag_run_3.run_id = "test_run_3"
        dag_run_3.conf = {"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"}

        dag_run_4 = Mock()
        dag_run_4.run_id = "test_run_4"
        dag_run_4.conf = {"ingest_instance": "PRIMARY", "state_code_filter": "US_OZ"}

        self.mock_get_all_active_dag_runs.return_value = [
            dag_run,
            dag_run_2,
            dag_run_3,
            dag_run_4,
        ]

        # continue since first in queue
        results = self.operator.execute(context={"dag_run": dag_run})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)

        # cancel since a state-agnostic is in front and matching state-specific is behind
        results = self.operator.execute(context={"dag_run": dag_run_2})
        self.assertEqual(results, QueuingActionType.CANCEL.value)

        # defer since state-agnostic ahead and it's the last matching
        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": dag_run_3})

        # defer since state-agnostic ahead and it's the last matching
        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": dag_run_4})

        dag_run_5 = Mock()
        dag_run_5.conf = {"ingest_instance": "PRIMARY", "state_code_filter": None}
        dag_run.run_id = "test_run"

        # when we add a state agnostic at the end....
        self.mock_get_all_active_dag_runs.return_value = [
            dag_run,
            dag_run_2,
            dag_run_3,
            dag_run_4,
            dag_run_5,
        ]

        # continue since first in queue
        results = self.operator.execute(context={"dag_run": dag_run})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)

        # cancel since a state-agnostic is in front and state agnostic is behind
        results = self.operator.execute(context={"dag_run": dag_run_2})
        self.assertEqual(results, QueuingActionType.CANCEL.value)

        # cancel since a state-agnostic is in front and state agnostic is behind
        results = self.operator.execute(context={"dag_run": dag_run_3})
        self.assertEqual(results, QueuingActionType.CANCEL.value)

        # cancel since a state-agnostic is in front and state agnostic is behind
        results = self.operator.execute(context={"dag_run": dag_run_4})
        self.assertEqual(results, QueuingActionType.CANCEL.value)

        # defer since last state agnostic
        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": dag_run_5})

    def test_specific_multiple(self) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY", "state_code_filter": "US_OZ"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"
        dag_run_2.conf = {"ingest_instance": "SECONDARY", "state_code_filter": "US_OZ"}

        dag_run_3 = Mock()
        dag_run_3.run_id = "test_run_3"
        dag_run_3.conf = {"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"}

        dag_run_4 = Mock()
        dag_run_4.run_id = "test_run_4"
        dag_run_4.conf = {"ingest_instance": "SECONDARY", "state_code_filter": "US_XX"}

        dag_run_5 = Mock()
        dag_run_5.run_id = "test_run_5"
        dag_run_5.conf = {"ingest_instance": "SECONDARY", "state_code_filter": "US_XX"}

        self.mock_get_all_active_dag_runs.return_value = [
            dag_run,
            dag_run_2,
            dag_run_3,
            dag_run_4,
            dag_run_5,
        ]

        # all continue except 5!! (lol kinda wacky)
        results = self.operator.execute(context={"dag_run": dag_run})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)
        results = self.operator.execute(context={"dag_run": dag_run_2})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)
        results = self.operator.execute(context={"dag_run": dag_run_3})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)
        results = self.operator.execute(context={"dag_run": dag_run_4})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)

        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": dag_run_5})

    def test_agnostic_specific_less_than_ideal_but_okay(self) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY", "state_code_filter": None}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"
        dag_run_2.conf = {"ingest_instance": "PRIMARY", "state_code_filter": None}

        dag_run_3 = Mock()
        dag_run_3.run_id = "test_run_3"
        dag_run_3.conf = {"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"}

        self.mock_get_all_active_dag_runs.return_value = [
            dag_run,
            dag_run_2,
            dag_run_3,
        ]

        # okay this is the case where our logic is less than ideal, but i think
        # fixing this would be quite a bit more complex so we'll just add a test
        # and cede this
        results = self.operator.execute(context={"dag_run": dag_run})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)
        # is last, so is okay
        with self.assertRaises(TaskDeferred):
            print(self.operator.execute(context={"dag_run": dag_run_2}))
        # is ALSO last, so doesn't. in an ideal world we'd also nix this
        with self.assertRaises(TaskDeferred):
            self.operator.execute(context={"dag_run": dag_run_3})

    def test_agnostic_not_blocked_by_specific(self) -> None:
        dag_run = Mock()
        dag_run.conf = {"ingest_instance": "PRIMARY", "state_code_filter": "US_XX"}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"

        dag_run_2 = Mock()
        dag_run_2.run_id = "test_run_2"
        dag_run_2.conf = {"ingest_instance": "PRIMARY", "state_code_filter": None}

        self.mock_get_all_active_dag_runs.return_value = [
            dag_run,
            dag_run_2,
        ]

        # both run
        results = self.operator.execute(context={"dag_run": dag_run})
        # this is okay bc we have protection from raw data locks
        self.assertEqual(results, QueuingActionType.CONTINUE.value)
        results = self.operator.execute(context={"dag_run": dag_run_2})
        self.assertEqual(results, QueuingActionType.CONTINUE.value)


class TestNoConcurrentDagsWaitUntilCanContinueOrCancelDelegate(unittest.TestCase):
    """Tests to validate queueing logic of
    NoConcurrentDagsWaitUntilCanContinueOrCancelDelegate
    """

    def setUp(self) -> None:
        self.get_all_active_dag_runs_patcher = mock.patch(
            "recidiviz.airflow.dags.operators.wait_until_can_continue_or_cancel_sensor_async._get_all_active_dag_runs"
        )
        self.mock_get_all_active_dag_runs = self.get_all_active_dag_runs_patcher.start()
        self.operator = WaitUntilCanContinueOrCancelSensorAsync(
            task_id="test_task",
            delegate=NoConcurrentDagsWaitUntilCanContinueOrCancelDelegate(),
        )

    def tearDown(self) -> None:
        self.get_all_active_dag_runs_patcher.stop()

    def _build_mock_dag_run(self, run_id: str) -> Mock:
        dag_run = Mock()
        dag_run.conf = {}
        dag_run.dag_id = "test_dag"
        dag_run.run_id = run_id
        return dag_run

    def test_only_in_queue(self) -> None:
        dag_run_1 = self._build_mock_dag_run("test_run_1")

        self.mock_get_all_active_dag_runs.return_value = [dag_run_1]

        results = self.operator.execute(context={"dag_run": dag_run_1})

        self.assertEqual(results, QueuingActionType.CONTINUE.value)

    def test_first_in_queue(self) -> None:
        dag_run_1 = self._build_mock_dag_run("test_run_1")
        dag_run_2 = self._build_mock_dag_run("test_run_2")
        dag_run_3 = self._build_mock_dag_run("test_run_3")

        self.mock_get_all_active_dag_runs.return_value = [
            dag_run_1,
            dag_run_2,
            dag_run_3,
        ]

        results = self.operator.execute(context={"dag_run": dag_run_1})

        self.assertEqual(results, QueuingActionType.CONTINUE.value)

    def test_middle_in_queue(self) -> None:
        dag_run_1 = self._build_mock_dag_run("test_run_1")
        dag_run_2 = self._build_mock_dag_run("test_run_2")
        dag_run_3 = self._build_mock_dag_run("test_run_3")

        self.mock_get_all_active_dag_runs.return_value = [
            dag_run_1,
            dag_run_2,
            dag_run_3,
        ]

        results = self.operator.execute(context={"dag_run": dag_run_2})

        self.assertEqual(results, QueuingActionType.CANCEL.value)

    def test_last_in_queue(self) -> None:
        dag_run_1 = self._build_mock_dag_run("test_run_1")
        dag_run_2 = self._build_mock_dag_run("test_run_2")
        dag_run_3 = self._build_mock_dag_run("test_run_3")

        self.mock_get_all_active_dag_runs.return_value = [
            dag_run_1,
            dag_run_2,
            dag_run_3,
        ]

        # last in queue, will defer
        with self.assertRaises(TaskDeferred):
            _ = self.operator.execute(context={"dag_run": dag_run_3})
