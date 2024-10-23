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
"""Unit tests for create_branch_by_bool"""
from datetime import datetime
from typing import Optional

from airflow.decorators import dag
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.utils.branch_by_bool import create_branch_by_bool
from recidiviz.airflow.dags.utils.branch_utils import BRANCH_END_TASK_NAME
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


def raise_task_failure(message: Optional[str] = None) -> None:
    raise AirflowFailException(message or "Task failed")


class TestBranchingByKey(AirflowIntegrationTest):
    """Test the branching_by_key.py helper function."""

    def test_creates_branches(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            left = EmptyOperator(task_id="left")
            right = EmptyOperator(task_id="right")

            create_branch_by_bool(left, right, True)

        test_dag = create_test_dag()
        branching_start = test_dag.get_task("branch_start")
        branching_end = test_dag.get_task("branch_end")

        self.assertEqual(len(test_dag.tasks), 4)
        self.assertEqual(branching_start.trigger_rule, TriggerRule.ALL_SUCCESS)
        self.assertEqual(branching_end.trigger_rule, TriggerRule.ALL_DONE)
        self.assertEqual(
            branching_start.downstream_task_ids,
            {"left", "right"},
        )
        self.assertEqual(branching_end.upstream_task_ids, {"left", "right"})

    def test_creates_start_with_trigger(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            left = EmptyOperator(task_id="left")
            right = EmptyOperator(task_id="right")

            create_branch_by_bool(
                left, right, True, start_trigger_rule=TriggerRule.ALL_FAILED
            )

        test_dag = create_test_dag()
        branching_start = test_dag.get_task("branch_start")
        branching_end = test_dag.get_task("branch_end")

        self.assertEqual(len(test_dag.tasks), 4)
        self.assertEqual(branching_start.trigger_rule, TriggerRule.ALL_FAILED)
        self.assertEqual(branching_end.trigger_rule, TriggerRule.ALL_DONE)
        self.assertEqual(
            branching_start.downstream_task_ids,
            {"left", "right"},
        )
        self.assertEqual(branching_end.upstream_task_ids, {"left", "right"})

    def test_succeeds(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            left = EmptyOperator(task_id="left")
            also_left = EmptyOperator(task_id="also_left")
            right = EmptyOperator(task_id="right")

            create_branch_by_bool([left, also_left], right, True)

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag, session=session, expected_skipped_task_id_regexes=["right"]
            )

    def test_succeeds_with_group(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            left = EmptyOperator(task_id="left")
            also_left = EmptyOperator(task_id="also_left")
            right = EmptyOperator(task_id="right")

            create_branch_by_bool([left, also_left], right, True)

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag, session=session, expected_skipped_task_id_regexes=["right"]
            )

    def test_fails(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            left = PythonOperator(task_id="left", python_callable=raise_task_failure)
            also_left = EmptyOperator(task_id="also_left")
            right = EmptyOperator(task_id="right")

            create_branch_by_bool([left, also_left], right, True)

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag,
                session=session,
                expected_skipped_task_id_regexes=["right"],
                expected_failure_task_id_regexes=["^left", BRANCH_END_TASK_NAME],
            )
