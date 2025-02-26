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
Unit test to test the branching_by_key.py helper functions.
"""
from datetime import datetime
from typing import Optional

from airflow.decorators import dag, task_group
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.calculation.initialize_calculation_dag_group import (
    get_state_code_filter,
)
from recidiviz.airflow.dags.utils.branching_by_key import create_branching_by_key
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
        @task_group(group_id="US_ZZ_group")
        def us_zz_task_group() -> None:
            operator1 = EmptyOperator(task_id="US_ZZ_1")
            operator2 = EmptyOperator(task_id="US_ZZ_2")
            operator1 >> operator2

        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            state_codes = {
                "US_XX": EmptyOperator(task_id="US_XX"),
                "US_YY": EmptyOperator(task_id="US_YY"),
                "US_ZZ": us_zz_task_group(),
            }

            create_branching_by_key(state_codes, get_state_code_filter)

        test_dag = create_test_dag()
        branching_start = test_dag.get_task("branch_start")
        branching_end = test_dag.get_task("branch_end")

        self.assertEqual(len(test_dag.tasks), 9)
        self.assertEqual(branching_end.trigger_rule, TriggerRule.ALL_DONE)
        self.assertEqual(
            branching_start.downstream_task_ids,
            {"US_XX_start", "US_YY_start", "US_ZZ_start"},
        )
        self.assertEqual(
            branching_end.upstream_task_ids, {"US_XX", "US_YY", "US_ZZ_group.US_ZZ_2"}
        )

    # TODO(#22762): Update test to use new AirflowIntegrationTest helper functions
    def test_all_states_succeed(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            state_codes = {
                "US_XX": EmptyOperator(task_id="US_XX"),
                "US_YY": EmptyOperator(task_id="US_YY"),
                "US_ZZ": EmptyOperator(task_id="US_ZZ"),
            }

            create_branching_by_key(state_codes, get_state_code_filter)

        test_dag = create_test_dag()
        test_dag.test()

    # TODO(#22762): This test currently fails ath the python operator script, but what we really want to test is that the end task fails
    def test_all_states_one_fails(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            state_codes = {
                "US_XX": EmptyOperator(task_id="US_XX"),
                "US_YY": EmptyOperator(task_id="US_YY"),
                "US_ZZ": PythonOperator(
                    task_id="US_ZZ", python_callable=raise_task_failure
                ),
            }

            create_branching_by_key(state_codes, get_state_code_filter)

        test_dag = create_test_dag()
        with self.assertRaises(AirflowFailException):
            test_dag.test()
