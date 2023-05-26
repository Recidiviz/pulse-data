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
Unit test to test the state_code_branch.py helper functions.
"""
import unittest
from datetime import datetime

from airflow.decorators import dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.utils.state_code_branch import create_state_code_branching

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement


class TestStateCodeBranch(unittest.TestCase):
    """Test the state_code_branch.py helper function."""

    def test_state_code_branch_creates_branches(self) -> None:
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

            create_state_code_branching(state_codes)

        test_dag = create_test_dag()
        branching_start = test_dag.get_task("state_code_branch_start")
        branching_end = test_dag.get_task("state_code_branch_end")

        self.assertEqual(len(test_dag.tasks), 9)
        self.assertEqual(
            branching_end.trigger_rule, TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )
        self.assertEqual(
            branching_start.downstream_task_ids,
            {"US_XX_start", "US_YY_start", "US_ZZ_start"},
        )
        self.assertEqual(
            branching_end.upstream_task_ids, {"US_XX", "US_YY", "US_ZZ_group.US_ZZ_2"}
        )
