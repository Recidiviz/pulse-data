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
from typing import List, Optional

from airflow.decorators import dag, task_group
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.utils.branch_utils import (
    BRANCH_END_TASK_NAME,
    BRANCH_START_TASK_NAME,
)
from recidiviz.airflow.dags.utils.branching_by_key import (
    TaskGroupOrOperator,
    create_branching_by_key,
    select_all_branches,
    select_state_code_parameter_branch,
)
from recidiviz.airflow.dags.utils.config_utils import STATE_CODE_FILTER
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

            create_branching_by_key(state_codes, select_state_code_parameter_branch)

        test_dag = create_test_dag()
        branching_start = test_dag.get_task(BRANCH_START_TASK_NAME)
        branching_end = test_dag.get_task(BRANCH_END_TASK_NAME)

        self.assertEqual(len(test_dag.tasks), 6)
        self.assertEqual(branching_end.trigger_rule, TriggerRule.ALL_DONE)
        self.assertEqual(
            branching_start.downstream_task_ids,
            {"US_XX", "US_YY", "US_ZZ_group.US_ZZ_1"},
        )
        self.assertEqual(
            branching_end.upstream_task_ids, {"US_XX", "US_YY", "US_ZZ_group.US_ZZ_2"}
        )

    def test_all_states_succeed(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            state_codes = {
                "US_XX": EmptyOperator(task_id="US_XX"),
                "US_YY": EmptyOperator(task_id="US_YY"),
                "US_ZZ": EmptyOperator(task_id="US_ZZ"),
            }

            create_branching_by_key(state_codes, select_all_branches)

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(test_dag, session=session)

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

            create_branching_by_key(state_codes, select_state_code_parameter_branch)

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag,
                session=session,
                expected_failure_task_id_regexes=["US_ZZ$", BRANCH_END_TASK_NAME],
            )

    def test_all_states_upstream_fails(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            upstream_failed = PythonOperator(
                task_id="upstream", python_callable=raise_task_failure
            )
            state_codes = {
                "US_XX": EmptyOperator(task_id="US_XX"),
                "US_ZZ": EmptyOperator(task_id="US_ZZ"),
            }

            branch_start, _ = create_branching_by_key(
                state_codes, select_state_code_parameter_branch
            )
            upstream_failed >> branch_start

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag,
                session=session,
                expected_failure_task_id_regexes=[
                    "upstream",
                    "US_XX",  # since upstream failed, status will be upstream_failed
                    "US_ZZ",  # since upstream failed, status will be upstream_failed
                    BRANCH_END_TASK_NAME,
                ],
                skip_checking_task_statuses=True,
            )

    def test_single_selected_state_succeeds_branch_succeeds(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            state_codes = {
                "US_XX": EmptyOperator(task_id="US_XX"),
                "US_ZZ": PythonOperator(
                    task_id="US_ZZ", python_callable=raise_task_failure
                ),
            }

            create_branching_by_key(state_codes, select_state_code_parameter_branch)

        test_dag = create_test_dag()

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag,
                session=session,
                run_conf={STATE_CODE_FILTER: "US_XX"},
                expected_skipped_task_id_regexes=["US_ZZ"],
            )

    def test_selected_state_succeeds_branch_succeeds(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            state_codes = {
                "US_XX": EmptyOperator(task_id="US_XX"),
                "US_YY": EmptyOperator(task_id="US_YY"),
                "US_ZZ": PythonOperator(
                    task_id="US_ZZ", python_callable=raise_task_failure
                ),
            }

            create_branching_by_key(state_codes, lambda _: ["US_XX", "US_YY"])

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag,
                session=session,
                expected_skipped_task_id_regexes=["US_ZZ"],
            )

    def test_selected_state_fails_branch_fails(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            state_codes = {
                "US_XX": EmptyOperator(task_id="US_XX"),
                "US_ZZ": PythonOperator(
                    task_id="US_ZZ", python_callable=raise_task_failure
                ),
            }

            create_branching_by_key(state_codes, select_state_code_parameter_branch)

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag,
                session=session,
                run_conf={STATE_CODE_FILTER: "US_ZZ"},
                expected_skipped_task_id_regexes=["US_XX"],
                expected_failure_task_id_regexes=["US_ZZ$", BRANCH_END_TASK_NAME],
            )

    def test_selected_state_trigger_rules_skipped_if_downstream(self) -> None:
        @task_group(group_id="US_ZZ_group")
        def us_zz_task_group() -> None:
            operator1 = EmptyOperator(
                task_id="US_ZZ_1", trigger_rule=TriggerRule.ALL_DONE
            )  # since this is the root of the task group, should get skipped if branch
            operator2 = EmptyOperator(task_id="US_ZZ_2")
            operator1 >> operator2

        def build_branch(code: str) -> List[TaskGroupOrOperator]:
            with TaskGroup(f"{code}_group") as group:
                operator1 = PythonOperator(
                    task_id=f"{code}_1", python_callable=raise_task_failure
                )
                # since this task is downstream of branch, should be skipped if branch
                # is skipped, but should run even if the upstream (code_1) fails
                operator2 = EmptyOperator(
                    task_id=f"{code}_2", trigger_rule=TriggerRule.ALL_DONE
                )
                operator1 >> operator2

            return [operator2, group]

        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            state_codes = {
                "US_WW": build_branch("US_WW"),
                "US_XX": build_branch("US_XX"),
                "US_YY": EmptyOperator(task_id="US_YY"),
                "US_ZZ": us_zz_task_group(),
            }
            create_branching_by_key(state_codes, lambda _: ["US_YY", "US_WW"])

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag,
                session=session,
                expected_failure_task_id_regexes=["US_WW_group.US_WW_1"],
                expected_skipped_task_id_regexes=["US_ZZ", "US_XX"],
                expected_success_task_id_regexes=[
                    BRANCH_START_TASK_NAME,
                    BRANCH_END_TASK_NAME,
                    "US_YY",
                    "US_WW_group.US_WW_2",
                ],
            )
