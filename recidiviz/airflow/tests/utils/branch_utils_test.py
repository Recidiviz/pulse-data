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
"""Unit tests for branch_utils"""
from datetime import datetime
from typing import Optional

from airflow.decorators import dag
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.utils.branch_utils import (
    BRANCH_END_TASK_NAME,
    create_branch_end,
)
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


def raise_task_failure(message: Optional[str] = None) -> None:
    raise AirflowFailException(message or "Task failed")


class TestCreateBranchEnd(AirflowIntegrationTest):
    """Tests for the create_branch_end helper task."""

    def test_no_upstream(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            create_branch_end()

        test_dag = create_test_dag()
        branching_end = test_dag.get_task(BRANCH_END_TASK_NAME)

        self.assertEqual(len(test_dag.tasks), 1)
        self.assertEqual(branching_end.trigger_rule, TriggerRule.ALL_DONE)

        with Session(bind=self.engine) as session:
            self.run_dag_test(test_dag, session=session)

    def test_all_upstream_succeed(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            us_xx = EmptyOperator(task_id="US_XX")
            us_yy = EmptyOperator(task_id="US_YY")
            us_zz = EmptyOperator(task_id="US_ZZ")

            end = create_branch_end()

            us_xx >> end
            us_yy >> end
            us_zz >> end

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(test_dag, session=session)

    def test_has_failures(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            us_xx = PythonOperator(task_id="US_XX", python_callable=raise_task_failure)
            us_yy = EmptyOperator(task_id="US_YY")
            us_zz = EmptyOperator(task_id="US_ZZ")

            end = create_branch_end()

            us_xx >> end
            us_yy >> end
            us_zz >> end

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag,
                session=session,
                expected_failure_task_id_regexes=["US_XX", BRANCH_END_TASK_NAME],
            )

    def test_has_upstream_failures(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            us_xx = PythonOperator(task_id="US_XX", python_callable=raise_task_failure)
            us_yy = EmptyOperator(task_id="US_YY")
            us_zz = EmptyOperator(task_id="US_ZZ")

            end = create_branch_end()

            us_xx >> us_yy
            us_yy >> end
            us_zz >> end

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag,
                session=session,
                expected_failure_task_id_regexes=[
                    "US_XX",
                    "US_YY",
                    BRANCH_END_TASK_NAME,
                ],
            )

    def test_has_skips(self) -> None:
        @dag(start_date=datetime(2021, 1, 1), schedule=None, catchup=False)
        def create_test_dag() -> None:
            us_xx = ShortCircuitOperator(
                task_id="US_XX",
                python_callable=lambda: False,
                ignore_downstream_trigger_rules=False,
            )
            us_yy = EmptyOperator(task_id="US_YY")
            us_zz = EmptyOperator(task_id="US_ZZ")

            end = create_branch_end()

            us_xx >> us_yy
            us_yy >> end
            us_zz >> end

        test_dag = create_test_dag()
        with Session(bind=self.engine) as session:
            self.run_dag_test(
                test_dag, session=session, expected_skipped_task_id_regexes=["US_YY"]
            )
