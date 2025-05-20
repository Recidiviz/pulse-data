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
Helper functions for testing Airflow DAGs.
"""
import os
from typing import Any, Callable, Type

from airflow.decorators import task
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.utils.environment import DAG_ID, MAP_INDEX, RUN_ID, TASK_ID


def fake_operator_constructor(*_args: Any, **kwargs: Any) -> EmptyOperator:
    return EmptyOperator(
        task_id=kwargs["task_id"],
        trigger_rule=(
            kwargs["trigger_rule"]
            if "trigger_rule" in kwargs
            else TriggerRule.ALL_SUCCESS
        ),
    )


def fake_failing_operator_constructor(*_args: Any, **kwargs: Any) -> BaseOperator:
    task_id = kwargs["task_id"]

    def fail() -> None:
        raise ValueError(f"FAIL: {task_id}")

    return PythonOperator(
        task_id=kwargs["task_id"],
        trigger_rule=(
            kwargs["trigger_rule"]
            if "trigger_rule" in kwargs
            else TriggerRule.ALL_SUCCESS
        ),
        python_callable=fail,
        retries=-1,
    )


def fake_operator_with_return_value(return_value: Any) -> Callable:
    """Returns a fake operator that returns a specified value."""

    class FakeOperator(BaseOperator):
        def __init__(self, *_args: Any, **kwargs: Any) -> None:
            super().__init__(
                task_id=kwargs["task_id"],
                trigger_rule=(
                    kwargs["trigger_rule"]
                    if "trigger_rule" in kwargs
                    else TriggerRule.ALL_SUCCESS
                ),
            )

        def execute(self, context: Context) -> Any:  # pylint: disable=unused-argument
            return return_value

    return FakeOperator


def fake_operator_from_callable(
    to_callable: Callable[[BaseOperator, Context], Any],
) -> Type[BaseOperator]:
    """Returns a fake operator that returns a specified value."""

    class FakeOperator(BaseOperator):
        def __init__(self, *_args: Any, **kwargs: Any) -> None:
            self.kwargs = kwargs
            super().__init__(
                task_id=kwargs["task_id"],
                trigger_rule=(
                    kwargs["trigger_rule"]
                    if "trigger_rule" in kwargs
                    else TriggerRule.ALL_SUCCESS
                ),
            )

        def execute(self, context: Context) -> Any:
            return to_callable(self, context)

    return FakeOperator


def fake_failure_task(*_args: Any, **kwargs: Any) -> EmptyOperator:
    @task(
        task_id=kwargs["task_id"],
        trigger_rule=(
            kwargs["trigger_rule"]
            if "trigger_rule" in kwargs
            else TriggerRule.ALL_SUCCESS
        ),
    )
    def create_fake_failure_task() -> Any:
        """
        Raises an exception to simulate a failure.
        """
        raise ValueError("Test failure")

    return create_fake_failure_task()


class FakeFailureOperator(BaseOperator):
    def __init__(self, *_args: Any, **kwargs: Any) -> None:
        super().__init__(
            task_id=kwargs["task_id"],
            trigger_rule=(
                kwargs["trigger_rule"]
                if "trigger_rule" in kwargs
                else TriggerRule.ALL_SUCCESS
            ),
        )

    def execute(self, context: Context) -> Any:
        raise ValueError("Test failure")


def fake_task_function_with_return_value(return_value: Any) -> Callable:
    def fake_func(*args: Any, **kwargs: Any) -> Any:  # pylint: disable=unused-argument
        return return_value

    return fake_func


def set_k8s_operator_env_vars(context: Context) -> None:
    ti = context["ti"]
    os.environ[DAG_ID] = ti.dag_id
    os.environ[TASK_ID] = ti.task_id
    os.environ[RUN_ID] = context["run_id"]
    os.environ[MAP_INDEX] = str(ti.map_index)
