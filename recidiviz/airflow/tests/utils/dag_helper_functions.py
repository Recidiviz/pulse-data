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
from typing import Any, Callable

from airflow.decorators import task
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule


def fake_operator_constructor(*_args: Any, **kwargs: Any) -> EmptyOperator:
    return EmptyOperator(
        task_id=kwargs["task_id"],
        trigger_rule=kwargs["trigger_rule"]
        if "trigger_rule" in kwargs
        else TriggerRule.ALL_SUCCESS,
    )


def fake_operator_with_return_value(return_value: Any) -> Callable:
    """
    Returns a fake operator that returns a specified value.
    """

    class FakeOperator(BaseOperator):
        def __init__(self, *_args: Any, **kwargs: Any) -> None:
            super().__init__(
                task_id=kwargs["task_id"],
                trigger_rule=kwargs["trigger_rule"]
                if "trigger_rule" in kwargs
                else TriggerRule.ALL_SUCCESS,
            )

        def execute(self, context: Context) -> Any:  # pylint: disable=unused-argument
            return return_value

    return FakeOperator


def fake_failure_task(*_args: Any, **kwargs: Any) -> EmptyOperator:
    @task(
        task_id=kwargs["task_id"],
        trigger_rule=kwargs["trigger_rule"]
        if "trigger_rule" in kwargs
        else TriggerRule.ALL_SUCCESS,
    )
    def create_fake_failure_task() -> Any:
        """
        Raises an exception to simulate a failure.
        """
        raise ValueError("Test failure")

    return create_fake_failure_task()
