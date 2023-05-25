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
Helper functions for creating branches based on state codes.
"""
from typing import Any, Dict, List, Tuple, Union

from airflow.decorators import task
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement


def _get_id_for_operator_or_group(
    operator_or_group: Union[BaseOperator, TaskGroup]
) -> str:
    return (
        operator_or_group.task_id
        if isinstance(operator_or_group, BaseOperator)
        else operator_or_group.group_id
    )


def create_state_code_branching(
    branch_by_state_code: Dict[str, Union[BaseOperator, TaskGroup]],
    branch_end_trigger_rule: TriggerRule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
) -> Tuple[BaseOperator, BaseOperator]:
    r"""
    Given a map of all possible branches by state code, creates a BranchPythonOperator, that
    selects only the branches for a given state if the DAG was launched with a `state_code_filter`
    parameter. If no `state_code_filter` parameter is supplied, creates a BranchPythonOperator
    that runs all the branches.

    The resulting structure looks like this:

                                                    US_XX
                                               /                   \
                      state_code_branch_start                        state_code_branch_end
                                               \                   /
                                                    US_YY

    If no branches are selected, the end node will fail.
    """

    branch_ids_by_state_code = {
        state_code: _get_id_for_operator_or_group(branch)
        for state_code, branch in branch_by_state_code.items()
    }

    @task.branch(task_id="state_code_branch_start")
    def get_selected_branch_ids(**context: Any) -> Any:
        state_code_filter = context["params"].get("state_code_filter")
        selected_state_codes = (
            [state_code_filter] if state_code_filter else branch_by_state_code.keys()
        )
        return [
            branch_ids_by_state_code[state_code]
            for state_code in selected_state_codes
            # If the selected state does not have a branch in this branching,
            # we just skip it and select no branches for that state.
            if state_code in branch_ids_by_state_code
        ]

    branch_start: BranchPythonOperator = get_selected_branch_ids()
    branch_end = EmptyOperator(
        task_id="state_code_branch_end",
        trigger_rule=branch_end_trigger_rule,
    )
    branches: List[Union[BaseOperator, TaskGroup]] = list(branch_by_state_code.values())
    branch_start >> branches >> branch_end
    return branch_start, branch_end
