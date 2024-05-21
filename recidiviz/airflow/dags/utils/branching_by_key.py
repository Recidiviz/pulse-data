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
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator, DagRun
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.utils.config_utils import get_state_code_filter

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement


def select_state_code_parameter_branch(dag_run: DagRun) -> Optional[List[str]]:
    state_code_filter = get_state_code_filter(dag_run)
    return [state_code_filter] if state_code_filter else None


def _get_id_for_operator_or_group(
    operator_or_group: Union[BaseOperator, TaskGroup]
) -> str:
    return (
        operator_or_group.task_id
        if isinstance(operator_or_group, BaseOperator)
        else operator_or_group.group_id
    )


BRANCH_START_TASK_NAME = "branch_start"
BRANCH_END_TASK_NAME = "branch_end"


def create_branching_by_key(
    branch_by_key: Dict[str, Union[BaseOperator, TaskGroup]],
    select_branches_fn: Callable[[DagRun], Optional[List[str]]],
) -> Tuple[BaseOperator, BaseOperator]:
    r"""Given a map of all possible branches and a branch filter function, creates a
    BranchPythonOperator, that selects only the branches that the select_branches_fn 
    returns. If the return value of select_branches_fn evaluates to False, all branches
    will be selected.

    The resulting structure looks like this:

                                                    US_XX
                                               /                   \
                                    branch_start                    branch_end
                                               \                   /
                                                    US_YY

    If no branches are selected, the end node will fail.
    """
    branch_ids_by_key: Dict[str, str] = {}
    start_branch_by_key: Dict[str, EmptyOperator] = {}

    for key, branch in branch_by_key.items():
        branch_ids_by_key[key] = _get_id_for_operator_or_group(branch)
        # Airflow does not allow branch operators to branch to TaskGroups, only Tasks,
        # so we insert an empty task before a TaskGroup if it is the branch result
        # to ensure we're able to go forward.
        start_branch_by_key[key] = EmptyOperator(task_id=f"{key}_start")

    @task.branch(task_id=BRANCH_START_TASK_NAME)
    def get_selected_branch_ids(dag_run: Optional[DagRun] = None) -> Any:
        if not dag_run:
            raise ValueError(
                "Dag run not passed to task. Should be automatically set due to function being a task."
            )

        selected_branch_keys_override = select_branches_fn(dag_run)
        selected_branch_keys = (
            selected_branch_keys_override
            if selected_branch_keys_override
            else branch_by_key.keys()
        )
        return [
            start_branch_by_key[selected_key].task_id
            for selected_key in selected_branch_keys
            # If the selected state does not have a branch in this branching,
            # we just skip it and select no branches for that state.
            if selected_key in branch_ids_by_key
        ]

    branch_start: BranchPythonOperator = get_selected_branch_ids()

    @task(task_id=BRANCH_END_TASK_NAME, trigger_rule=TriggerRule.ALL_DONE)
    def create_branch_end(dag_run: Optional[DagRun] = None, **kwargs: Any) -> Any:
        if not dag_run:
            raise ValueError(
                "Dag run not passed to task. Should be automatically set due to function being a task."
            )

        current_task = kwargs["task"]
        failed_task_instances = dag_run.get_task_instances(
            state=[TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]
        )
        failed_upstream_task_ids = [
            t.task_id
            for t in failed_task_instances
            if t.task_id in current_task.upstream_task_ids
        ]
        if failed_upstream_task_ids:
            raise AirflowFailException(
                f"Failing - upstream nodes failed: {failed_upstream_task_ids}"
            )

    branch_end: PythonOperator = create_branch_end()
    for key, branch in branch_by_key.items():
        branch_start >> start_branch_by_key[key] >> branch >> branch_end
    return branch_start, branch_end
