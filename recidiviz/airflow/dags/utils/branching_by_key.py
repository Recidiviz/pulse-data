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
from airflow.models import BaseOperator, DagRun
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.utils.branch_utils import (
    BRANCH_START_TASK_NAME,
    create_branch_end,
    get_branch_leaf_tasks,
    get_branch_root_tasks,
)
from recidiviz.airflow.dags.utils.config_utils import get_state_code_filter

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement


TaskGroupOrOperator = Union[BaseOperator, TaskGroup]


def select_state_code_parameter_branch(dag_run: DagRun) -> Optional[List[str]]:
    state_code_filter = get_state_code_filter(dag_run)
    return [state_code_filter] if state_code_filter else None


def select_all_branches(
    dag_run: DagRun,  # pylint: disable=unused-argument
) -> None:
    return None


def create_branching_by_key(
    branch_by_key: Dict[str, Union[TaskGroupOrOperator, List[TaskGroupOrOperator]]],
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
    branch_roots_by_key: Dict[str, List[BaseOperator]] = {}
    branch_leaves_by_key: Dict[str, List[BaseOperator]] = {}

    for key, branch in branch_by_key.items():
        # TODO(#33716) Airflow does not allow branch operators to branch to TaskGroups,
        # only Tasks in Airflow versions pre-2.10.
        # so for now, instead of branching to the TaskGroup, we can branch to the roots
        # of the task group and then from the leaves to branch_end
        branch_roots_by_key[key] = get_branch_root_tasks(branch)
        branch_leaves_by_key[key] = get_branch_leaf_tasks(branch)

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
            branch_root.task_id
            for selected_key in selected_branch_keys
            # If the selected state does not have a branch in this branching,
            # we just skip it and select no branches for that state.
            if selected_key in branch_by_key
            for branch_root in branch_roots_by_key[selected_key]
        ]

    branch_start: BranchPythonOperator = get_selected_branch_ids()
    branch_end: PythonOperator = create_branch_end()

    for key in branch_by_key:
        for branch_root in branch_roots_by_key[key]:
            branch_start >> branch_root
        for branch_leaf in branch_leaves_by_key[key]:
            branch_leaf >> branch_end
    return branch_start, branch_end
