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
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.utils.config_utils import get_state_code_filter

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement


TaskGroupOrOperator = Union[BaseOperator, TaskGroup]


def select_state_code_parameter_branch(dag_run: DagRun) -> Optional[List[str]]:
    state_code_filter = get_state_code_filter(dag_run)
    return [state_code_filter] if state_code_filter else None


def get_branch_root_tasks(
    branch_root_nodes: Union[TaskGroupOrOperator, List[TaskGroupOrOperator]]
) -> List[BaseOperator]:
    """Builds a list of branch root tasks by expanding any TaskGroups in |branch_root_nodes|
    into a list of their root tasks (or tasks that are defined as not having any
    upstream dependencies within the the TaskGroup).
    """
    branch_root_tasks: List[BaseOperator] = []
    branch_root_nodes = (
        [branch_root_nodes]
        if not isinstance(branch_root_nodes, list)
        else branch_root_nodes
    )
    for branch_root_node in branch_root_nodes:
        if isinstance(branch_root_node, BaseOperator):
            branch_root_tasks.append(branch_root_node)
        else:
            branch_root_tasks.extend(branch_root_node.roots)
    return branch_root_tasks


def get_branch_leaf_tasks(
    branch_leaf_nodes: Union[TaskGroupOrOperator, List[TaskGroupOrOperator]]
) -> List[BaseOperator]:
    """Builds a list of branch leaf tasks by expanding any TaskGroups in |branch_leaf_nodes|
    into a list of their leaf tasks (or tasks that are defined as not having any
    downstream dependencies within the the TaskGroup).
    """
    branch_leaf_tasks: List[BaseOperator] = []
    branch_leaf_nodes = (
        [branch_leaf_nodes]
        if not isinstance(branch_leaf_nodes, list)
        else branch_leaf_nodes
    )
    for branch_leaf_node in branch_leaf_nodes:
        if isinstance(branch_leaf_node, BaseOperator):
            branch_leaf_tasks.append(branch_leaf_node)
        else:
            branch_leaf_tasks.extend(branch_leaf_node.leaves)
    return branch_leaf_tasks


BRANCH_START_TASK_NAME = "branch_start"
BRANCH_END_TASK_NAME = "branch_end"


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
    for key in branch_by_key:
        for branch_root in branch_roots_by_key[key]:
            branch_start >> branch_root
        for branch_leaf in branch_leaves_by_key[key]:
            branch_leaf >> branch_end
    return branch_start, branch_end
