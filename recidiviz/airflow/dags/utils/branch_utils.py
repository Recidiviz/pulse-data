# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpful utils for working with branch operators."""
from typing import Any, List, Optional, Union

from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator, DagRun
from airflow.models.taskmixin import DAGNode
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.utils.types import assert_type

BRANCH_START_TASK_NAME = "branch_start"
BRANCH_END_TASK_NAME = "branch_end"


def get_branch_root_tasks(
    branch_root_nodes: Union[DAGNode, List[DAGNode]],
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
            branch_root_tasks.extend(
                [assert_type(root, BaseOperator) for root in branch_root_node.roots]
            )
    return branch_root_tasks


def get_branch_leaf_tasks(
    branch_leaf_nodes: Union[DAGNode, List[DAGNode]],
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
            branch_leaf_tasks.extend(
                [assert_type(root, BaseOperator) for root in branch_leaf_node.leaves]
            )
    return branch_leaf_tasks


@task(task_id=BRANCH_END_TASK_NAME, trigger_rule=TriggerRule.ALL_DONE)
def create_branch_end(dag_run: Optional[DagRun] = None, **kwargs: Any) -> None:
    """Creates a branch ending task that will fail if any of the tasks directly above
    it also failed.
    """
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
