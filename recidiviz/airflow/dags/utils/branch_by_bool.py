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
"""Helper functions for creating branches based on a boolean value"""
from typing import List, Tuple

from airflow.decorators import task
from airflow.models import BaseOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.utils.branch_utils import (
    BRANCH_START_TASK_NAME,
    TaskGroupOrOperator,
    create_branch_end,
    get_branch_leaf_tasks,
    get_branch_root_tasks,
)

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement


def create_branch_by_bool(
    *,
    branch_if_true: List[TaskGroupOrOperator] | TaskGroupOrOperator,
    branch_if_false: List[TaskGroupOrOperator] | TaskGroupOrOperator,
    bool_value: bool,
    start_trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
) -> Tuple[BaseOperator, BaseOperator]:
    """Creates a branching operator that will proceed down the appropriate branch
    based on |bool_value|. Because |bool_value| is a static value that is accessible at
    compile time, use this branching util when you want both branches to be present in
    the DAG irrespective of |bool_value| in order to better visualize that one path was
    not chosen. If you don't care about both paths being in the DAG, add conditional logic
    directly into the DAG itself!
    """

    branch_roots_by_bool = {
        True: get_branch_root_tasks(branch_if_true),
        False: get_branch_root_tasks(branch_if_false),
    }

    branch_leaves_by_bool = {
        True: get_branch_leaf_tasks(branch_if_true),
        False: get_branch_leaf_tasks(branch_if_false),
    }

    @task.branch(task_id=BRANCH_START_TASK_NAME, trigger_rule=start_trigger_rule)
    def get_selected_branch_ids() -> List[str]:
        return [task.task_id for task in branch_roots_by_bool[bool_value]]

    branch_start: BranchPythonOperator = get_selected_branch_ids()
    branch_end: PythonOperator = create_branch_end()

    for branch_root in branch_roots_by_bool.values():
        branch_start >> branch_root
    for branch_leaf in branch_leaves_by_bool.values():
        branch_leaf >> branch_end
    return branch_start, branch_end
