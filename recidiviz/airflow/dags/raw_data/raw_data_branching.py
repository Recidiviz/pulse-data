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
"""Logic for raw-data-import-specfic branching"""
from typing import Callable, Dict, List, Optional, Union

from airflow.models import BaseOperator, DagRun
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.utils.config_utils import (
    get_ingest_instance,
    get_state_code_filter,
)
from recidiviz.airflow.dags.utils.dag_orchestration_utils import (
    get_raw_data_dag_enabled_state_and_instance_pairs,
    get_raw_data_dag_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def get_raw_data_import_branch_key(state_code: str, ingest_instance: str) -> str:
    """Given a |state_code| and |ingest_instance|, returns the name of the branch that
    will execute the raw data import.
    """
    return f"{state_code.lower()}_{ingest_instance.lower()}_import_branch"


def get_raw_data_branch_filter(dag_run: DagRun) -> Optional[List[str]]:
    """Returns the branch key associated with the parameters passed to |dag_run|;
    otherwise, returns None if no parameters are provided.
    """
    state_code = get_state_code_filter(dag_run)
    raw_data_instance = get_ingest_instance(dag_run)

    if state_code and raw_data_instance:
        return [get_raw_data_import_branch_key(state_code, raw_data_instance)]
    if raw_data_instance:
        return [
            get_raw_data_import_branch_key(state_code.value, raw_data_instance)
            for state_code in get_raw_data_dag_enabled_states()
        ]
    if state_code:
        raise ValueError("Cannot build branch filter with only a state code")

    return None


def create_raw_data_branch_map(
    branched_task_function: Callable[
        [StateCode, DirectIngestInstance], Union[BaseOperator, TaskGroup]
    ],
) -> Dict[str, Union[BaseOperator, TaskGroup]]:
    """Creates a branching operator for each state_code and raw_data_instance enabled
    in the current environment
    """

    task_group_by_task_id: Dict[str, Union[BaseOperator, TaskGroup]] = {}

    for (
        state_code,
        ingest_instance,
    ) in get_raw_data_dag_enabled_state_and_instance_pairs():
        task_group_by_task_id[
            get_raw_data_import_branch_key(state_code.value, ingest_instance.value)
        ] = branched_task_function(state_code, ingest_instance)

    return task_group_by_task_id
