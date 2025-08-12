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
from itertools import product
from typing import Callable, Dict, List, Optional, Union

from airflow.models import DagRun

from recidiviz.airflow.dags.utils.branching_by_key import DAGNode
from recidiviz.airflow.dags.utils.config_utils import (
    get_ingest_instance,
    get_state_code_filter,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def get_raw_data_import_branch_key(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> str:
    """Given a |state_code| and |ingest_instance|, returns the name of the branch that
    will execute the raw data import.
    """
    return f"{state_code.value.lower()}_{ingest_instance.value.lower()}_import_branch"


def get_raw_data_branch_filter(dag_run: DagRun) -> Optional[List[str]]:
    """Returns the branch key associated with the parameters passed to |dag_run|;
    otherwise, returns None if no parameters are provided.
    """
    selected_state_code_str = get_state_code_filter(dag_run)
    selected_state_code = (
        StateCode(selected_state_code_str.upper()) if selected_state_code_str else None
    )
    selected_raw_data_instance_str = get_ingest_instance(dag_run)
    selected_raw_data_instance = (
        DirectIngestInstance(selected_raw_data_instance_str.upper())
        if selected_raw_data_instance_str
        else None
    )

    if selected_state_code and selected_raw_data_instance:
        return [
            get_raw_data_import_branch_key(
                selected_state_code, selected_raw_data_instance
            )
        ]
    if selected_raw_data_instance:
        return [
            get_raw_data_import_branch_key(state_code, selected_raw_data_instance)
            for state_code in get_direct_ingest_states_launched_in_env()
        ]

    if selected_state_code:
        raise ValueError("Cannot build branch filter with only a state code")

    return None


def create_raw_data_branch_map(
    branched_task_function: Callable[
        [StateCode, DirectIngestInstance],
        Union[DAGNode, List[DAGNode]],
    ],
) -> Dict[str, Union[List[DAGNode], DAGNode]]:
    """Creates a branching operator for each state_code and raw_data_instance launched
    in the current environment
    """

    task_group_by_task_id: Dict[str, Union[DAGNode, List[DAGNode]]] = {}

    launched_state_and_ingest_paris = product(
        get_direct_ingest_states_launched_in_env(), DirectIngestInstance
    )

    # sort to maintain DAG insertion order in topological_sort which determines ui visual sorting
    for (state_code, ingest_instance,) in sorted(
        launched_state_and_ingest_paris,
        key=lambda x: x[0].value + x[1].value,
    ):
        task_group_by_task_id[
            get_raw_data_import_branch_key(state_code, ingest_instance)
        ] = branched_task_function(state_code, ingest_instance)

    return task_group_by_task_id
