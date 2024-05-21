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
Logic for state and ingest instance specific branching.
"""
from typing import Callable, Dict, List, Optional, Union

from airflow.models import BaseOperator, DagRun
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.utils.config_utils import (
    get_ingest_instance,
    get_state_code_filter,
)
from recidiviz.airflow.dags.utils.dag_orchestration_utils import (
    get_ingest_pipeline_enabled_state_and_instance_pairs,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a "disable expression-not-assigned" because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


def get_state_code_and_ingest_instance_key(dag_run: DagRun) -> Optional[List[str]]:
    state_code = get_state_code_filter(dag_run)
    instance = get_ingest_instance(dag_run)

    if state_code and instance:
        return [get_ingest_branch_key(state_code, instance)]
    if state_code or instance:
        raise ValueError(
            "expected state code and ingest instance to be set together, but only one was set"
        )
    return None


def get_ingest_branch_key(state_code: str, ingest_instance: str) -> str:
    return f"{state_code.lower()}_{ingest_instance.lower()}_dataflow"


def create_ingest_branch_map(
    branched_task_function: Callable[
        [StateCode, DirectIngestInstance], Union[BaseOperator, TaskGroup]
    ],
) -> Dict[str, Union[BaseOperator, TaskGroup]]:
    """
    Creates a branching operator for the given state and ingest instance.
    """

    pipeline_task_group_by_task_id: Dict[str, Union[BaseOperator, TaskGroup]] = {}

    for (
        state_code,
        ingest_instance,
    ) in get_ingest_pipeline_enabled_state_and_instance_pairs():
        pipeline_task_group_by_task_id[
            get_ingest_branch_key(state_code.value, ingest_instance.value)
        ] = branched_task_function(state_code, ingest_instance)

    return pipeline_task_group_by_task_id
