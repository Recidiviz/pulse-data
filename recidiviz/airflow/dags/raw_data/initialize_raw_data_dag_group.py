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
"""Logic for handling raw data import DAG initialization"""
from typing import Any, Optional

from airflow.decorators import task, task_group
from airflow.models import DagRun

from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_known_configuration_parameters,
)
from recidiviz.airflow.dags.operators.wait_until_can_continue_or_cancel_sensor_async import (
    WaitUntilCanContinueOrCancelSensorAsync,
)
from recidiviz.airflow.dags.utils.config_utils import (
    get_ingest_instance,
    get_state_code_filter,
    handle_params_check,
    handle_queueing_result,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.airflow.dags.utils.wait_until_can_continue_or_cancel_delegates import (
    StateSpecificNonBlockingDagWaitUntilCanContinueOrCancelDelegate,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


@task
def verify_parameters(dag_run: Optional[DagRun] = None) -> bool:
    """Verifies that the required parameters are set in the dag_run configuration."""
    if not dag_run:
        raise ValueError(
            "Dag run not passed to task. Should be automatically set due to function "
            "being a task."
        )

    unknown_parameters = {
        parameter
        for parameter in dag_run.conf.keys()
        if parameter
        not in get_known_configuration_parameters(
            project_id=get_project_id(), dag_id=dag_run.dag_id
        )
    }

    if unknown_parameters:
        raise ValueError(
            f"Unknown configuration parameters supplied: {unknown_parameters}"
        )

    ingest_instance = get_ingest_instance(dag_run)
    state_code_filter = get_state_code_filter(dag_run)

    # validate that parameters are valid enum members
    if ingest_instance:
        _ = DirectIngestInstance(ingest_instance)

    if state_code_filter:
        _ = StateCode(state_code_filter)

    if (
        ingest_instance == DirectIngestInstance.SECONDARY.value
        and not state_code_filter
    ):
        raise ValueError("Must supply [state_code_filter] with a SECONDARY run")

    return True


@task_group(group_id="initialize_dag")
def initialize_raw_data_dag_group() -> Any:
    wait_to_continue_or_cancel = WaitUntilCanContinueOrCancelSensorAsync(
        delegate=StateSpecificNonBlockingDagWaitUntilCanContinueOrCancelDelegate(),
        task_id="wait_to_continue_or_cancel",
    )
    (
        handle_params_check(verify_parameters())
        >> wait_to_continue_or_cancel
        >> handle_queueing_result(wait_to_continue_or_cancel.output)
    )
