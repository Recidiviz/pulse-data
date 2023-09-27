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
"""Handles queueing of ingest dags."""
from typing import Optional

from airflow.decorators import task, task_group
from airflow.models import DagRun
from airflow.operators.empty import EmptyOperator

from recidiviz.airflow.dags.monitoring.task_failure_alerts import (
    KNOWN_CONFIGURATION_PARAMETERS,
)
from recidiviz.airflow.dags.utils.config_utils import (
    get_ingest_instance,
    get_state_code_filter,
    handle_params_check,
)
from recidiviz.airflow.dags.utils.ingest_dag_orchestration_utils import (
    get_all_enabled_state_and_instance_pairs,
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
        if parameter not in KNOWN_CONFIGURATION_PARAMETERS[dag_run.dag_id]
    }

    if unknown_parameters:
        raise ValueError(
            f"Unknown configuration parameters supplied: {unknown_parameters}"
        )

    ingest_instance = get_ingest_instance(dag_run)
    state_code_filter = get_state_code_filter(dag_run)

    if (ingest_instance and not state_code_filter) or (
        state_code_filter and not ingest_instance
    ):
        raise ValueError(
            "[ingest_instance] and [state_code_filter] must both be set or both be unset."
        )

    if (state_code_filter and ingest_instance) and (
        StateCode(state_code_filter),
        DirectIngestInstance(ingest_instance),
    ) not in get_all_enabled_state_and_instance_pairs():
        raise ValueError(
            f"{state_code_filter} in {ingest_instance} must be a enabled for ingest. "
            f"Valid pairs are: {get_all_enabled_state_and_instance_pairs()}"
        )

    return True


@task_group(group_id="initialize_ingest_dag")
def create_initialize_ingest_dag() -> None:
    """
    Creates the initialize ingest dag.
    """
    (
        handle_params_check(verify_parameters())
        # TODO(#23963): Implement waiting for or short-circuiting if the another airflow dag is already running
        >> EmptyOperator(task_id="check_for_running_dags")  # type: ignore
    )
