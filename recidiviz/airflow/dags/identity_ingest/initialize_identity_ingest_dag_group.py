# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Logic for handling identity ingest DAG initialization."""

from typing import Any

from airflow.decorators import task, task_group
from airflow.models import DagRun

from recidiviz.airflow.dags.monitoring.dag_registry import (
    INITIALIZE_DAG_GROUP_ID,
    get_known_configuration_parameters,
)
from recidiviz.airflow.dags.operators.wait_until_can_continue_or_cancel_sensor_async import (
    WaitUntilCanContinueOrCancelSensorAsync,
)
from recidiviz.airflow.dags.utils.config_utils import (
    get_tenant_filter,
    handle_params_check,
    handle_queueing_result,
)
from recidiviz.airflow.dags.utils.dag_run_metadata import record_dag_run_metadata
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.airflow.dags.utils.wait_until_can_continue_or_cancel_delegates import (
    NoConcurrentDagsWaitUntilCanContinueOrCancelDelegate,
)
from recidiviz.common.constants.tenants import Tenant

# pylint: disable=W0104 pointless-statement
# pylint: disable=W0106 expression-not-assigned


@task
def verify_parameters(dag_run: DagRun | None = None) -> bool:
    """Verifies that only known parameters with valid values are set in the
    dag_run configuration. This matters more here than in most DAGs: a
    misspelled tenant_filter key would otherwise be silently ignored, and a run
    with no tenant_filter executes every tenant's pipeline."""
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

    tenant_filter = get_tenant_filter(dag_run)
    if tenant_filter:
        # Raises if the filter value is not a valid Tenant; an invalid value
        # would otherwise match no branch and the run would silently do nothing.
        _ = Tenant(tenant_filter)

    return True


@task_group(group_id=INITIALIZE_DAG_GROUP_ID)
def initialize_identity_ingest_dag_group() -> Any:
    # Same one-run-at-a-time policy as the calculation DAG: a new run waits for
    # the active run to finish, and any runs stacked between them are canceled.
    # Concurrent runs for the same tenant would race on the tenant's cluster
    # datasets and import, and runs are rare enough that serializing across
    # tenants costs little.
    wait_to_continue_or_cancel = WaitUntilCanContinueOrCancelSensorAsync(
        delegate=NoConcurrentDagsWaitUntilCanContinueOrCancelDelegate(),
        task_id="wait_to_continue_or_cancel",
    )
    (
        handle_params_check(verify_parameters())
        >> record_dag_run_metadata()
        >> wait_to_continue_or_cancel
        >> handle_queueing_result(wait_to_continue_or_cancel.output)
    )
