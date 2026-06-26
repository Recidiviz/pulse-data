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
"""DAG that runs the identity ingest Dataflow pipeline, branched per tenant.

By default all tenant branches run. To target a single tenant, pass
`tenant_filter` in the DAG run conf.
"""
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group import (
    create_single_ingest_pipeline_group,
)
from recidiviz.airflow.dags.identity_ingest.identity_ingest_dataflow_pipeline_task_group_delegate import (
    IdentityIngestDataflowPipelineTaskGroupDelegate,
)
from recidiviz.airflow.dags.monitoring.dag_registry import get_identity_ingest_dag_id
from recidiviz.airflow.dags.utils.branching_by_key import (
    create_branching_by_key,
    select_tenant_parameter_branch,
)
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType


@dag(
    dag_id=get_identity_ingest_dag_id(get_project_id()),
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def create_identity_ingest_dag() -> None:
    """Identity ingest Dataflow pipeline branched per tenant."""
    with TaskGroup(group_id="identity_ingest_pipelines"):
        branches_by_tenant = {}
        for state_code in get_direct_ingest_states_launched_in_env():
            tenant = Tenant.from_state_code(state_code).value
            with TaskGroup(tenant) as tenant_group:
                create_single_ingest_pipeline_group(
                    state_code=state_code,
                    pipeline_type=IngestPipelineType.IDENTITY,
                    delegate_class=IdentityIngestDataflowPipelineTaskGroupDelegate,
                )
            branches_by_tenant[tenant] = tenant_group
        create_branching_by_key(
            branches_by_tenant,
            select_tenant_parameter_branch,
        )


identity_ingest_dag = create_identity_ingest_dag()
