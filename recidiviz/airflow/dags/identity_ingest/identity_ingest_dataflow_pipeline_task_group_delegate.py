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
"""Implementation of DataflowPipelineTaskGroupDelegate for the identity ingest
Dataflow pipeline.
"""
import json
from typing import Any

from airflow.models import BaseOperator, DagRun

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.utils.dataflow_pipeline_group import (
    DataflowPipelineTaskGroupDelegate,
    UpstreamTaskOutputs,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.activity.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.pipelines.ingest.identity.pipeline_parameters import (
    IdentityIngestPipelineParameters,
)
from recidiviz.pipelines.pipeline_names import IDENTITY_INGEST_PIPELINE_NAME
from recidiviz.utils.types import assert_type


class IdentityIngestDataflowPipelineTaskGroupDelegate(
    DataflowPipelineTaskGroupDelegate[IdentityIngestPipelineParameters]
):
    """Implementation of DataflowPipelineTaskGroupDelegate for the identity ingest
    Dataflow pipeline.
    """

    def __init__(
        self,
        state_code: StateCode,
        max_update_datetimes_operator: CloudSqlQueryOperator,
    ) -> None:
        self._state_code = state_code
        self._max_update_datetimes_operator = max_update_datetimes_operator

    def get_default_parameters(self) -> IdentityIngestPipelineParameters:
        return IdentityIngestPipelineParameters(
            project=get_project_id(),
            # Overwritten with a dynamic value at runtime; see
            # get_pipeline_specific_dynamic_args.
            raw_data_upper_bound_dates_json=json.dumps({}),
            pipeline=IDENTITY_INGEST_PIPELINE_NAME,
            tenant=Tenant.from_state_code(self._state_code).value,
            raw_data_source_instance=DirectIngestInstance.PRIMARY.value,
            # TODO(OBT-33491): The activity ingest delegate overrides machine_type
            # to "c4a-standard-32" because the highcpu default ran out of memory.
            # It is unclear if the identity ingest delegate should do the same.
            # Revisit later to decide.
            region=DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE[self._state_code],
        )

    def get_input_operators(self) -> list[BaseOperator]:
        return [self._max_update_datetimes_operator]

    def get_pipeline_specific_dynamic_args(
        self, dag_run: DagRun, upstream_task_outputs: UpstreamTaskOutputs
    ) -> dict[str, Any]:
        max_update_datetimes = assert_type(
            upstream_task_outputs.get_output_for_operator(
                self._max_update_datetimes_operator
            ),
            dict,
        )

        return {
            "raw_data_upper_bound_dates_json": json.dumps(max_update_datetimes),
        }
