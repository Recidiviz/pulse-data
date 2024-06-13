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
"""Implementation of DataflowPipelineTaskGroupDelegate for ingest Dataflow pipeline
task groups.
"""
import json
from typing import Any, Dict, List

from airflow.models import BaseOperator, DagRun

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.utils.config_utils import get_ingest_instance
from recidiviz.airflow.dags.utils.dataflow_pipeline_group import (
    DataflowPipelineTaskGroupDelegate,
    UpstreamTaskOutputs,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.pipelines.pipeline_names import INGEST_PIPELINE_NAME
from recidiviz.utils.types import assert_type


class IngestDataflowPipelineTaskGroupDelegate(
    DataflowPipelineTaskGroupDelegate[IngestPipelineParameters]
):
    """Implementation of DataflowPipelineTaskGroupDelegate for ingest Dataflow pipeline
    task groups.
    """

    def __init__(
        self,
        state_code: StateCode,
        default_ingest_instance: DirectIngestInstance,
        max_update_datetimes_operator: CloudSqlQueryOperator,
    ) -> None:
        self._state_code = state_code
        self._default_ingest_instance = default_ingest_instance
        self._max_update_datetimes_operator = max_update_datetimes_operator

    def get_default_parameters(self) -> IngestPipelineParameters:
        return IngestPipelineParameters(
            project=get_project_id(),
            # This will get overwritten with a dynamic value at runtime
            raw_data_upper_bound_dates_json=json.dumps({}),
            pipeline=INGEST_PIPELINE_NAME,
            state_code=self._state_code.value,
            region=DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE[self._state_code],
            ingest_instance=self._default_ingest_instance.value,
        )

    def get_input_operators(self) -> List[BaseOperator]:
        return [self._max_update_datetimes_operator]

    def get_pipeline_specific_dynamic_args(
        self, dag_run: DagRun, upstream_task_outputs: UpstreamTaskOutputs
    ) -> Dict[str, Any]:
        ingest_instance = get_ingest_instance(dag_run)
        if not ingest_instance:
            ingest_instance = self._default_ingest_instance.value

        max_update_datetimes = assert_type(
            upstream_task_outputs.get_output_for_operator(
                self._max_update_datetimes_operator
            ),
            dict,
        )

        return {
            "raw_data_upper_bound_dates_json": json.dumps(max_update_datetimes),
            "ingest_instance": ingest_instance,
        }
