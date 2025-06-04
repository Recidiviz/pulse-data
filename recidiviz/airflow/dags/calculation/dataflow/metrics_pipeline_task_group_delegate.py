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
"""Implementation of DataflowPipelineTaskGroupDelegate for metrics Dataflow pipeline
task groups.
"""
from typing import Any, Dict, List

from airflow.models import BaseOperator, DagRun

from recidiviz.airflow.dags.utils.dataflow_pipeline_group import (
    DataflowPipelineTaskGroupDelegate,
    UpstreamTaskOutputs,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.utils.yaml_dict import YAMLDict


class MetricsDataflowPipelineTaskGroupDelegate(
    DataflowPipelineTaskGroupDelegate[MetricsPipelineParameters]
):
    """Implementation of DataflowPipelineTaskGroupDelegate for metrics Dataflow pipeline
    task groups.
    """

    def __init__(self, state_code: StateCode, pipeline_config: YAMLDict) -> None:
        self._state_code = state_code
        self._pipeline_config = pipeline_config

    def get_default_parameters(self) -> MetricsPipelineParameters:
        return MetricsPipelineParameters(
            project=get_project_id(),
            region=DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE[self._state_code],
            **self._pipeline_config.get(),  # type: ignore
        )

    def get_input_operators(self) -> List[BaseOperator]:
        return []

    def get_pipeline_specific_dynamic_args(
        self, dag_run: DagRun, upstream_task_outputs: UpstreamTaskOutputs
    ) -> Dict[str, Any]:
        return {}
