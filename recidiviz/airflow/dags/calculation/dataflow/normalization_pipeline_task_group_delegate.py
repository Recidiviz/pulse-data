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
"""Implementation of DataflowPipelineTaskGroupDelegate for normalization Dataflow
pipeline task groups.
"""
from typing import Any, Dict

from airflow.models import DagRun

from recidiviz.airflow.dags.utils.dataflow_pipeline_group import (
    DataflowPipelineTaskGroupDelegate,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.pipelines.normalization.pipeline_parameters import (
    NormalizationPipelineParameters,
)


class NormalizationDataflowPipelineTaskGroupDelegate(
    DataflowPipelineTaskGroupDelegate[NormalizationPipelineParameters]
):
    def __init__(self, state_code: StateCode) -> None:
        self._state_code = state_code

    def get_default_parameters(self) -> NormalizationPipelineParameters:
        return NormalizationPipelineParameters(
            project=get_project_id(),
            pipeline="comprehensive_normalization",
            state_code=self._state_code.value,
            region=DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE[self._state_code],
        )

    def get_pipeline_specific_dynamic_args(self, dag_run: DagRun) -> Dict[str, Any]:
        return {}