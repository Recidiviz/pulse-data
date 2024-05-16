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
"""Implementation of DataflowPipelineTaskGroupDelegate for supplemental Dataflow
pipeline task groups.
"""
from typing import Any, Dict

from airflow.models import DagRun

from recidiviz.airflow.dags.utils.dataflow_pipeline_group import (
    DataflowPipelineTaskGroupDelegate,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.utils.yaml_dict import YAMLDict


class SupplementalDataflowPipelineTaskGroupDelegate(
    DataflowPipelineTaskGroupDelegate[SupplementalPipelineParameters]
):
    """Implementation of DataflowPipelineTaskGroupDelegate for supplemental Dataflow
    pipeline task groups.
    """

    def __init__(self, pipeline_config: YAMLDict) -> None:
        self._pipeline_config = pipeline_config

    def get_default_parameters(self) -> SupplementalPipelineParameters:
        return SupplementalPipelineParameters(
            project=get_project_id(),
            **self._pipeline_config.get(),  # type: ignore
        )

    def get_pipeline_specific_dynamic_args(self, dag_run: DagRun) -> Dict[str, Any]:
        return {}
