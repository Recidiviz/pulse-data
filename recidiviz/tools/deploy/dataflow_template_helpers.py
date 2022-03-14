# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Shared helpers for the dataflow deployment tools """
import logging
from typing import List, Optional, TypedDict

import attr
import yaml


class PipelineConfig(TypedDict):
    pipeline: str
    job_name: str
    state_code: str
    region: str
    metric_types: Optional[str]
    staging_only: Optional[bool]


@attr.s(auto_attribs=True)
class PipelineYaml:
    normalization_pipelines: List[PipelineConfig]
    incremental_metric_pipelines: List[PipelineConfig]
    historical_metric_pipelines: List[PipelineConfig]
    supplemental_dataset_pipelines: List[PipelineConfig]

    @property
    def all_pipelines(self) -> List[PipelineConfig]:
        return (
            self.normalization_pipelines
            + self.incremental_metric_pipelines
            + self.historical_metric_pipelines
            + self.supplemental_dataset_pipelines
        )


def load_pipeline_config_yaml(template_yaml_path: str) -> PipelineYaml:
    logging.info("Loading templates at path %s", template_yaml_path)

    with open(template_yaml_path, "r", encoding="utf-8") as yaml_file:
        pipeline_config_yaml = yaml.full_load(yaml_file)

        if pipeline_config_yaml:
            return PipelineYaml(**pipeline_config_yaml)

        raise ValueError(
            f"Could not find any configured templates at {template_yaml_path}"
        )
