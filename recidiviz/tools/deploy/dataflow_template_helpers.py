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
from typing import List, TypedDict

import attr
import yaml

from recidiviz.calculator.dataflow_config import (
    PRODUCTION_TEMPLATES_PATH,
    STAGING_ONLY_TEMPLATES_PATH,
)

PIPELINE_CONFIG_YAML_PATHS = {
    "production": PRODUCTION_TEMPLATES_PATH,
    "staging": STAGING_ONLY_TEMPLATES_PATH,
}


class PipelineConfig(TypedDict):
    pipeline: str
    job_name: str
    state_code: str
    metric_types: str
    region: str


@attr.s(auto_attribs=True)
class PipelineYaml:
    daily_pipelines: List[PipelineConfig]
    historical_pipelines: List[PipelineConfig]

    @property
    def all_pipelines(self) -> List[PipelineConfig]:
        return self.daily_pipelines + self.historical_pipelines


def load_pipeline_config_yaml(template_yaml_path: str) -> PipelineYaml:
    logging.info("Loading templates at path %s", template_yaml_path)

    with open(template_yaml_path, "r") as yaml_file:
        pipeline_config_yaml = yaml.full_load(yaml_file)

        if pipeline_config_yaml:
            return PipelineYaml(**pipeline_config_yaml)

        raise ValueError(
            f"Could not find any configured templates at {template_yaml_path}"
        )
