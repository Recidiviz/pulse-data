# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Utils for working with orchestrated Dataflow pipelines."""
from typing import Set

from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.config_paths import PIPELINE_CONFIG_YAML_PATH
from recidiviz.utils.yaml_dict import YAMLDict


def get_metric_pipeline_enabled_states() -> Set[StateCode]:
    """Returns all states that have scheduled metric pipelines that run."""
    pipeline_templates_yaml = YAMLDict.from_path(PIPELINE_CONFIG_YAML_PATH)

    metric_pipelines = pipeline_templates_yaml.pop_dicts("metric_pipelines")
    return {
        StateCode(pipeline.peek("state_code", str)) for pipeline in metric_pipelines
    }
