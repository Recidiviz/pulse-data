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
"""Tests for the calculation_pipeline_templates.yaml file."""
import unittest
from collections import defaultdict
from typing import Dict, List
from unittest.mock import Mock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.pipelines.config_paths import PIPELINE_CONFIG_YAML_PATH
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.yaml_dict import YAMLDict


class TestConfiguredPipelines(unittest.TestCase):
    """Tests the configuration of pipelines."""

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_production_pipeline_parity(self) -> None:
        states_launched_in_production = get_direct_ingest_states_launched_in_env()

        pipeline_templates_yaml = YAMLDict.from_path(PIPELINE_CONFIG_YAML_PATH)

        metric_pipelines = pipeline_templates_yaml.pop_dicts("metric_pipelines")
        production_pipelines_by_state: Dict[StateCode, List[str]] = defaultdict(list)
        for pipeline in metric_pipelines:
            state_code = StateCode(pipeline.peek("state_code", str))

            pipeline_params = MetricsPipelineParameters(
                project=GCP_PROJECT_PRODUCTION,
                region=DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE[state_code],
                **pipeline.get(),  # type: ignore
            )
            state_code = StateCode(pipeline_params.state_code)
            if (
                state_code not in states_launched_in_production
                and not pipeline_params.staging_only
            ):
                production_pipelines_by_state[state_code].append(
                    pipeline_params.job_name
                )

        self.assertDictEqual(
            production_pipelines_by_state,
            {},
            "A pipeline is configured that will run in production for a state that "
            "does not have `environment: production` set in the `manifest.yaml`. "
            "Either set it to production or set `staging_only: True` on the pipeline.",
        )
