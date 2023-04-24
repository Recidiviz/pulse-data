# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Unit tests to test the PipelineParameters classes"""
import os
import unittest
from typing import Any

import recidiviz
from recidiviz.calculator.pipeline.metrics.pipeline_parameters import (
    MetricsPipelineParameters,
)
from recidiviz.calculator.pipeline.normalization.pipeline_parameters import (
    NormalizationPipelineParameters,
)
from recidiviz.calculator.pipeline.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.utils.yaml_dict import YAMLDict


class TestValidPipelineParameters(unittest.TestCase):
    """
    Unit tests to ensure validity of files in recidiviz/calculator/pipeline/calculation_pipeline_templates.yaml.
    Initialization of a PipelineParameters class will throw if there are missing or invalid parameters.
    """

    ROOT = os.path.dirname(recidiviz.__file__)
    YAML_PATH = "calculator/pipeline/calculation_pipeline_templates.yaml"
    FULL_PATH = os.path.join(ROOT, YAML_PATH)

    PIPELINE_CONFIG = YAMLDict.from_path(FULL_PATH)

    PROJECT_ID = "project"

    def test_metrics_pipelines_for_valid_parameters(self) -> None:
        metric_pipelines = self.PIPELINE_CONFIG.pop_dicts("metric_pipelines")

        for pipeline in metric_pipelines:
            d: dict[str, Any] = pipeline.get()
            MetricsPipelineParameters(project=self.PROJECT_ID, **d)

    def test_normalization_pipelines_for_valid_parameters(self) -> None:
        normalization_pipelines = self.PIPELINE_CONFIG.pop_dicts(
            "normalization_pipelines"
        )

        for pipeline in normalization_pipelines:
            d: dict[str, Any] = pipeline.get()
            NormalizationPipelineParameters(project=self.PROJECT_ID, **d)

    def test_supplemental_pipelines_for_valid_parameters(self) -> None:
        supplemental_pipelines = self.PIPELINE_CONFIG.pop_dicts(
            "supplemental_dataset_pipelines"
        )

        for pipeline in supplemental_pipelines:
            d: dict[str, Any] = pipeline.get()
            SupplementalPipelineParameters(project=self.PROJECT_ID, **d)
