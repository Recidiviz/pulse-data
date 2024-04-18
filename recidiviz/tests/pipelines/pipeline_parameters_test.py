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
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.normalization.pipeline_parameters import (
    NormalizationPipelineParameters,
)
from recidiviz.pipelines.pipeline_parameters import PipelineParameters
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.utils.yaml_dict import YAMLDict


class TestValidPipelineParameters(unittest.TestCase):
    """
    Unit tests to ensure validity of files in recidiviz/pipelines/calculation_pipeline_templates.yaml.
    Initialization of a PipelineParameters class will throw if there are missing or invalid parameters.
    """

    ROOT = os.path.dirname(recidiviz.__file__)
    YAML_PATH = "pipelines/calculation_pipeline_templates.yaml"
    FULL_PATH = os.path.join(ROOT, YAML_PATH)

    PROJECT_ID = "project"

    def setUp(self) -> None:
        self.PIPELINE_CONFIG = YAMLDict.from_path(self.FULL_PATH)

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

    def test_valid_input_dataset_param_names(self) -> None:
        for pipeline_params_subclass in PipelineParameters.__subclasses__():
            for param_name in pipeline_params_subclass.get_input_dataset_param_names():
                self.assertTrue(
                    hasattr(pipeline_params_subclass, param_name),
                    f"Found invalid param name returned from get_input_dataset_param_names() "
                    f"for class [{pipeline_params_subclass.__class__}]: {param_name}."
                    f"That field does not exist on that class.",
                )

    def test_valid_output_dataset_param_names(self) -> None:
        for pipeline_params_subclass in PipelineParameters.__subclasses__():
            for param_name in pipeline_params_subclass.get_output_dataset_param_names():
                self.assertTrue(
                    hasattr(pipeline_params_subclass, param_name),
                    f"Found invalid param name returned from get_output_dataset_param_names() "
                    f"for class [{pipeline_params_subclass.__class__}]: {param_name}."
                    f"That field does not exist on that class.",
                )
