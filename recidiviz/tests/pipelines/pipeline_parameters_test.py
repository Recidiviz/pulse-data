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
from typing import Any, List, Type

import attr

import recidiviz
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.pipeline_parameters import PipelineParameters
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.utils.yaml_dict import YAMLDict

ALL_PARAMETERS_SUBCLASSES: List[Type[PipelineParameters]] = [
    IngestPipelineParameters,
    MetricsPipelineParameters,
    SupplementalPipelineParameters,
]

C4A_MACHINE_TYPE_AVAILABILITY = {
    "us-central1-a",
    "us-central1-b",
    "us-central1-c",
    "us-west1-a",
    "us-west1-c",
    "us-east1-b",
    "us-east1-c",
    "us-east1-d",
    "us-east4-a",
    "us-east4-b",
    "us-east4-c",
}


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
            state_code = StateCode(pipeline.peek("state_code", str))
            parameters = MetricsPipelineParameters(
                project=self.PROJECT_ID,
                region=DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE[state_code],
                **d,
            )
            self.assertIn(parameters.worker_zone, C4A_MACHINE_TYPE_AVAILABILITY)

    def test_supplemental_pipelines_for_valid_parameters(self) -> None:
        supplemental_pipelines = self.PIPELINE_CONFIG.pop_dicts(
            "supplemental_dataset_pipelines"
        )

        for pipeline in supplemental_pipelines:
            d: dict[str, Any] = pipeline.get()
            state_code = StateCode(pipeline.peek("state_code", str))

            parameters = SupplementalPipelineParameters(
                project=self.PROJECT_ID,
                region=DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE[state_code],
                **d,
            )
            self.assertIn(parameters.worker_zone, C4A_MACHINE_TYPE_AVAILABILITY)

    def test_valid_get_input_dataset_property_names(self) -> None:
        for pipeline_params_subclass in ALL_PARAMETERS_SUBCLASSES:
            # These are values that can be passed as a keyword to the class
            input_arg_fields = set(attr.fields_dict(pipeline_params_subclass))
            for (
                property_name
            ) in pipeline_params_subclass.get_input_dataset_property_names():
                self.assertTrue(
                    hasattr(pipeline_params_subclass, property_name),
                    f"Found invalid property name returned from get_input_dataset_property_names() "
                    f"for class [{pipeline_params_subclass.__name__}]: {property_name}."
                    f"That field does not exist on that class.",
                )
                self.assertNotIn(
                    property_name,
                    input_arg_fields,
                    f"Found invalid property name returned from get_input_dataset_property_names() "
                    f"for class [{pipeline_params_subclass.__name__}]: {property_name}."
                    "Input dataset properties cannot be values passed to the class at "
                    "instantiation time. These are derived from other inputs.",
                )

    def test_valid_output_dataset_property_names(self) -> None:
        for pipeline_params_subclass in ALL_PARAMETERS_SUBCLASSES:
            # These are values that can be passed as a keyword to the class
            input_arg_fields = set(attr.fields_dict(pipeline_params_subclass))
            for (
                property_name
            ) in pipeline_params_subclass.get_output_dataset_property_names():
                self.assertTrue(
                    hasattr(pipeline_params_subclass, property_name),
                    f"Found invalid property name returned from get_output_dataset_property_names() "
                    f"for class [{pipeline_params_subclass.__name__}]: {property_name}."
                    f"That field does not exist on that class.",
                )
                self.assertNotIn(
                    property_name,
                    input_arg_fields,
                    f"Found invalid property name returned from get_output_dataset_property_names() "
                    f"for class [{pipeline_params_subclass.__name__}]: {property_name}."
                    "Output dataset properties cannot be values passed to the class at "
                    "instantiation time. These are derived from other inputs.",
                )

    def test_valid_sandbox_indicator_parameters(self) -> None:
        for pipeline_params_subclass in ALL_PARAMETERS_SUBCLASSES:
            # These are values that can be passed as a keyword to the class
            input_arg_fields = set(attr.fields_dict(pipeline_params_subclass))
            for param in pipeline_params_subclass.all_sandbox_indicator_parameters():
                self.assertTrue(
                    hasattr(pipeline_params_subclass, param),
                    f"Found invalid parameter name returned from all_sandbox_indicator_parameters() "
                    f"for class [{pipeline_params_subclass.__name__}]: {param}."
                    f"That field does not exist on that class.",
                )

                if param not in input_arg_fields:
                    self.fail(
                        f"Found invalid parameter name returned from all_sandbox_indicator_parameters() "
                        f"for class [{pipeline_params_subclass.__name__}]: {param}."
                        f"That field is not a parameter that can be passed to the class at "
                        f"instantiation time.",
                    )
