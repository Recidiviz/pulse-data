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
"""Unit tests for NormalizationPipelineParameters"""
import json
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.normalization.comprehensive.pipeline import (
    ComprehensiveNormalizationPipeline,
)
from recidiviz.pipelines.normalization.pipeline_parameters import (
    NormalizationPipelineParameters,
)
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    get_all_reference_query_input_datasets_for_pipeline,
)


class TestNormalizationPipelineParameters(unittest.TestCase):
    """Unit tests for NormalizationPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = NormalizationPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "us-oz-normalization")

        self.assertEqual("us_oz_state", pipeline_parameters.state_data_input)
        self.assertEqual("us_oz_normalized_state", pipeline_parameters.output)
        self.assertFalse(pipeline_parameters.is_sandbox_pipeline)

    def test_parameters_with_sandbox_prefix(self) -> None:
        input_dataset_overrides_json = json.dumps(
            {"us_oz_state": "some_completely_different_dataset"}
        )
        pipeline_parameters = NormalizationPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            person_filter_ids="123 12323 324",
            input_dataset_overrides_json=input_dataset_overrides_json,
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "person_filter_ids": "123 12323 324",
            "output_sandbox_prefix": "my_prefix",
            "sandbox_username": "annag",
            "input_dataset_overrides_json": input_dataset_overrides_json,
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)
        self.assertEqual(
            pipeline_parameters.job_name, "my-prefix-us-oz-normalization-test"
        )

        self.assertEqual(
            "some_completely_different_dataset", pipeline_parameters.state_data_input
        )
        # Output dataset has prefix added because there are input prefixes
        self.assertEqual("my_prefix_us_oz_normalized_state", pipeline_parameters.output)
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
    )
    def test_check_for_valid_input_dataset_overrides(self) -> None:
        input_dataset_overrides_json = json.dumps(
            # The normalization pipelines read from us_xx_state, not state
            {"state": "some_completely_different_dataset"}
        )
        pipeline_parameters = NormalizationPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline="test_pipeline_name",
            region="us-west1",
            person_filter_ids="123 12323 324",
            input_dataset_overrides_json=input_dataset_overrides_json,
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found original dataset \[state\] in overrides which is not "
            r"a dataset this pipeline reads from. Datasets you can override: "
            r"\['us_xx_state'\].",
        ):
            pipeline_parameters.check_for_valid_input_dataset_overrides(
                get_all_reference_query_input_datasets_for_pipeline(
                    ComprehensiveNormalizationPipeline,
                    StateCode(pipeline_parameters.state_code),
                )
            )

        input_dataset_overrides_json = json.dumps(
            # This is a valid override
            {"us_xx_state": "some_completely_different_dataset"}
        )
        pipeline_parameters = NormalizationPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline="test_pipeline_name",
            region="us-west1",
            person_filter_ids="123 12323 324",
            input_dataset_overrides_json=input_dataset_overrides_json,
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
        )
        pipeline_parameters.check_for_valid_input_dataset_overrides(
            get_all_reference_query_input_datasets_for_pipeline(
                ComprehensiveNormalizationPipeline,
                StateCode(pipeline_parameters.state_code),
            )
        )
