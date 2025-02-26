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
import unittest

from recidiviz.calculator.pipeline.normalization.pipeline_parameters import (
    NormalizationPipelineParameters,
)
from recidiviz.calculator.query.state.dataset_config import (
    REFERENCE_VIEWS_DATASET,
    STATE_BASE_DATASET,
    normalized_state_dataset_for_state_code,
)
from recidiviz.common.constants.states import StateCode


class TestNormalizationPipelineParameters(unittest.TestCase):
    """Unit tests for NormalizationPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = NormalizationPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            output="test_output",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "test_output",
            "data_input": STATE_BASE_DATASET,
            "reference_view_input": REFERENCE_VIEWS_DATASET,
            "normalized_input": normalized_state_dataset_for_state_code(
                StateCode("US_OZ")
            ),
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")

    def test_creation_no_output(self) -> None:
        pipeline_parameters = NormalizationPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": normalized_state_dataset_for_state_code(StateCode("US_OZ")),
            "data_input": STATE_BASE_DATASET,
            "reference_view_input": REFERENCE_VIEWS_DATASET,
            "normalized_input": normalized_state_dataset_for_state_code(
                StateCode("US_OZ")
            ),
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")
