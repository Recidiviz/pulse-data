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
"""Unit tests for SupplementalPipelineParameters"""
import unittest

from recidiviz.calculator.query.state.dataset_config import REFERENCE_VIEWS_DATASET
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)


class TestSupplementalPipelineParameters(unittest.TestCase):
    """Unit tests for SupplementalPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = SupplementalPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            output="test_output",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "test_output",
            "reference_view_input": REFERENCE_VIEWS_DATASET,
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test-job")

    def test_creation_no_output(self) -> None:
        pipeline_parameters = SupplementalPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": SUPPLEMENTAL_DATA_DATASET,
            "reference_view_input": REFERENCE_VIEWS_DATASET,
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test-job")

    def test_update_with_sandbox_prefix(self) -> None:
        pipeline_parameters = SupplementalPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            output="test_output",
            reference_view_input="test_view",
        ).update_with_sandbox_prefix("my_prefix")

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "reference_view_input": "my_prefix_test_view",
            "output": "my_prefix_test_output",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)
        self.assertEqual(pipeline_parameters.job_name, "my-prefix-test-job-test")
