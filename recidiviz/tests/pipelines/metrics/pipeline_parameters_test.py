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
"""Unit tests for MetricsPipelineParameters"""
import unittest

from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    NORMALIZED_STATE_DATASET,
    REFERENCE_VIEWS_DATASET,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters


class TestMetricsPipelineParameters(unittest.TestCase):
    """Unit tests for MetricsPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            metric_types="TEST_METRIC",
            output="test_output",
            calculation_month_count=36,
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "reference_view_input": REFERENCE_VIEWS_DATASET,
            "normalized_input": NORMALIZED_STATE_DATASET,
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "36",
            "output": "test_output",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test-job")

    def test_creation_no_output(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            metric_types="TEST_METRIC",
            calculation_month_count=36,
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": DATAFLOW_METRICS_DATASET,
            "reference_view_input": REFERENCE_VIEWS_DATASET,
            "normalized_input": NORMALIZED_STATE_DATASET,
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "36",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test-job")

    def test_creation_without_calculation_month_count(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            metric_types="TEST_METRIC",
            output="test_output",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "reference_view_input": REFERENCE_VIEWS_DATASET,
            "normalized_input": NORMALIZED_STATE_DATASET,
            "output": "test_output",
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "-1",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test-job")

    def test_creation_debug_params(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            metric_types="TEST_METRIC",
            output="test_output",
            calculation_month_count=36,
            reference_view_input="test_view",
            normalized_input="normalized_input",
            person_filter_ids="123 12323 324",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "36",
            "output": "test_output",
            "reference_view_input": "test_view",
            "normalized_input": "normalized_input",
            "person_filter_ids": "123 12323 324",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test-job")

    def test_update_with_sandbox_prefix(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            metric_types="TEST_METRIC",
            output="test_output",
            calculation_month_count=36,
            reference_view_input="test_view",
            normalized_input="normalized_input",
            person_filter_ids="123 12323 324",
        ).update_with_sandbox_prefix("my_prefix")

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "reference_view_input": "my_prefix_test_view",
            "normalized_input": "my_prefix_normalized_input",
            "metric_types": "TEST_METRIC",
            "person_filter_ids": "123 12323 324",
            "calculation_month_count": "36",
            "output": "my_prefix_test_output",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)
        self.assertEqual(pipeline_parameters.job_name, "my-prefix-test-job-test")
