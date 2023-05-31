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
    REFERENCE_VIEWS_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
    normalized_state_dataset_for_state_code,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters


class TestMetricsPipelineParameters(unittest.TestCase):
    """Unit tests for MetricsPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            metric_types="TEST_METRIC",
            output="test_output",
            calculation_month_count=36,
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "state_data_input": STATE_BASE_DATASET,
            "reference_view_input": REFERENCE_VIEWS_DATASET,
            "normalized_input": normalized_state_dataset_for_state_code(
                StateCode("US_OZ")
            ),
            "static_reference_input": STATIC_REFERENCE_TABLES_DATASET,
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "36",
            "output": "test_output",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")

    def test_creation_no_output(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            metric_types="TEST_METRIC",
            calculation_month_count=36,
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": DATAFLOW_METRICS_DATASET,
            "state_data_input": STATE_BASE_DATASET,
            "reference_view_input": REFERENCE_VIEWS_DATASET,
            "normalized_input": normalized_state_dataset_for_state_code(
                StateCode("US_OZ")
            ),
            "static_reference_input": STATIC_REFERENCE_TABLES_DATASET,
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "36",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")

    def test_creation_without_calculation_month_count(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            metric_types="TEST_METRIC",
            output="test_output",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "state_data_input": STATE_BASE_DATASET,
            "reference_view_input": REFERENCE_VIEWS_DATASET,
            "normalized_input": normalized_state_dataset_for_state_code(
                StateCode("US_OZ")
            ),
            "static_reference_input": STATIC_REFERENCE_TABLES_DATASET,
            "output": "test_output",
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "-1",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")

    def test_creation_debug_params(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            metric_types="TEST_METRIC",
            output="test_output",
            calculation_month_count=36,
            reference_view_input="test_view",
            state_data_input="test_input",
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
            "state_data_input": "test_input",
            "normalized_input": "normalized_input",
            "static_reference_input": STATIC_REFERENCE_TABLES_DATASET,
            "person_filter_ids": "123 12323 324",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")
