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
import json
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    NORMALIZED_STATE_DATASET,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.metrics.supervision.pipeline import SupervisionMetricsPipeline
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    get_all_reference_query_input_datasets_for_pipeline,
)


class TestMetricsPipelineParameters(unittest.TestCase):
    """Unit tests for MetricsPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="supervision_metrics",
            region="us-west1",
            metric_types="TEST_METRIC",
            calculation_month_count=36,
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "supervision_metrics",
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "36",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "us-oz-supervision-metrics-36")

        self.assertEqual(NORMALIZED_STATE_DATASET, pipeline_parameters.normalized_input)
        self.assertEqual(DATAFLOW_METRICS_DATASET, pipeline_parameters.output)
        self.assertFalse(pipeline_parameters.is_sandbox_pipeline)

    def test_creation_without_calculation_month_count(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="population_span_metrics",
            region="us-west1",
            metric_types="TEST_METRIC",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "population_span_metrics",
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "-1",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(
            pipeline_parameters.job_name, "full-us-oz-population-span-metrics"
        )

        self.assertEqual(NORMALIZED_STATE_DATASET, pipeline_parameters.normalized_input)
        self.assertEqual(DATAFLOW_METRICS_DATASET, pipeline_parameters.output)
        self.assertFalse(pipeline_parameters.is_sandbox_pipeline)

    def test_creation_debug_params_no_prefix(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "This sandbox pipeline must define an output_sandbox_prefix."
        ):
            _ = MetricsPipelineParameters(
                project="recidiviz-456",
                state_code="US_OZ",
                pipeline="test_pipeline_name",
                region="us-west1",
                metric_types="TEST_METRIC",
                calculation_month_count=36,
                # Raises because this debug value is set
                person_filter_ids="123 12323 324",
            )

    def test_creation_with_sandbox_prefix(self) -> None:
        input_dataset_overrides_json = json.dumps(
            {NORMALIZED_STATE_DATASET: "some_completely_different_dataset"}
        )
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="incarceration_metrics",
            region="us-west1",
            metric_types="TEST_METRIC",
            calculation_month_count=36,
            person_filter_ids="123 12323 324",
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            input_dataset_overrides_json=input_dataset_overrides_json,
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "incarceration_metrics",
            "metric_types": "TEST_METRIC",
            "person_filter_ids": "123 12323 324",
            "calculation_month_count": "36",
            "output_sandbox_prefix": "my_prefix",
            "sandbox_username": "annag",
            "input_dataset_overrides_json": input_dataset_overrides_json,
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)
        self.assertEqual(
            pipeline_parameters.job_name,
            "my-prefix-us-oz-incarceration-metrics-36-test",
        )

        self.assertEqual(
            "some_completely_different_dataset", pipeline_parameters.normalized_input
        )

        self.assertEqual("my_prefix_dataflow_metrics", pipeline_parameters.output)
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
    )
    def test_check_for_valid_input_dataset_overrides(self) -> None:
        input_dataset_overrides_json = json.dumps(
            # The normalization pipelines read from normalized_state, not
            # us_xx_normalized_state
            {"us_xx_normalized_state": "some_completely_different_dataset"}
        )
        pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline="test_pipeline_name",
            region="us-west1",
            metric_types="TEST_METRIC",
            calculation_month_count=36,
            person_filter_ids="123 12323 324",
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            input_dataset_overrides_json=input_dataset_overrides_json,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found original dataset \[us_xx_normalized_state\] in overrides which is "
            r"not a dataset this pipeline reads from. Datasets you can override: "
            r"\['normalized_state', 'state', 'us_mo_raw_data_up_to_date_views'\].",
        ):
            pipeline_parameters.check_for_valid_input_dataset_overrides(
                get_all_reference_query_input_datasets_for_pipeline(
                    SupervisionMetricsPipeline
                )
            )
