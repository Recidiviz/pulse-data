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
"""
    Unit tests to test the PipelineParameters classes
"""
import os
import unittest
from typing import Any

import recidiviz
from recidiviz.airflow.dags.utils.pipeline_parameters import (
    MetricsPipelineParameters,
    NormalizationPipelineParameters,
    SupplementalPipelineParameters,
)
from recidiviz.utils.yaml_dict import YAMLDict


class TestPipelineParameters(unittest.TestCase):
    """Unit tests to test that fields are set and fetched correctly for PipelineParameters classes."""

    def test_pipeline_parameters_creation_all_fields(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            metric_types="TEST_METRIC",
            output="test_output",
            calculation_month_count=36,
        )

        template_parameters = pipeline_parameters.template_parameters()

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "36",
            "output": "test_output",
        }

        self.assertEqual(expected_parameters, template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")

    def test_pipeline_parameters_creation_no_output(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            metric_types="TEST_METRIC",
            calculation_month_count=36,
        )
        template_parameters = pipeline_parameters.template_parameters()

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "36",
        }

        self.assertEqual(expected_parameters, template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")

    def test_creation_without_calculation_month_count(self) -> None:
        pipeline_parameters = MetricsPipelineParameters(
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            metric_types="TEST_METRIC",
            output="test_output",
        )
        template_parameters = pipeline_parameters.template_parameters()

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "test_output",
            "metric_types": "TEST_METRIC",
            "calculation_month_count": "-1",
        }

        self.assertEqual(expected_parameters, template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")

    def test_normalization_pipeline_parameters_creation(self) -> None:
        normalization_pp = NormalizationPipelineParameters(
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            output="test_output",
        )

        template_parameters = normalization_pp.template_parameters()

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "test_output",
        }

        self.assertEqual(expected_parameters, template_parameters)

        self.assertEqual(normalization_pp.region, "us-west1")
        self.assertEqual(normalization_pp.job_name, "test_job")

    def test_supplemental_pipeline_parameters_creation(self) -> None:
        supplemental_pp = SupplementalPipelineParameters(
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            output="test_output",
        )

        template_parameters = supplemental_pp.template_parameters()

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "test_output",
        }

        self.assertEqual(expected_parameters, template_parameters)


class TestValidPipelineParameters(unittest.TestCase):
    """
    Unit tests to ensure validity of files in recidiviz/calculator/pipeline/calculation_pipeline_templates.yaml.
    Initialization of a PipelineParameters class will throw if there are missing or invalid parameters.
    """

    ROOT = os.path.dirname(recidiviz.__file__)
    YAML_PATH = "calculator/pipeline/calculation_pipeline_templates.yaml"
    FULL_PATH = os.path.join(ROOT, YAML_PATH)

    PIPELINE_CONFIG = YAMLDict.from_path(FULL_PATH)

    def test_metrics_pipelines_for_valid_parameters(self) -> None:
        metric_pipelines = self.PIPELINE_CONFIG.pop_dicts("metric_pipelines")

        for pipeline in metric_pipelines:
            d: dict[str, Any] = pipeline.get()
            MetricsPipelineParameters(**d)

    def test_normalization_pipelines_for_valid_parameters(self) -> None:
        normalization_pipelines = self.PIPELINE_CONFIG.pop_dicts(
            "normalization_pipelines"
        )

        for pipeline in normalization_pipelines:
            d: dict[str, Any] = pipeline.get()
            NormalizationPipelineParameters(**d)

    def test_supplemental_pipelines_for_valid_parameters(self) -> None:
        supplemental_pipelines = self.PIPELINE_CONFIG.pop_dicts(
            "supplemental_dataset_pipelines"
        )

        for pipeline in supplemental_pipelines:
            d: dict[str, Any] = pipeline.get()
            SupplementalPipelineParameters(**d)
