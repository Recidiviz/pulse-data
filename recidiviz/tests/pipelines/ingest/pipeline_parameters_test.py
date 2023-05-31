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
"""Unit tests for IngestPipelineParameters"""
import unittest

from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters


class TestIngestPipelineParameters(unittest.TestCase):
    """Unit tests for IngestPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            output="test_output",
            ingest_view_results_output="test_ingest_view_output",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "test_output",
            "raw_data_table_input": "us_oz_raw_data",
            "reference_view_input": "reference_views",
            "ingest_view_results_output": "test_ingest_view_output",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")

    def test_creation_all_fields_no_output(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "us_oz_state_primary",
            "raw_data_table_input": "us_oz_raw_data",
            "reference_view_input": "reference_views",
            "ingest_view_results_output": "us_oz_dataflow_ingest_view_results_primary",
            "ingest_instance": "PRIMARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")

    def test_creation_all_fields_no_output_secondary(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test_job",
            ingest_instance="SECONDARY",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "us_oz_state_secondary",
            "reference_view_input": "reference_views",
            "raw_data_table_input": "us_oz_raw_data_secondary",
            "ingest_view_results_output": "us_oz_dataflow_ingest_view_results_secondary",
            "ingest_instance": "SECONDARY",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test_job")

    def test_creation_output_mismatch(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Invalid pipeline parameters for output datasets. *"
        ) as _:
            _ = IngestPipelineParameters(
                project="recidiviz-456",
                state_code="US_OZ",
                pipeline="test_pipeline_name",
                region="us-west1",
                job_name="test_job",
                output="test_output",
            )
