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

from recidiviz.airflow.dags.utils.ingest_dag_orchestration_utils import (
    get_ingest_pipeline_enabled_state_and_instance_pairs,
)
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE,
)


class TestIngestPipelineParameters(unittest.TestCase):
    """Unit tests for IngestPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            output="test_output",
            ingest_view_results_output="test_ingest_view_output",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000","TEST_RAW_DATA_2":"2020-01-01T00:00:00.00000"}',
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "test_output",
            "raw_data_table_input": "us_oz_raw_data",
            "reference_view_input": "reference_views",
            "ingest_view_results_output": "test_ingest_view_output",
            "ingest_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000","TEST_RAW_DATA_2":"2020-01-01T00:00:00.00000"}',
            "ingest_view_results_only": "False",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test-job")
        self.assertEqual(
            pipeline_parameters.service_account_email,
            "direct-ingest-state-us-oz-df@recidiviz-456.iam.gserviceaccount.com",
        )

    def test_creation_all_fields_no_output(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "us_oz_state_primary",
            "raw_data_table_input": "us_oz_raw_data",
            "reference_view_input": "reference_views",
            "ingest_view_results_output": "us_oz_dataflow_ingest_view_results_primary",
            "ingest_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "False",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test-job")

    def test_creation_all_fields_no_output_secondary(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            ingest_instance="SECONDARY",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "us_oz_state_secondary",
            "reference_view_input": "reference_views",
            "raw_data_table_input": "us_oz_raw_data_secondary",
            "ingest_view_results_output": "us_oz_dataflow_ingest_view_results_secondary",
            "ingest_instance": "SECONDARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "False",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "test-job")

    def test_creation_output_mismatch(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Invalid pipeline parameters for output datasets. *"
        ) as _:
            _ = IngestPipelineParameters(
                project="recidiviz-456",
                state_code="US_OZ",
                pipeline="test_pipeline_name",
                region="us-west1",
                job_name="test-job",
                output="test_output",
                raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            )

    def test_creation_valid_service_account_email(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            service_account_email="some-test-account@recidiviz-staging.iam.gserviceaccount.com",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )
        self.assertEqual(
            pipeline_parameters.service_account_email,
            "some-test-account@recidiviz-staging.iam.gserviceaccount.com",
        )

    def test_creation_valid_service_account_email_default_compute(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            service_account_email="12345-compute@developer.gserviceaccount.com",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )
        self.assertEqual(
            pipeline_parameters.service_account_email,
            "12345-compute@developer.gserviceaccount.com",
        )

    def test_creation_invalid_service_account_email(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"service_account_email must be a valid service account email address.*",
        ):
            _ = IngestPipelineParameters(
                project="recidiviz-456",
                state_code="US_OZ",
                pipeline="test_pipeline_name",
                region="us-west1",
                job_name="test-job",
                service_account_email="some-test-account@somerandomwebsite.com",
                raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            )

    def test_update_with_sandbox_prefix(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            output="test_output",
            ingest_view_results_output="test_ingest_view_output",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        ).update_with_sandbox_prefix("my_prefix")

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "my_prefix_test_output",
            "raw_data_table_input": "my_prefix_us_oz_raw_data",
            "reference_view_input": "my_prefix_reference_views",
            "ingest_view_results_output": "my_prefix_test_ingest_view_output",
            "ingest_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "False",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)
        self.assertEqual(pipeline_parameters.job_name, "my-prefix-test-job-test")

    def test_update_with_sandbox_prefix_secondary(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            output="test_output",
            ingest_view_results_output="test_ingest_view_output",
            ingest_instance="SECONDARY",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        ).update_with_sandbox_prefix("my_prefix")

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "my_prefix_test_output",
            "raw_data_table_input": "my_prefix_us_oz_raw_data_secondary",
            "reference_view_input": "my_prefix_reference_views",
            "ingest_view_results_output": "my_prefix_test_ingest_view_output",
            "ingest_instance": "SECONDARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "False",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)
        self.assertEqual(pipeline_parameters.job_name, "my-prefix-test-job-test")

    def test_ingest_view_results_only(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            output="test_output",
            ingest_view_results_output="test_ingest_view_output",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            ingest_view_results_only="True",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "test_output",
            "raw_data_table_input": "us_oz_raw_data",
            "reference_view_input": "reference_views",
            "ingest_view_results_output": "test_ingest_view_output",
            "ingest_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "True",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

    def test_ingest_views_to_run(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            job_name="test-job",
            output="test_output",
            ingest_view_results_output="test_ingest_view_output",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            ingest_views_to_run="view1 view2",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output": "test_output",
            "raw_data_table_input": "us_oz_raw_data",
            "reference_view_input": "reference_views",
            "ingest_view_results_output": "test_ingest_view_output",
            "ingest_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "False",
            "ingest_views_to_run": "view1 view2",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

    def test_ingest_views_to_run_non_sandbox(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Invalid pipeline parameters for ingest_views_to_run. *"
        ):
            _ = IngestPipelineParameters(
                project="recidiviz-456",
                state_code="US_OZ",
                pipeline="test_pipeline_name",
                region="us-west1",
                job_name="test-job",
                raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
                ingest_views_to_run="view1 view2",
            )

    def test_default_ingest_pipeline_regions_by_state_code_filled_out(self) -> None:
        pipeline_enabled_states = {
            state_code
            for state_code, _instance in get_ingest_pipeline_enabled_state_and_instance_pairs()
        }

        states_with_regions = set(DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE.keys())
        states_missing_regions = pipeline_enabled_states - states_with_regions
        if states_missing_regions:
            self.fail(
                f"Missing a region in DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE "
                f"for these ingest pipeline enabled states: {states_missing_regions}."
            )
