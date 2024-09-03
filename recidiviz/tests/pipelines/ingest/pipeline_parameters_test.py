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
import json
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.pipelines.ingest.state.pipeline import StateIngestPipeline
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    get_all_reference_query_input_datasets_for_pipeline,
)


class TestIngestPipelineParameters(unittest.TestCase):
    """Unit tests for IngestPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000","TEST_RAW_DATA_2":"2020-01-01T00:00:00.00000"}',
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "raw_data_source_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000","TEST_RAW_DATA_2":"2020-01-01T00:00:00.00000"}',
            "ingest_view_results_only": "False",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "us-oz-ingest")
        self.assertEqual(
            pipeline_parameters.service_account_email,
            "direct-ingest-state-us-oz-df@recidiviz-456.iam.gserviceaccount.com",
        )

        self.assertEqual("us_oz_raw_data", pipeline_parameters.raw_data_table_input)
        self.assertEqual(
            "us_oz_ingest_view_results",
            pipeline_parameters.ingest_view_results_output,
        )
        self.assertEqual("us_oz_state", pipeline_parameters.pre_normalization_output)
        self.assertEqual(
            "us_oz_normalized_state_new", pipeline_parameters.normalized_output
        )
        self.assertFalse(pipeline_parameters.is_sandbox_pipeline)
        self.assertTrue(pipeline_parameters.run_normalization)

    def test_creation_all_fields_secondary(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            raw_data_source_instance=DirectIngestInstance.SECONDARY.value,
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000","TEST_RAW_DATA_2":"2020-01-01T00:00:00.00000"}',
            output_sandbox_prefix="my_prefix",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "output_sandbox_prefix": "my_prefix",
            "raw_data_source_instance": "SECONDARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000","TEST_RAW_DATA_2":"2020-01-01T00:00:00.00000"}',
            "ingest_view_results_only": "False",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-west1")
        self.assertEqual(pipeline_parameters.job_name, "my-prefix-us-oz-ingest-test")
        self.assertEqual(
            pipeline_parameters.service_account_email,
            "direct-ingest-state-us-oz-df@recidiviz-456.iam.gserviceaccount.com",
        )

        self.assertEqual(
            "us_oz_raw_data_secondary", pipeline_parameters.raw_data_table_input
        )
        self.assertEqual(
            "my_prefix_us_oz_ingest_view_results",
            pipeline_parameters.ingest_view_results_output,
        )
        self.assertEqual(
            "my_prefix_us_oz_state", pipeline_parameters.pre_normalization_output
        )
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    def test_creation_valid_service_account_email(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            service_account_email="some-test-account@recidiviz-staging.iam.gserviceaccount.com",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )
        self.assertEqual(
            pipeline_parameters.service_account_email,
            "some-test-account@recidiviz-staging.iam.gserviceaccount.com",
        )
        self.assertFalse(pipeline_parameters.is_sandbox_pipeline)

    def test_creation_valid_service_account_email_default_compute(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            service_account_email="12345-compute@developer.gserviceaccount.com",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )
        self.assertEqual(
            pipeline_parameters.service_account_email,
            "12345-compute@developer.gserviceaccount.com",
        )
        self.assertFalse(pipeline_parameters.is_sandbox_pipeline)

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
                service_account_email="some-test-account@somerandomwebsite.com",
                raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            )

    def test_parameters_with_sandbox_prefix(self) -> None:
        input_overrides_json = json.dumps(
            {"us_oz_raw_data": "some_other_raw_data_table"}
        )
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            input_dataset_overrides_json=input_overrides_json,
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "raw_data_source_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "False",
            "output_sandbox_prefix": "my_prefix",
            "sandbox_username": "annag",
            "input_dataset_overrides_json": input_overrides_json,
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)
        self.assertEqual(pipeline_parameters.job_name, "my-prefix-us-oz-ingest-test")

        self.assertEqual(
            "some_other_raw_data_table", pipeline_parameters.raw_data_table_input
        )
        self.assertEqual(
            "my_prefix_us_oz_ingest_view_results",
            pipeline_parameters.ingest_view_results_output,
        )
        self.assertEqual(
            "my_prefix_us_oz_state", pipeline_parameters.pre_normalization_output
        )
        self.assertEqual(
            "my_prefix_us_oz_normalized_state_new",
            pipeline_parameters.normalized_output,
        )
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    def test_parameters_with_sandbox_prefix_secondary(self) -> None:
        input_overrides_json = json.dumps(
            {"us_oz_raw_data_secondary": "some_other_raw_data_table"}
        )
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            raw_data_source_instance="SECONDARY",
            input_dataset_overrides_json=input_overrides_json,
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "raw_data_source_instance": "SECONDARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "False",
            "output_sandbox_prefix": "my_prefix",
            "sandbox_username": "annag",
            "input_dataset_overrides_json": input_overrides_json,
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)
        self.assertEqual(pipeline_parameters.job_name, "my-prefix-us-oz-ingest-test")

        self.assertEqual(
            "some_other_raw_data_table", pipeline_parameters.raw_data_table_input
        )
        self.assertEqual(
            "my_prefix_us_oz_ingest_view_results",
            pipeline_parameters.ingest_view_results_output,
        )
        self.assertEqual(
            "my_prefix_us_oz_state", pipeline_parameters.pre_normalization_output
        )
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    def test_ingest_view_results_only(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            ingest_view_results_only="True",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "raw_data_source_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "True",
            "output_sandbox_prefix": "my_prefix",
            "sandbox_username": "annag",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual("us_oz_raw_data", pipeline_parameters.raw_data_table_input)
        self.assertEqual(
            "my_prefix_us_oz_ingest_view_results",
            pipeline_parameters.ingest_view_results_output,
        )
        self.assertEqual(
            "my_prefix_us_oz_state", pipeline_parameters.pre_normalization_output
        )
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    def test_ingest_view_results_only_no_prefix_set(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"This sandbox pipeline must define an output_sandbox_prefix. "
            r"Found non-default values for these fields\: \{'ingest_view_results_only'\}",
        ):
            _ = IngestPipelineParameters(
                project="recidiviz-456",
                state_code="US_OZ",
                pipeline="test_pipeline_name",
                region="us-west1",
                raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
                ingest_view_results_only="True",
            )

    def test_ingest_views_to_run(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            ingest_views_to_run="view1 view2",
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "raw_data_source_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "False",
            "ingest_views_to_run": "view1 view2",
            "output_sandbox_prefix": "my_prefix",
            "sandbox_username": "annag",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual("us_oz_raw_data", pipeline_parameters.raw_data_table_input)
        self.assertEqual(
            "my_prefix_us_oz_ingest_view_results",
            pipeline_parameters.ingest_view_results_output,
        )
        self.assertEqual(
            "my_prefix_us_oz_state", pipeline_parameters.pre_normalization_output
        )
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    def test_run_normalization_override(self) -> None:
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_OZ",
            pipeline="test_pipeline_name",
            region="us-west1",
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            run_normalization_override=True,
        )

        expected_parameters = {
            "state_code": "US_OZ",
            "pipeline": "test_pipeline_name",
            "raw_data_source_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            "ingest_view_results_only": "False",
            "output_sandbox_prefix": "my_prefix",
            "run_normalization_override": "True",
            "sandbox_username": "annag",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual("us_oz_raw_data", pipeline_parameters.raw_data_table_input)
        self.assertEqual(
            "my_prefix_us_oz_ingest_view_results",
            pipeline_parameters.ingest_view_results_output,
        )
        self.assertEqual(
            "my_prefix_us_oz_state", pipeline_parameters.pre_normalization_output
        )
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    def test_ingest_views_to_run_no_prefix_set(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"This sandbox pipeline must define an output_sandbox_prefix. "
            r"Found non-default values for these fields\: \{'ingest_views_to_run'\}",
        ):
            _ = IngestPipelineParameters(
                project="recidiviz-456",
                state_code="US_OZ",
                pipeline="test_pipeline_name",
                region="us-west1",
                raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
                ingest_views_to_run="view1 view2",
            )

    def test_secondary_raw_data_source_instance_no_prefix_set(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"This sandbox pipeline must define an output_sandbox_prefix. "
            r"Found non-default values for these fields\: \{'raw_data_source_instance'\}",
        ):
            _ = IngestPipelineParameters(
                project="recidiviz-456",
                state_code="US_OZ",
                pipeline="test_pipeline_name",
                raw_data_source_instance=DirectIngestInstance.SECONDARY.value,
                region="us-west1",
                raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
            )

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
    )
    def test_check_for_valid_input_dataset_overrides(self) -> None:
        input_overrides_json = json.dumps(
            # This pipeline is for XX so doesn't read from us_yy_raw_data
            {"us_yy_raw_data": "some_other_raw_data_table"}
        )
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline="test_pipeline_name",
            region="us-west1",
            input_dataset_overrides_json=input_overrides_json,
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found original dataset \[us_yy_raw_data\] in overrides which is not a "
            r"dataset this pipeline reads from. Datasets you can override: "
            r"\['us_xx_raw_data'\].",
        ):
            pipeline_parameters.check_for_valid_input_dataset_overrides(
                get_all_reference_query_input_datasets_for_pipeline(
                    StateIngestPipeline,
                    StateCode(pipeline_parameters.state_code),
                )
            )

        input_overrides_json = json.dumps(
            # This is a valid override
            {"us_xx_raw_data": "some_other_raw_data_table"}
        )
        pipeline_parameters = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline="test_pipeline_name",
            region="us-west1",
            input_dataset_overrides_json=input_overrides_json,
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )
        pipeline_parameters.check_for_valid_input_dataset_overrides(
            get_all_reference_query_input_datasets_for_pipeline(
                StateIngestPipeline,
                StateCode(pipeline_parameters.state_code),
            )
        )

    def test_default_ingest_pipeline_regions_by_state_code_filled_out(self) -> None:
        existing_states = set(get_direct_ingest_states_existing_in_env())
        states_with_regions = set(DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE.keys())
        states_missing_regions = existing_states - states_with_regions
        if states_missing_regions:
            self.fail(
                f"Missing a region in DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE "
                f"for these states: {states_missing_regions}."
            )
