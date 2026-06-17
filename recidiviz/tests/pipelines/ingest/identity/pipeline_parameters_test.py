# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Unit tests for IdentityIngestPipelineParameters"""
import json
import unittest

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.identity.pipeline_parameters import (
    IdentityIngestPipelineParameters,
)

_UPPER_BOUND_DATES_JSON = (
    '{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000",'
    '"TEST_RAW_DATA_2":"2020-01-01T00:00:00.000000"}'
)


class TestIdentityIngestPipelineParameters(unittest.TestCase):
    """Unit tests for IdentityIngestPipelineParameters"""

    def test_creation_all_fields(self) -> None:
        pipeline_parameters = IdentityIngestPipelineParameters(
            project="recidiviz-456",
            tenant="US_OZ",
            pipeline="test_pipeline_name",
            region="us-east1",
            raw_data_upper_bound_dates_json=_UPPER_BOUND_DATES_JSON,
        )

        expected_parameters = {
            "tenant": "US_OZ",
            "pipeline": "test_pipeline_name",
            "raw_data_source_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": _UPPER_BOUND_DATES_JSON,
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(pipeline_parameters.region, "us-east1")
        self.assertEqual(pipeline_parameters.job_name, "us-oz-identity-ingest")
        self.assertEqual(
            pipeline_parameters.service_account_email,
            "direct-ingest-state-us-oz-df@recidiviz-456.iam.gserviceaccount.com",
        )

        self.assertEqual("us_oz_raw_data", pipeline_parameters.raw_data_input_dataset)
        self.assertEqual(
            "us_oz_identity_cluster",
            pipeline_parameters.clustering_output_dataset,
        )
        self.assertFalse(pipeline_parameters.is_sandbox_pipeline)

    def test_creation_secondary_instance(self) -> None:
        pipeline_parameters = IdentityIngestPipelineParameters(
            project="recidiviz-456",
            tenant="US_OZ",
            pipeline="test_pipeline_name",
            region="us-east1",
            raw_data_source_instance=DirectIngestInstance.SECONDARY.value,
            raw_data_upper_bound_dates_json=_UPPER_BOUND_DATES_JSON,
            output_sandbox_prefix="my_prefix",
        )

        self.assertEqual(
            "us_oz_raw_data_secondary", pipeline_parameters.raw_data_input_dataset
        )
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    def test_creation_with_sandbox_prefix(self) -> None:
        pipeline_parameters = IdentityIngestPipelineParameters(
            project="recidiviz-456",
            tenant="US_OZ",
            pipeline="test_pipeline_name",
            region="us-east1",
            raw_data_upper_bound_dates_json=_UPPER_BOUND_DATES_JSON,
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
        )

        expected_parameters = {
            "tenant": "US_OZ",
            "pipeline": "test_pipeline_name",
            "raw_data_source_instance": "PRIMARY",
            "raw_data_upper_bound_dates_json": _UPPER_BOUND_DATES_JSON,
            "output_sandbox_prefix": "my_prefix",
            "sandbox_username": "annag",
        }

        self.assertEqual(expected_parameters, pipeline_parameters.template_parameters)

        self.assertEqual(
            pipeline_parameters.job_name,
            "my-prefix-us-oz-identity-ingest-test",
        )

        self.assertEqual("us_oz_raw_data", pipeline_parameters.raw_data_input_dataset)
        self.assertEqual(
            "my_prefix_us_oz_identity_cluster",
            pipeline_parameters.clustering_output_dataset,
        )
        self.assertTrue(pipeline_parameters.is_sandbox_pipeline)

    def test_creation_with_input_dataset_override(self) -> None:
        input_overrides_json = json.dumps({"us_oz_raw_data": "some_other_raw_data"})
        pipeline_parameters = IdentityIngestPipelineParameters(
            project="recidiviz-456",
            tenant="US_OZ",
            pipeline="test_pipeline_name",
            region="us-east1",
            raw_data_upper_bound_dates_json=_UPPER_BOUND_DATES_JSON,
            output_sandbox_prefix="my_prefix",
            sandbox_username="annag",
            input_dataset_overrides_json=input_overrides_json,
        )

        self.assertEqual(
            "some_other_raw_data", pipeline_parameters.raw_data_input_dataset
        )
        self.assertEqual(
            "my_prefix_us_oz_identity_cluster",
            pipeline_parameters.clustering_output_dataset,
        )

    def test_secondary_instance_no_prefix_set(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"This sandbox pipeline must define an output_sandbox_prefix. "
            r"Found non-default values for these fields\: \{'raw_data_source_instance'\}",
        ):
            _ = IdentityIngestPipelineParameters(
                project="recidiviz-456",
                tenant="US_OZ",
                pipeline="test_pipeline_name",
                region="us-east1",
                raw_data_source_instance=DirectIngestInstance.SECONDARY.value,
                raw_data_upper_bound_dates_json=_UPPER_BOUND_DATES_JSON,
            )

    def test_non_state_tenant_raw_data_raises(self) -> None:
        pipeline_parameters = IdentityIngestPipelineParameters(
            project="recidiviz-456",
            tenant="NYC",
            pipeline="test_pipeline_name",
            region="us-east1",
            raw_data_upper_bound_dates_json=_UPPER_BOUND_DATES_JSON,
            service_account_email="some-sa@recidiviz-456.iam.gserviceaccount.com",
        )

        with self.assertRaisesRegex(
            ValueError,
            r"No support for identity ingest pipeline with non-state tenants\.",
        ):
            _ = pipeline_parameters.raw_data_input_dataset

    def test_non_state_tenant_without_service_account_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"No default service account for tenant NYC\.",
        ):
            _ = IdentityIngestPipelineParameters(
                project="recidiviz-456",
                tenant="NYC",
                pipeline="test_pipeline_name",
                region="us-east1",
                raw_data_upper_bound_dates_json=_UPPER_BOUND_DATES_JSON,
            )

    def test_pipeline_entry_point_parses(self) -> None:
        """Tests that the pipeline entry point can parse arguments without errors."""
        argv = [
            "--project=recidiviz-456",
            "--tenant=US_OZ",
            "--pipeline=test_pipeline_name",
            "--region=us-east1",
            f"--raw_data_upper_bound_dates_json={_UPPER_BOUND_DATES_JSON}",
        ]
        args, _ = IdentityIngestPipelineParameters.parse_args(
            argv, sandbox_pipeline=False
        )
        self.assertEqual(args.tenant, "US_OZ")
        self.assertEqual(args.project, "recidiviz-456")
        self.assertEqual(args.pipeline, "test_pipeline_name")
        self.assertEqual(args.region, "us-east1")
        self.assertEqual(args.raw_data_upper_bound_dates_json, _UPPER_BOUND_DATES_JSON)
