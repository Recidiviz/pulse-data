# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for functionality in load_end_to_end_sandbox.py"""
import json
import unittest
from typing import Dict, List, Type
from unittest import mock
from unittest.mock import ANY

from apache_beam import Pipeline

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import (
    BigQueryQueryProvider,
    SimpleBigQueryQueryProvider,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.pipeline_parameters import IngestPipelineParameters
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.pipelines.metrics.base_identifier import BaseIdentifier
from recidiviz.pipelines.metrics.base_metric_pipeline import MetricPipeline
from recidiviz.pipelines.metrics.base_metric_producer import BaseMetricProducer
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.pipeline_names import INGEST_PIPELINE_NAME
from recidiviz.pipelines.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipeline,
)
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.tests.admin_panel.ingest_dataflow_operations_test import (
    FAKE_PIPELINE_CONFIG_YAML_PATH,
)
from recidiviz.tools.load_end_to_end_data_sandbox import (
    get_sandbox_post_ingest_pipeline_params,
    get_view_update_input_dataset_overrides_dict,
)
from recidiviz.utils.metadata import local_project_id_override

FAKE_DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE = {
    **DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
    StateCode.US_XX: "us-east1",
    StateCode.US_YY: "us-west1",
}


class _TestMetricPipeline(MetricPipeline):
    @classmethod
    def required_entities(cls) -> List[Type[Entity]]:
        raise NotImplementedError("Shouldn't need this!")

    @classmethod
    def identifier(cls, state_code: StateCode) -> BaseIdentifier:
        raise NotImplementedError("Shouldn't need this!")

    @classmethod
    def metric_producer(cls) -> BaseMetricProducer:
        raise NotImplementedError("Shouldn't need this!")

    @classmethod
    def include_calculation_limit_args(cls) -> bool:
        raise NotImplementedError("Shouldn't need this!")

    @classmethod
    def pipeline_name(cls) -> str:
        raise NotImplementedError("Shouldn't need this!")


class _TestSupplementalPipeline(SupplementalDatasetPipeline):
    @classmethod
    def input_reference_query_providers(
        cls, state_code: StateCode, address_overrides: BigQueryAddressOverrides | None
    ) -> Dict[str, BigQueryQueryProvider]:
        return {
            "reference_query_name": SimpleBigQueryQueryProvider(
                query="SELECT * FROM `recidiviz-456.reference_dataset.reference_table`"
            )
        }

    @classmethod
    def table_id(cls) -> str:
        raise NotImplementedError("Shouldn't need this!")

    @classmethod
    def table_fields(cls) -> Dict[str, Type]:
        raise NotImplementedError("Shouldn't need this!")

    @classmethod
    def pipeline_name(cls) -> str:
        raise NotImplementedError("Shouldn't need this!")

    def run_pipeline(self, p: Pipeline) -> None:
        raise NotImplementedError("Shouldn't need this!")


def fake_pipeline_cls_for_pipeline_name(pipeline_name: str) -> type[BasePipeline]:
    if pipeline_name in {
        "pipeline_with_limit",
        "pipeline_no_limit",
        "pipeline_staging_only",
    }:
        return _TestMetricPipeline

    if pipeline_name in {"pipeline_to_run_supplemental"}:
        return _TestSupplementalPipeline

    raise ValueError(f"Unexpected pipeline [{pipeline_name}]")


class TestGetSandboxPostIngestPipelineParams(unittest.TestCase):
    """Tests for get_sandbox_post_ingest_pipeline_params()"""

    def setUp(self) -> None:
        self.pipeline_config_patcher = mock.patch(
            "recidiviz.tools.load_end_to_end_data_sandbox.PIPELINE_CONFIG_YAML_PATH",
            FAKE_PIPELINE_CONFIG_YAML_PATH,
        )
        self.pipeline_config_patcher.start()

        self.default_regions_patcher = mock.patch(
            "recidiviz.tools.load_end_to_end_data_sandbox.DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE",
            FAKE_DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
        )
        self.default_regions_patcher.start()

        self.pipeline_cls_for_name_patcher = mock.patch(
            "recidiviz.tools.load_end_to_end_data_sandbox.pipeline_cls_for_pipeline_name",
            fake_pipeline_cls_for_pipeline_name,
        )
        self.pipeline_cls_for_name_patcher.start()

        self.us_xx_ingest_pipeline_params = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline=INGEST_PIPELINE_NAME,
            region="us-west1",
            input_dataset_overrides_json=json.dumps({}),
            output_sandbox_prefix="my_prefix",
            sandbox_username="my_username",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )

        self.us_yy_ingest_pipeline_params = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_YY",
            pipeline=INGEST_PIPELINE_NAME,
            region="us-west1",
            input_dataset_overrides_json=json.dumps({}),
            output_sandbox_prefix="my_prefix",
            sandbox_username="my_username",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )

    def tearDown(self) -> None:
        self.pipeline_config_patcher.stop()
        self.default_regions_patcher.stop()
        self.pipeline_cls_for_name_patcher.stop()

    def test_get_sandbox_post_ingest_pipeline_params_us_xx(self) -> None:
        post_ingest_params = get_sandbox_post_ingest_pipeline_params(
            state_code_filter=StateCode.US_XX,
            ingest_pipeline_params=self.us_xx_ingest_pipeline_params,
        )

        expected_input_dataset_overrides_json = json.dumps(
            {
                "us_xx_normalized_state": "my_prefix_us_xx_normalized_state",
            }
        )
        expected_params = [
            MetricsPipelineParameters(
                project="recidiviz-456",
                state_code="US_XX",
                pipeline="pipeline_no_limit",
                output_sandbox_prefix="my_prefix",
                sandbox_username="my_username",
                input_dataset_overrides_json=expected_input_dataset_overrides_json,
                region="us-east1",
                worker_zone=ANY,
                metric_types="METRIC_1",
            ),
            MetricsPipelineParameters(
                project="recidiviz-456",
                state_code="US_XX",
                pipeline="pipeline_with_limit",
                output_sandbox_prefix="my_prefix",
                sandbox_username="my_username",
                input_dataset_overrides_json=expected_input_dataset_overrides_json,
                region="us-east1",
                worker_zone=ANY,
                metric_types="METRIC_2 METRIC_3",
                calculation_month_count=36,
            ),
        ]
        self.assertEqual(expected_params, post_ingest_params)

    def test_get_sandbox_post_ingest_pipeline_params_us_yy(self) -> None:
        post_ingest_params = get_sandbox_post_ingest_pipeline_params(
            state_code_filter=StateCode.US_YY,
            ingest_pipeline_params=self.us_yy_ingest_pipeline_params,
        )

        expected_input_dataset_overrides_json = json.dumps(
            {
                "us_yy_normalized_state": "my_prefix_us_yy_normalized_state",
            }
        )
        expected_params = [
            MetricsPipelineParameters(
                project="recidiviz-456",
                state_code="US_YY",
                pipeline="pipeline_with_limit",
                output_sandbox_prefix="my_prefix",
                sandbox_username="my_username",
                input_dataset_overrides_json=expected_input_dataset_overrides_json,
                region="us-west1",
                worker_zone=ANY,
                metric_types="METRIC_2 METRIC_3",
                calculation_month_count=24,
            ),
            MetricsPipelineParameters(
                project="recidiviz-456",
                state_code="US_YY",
                pipeline="pipeline_staging_only",
                output_sandbox_prefix="my_prefix",
                sandbox_username="my_username",
                input_dataset_overrides_json=expected_input_dataset_overrides_json,
                region="us-west1",
                worker_zone=ANY,
                staging_only=True,
                metric_types="METRIC_3",
                calculation_month_count=36,
            ),
            SupplementalPipelineParameters(
                project="recidiviz-456",
                state_code="US_YY",
                pipeline="pipeline_to_run_supplemental",
                output_sandbox_prefix="my_prefix",
                sandbox_username="my_username",
                # This pipeline does not read from any of the ingest outputs so we have
                # empty input overrides.
                input_dataset_overrides_json=json.dumps({}),
                region="us-west1",
                worker_zone=ANY,
            ),
        ]
        self.assertEqual(expected_params, post_ingest_params)

    def test_get_sandbox_post_ingest_pipeline_params_mismatched_state_codes(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found state_code for ingest parameters \[US_XX\] which does not match "
            r"state_code_filter \[US_YY\].",
        ):
            _ = get_sandbox_post_ingest_pipeline_params(
                state_code_filter=StateCode.US_YY,
                ingest_pipeline_params=self.us_xx_ingest_pipeline_params,
            )


class TestGetViewUpdateInputDatasetOverridesDict(unittest.TestCase):
    """Tests for get_view_update_input_dataset_overrides_dict()"""

    def test_no_post_ingest_pipelines(self) -> None:
        ingest_pipeline_params = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_CA",
            pipeline=INGEST_PIPELINE_NAME,
            region="us-west1",
            input_dataset_overrides_json=json.dumps({}),
            output_sandbox_prefix="my_prefix",
            sandbox_username="my_username",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )

        with local_project_id_override("recidiviz-456"):
            overrides_dict = get_view_update_input_dataset_overrides_dict(
                state_code=StateCode.US_CA,
                ingest_pipeline_params=ingest_pipeline_params,
                post_ingest_pipeline_params=[],
            )

        self.assertEqual(
            {
                "us_ca_ingest_view_results": "my_prefix_us_ca_ingest_view_results",
                "us_ca_normalized_state": "my_prefix_us_ca_normalized_state",
                "us_ca_state": "my_prefix_us_ca_state",
            },
            overrides_dict,
        )

    def test_complex(self) -> None:
        ingest_pipeline_params = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_CA",
            pipeline=INGEST_PIPELINE_NAME,
            region="us-west1",
            input_dataset_overrides_json=json.dumps({}),
            output_sandbox_prefix="my_prefix",
            sandbox_username="my_username",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )

        post_ingest_pipeline_params: list[
            MetricsPipelineParameters | SupplementalPipelineParameters
        ] = [
            MetricsPipelineParameters(
                project="recidiviz-456",
                state_code="US_CA",
                pipeline="some_metric_pipeline",
                output_sandbox_prefix="my_prefix",
                sandbox_username="my_username",
                input_dataset_overrides_json=json.dumps(
                    {
                        "us_ca_normalized_state": "my_prefix_us_ca_normalized_state",
                    }
                ),
                region="us-east1",
                staging_only=True,
                metric_types="METRIC_NAME",
            ),
            SupplementalPipelineParameters(
                project="recidiviz-456",
                state_code="US_CA",
                pipeline="some_pipeline_supplemental",
                output_sandbox_prefix="my_prefix",
                sandbox_username="my_username",
                input_dataset_overrides_json=json.dumps(
                    {
                        "us_ca_normalized_state": "my_prefix_us_ca_normalized_state",
                    }
                ),
                region="us-east1",
            ),
        ]

        with local_project_id_override("recidiviz-456"):
            overrides_dict = get_view_update_input_dataset_overrides_dict(
                state_code=StateCode.US_CA,
                ingest_pipeline_params=ingest_pipeline_params,
                post_ingest_pipeline_params=post_ingest_pipeline_params,
            )

        self.assertEqual(
            {
                "us_ca_ingest_view_results": "my_prefix_us_ca_ingest_view_results",
                "us_ca_normalized_state": "my_prefix_us_ca_normalized_state",
                "us_ca_state": "my_prefix_us_ca_state",
                "dataflow_metrics": "my_prefix_dataflow_metrics",
                "supplemental_data": "my_prefix_supplemental_data",
            },
            overrides_dict,
        )
