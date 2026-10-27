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
import argparse
import contextlib
import io
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
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.pipelines.base_pipeline import BasePipeline
from recidiviz.pipelines.ingest.activity.pipeline_parameters import (
    IngestPipelineParameters,
)
from recidiviz.pipelines.ingest.activity.pipeline_utils import (
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
    load_end_to_end_sandbox,
    parse_arguments,
)
from recidiviz.tools.utils.run_sandbox_dataflow_pipeline_utils import (
    LaunchedDataflowJob,
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


_E2E_MODULE = "recidiviz.tools.load_end_to_end_data_sandbox"

_INGEST_JOB = LaunchedDataflowJob(
    job_id="ingest-job-id", job_name="my-prefix-us-xx-ingest-test", region="us-west1"
)
_NO_LIMIT_JOB = LaunchedDataflowJob(
    job_id="no-limit-job-id",
    job_name="my-prefix-full-us-xx-pipeline-no-limit-test",
    region="us-east1",
)
_WITH_LIMIT_JOB = LaunchedDataflowJob(
    job_id="with-limit-job-id",
    job_name="my-prefix-us-xx-pipeline-with-limit-36-test",
    region="us-east1",
)
_LAUNCHED_JOBS_BY_PIPELINE = {
    INGEST_PIPELINE_NAME: _INGEST_JOB,
    "pipeline_no_limit": _NO_LIMIT_JOB,
    "pipeline_with_limit": _WITH_LIMIT_JOB,
}


class TestLoadEndToEndSandbox(unittest.TestCase):
    """Tests for load_end_to_end_sandbox(), with every step that touches GCP replaced
    by a mock, so they check which steps run, what those steps are given, and what
    the person at the keyboard is asked."""

    def setUp(self) -> None:
        self.ingest_pipeline_params = IngestPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline=INGEST_PIPELINE_NAME,
            region="us-west1",
            input_dataset_overrides_json=None,
            output_sandbox_prefix="my_prefix",
            sandbox_username="my_username",
            raw_data_upper_bound_dates_json='{"TEST_RAW_DATA":"2020-01-01T00:00:00.000000"}',
        )
        mock.patch(
            f"{_E2E_MODULE}.PIPELINE_CONFIG_YAML_PATH", FAKE_PIPELINE_CONFIG_YAML_PATH
        ).start()
        mock.patch(
            f"{_E2E_MODULE}.DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE",
            FAKE_DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
        ).start()
        mock.patch(
            f"{_E2E_MODULE}.pipeline_cls_for_pipeline_name",
            fake_pipeline_cls_for_pipeline_name,
        ).start()
        mock.patch(
            f"{_E2E_MODULE}.get_sandbox_pipeline_username", return_value="my_username"
        ).start()
        mock.patch(
            f"{_E2E_MODULE}.get_ingest_pipeline_params",
            return_value=self.ingest_pipeline_params,
        ).start()
        # The override map keeps only datasets the project's source table repository
        # knows, which does not include the fake state's.
        mock.patch(
            f"{_E2E_MODULE}.get_source_table_datasets",
            return_value={
                "us_xx_ingest_view_results",
                "us_xx_normalized_state",
                "us_xx_state",
                "dataflow_metrics",
                "supplemental_data",
            },
        ).start()
        self.mock_collect_views = mock.patch(
            f"{_E2E_MODULE}.collect_changed_views_and_descendants_to_load",
            return_value=[],
        ).start()
        self.mock_push_image = mock.patch(
            f"{_E2E_MODULE}.push_sandbox_dataflow_pipeline_docker_image"
        ).start()
        self.mock_create_datasets = mock.patch(
            f"{_E2E_MODULE}.create_or_update_dataflow_sandbox"
        ).start()
        self.mock_run_pipeline = mock.patch(
            f"{_E2E_MODULE}.run_sandbox_dataflow_pipeline",
            side_effect=lambda params, skip_build: _LAUNCHED_JOBS_BY_PIPELINE[
                params.pipeline
            ],
        ).start()
        self.mock_wait_for_jobs = mock.patch(
            f"{_E2E_MODULE}.wait_for_sandbox_dataflow_jobs"
        ).start()
        self.mock_load_views = mock.patch(
            f"{_E2E_MODULE}.load_collected_views_to_sandbox"
        ).start()
        self.mock_check_access = mock.patch(
            f"{_E2E_MODULE}.check_can_poll_sandbox_dataflow_jobs"
        ).start()
        self.mock_input = mock.patch("builtins.input").start()

    def tearDown(self) -> None:
        mock.patch.stopall()

    def _run(self, *, unattended: bool, export_names: set[str] | None = None) -> None:
        with contextlib.redirect_stdout(io.StringIO()):
            load_end_to_end_sandbox(
                project_id="recidiviz-456",
                state_code=StateCode.US_XX,
                output_sandbox_prefix="my_prefix",
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                load_up_to_addresses=None,
                load_up_to_datasets=["sessions"],
                export_names=export_names or set(),
                changed_datasets_to_include=None,
                changed_datasets_to_ignore=None,
                changed_addresses_to_include=None,
                changed_addresses_to_ignore=None,
                unattended=unattended,
            )

    def test_unattended_asks_only_up_front_then_waits_for_each_pipeline_stage(
        self,
    ) -> None:
        self.mock_input.side_effect = ["y"]

        self._run(unattended=True)

        self.mock_input.assert_called_once_with("Continue?  [y/n]: \n")
        self.mock_collect_views.assert_called_once_with(
            prompt=True,
            input_source_table_dataset_overrides_dict={
                "us_xx_ingest_view_results": "my_prefix_us_xx_ingest_view_results",
                "us_xx_normalized_state": "my_prefix_us_xx_normalized_state",
                "us_xx_state": "my_prefix_us_xx_state",
                "dataflow_metrics": "my_prefix_dataflow_metrics",
            },
            changed_datasets_to_include=None,
            changed_datasets_to_ignore=None,
            changed_addresses_to_include=None,
            changed_addresses_to_ignore=None,
            state_code_filter=StateCode.US_XX,
            load_changed_views_only=False,
            load_up_to_addresses=[],
            load_up_to_datasets=["sessions"],
        )
        self.mock_check_access.assert_called_once_with("recidiviz-456")
        self.mock_push_image.assert_called_once_with(
            project_id="recidiviz-456", sandbox_username="my_username"
        )
        self.mock_create_datasets.assert_called_once()
        self.assertEqual(3, self.mock_run_pipeline.call_count)
        self.mock_wait_for_jobs.assert_has_calls(
            [
                mock.call(project_id="recidiviz-456", jobs=[_INGEST_JOB]),
                mock.call(
                    project_id="recidiviz-456", jobs=[_NO_LIMIT_JOB, _WITH_LIMIT_JOB]
                ),
            ]
        )
        self.assertEqual(2, self.mock_wait_for_jobs.call_count)
        self.mock_load_views.assert_called_once()

    def test_unattended_stops_naming_the_pipeline_dataflow_refused_to_launch(
        self,
    ) -> None:
        self.mock_input.side_effect = ["y"]
        self.mock_run_pipeline.side_effect = lambda params, skip_build: (
            None
            if params.pipeline == "pipeline_with_limit"
            else _LAUNCHED_JOBS_BY_PIPELINE[params.pipeline]
        )

        with self.assertRaisesRegex(
            RuntimeError,
            r"^Dataflow refused to launch \['my-prefix-us-xx-pipeline-with-limit-36-"
            r"test'\]; its response is printed above\. Any other jobs in this stage "
            r"launched and keep running; check them at "
            r"https://console\.cloud\.google\.com/dataflow/jobs\?project=recidiviz-456 "
            r"before rerunning\. Fix the cause and rerun\.$",
        ):
            self._run(unattended=True)

        self.mock_wait_for_jobs.assert_called_once_with(
            project_id="recidiviz-456", jobs=[_INGEST_JOB]
        )
        self.mock_load_views.assert_not_called()

    def test_interactive_continues_past_a_refused_launch_as_before(self) -> None:
        self.mock_input.side_effect = ["y", "p", "p", "p", "y", "p", "y", "p"]
        self.mock_run_pipeline.side_effect = lambda params, skip_build: (
            None
            if params.pipeline == "pipeline_with_limit"
            else _LAUNCHED_JOBS_BY_PIPELINE[params.pipeline]
        )

        self._run(unattended=False)

        self.assertEqual(
            mock.call(
                "The metric/supplemental pipeline(s) will take some time to run. "
                "Have they completed? [y/n]: \n"
            ),
            self.mock_input.call_args_list[6],
        )
        self.mock_wait_for_jobs.assert_not_called()
        self.mock_load_views.assert_called_once()

    def test_unattended_stops_before_asking_anything_when_it_cannot_read_jobs(
        self,
    ) -> None:
        self.mock_check_access.side_effect = RuntimeError("no access")
        self.mock_input.side_effect = AssertionError("asked the person a question")

        with self.assertRaisesRegex(RuntimeError, r"^no access$"):
            self._run(unattended=True)

        self.mock_push_image.assert_not_called()
        self.mock_create_datasets.assert_not_called()
        self.mock_run_pipeline.assert_not_called()

    def test_unattended_refuses_exports_before_anything_runs(self) -> None:
        self.mock_input.side_effect = AssertionError("asked the person a question")

        with self.assertRaisesRegex(
            ValueError,
            r"^Exports overwrite what the staging products display and require a "
            r"person to confirm they notified the state channel; run them without "
            r"--unattended\.$",
        ):
            self._run(unattended=True, export_names={"WORKFLOWS_FIRESTORE"})

        self.mock_check_access.assert_not_called()
        self.mock_push_image.assert_not_called()
        self.mock_run_pipeline.assert_not_called()

    def test_interactive_asks_at_every_step_and_never_polls_dataflow(self) -> None:
        self.mock_input.side_effect = ["y", "p", "p", "p", "y", "p", "y", "p"]

        self._run(unattended=False)

        self.mock_input.assert_has_calls(
            [
                mock.call("Continue?  [y/n]: \n"),
                mock.call("Continue? [(P)roceed, (S)kip, or (E)xit] "),
                mock.call("Continue? [(P)roceed, (S)kip, or (E)xit] "),
                mock.call("Continue? [(P)roceed, (S)kip, or (E)xit] "),
                mock.call(
                    "The ingest pipeline will take some time to run. Has it completed? "
                    "[y/n]: \n"
                ),
                mock.call("Continue? [(P)roceed, (S)kip, or (E)xit] "),
                mock.call(
                    "The metric/supplemental pipeline(s) will take some time to run. "
                    "Have they completed? [y/n]: \n"
                ),
                mock.call("Continue? [(P)roceed, (S)kip, or (E)xit] "),
            ]
        )
        self.assertEqual(8, self.mock_input.call_count)
        self.assertTrue(self.mock_collect_views.call_args.kwargs["prompt"])
        self.mock_wait_for_jobs.assert_not_called()
        self.mock_check_access.assert_not_called()
        self.assertEqual(3, self.mock_run_pipeline.call_count)
        self.mock_load_views.assert_called_once()


class TestParseArguments(unittest.TestCase):
    """Tests for parse_arguments()"""

    def _parse(self, *argv: str) -> argparse.Namespace:
        with mock.patch(
            "sys.argv",
            ["load_end_to_end_data_sandbox", "--state_code", "US_XX", *argv],
        ):
            return parse_arguments()

    def test_unattended_flag(self) -> None:
        args = self._parse(
            "--sandbox_prefix",
            "my_prefix",
            "--load_up_to_datasets",
            "sessions",
            "--unattended",
        )
        self.assertTrue(args.unattended)

    def test_flag_defaults_to_the_interactive_run(self) -> None:
        args = self._parse(
            "--sandbox_prefix", "my_prefix", "--load_up_to_datasets", "sessions"
        )
        self.assertFalse(args.unattended)

    def test_unattended_rejects_exports(self) -> None:
        with self.assertRaisesRegex(
            argparse.ArgumentTypeError,
            r"^Cannot specify --exports with --unattended\. ",
        ):
            self._parse(
                "--sandbox_prefix",
                "my_prefix",
                "--exports",
                "WORKFLOWS_FIRESTORE",
                "--unattended",
            )

    def test_unattended_rejects_a_prefix_naming_another_state_before_anything_runs(
        self,
    ) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self._parse(
                "--sandbox_prefix",
                "my_us_yy_prefix",
                "--load_up_to_datasets",
                "sessions",
                "--unattended",
            )

    def test_interactive_leaves_the_prefix_check_to_the_view_stage_as_before(
        self,
    ) -> None:
        args = self._parse(
            "--sandbox_prefix", "my_us_yy_prefix", "--load_up_to_datasets", "sessions"
        )
        self.assertEqual("my_us_yy_prefix", args.sandbox_prefix)
