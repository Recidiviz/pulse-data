# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

"""Implements tests for the IngestDataflowOperations."""
import datetime
import os
from unittest import mock
from unittest.case import TestCase
from unittest.mock import patch

from mock import MagicMock

import recidiviz
from recidiviz.admin_panel.ingest_dataflow_operations import (
    DataflowPipelineMetadataResponse,
    get_all_latest_ingest_jobs,
    get_latest_run_ingest_view_results,
    ingest_pipeline_name,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests import pipelines as recidiviz_pipelines_tests_module
from recidiviz.utils.yaml_dict import YAMLDict

PIPELINES_TESTS_WORKING_DIRECTORY = os.path.dirname(
    recidiviz_pipelines_tests_module.__file__
)

FAKE_PIPELINE_CONFIG_YAML_PATH = os.path.join(
    PIPELINES_TESTS_WORKING_DIRECTORY,
    "fake_calculation_pipeline_templates.yaml",
)


class IngestDataflowOperations(TestCase):
    """Implements tests for get_all_latest_ingest_dataflow_jobs and helpers."""

    def setUp(self) -> None:
        super().setUp()

        self.latest_jobs_from_location_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_dataflow_operations.get_latest_jobs_from_location_by_name"
        )
        self.latest_jobs_from_location_patcher.start()

        self.state_code_list_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_dataflow_operations.get_direct_ingest_states_launched_in_env",
            return_value=[
                StateCode.US_XX,
                StateCode.US_YY,
            ],
        )
        self.state_code_list_patcher.start()

        self.pipeline_config_yaml_path_patcher = patch(
            "recidiviz.admin_panel.ingest_dataflow_operations.PIPELINE_CONFIG_YAML_PATH",
            FAKE_PIPELINE_CONFIG_YAML_PATH,
        )
        self.pipeline_config_yaml_path_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.pipeline_config_yaml_path_patcher.stop()
        self.state_code_list_patcher.stop()

    @mock.patch(
        "recidiviz.admin_panel.ingest_dataflow_operations.is_ingest_enabled_in_secondary",
        True,
    )
    def test_get_all_latest_ingest_jobs_simple(self) -> None:
        pipeline = DataflowPipelineMetadataResponse(
            id="1234",
            project_id="test-project",
            name="us-xx-ingest",
            create_time=datetime.datetime(2023, 7, 10).timestamp(),
            start_time=datetime.datetime(2023, 7, 10).timestamp(),
            termination_time=datetime.datetime(2023, 7, 11).timestamp(),
            termination_state="JOB_STATE_FAILED",
            location="us-east1",
        )

        pipeline2 = DataflowPipelineMetadataResponse(
            id="1235",
            project_id="test-project",
            name="us-xx-ingest-secondary",
            create_time=datetime.datetime(2023, 9, 10).timestamp(),
            start_time=datetime.datetime(2023, 9, 10).timestamp(),
            termination_time=datetime.datetime(2023, 9, 11).timestamp(),
            termination_state="JOB_STATE_FAILED",
            location="us-east1",
        )

        pipeline3 = DataflowPipelineMetadataResponse(
            id="1236",
            project_id="test-project",
            name="us-yy-ingest",
            create_time=datetime.datetime(2023, 8, 10).timestamp(),
            start_time=datetime.datetime(2023, 8, 10).timestamp(),
            termination_time=datetime.datetime(2023, 8, 11).timestamp(),
            termination_state="JOB_STATE_DONE",
            location="us-east1",
        )

        pipeline4 = DataflowPipelineMetadataResponse(
            id="1237",
            project_id="test-project",
            name="us-yy-ingest-secondary",
            create_time=datetime.datetime(2023, 10, 10).timestamp(),
            start_time=datetime.datetime(2023, 10, 10).timestamp(),
            termination_time=datetime.datetime(2023, 10, 11).timestamp(),
            termination_state="JOB_STATE_DONE",
            location="us-east1",
        )
        expected = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: pipeline,
                DirectIngestInstance.SECONDARY: pipeline2,
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: pipeline3,
                DirectIngestInstance.SECONDARY: pipeline4,
            },
        }
        with mock.patch(
            "recidiviz.admin_panel.ingest_dataflow_operations.get_latest_jobs_from_location_by_name",
            return_value={
                "us-xx-ingest": pipeline,
                "us-xx-ingest-secondary": pipeline2,
                "us-yy-ingest": pipeline3,
                "us-yy-ingest-secondary": pipeline4,
            },
        ):
            self.assertEqual(expected, get_all_latest_ingest_jobs())

    def test_get_all_latest_ingest_jobs_no_secondary(self) -> None:
        pipeline = DataflowPipelineMetadataResponse(
            id="1234",
            project_id="test-project",
            name="us-xx-ingest",
            create_time=datetime.datetime(2023, 7, 10).timestamp(),
            start_time=datetime.datetime(2023, 7, 10).timestamp(),
            termination_time=datetime.datetime(2023, 7, 11).timestamp(),
            termination_state="JOB_STATE_FAILED",
            location="us-east1",
        )

        pipeline2 = DataflowPipelineMetadataResponse(
            id="1236",
            project_id="test-project",
            name="us-yy-ingest",
            create_time=datetime.datetime(2023, 8, 10).timestamp(),
            start_time=datetime.datetime(2023, 8, 10).timestamp(),
            termination_time=datetime.datetime(2023, 8, 11).timestamp(),
            termination_state="JOB_STATE_FAILED",
            location="us-east1",
        )

        expected = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: pipeline,
                DirectIngestInstance.SECONDARY: None,
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: pipeline2,
                DirectIngestInstance.SECONDARY: None,
            },
        }
        with mock.patch(
            "recidiviz.admin_panel.ingest_dataflow_operations.get_latest_jobs_from_location_by_name",
            return_value={
                "us-xx-ingest": pipeline,
                "us-yy-ingest": pipeline2,
            },
        ):
            self.assertEqual(expected, get_all_latest_ingest_jobs())

    def test_ingest_pipelines_correct_job_names(self) -> None:
        PIPELINE_CONFIG_YAML_PATH = os.path.join(
            os.path.dirname(recidiviz.__file__),
            "pipelines/calculation_pipeline_templates.yaml",
        )
        pipeline_configs = YAMLDict.from_path(PIPELINE_CONFIG_YAML_PATH)

        for ingest_pipeline in pipeline_configs.pop_dicts("ingest_pipelines"):
            state_code = ingest_pipeline.peek("state_code", str)
            expected_name = ingest_pipeline_name(
                StateCode(state_code), DirectIngestInstance.PRIMARY
            )
            name = ingest_pipeline.peek("job_name", str)
            self.assertEqual(expected_name, name)

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
    )
    @patch(
        "recidiviz.admin_panel.ingest_dataflow_operations.BigQueryClientImpl",
    )
    def test_get_latest_run_ingest_view_results(self, mock_bq_class: MagicMock) -> None:
        # Arrange
        mock_bq_client = mock_bq_class.return_value
        mock_bq_client.dataset_exists.return_value = True
        mock_bq_client.run_query_async.return_value = [
            {"ingest_view_name": "foo", "num_rows": 120},
            {"ingest_view_name": "bar", "num_rows": 0},
        ]

        # Act
        results = get_latest_run_ingest_view_results(
            state_code=StateCode.US_XX, ingest_instance=DirectIngestInstance.PRIMARY
        )

        # Assert
        self.assertEqual(results, {"foo": 120, "bar": 0})

    @patch(
        "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
    )
    @patch(
        "recidiviz.admin_panel.ingest_dataflow_operations.BigQueryClientImpl",
    )
    def test_get_latest_run_ingest_view_results_no_dataset(
        self, mock_bq_class: MagicMock
    ) -> None:
        # Arrange
        mock_bq_client = mock_bq_class.return_value
        mock_bq_client.dataset_exists.return_value = False

        # Act
        results = get_latest_run_ingest_view_results(
            state_code=StateCode.US_XX, ingest_instance=DirectIngestInstance.PRIMARY
        )

        # Assert
        self.assertEqual(results, {})
