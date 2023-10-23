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

from recidiviz.admin_panel.ingest_dataflow_operations import (
    DataflowPipelineMetadataResponse,
    get_all_latest_ingest_jobs,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.tests import pipelines as recidiviz_pipelines_tests_module

PIPELINES_TESTS_WORKING_DIRECTORY = os.path.dirname(
    recidiviz_pipelines_tests_module.__file__
)

FAKE_PIPELINE_CONFIG_YAML_PATH = os.path.join(
    PIPELINES_TESTS_WORKING_DIRECTORY,
    "fake_calculation_pipeline_templates.yaml",
)


@patch.dict(
    DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE,
    values={StateCode.US_XX: "us-east1", StateCode.US_YY: "us-east2"},
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
