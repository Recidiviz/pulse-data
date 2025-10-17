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
from typing import Optional
from unittest import mock
from unittest.case import TestCase
from unittest.mock import patch

import pytest

from recidiviz.admin_panel.ingest_dataflow_operations import (
    DataflowPipelineMetadataResponse,
    get_all_latest_ingest_jobs,
    get_raw_data_tags_not_meeting_watermark,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_dataflow_job_manager import (
    DataflowJobLocationID,
    DirectIngestDataflowJobManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_dataflow_watermark_manager import (
    DirectIngestDataflowWatermarkManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.tests import pipelines as recidiviz_pipelines_tests_module
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.utils.types import assert_type

PIPELINES_TESTS_WORKING_DIRECTORY = os.path.dirname(
    recidiviz_pipelines_tests_module.__file__
)

FAKE_PIPELINE_CONFIG_YAML_PATH = os.path.join(
    PIPELINES_TESTS_WORKING_DIRECTORY,
    "fake_calculation_pipeline_templates.yaml",
)


@patch.dict(
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
    values={StateCode.US_XX: "us-east1", StateCode.US_YY: "us-east4"},
)
@pytest.mark.uses_db
class IngestDataflowOperations(TestCase):
    """Implements tests for get_all_latest_ingest_dataflow_jobs and helpers."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        super().setUp()

        self.latest_jobs_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_dataflow_operations.get_latest_job_for_state_instance"
        )
        self.latest_jobs_patcher.start()

        self.state_code_list_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_dataflow_operations.get_direct_ingest_states_launched_in_env",
            return_value=[
                StateCode.US_XX,
                StateCode.US_YY,
            ],
        )
        self.state_code_list_patcher.start()
        self.watermark_manager = DirectIngestDataflowWatermarkManager()
        self.job_manager = DirectIngestDataflowJobManager()
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.watermark_manager.database_key
        )
        self.raw_file_manager = DirectIngestRawFileMetadataManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )

    def tearDown(self) -> None:
        super().tearDown()
        self.state_code_list_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.watermark_manager.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
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
            name="us-xx-ingest",
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
            name="us-yy-ingest",
            create_time=datetime.datetime(2023, 10, 10).timestamp(),
            start_time=datetime.datetime(2023, 10, 10).timestamp(),
            termination_time=datetime.datetime(2023, 10, 11).timestamp(),
            termination_state="JOB_STATE_DONE",
            location="us-east1",
        )
        expected = {
            StateCode.US_XX: pipeline,
            StateCode.US_YY: pipeline3,
        }

        most_recent_job_id_map = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: (pipeline.location, pipeline.id),
                DirectIngestInstance.SECONDARY: (pipeline2.location, pipeline2.id),
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: (pipeline3.location, pipeline3.id),
                DirectIngestInstance.SECONDARY: (pipeline4.location, pipeline4.id),
            },
        }

        jobs_by_job_location_id: dict[
            DataflowJobLocationID, DataflowPipelineMetadataResponse
        ] = {
            (pipeline.location, pipeline.id): pipeline,
            (pipeline2.location, pipeline2.id): pipeline2,
            (pipeline3.location, pipeline3.id): pipeline3,
            (pipeline4.location, pipeline4.id): pipeline4,
        }

        def return_pipeline_by_id(
            state_code: StateCode,  # pylint: disable=unused-argument
            job: DataflowJobLocationID,
        ) -> Optional[DataflowPipelineMetadataResponse]:
            if job:
                return jobs_by_job_location_id[job]
            return None

        with (
            mock.patch(
                "recidiviz.admin_panel.ingest_dataflow_operations.DirectIngestDataflowJobManager.get_most_recent_jobs_location_and_id_by_state_and_instance",
                return_value=most_recent_job_id_map,
            ),
            mock.patch(
                "recidiviz.admin_panel.ingest_dataflow_operations.get_latest_job_for_state_instance",
                side_effect=return_pipeline_by_id,
            ),
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
            StateCode.US_XX: pipeline,
            StateCode.US_YY: pipeline2,
        }

        most_recent_job_id_map = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: pipeline.id,
                DirectIngestInstance.SECONDARY: None,
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: pipeline2.id,
                DirectIngestInstance.SECONDARY: None,
            },
        }

        jobs_by_id = {
            pipeline.id: pipeline,
            pipeline2.id: pipeline2,
        }

        def return_pipeline_by_id(
            state_code: StateCode,  # pylint: disable=unused-argument
            job_id: str,
        ) -> Optional[DataflowPipelineMetadataResponse]:
            if job_id:
                return jobs_by_id[job_id]
            return None

        with (
            mock.patch(
                "recidiviz.admin_panel.ingest_dataflow_operations.DirectIngestDataflowJobManager.get_most_recent_jobs_location_and_id_by_state_and_instance",
                return_value=most_recent_job_id_map,
            ),
            mock.patch(
                "recidiviz.admin_panel.ingest_dataflow_operations.get_latest_job_for_state_instance",
                side_effect=return_pipeline_by_id,
            ),
        ):
            self.assertEqual(expected, get_all_latest_ingest_jobs())

    def test_get_stale_raw_data_watermarks_for_latest_run_simple(self) -> None:
        # most recent job
        most_recent_job_id = "2020-10-01_00_00_00"
        self.job_manager.add_job(
            location="us-east1",
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 10, 1, 0, 0, 0),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="normal",
            watermark_datetime=datetime.datetime(2020, 8, 1, 0, 0, 0),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="normal2",
            watermark_datetime=datetime.datetime(2020, 6, 29, 6, 0, 30),
        )

        file_path = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2020-10-01T00:11:30:000000_raw_normal.csv"
        )
        metadata = self.raw_file_manager.mark_raw_gcs_file_as_discovered(file_path)
        self.raw_file_manager.mark_raw_big_query_file_as_processed(
            assert_type(metadata.file_id, int)
        )

        file_path1 = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2020-07-01T00:12:45:000000_raw_normal2.csv"
        )
        metadata1 = self.raw_file_manager.mark_raw_gcs_file_as_discovered(file_path1)
        self.raw_file_manager.mark_raw_big_query_file_as_processed(
            assert_type(metadata1.file_id, int)
        )

        # Act
        actual = get_raw_data_tags_not_meeting_watermark(
            state_code=StateCode.US_XX, ingest_instance=DirectIngestInstance.PRIMARY
        )

        # Assert
        self.assertCountEqual(
            actual,
            [],
        )

    def test_get_stale_raw_data_watermarks_for_latest_run_one_expired(self) -> None:
        # most recent job
        most_recent_job_id = "2020-10-01_00_00_00"
        self.job_manager.add_job(
            location="us-east1",
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 10, 1, 0, 0, 0),
        )

        self.watermark_manager.add_raw_data_watermark(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="non_expired",
            watermark_datetime=datetime.datetime(2020, 7, 1, 12, 0, 50),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="expired",
            watermark_datetime=datetime.datetime(2020, 7, 1, 10, 0, 0),
        )

        file_path = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2020-08-01T00:11:30:000000_raw_non_expired.csv"
        )
        metadata = self.raw_file_manager.mark_raw_gcs_file_as_discovered(file_path)
        self.raw_file_manager.mark_raw_big_query_file_as_processed(
            assert_type(metadata.file_id, int)
        )

        file_path1 = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2020-07-01T00:08:00:000000_raw_expired.csv"
        )
        metadata1 = self.raw_file_manager.mark_raw_gcs_file_as_discovered(file_path1)
        self.raw_file_manager.mark_raw_big_query_file_as_processed(
            assert_type(metadata1.file_id, int)
        )
        # Act
        actual = get_raw_data_tags_not_meeting_watermark(
            state_code=StateCode.US_XX, ingest_instance=DirectIngestInstance.PRIMARY
        )

        # Assert
        self.assertListEqual(
            actual,
            ["expired"],
        )

    def test_get_stale_raw_data_watermarks_for_latest_run_multiple(self) -> None:
        most_recent_job_id = "2020-10-01_00_00_00"
        self.job_manager.add_job(
            location="us-east1",
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            completion_time=datetime.datetime(2020, 10, 1, 0, 0, 0),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="expired",
            watermark_datetime=datetime.datetime(2020, 7, 1, 12, 0, 30),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="expired2",
            watermark_datetime=datetime.datetime(2020, 8, 1, 0, 0, 0),
        )
        self.watermark_manager.add_raw_data_watermark(
            job_id=most_recent_job_id,
            state_code=StateCode.US_XX,
            raw_data_file_tag="expired3",
            watermark_datetime=datetime.datetime(2020, 6, 1, 0, 0, 0),
        )

        file_path = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2020-07-01T00:11:30:000000_raw_expired.csv"
        )
        metadata = self.raw_file_manager.mark_raw_gcs_file_as_discovered(file_path)
        self.raw_file_manager.mark_raw_big_query_file_as_processed(
            assert_type(metadata.file_id, int)
        )

        file_path1 = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2020-07-01T00:08:00:000000_raw_expired2.csv"
        )
        metadata1 = self.raw_file_manager.mark_raw_gcs_file_as_discovered(file_path1)
        self.raw_file_manager.mark_raw_big_query_file_as_processed(
            assert_type(metadata1.file_id, int)
        )

        file_path2 = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2020-05-30T00:08:00:000000_raw_expired3.csv"
        )
        metadata2 = self.raw_file_manager.mark_raw_gcs_file_as_discovered(file_path2)
        self.raw_file_manager.mark_raw_big_query_file_as_processed(
            assert_type(metadata2.file_id, int)
        )
        # Act
        actual = get_raw_data_tags_not_meeting_watermark(
            state_code=StateCode.US_XX, ingest_instance=DirectIngestInstance.PRIMARY
        )

        # Assert
        self.assertCountEqual(
            actual,
            ["expired", "expired2", "expired3"],
        )
