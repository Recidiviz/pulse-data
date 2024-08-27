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

"""Implements tests for the IngestOperationsStore."""
import unittest
from datetime import datetime
from typing import Dict, Optional
from unittest import mock
from unittest.case import TestCase
from unittest.mock import patch

import attr
import pytest
import pytz
from fakeredis import FakeRedis
from freezegun import freeze_time
from google.cloud import tasks_v2
from mock import create_autospec

from recidiviz.admin_panel.ingest_dataflow_operations import (
    DataflowPipelineMetadataResponse,
)
from recidiviz.admin_panel.ingest_operations_store import IngestOperationsStore
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    DirectIngestCloudTaskQueueManagerImpl,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import DirectIngestInstanceStatus
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


@pytest.mark.uses_db
class IngestOperationsStoreTestBase(TestCase):
    """Base Class to Implement tests for IngestOperationsStoreTest."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.operations_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(self.operations_key)

        self.state_code_list_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_operations_store.get_direct_ingest_states_launched_in_env",
            return_value=[StateCode.US_XX, StateCode.US_YY],
        )

        self.state_code_list_patcher.start()

        self.fs = FakeGCSFileSystem()
        self.fs_patcher = mock.patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()

        self.task_manager_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_operations_store.DirectIngestCloudTaskQueueManagerImpl",
            return_value=create_autospec(DirectIngestCloudTaskQueueManagerImpl),
        )
        self.task_manager_patcher.start()

        self.cloud_task_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_operations_store.tasks_v2.CloudTasksClient",
            return_value=create_autospec(tasks_v2.CloudTasksClient),
        )
        self.cloud_task_patcher.start()

        self.bq_client_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_operations_store.BigQueryClientImpl",
            return_value=create_autospec(BigQueryClientImpl),
        )
        self.bq_client_patcher.start()

        self.operations_store = IngestOperationsStore()

        self.redis_patcher = mock.patch(
            "recidiviz.admin_panel.admin_panel_store.get_admin_panel_redis"
        )
        self.mock_redis_patcher = self.redis_patcher.start()
        self.mock_redis_patcher.return_value = FakeRedis()

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.operations_key
        )
        self.state_code_list_patcher.stop()
        self.fs_patcher.stop()
        self.task_manager_patcher.stop()
        self.cloud_task_patcher.stop()
        self.bq_client_patcher.stop()
        self.mock_redis_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )


class IngestOperationsStoreGetAllCurrentIngestInstanceStatusesTest(
    IngestOperationsStoreTestBase
):
    """Implements tests for get_all_current_ingest_instance_statuses."""

    @freeze_time(datetime(2022, 8, 29, tzinfo=pytz.UTC))
    def test_all_different_statuses(self) -> None:
        """
        Assert that the correct dictionary exists when all primary and secondary statuses
        are different
        """

        us_xx_primary_status_manager = DirectIngestInstanceStatusManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )

        us_xx_secondary_status_manager = DirectIngestInstanceStatusManager(
            StateCode.US_XX.value,
            DirectIngestInstance.SECONDARY,
        )
        us_yy_primary_status_manager = DirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.PRIMARY,
        )

        us_yy_secondary_status_manager = DirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.SECONDARY,
        )

        timestamp = datetime(2022, 8, 29, tzinfo=pytz.UTC)
        us_xx_primary_status_manager.add_instance_status(
            DirectIngestStatus.INITIAL_STATE
        )
        us_xx_secondary_status_manager.add_instance_status(
            DirectIngestStatus.READY_TO_FLASH
        )
        us_yy_primary_status_manager.add_instance_status(
            DirectIngestStatus.FLASH_IN_PROGRESS
        )
        us_yy_secondary_status_manager.add_instance_status(
            DirectIngestStatus.FLASH_COMPLETED
        )

        dif_statuses = self.operations_store.get_all_current_ingest_instance_statuses()

        expected = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_XX.value,
                    instance=DirectIngestInstance.PRIMARY,
                    status=DirectIngestStatus.INITIAL_STATE,
                    status_timestamp=timestamp,
                ),
                DirectIngestInstance.SECONDARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_XX.value,
                    instance=DirectIngestInstance.SECONDARY,
                    status=DirectIngestStatus.READY_TO_FLASH,
                    status_timestamp=timestamp,
                ),
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_YY.value,
                    instance=DirectIngestInstance.PRIMARY,
                    status=DirectIngestStatus.FLASH_IN_PROGRESS,
                    status_timestamp=timestamp,
                ),
                DirectIngestInstance.SECONDARY: DirectIngestInstanceStatus(
                    region_code=StateCode.US_YY.value,
                    instance=DirectIngestInstance.SECONDARY,
                    status=DirectIngestStatus.FLASH_COMPLETED,
                    status_timestamp=timestamp,
                ),
            },
        }
        self.assertEqual(expected, dif_statuses)


class IngestOperationsStoreRawFileProcessingStatusTest(IngestOperationsStoreTestBase):
    """Implements tests for IngestOperationsStore get_ingest_raw_file_processing_status."""

    def setUp(self) -> None:
        super().setUp()

        self.us_xx_expected_file_tags = DirectIngestRegionRawFileConfig(
            region_code=StateCode.US_XX.value,
            region_module=fake_regions,
        ).raw_file_tags

        self.region_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_operations_store.get_direct_ingest_region",
            return_value=fake_region(
                region_code=StateCode.US_XX.value.lower(),
                environment="staging",
                region_module=fake_regions,
            ),
        )
        self.region_patcher.start()

    def tearDown(self) -> None:
        self.region_patcher.stop()
        super().tearDown()

    def test_get_ingest_file_processing_status_returns_expected_list(self) -> None:
        manager = DirectIngestRawFileMetadataManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )
        manager.mark_raw_file_as_discovered(
            GcsfsFilePath.from_absolute_path(
                "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2022-01-24T00:00:00:000000_raw_tagBasicData.csv"
            )
        )

        with local_project_id_override(GCP_PROJECT_STAGING):
            result = self.operations_store.get_ingest_raw_file_processing_status(
                StateCode.US_XX, DirectIngestInstance.PRIMARY
            )

        self.assertEqual(len(self.us_xx_expected_file_tags), len(result))
        self.assertIn("tagBasicData", [x["fileTag"] for x in result])
        for status in result:
            if status["fileTag"] == "tagBasicData":
                self.assertTrue(status["hasConfig"])
                self.assertEqual(status["numberFilesInBucket"], 0)
                self.assertEqual(status["numberUnprocessedFiles"], 1)
                self.assertEqual(status["numberProcessedFiles"], 0)
                self.assertEqual(status["fileTag"], "tagBasicData")
                self.assertIsNotNone(status["latestDiscoveryTime"])
                self.assertIsNone(status["latestProcessedTime"])
                self.assertIsNone(status["latestUpdateDatetime"])
                self.assertIsNotNone(status["isStale"])
                break

    def test_get_ingest_file_processing_status_returns_processed_list(self) -> None:
        manager = DirectIngestRawFileMetadataManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )
        file_path = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2022-01-24T00:00:00:000000_raw_tagBasicData.csv"
        )
        file_path2 = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2022-02-24T00:00:00:000000_raw_tagBasicData.csv"
        )
        manager.mark_raw_file_as_discovered(file_path)
        manager.mark_raw_file_as_discovered(file_path2)
        manager.mark_raw_file_as_processed(file_path)

        with local_project_id_override(GCP_PROJECT_STAGING):
            result = self.operations_store.get_ingest_raw_file_processing_status(
                StateCode.US_XX, DirectIngestInstance.PRIMARY
            )

        self.assertEqual(len(self.us_xx_expected_file_tags), len(result))
        self.assertIn("tagBasicData", [x["fileTag"] for x in result])
        for status in result:
            if status["fileTag"] == "tagBasicData":
                self.assertTrue(status["hasConfig"])
                self.assertEqual(status["numberFilesInBucket"], 0)
                self.assertEqual(status["numberUnprocessedFiles"], 1)
                self.assertEqual(status["numberProcessedFiles"], 1)
                self.assertEqual(status["fileTag"], "tagBasicData")
                self.assertIsNotNone(status["latestDiscoveryTime"])
                self.assertIsNotNone(status["latestProcessedTime"])
                self.assertIsNotNone(status["latestUpdateDatetime"])
                self.assertIsNotNone(status["isStale"])
                break

    def test_get_ingest_file_processing_status_returns_list_with_files_in_bucket(
        self,
    ) -> None:
        manager = DirectIngestRawFileMetadataManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )
        file_path = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2022-01-24T00:00:00:000000_raw_tagBasicData.csv"
        )
        file_path2 = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2022-02-24T00:00:00:000000_raw_tagBasicData.csv"
        )
        manager.mark_raw_file_as_discovered(file_path)
        manager.mark_raw_file_as_processed(file_path)

        manager.mark_raw_file_as_discovered(file_path2)
        self.fs.test_add_path(path=file_path2, local_path=None)

        with local_project_id_override(GCP_PROJECT_STAGING):
            result = self.operations_store.get_ingest_raw_file_processing_status(
                StateCode.US_XX, DirectIngestInstance.PRIMARY
            )

        self.assertEqual(len(self.us_xx_expected_file_tags), len(result))
        self.assertIn("tagBasicData", [x["fileTag"] for x in result])
        for status in result:
            if status["fileTag"] == "tagBasicData":
                self.assertTrue(status["hasConfig"])
                self.assertEqual(status["numberFilesInBucket"], 1)
                self.assertEqual(status["numberUnprocessedFiles"], 1)
                self.assertEqual(status["numberProcessedFiles"], 1)
                self.assertEqual(status["fileTag"], "tagBasicData")
                self.assertIsNotNone(status["latestDiscoveryTime"])
                self.assertIsNotNone(status["latestProcessedTime"])
                self.assertIsNotNone(status["latestUpdateDatetime"])
                self.assertIsNotNone(status["isStale"])
                break

    def test_get_ingest_file_processing_status_returns_list_multiple_file_tags(
        self,
    ) -> None:
        manager = DirectIngestRawFileMetadataManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )
        file_path = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2022-01-24T00:00:00:000000_raw_tagBasicData.csv"
        )
        file_path2 = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2022-02-24T00:00:00:000000_raw_tagMoreBasicData.csv"
        )
        manager.mark_raw_file_as_discovered(file_path)
        manager.mark_raw_file_as_discovered(file_path2)

        with local_project_id_override(GCP_PROJECT_STAGING):
            result = self.operations_store.get_ingest_raw_file_processing_status(
                StateCode.US_XX, DirectIngestInstance.PRIMARY
            )

        self.assertEqual(len(self.us_xx_expected_file_tags), len(result))
        self.assertIn("tagBasicData", [x["fileTag"] for x in result])
        self.assertIn("tagMoreBasicData", [x["fileTag"] for x in result])
        for status in result:
            if status["fileTag"] == "tagBasicData":
                self.assertTrue(status["hasConfig"])
                self.assertEqual(status["numberUnprocessedFiles"], 1)
                self.assertEqual(status["numberProcessedFiles"], 0)
                self.assertEqual(status["fileTag"], "tagBasicData")
            elif status["fileTag"] == "tagMoreBasicData":
                self.assertTrue(status["hasConfig"])
                self.assertEqual(status["numberUnprocessedFiles"], 1)
                self.assertEqual(status["numberProcessedFiles"], 0)
                self.assertEqual(status["fileTag"], "tagMoreBasicData")

    def test_get_ingest_file_processing_status_returns_list_with_unrecognized_files_in_bucket(
        self,
    ) -> None:
        file_path = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2022-01-24T00:00:00:000000_raw_UNRECOGNIZED_FILE_TAG.csv"
        )
        file_path2 = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/unprocessed_2022-02-24T00:00:00:000000_raw_UNRECOGNIZED_FILE_TAG.csv"
        )
        self.fs.test_add_path(path=file_path, local_path=None)
        self.fs.test_add_path(path=file_path2, local_path=None)

        with local_project_id_override(GCP_PROJECT_STAGING):
            result = self.operations_store.get_ingest_raw_file_processing_status(
                StateCode.US_XX, DirectIngestInstance.PRIMARY
            )

        self.assertEqual(len(self.us_xx_expected_file_tags) + 1, len(result))
        self.assertIn("UNRECOGNIZED_FILE_TAG", [x["fileTag"] for x in result])
        for status in result:
            if status["fileTag"] == "UNRECOGNIZED_FILE_TAG":
                self.assertFalse(status["hasConfig"])
                self.assertEqual(2, status["numberFilesInBucket"])
                self.assertEqual(0, status["numberUnprocessedFiles"])
                self.assertEqual(0, status["numberProcessedFiles"])
                self.assertEqual(status["fileTag"], "UNRECOGNIZED_FILE_TAG")
                self.assertIsNone(status["latestDiscoveryTime"])
                self.assertIsNone(status["latestProcessedTime"])
                self.assertIsNone(status["latestUpdateDatetime"])
                self.assertFalse(result[0]["isStale"])

    def test_get_ingest_file_processing_status_returns_list_with_secondary_instance(
        self,
    ) -> None:
        with local_project_id_override(GCP_PROJECT_STAGING):
            result = self.operations_store.get_ingest_raw_file_processing_status(
                StateCode.US_XX, DirectIngestInstance.SECONDARY
            )

        self.assertEqual(len(self.us_xx_expected_file_tags), len(result))
        self.assertTrue(result[0]["hasConfig"])
        self.assertEqual(result[0]["numberFilesInBucket"], 0)
        self.assertEqual(result[0]["numberUnprocessedFiles"], 0)
        self.assertEqual(result[0]["numberProcessedFiles"], 0)
        self.assertIsNotNone(result[0]["fileTag"])
        self.assertIsNone(result[0]["latestDiscoveryTime"])
        self.assertIsNone(result[0]["latestProcessedTime"])
        self.assertIsNone(result[0]["latestUpdateDatetime"])
        self.assertFalse(result[0]["isStale"])

    def test_get_ingest_file_processing_status_catches_file_in_subdir(
        self,
    ) -> None:
        file_path = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/subdir/unprocessed_2022-01-24T00:00:00:000000_raw_UNRECOGNIZED_FILE_TAG.csv"
        )
        file_path2 = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/another_subdir/unprocessed_2022-02-24T00:00:00:000000_raw_UNRECOGNIZED_FILE_TAG.csv"
        )
        self.fs.test_add_path(path=file_path, local_path=None)
        self.fs.test_add_path(path=file_path2, local_path=None)

        with local_project_id_override(GCP_PROJECT_STAGING):
            result = self.operations_store.get_ingest_raw_file_processing_status(
                StateCode.US_XX, DirectIngestInstance.PRIMARY
            )

        self.assertEqual(len(self.us_xx_expected_file_tags) + 1, len(result))
        self.assertIn("IGNORED_IN_SUBDIRECTORY", [x["fileTag"] for x in result])
        for status in result:
            if status["fileTag"] == "IGNORED_IN_SUBDIRECTORY":
                self.assertFalse(status["hasConfig"])
                self.assertEqual(2, status["numberFilesInBucket"])
                self.assertEqual(0, status["numberUnprocessedFiles"])
                self.assertEqual(0, status["numberProcessedFiles"])
                self.assertEqual(status["fileTag"], "IGNORED_IN_SUBDIRECTORY")
                self.assertIsNone(status["latestDiscoveryTime"])
                self.assertIsNone(status["latestProcessedTime"])
                self.assertIsNone(status["latestUpdateDatetime"])
                self.assertFalse(status["isStale"])

    def test_get_ingest_file_processing_status_catches_unnormalized_file(
        self,
    ) -> None:
        file_path = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/random_state_file.csv"
        )
        file_path2 = GcsfsFilePath.from_absolute_path(
            "recidiviz-staging-direct-ingest-state-us-xx/another_random_state_file.csv"
        )
        self.fs.test_add_path(path=file_path, local_path=None)
        self.fs.test_add_path(path=file_path2, local_path=None)

        with local_project_id_override(GCP_PROJECT_STAGING):
            result = self.operations_store.get_ingest_raw_file_processing_status(
                StateCode.US_XX, DirectIngestInstance.PRIMARY
            )

        self.assertEqual(len(self.us_xx_expected_file_tags) + 1, len(result))
        self.assertIn("UNNORMALIZED", [x["fileTag"] for x in result])
        for status in result:
            if status["fileTag"] == "UNNORMALIZED":
                self.assertFalse(status["hasConfig"])
                self.assertEqual(2, status["numberFilesInBucket"])
                self.assertEqual(0, status["numberUnprocessedFiles"])
                self.assertEqual(0, status["numberProcessedFiles"])
                self.assertEqual(status["fileTag"], "UNNORMALIZED")
                self.assertIsNone(status["latestDiscoveryTime"])
                self.assertIsNone(status["latestProcessedTime"])
                self.assertIsNone(status["latestUpdateDatetime"])
                self.assertFalse(status["isStale"])


class IngestOperationsStoreCachingTest(IngestOperationsStoreTestBase):
    """Tests for Redis caching behavior in the ingest admin panel store."""

    US_XX_PIPELINE_INFO_1 = DataflowPipelineMetadataResponse(
        id="1234",
        project_id="test-project",
        name="us-xx-ingest",
        create_time=datetime(2023, 7, 10).timestamp(),
        start_time=datetime(2023, 7, 10).timestamp(),
        termination_time=datetime(2023, 7, 11).timestamp(),
        termination_state="JOB_STATE_FAILED",
        location="us-east1",
    )

    @patch("recidiviz.admin_panel.ingest_operations_store.get_all_latest_ingest_jobs")
    def test_get_most_recent_dataflow_jobs_no_cache(
        self, mock_get_latest_jobs: mock.MagicMock
    ) -> None:
        # Arrange
        latest_jobs = {
            StateCode.US_XX: self.US_XX_PIPELINE_INFO_1,
            StateCode.US_YY: None,
        }
        mock_get_latest_jobs.return_value = latest_jobs
        # Act
        statuses_map = self.operations_store.get_most_recent_dataflow_job_statuses()

        # Assert
        mock_get_latest_jobs.assert_called_once()
        self.assertEqual(latest_jobs, statuses_map)

    @patch("recidiviz.admin_panel.ingest_operations_store.get_all_latest_ingest_jobs")
    def test_get_most_recent_dataflow_jobs_with_cache(
        self, mock_get_latest_jobs: mock.MagicMock
    ) -> None:
        # Arrange
        latest_jobs: Dict[StateCode, Optional[DataflowPipelineMetadataResponse],] = {
            StateCode.US_XX: self.US_XX_PIPELINE_INFO_1,
            StateCode.US_YY: None,
        }

        # Act
        self.operations_store.set_cache(latest_jobs=latest_jobs)
        statuses_map = self.operations_store.get_most_recent_dataflow_job_statuses()

        # Assert
        mock_get_latest_jobs.assert_not_called()
        self.assertEqual(latest_jobs, statuses_map)

    @patch("recidiviz.admin_panel.ingest_operations_store.get_all_latest_ingest_jobs")
    def test_get_most_recent_dataflow_jobs_with_cache_missing_a_state(
        self, mock_get_latest_jobs: mock.MagicMock
    ) -> None:
        # Arrange
        latest_jobs: Dict[StateCode, Optional[DataflowPipelineMetadataResponse]] = {
            StateCode.US_XX: self.US_XX_PIPELINE_INFO_1,
        }

        # Act
        self.operations_store.set_cache(latest_jobs=latest_jobs)
        statuses_map = self.operations_store.get_most_recent_dataflow_job_statuses()

        # Assert
        mock_get_latest_jobs.assert_not_called()

        expected = {
            StateCode.US_XX: self.US_XX_PIPELINE_INFO_1,
            # None values filled in for US_YY which is brand new since the last time we
            # cached.
            StateCode.US_YY: None,
        }
        self.assertEqual(expected, statuses_map)


class FileStalenessTest(unittest.TestCase):
    """Tests for calculating file staleness"""

    def setUp(self) -> None:
        self.file_config = DirectIngestRawFileConfig(
            state_code=StateCode.US_XX,
            file_tag="myFile",
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[],
            custom_line_terminator=None,
            primary_key_cols=[],
            supplemental_order_by_clause="",
            encoding="UTF-8",
            separator=",",
            ignore_quotes=False,
            always_historical_export=True,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
            is_code_file=False,
        )

    def test_weekly_file_stale(self) -> None:
        with freeze_time(datetime(year=2020, month=1, day=3, tzinfo=pytz.UTC)):
            discovery_time = datetime(year=2020, month=1, day=1, tzinfo=pytz.UTC)
            self.assertFalse(
                IngestOperationsStore.calculate_if_file_is_stale(
                    discovery_time, self.file_config
                )
            )

        with freeze_time(datetime(year=2020, month=1, day=3, tzinfo=pytz.UTC)):
            discovery_time = datetime(year=2019, month=12, day=25, tzinfo=pytz.UTC)
            self.assertTrue(
                IngestOperationsStore.calculate_if_file_is_stale(
                    discovery_time, self.file_config
                )
            )

    def test_daily_file_stale(self) -> None:
        daily_config = attr.evolve(
            self.file_config, update_cadence=RawDataFileUpdateCadence.DAILY
        )
        with freeze_time(datetime(year=2020, month=1, day=3, tzinfo=pytz.UTC)):
            discovery_time = datetime(year=2020, month=1, day=2, tzinfo=pytz.UTC)
            self.assertFalse(
                IngestOperationsStore.calculate_if_file_is_stale(
                    discovery_time, daily_config
                )
            )

        with freeze_time(datetime(year=2020, month=1, day=3, tzinfo=pytz.UTC)):
            discovery_time = datetime(year=2019, month=1, day=1, tzinfo=pytz.UTC)
            self.assertTrue(
                IngestOperationsStore.calculate_if_file_is_stale(
                    discovery_time, daily_config
                )
            )

    def test_irregular_file_stale(self) -> None:
        irregular_config = attr.evolve(
            self.file_config, update_cadence=RawDataFileUpdateCadence.IRREGULAR
        )
        with freeze_time(datetime(year=2020, month=1, day=3, tzinfo=pytz.UTC)):
            discovery_time = datetime(year=2020, month=1, day=2, tzinfo=pytz.UTC)
            self.assertFalse(
                IngestOperationsStore.calculate_if_file_is_stale(
                    discovery_time, irregular_config
                )
            )

        with freeze_time(datetime(year=2020, month=1, day=3, tzinfo=pytz.UTC)):
            discovery_time = datetime(year=2012, month=1, day=1, tzinfo=pytz.UTC)
            self.assertFalse(
                IngestOperationsStore.calculate_if_file_is_stale(
                    discovery_time, irregular_config
                )
            )
