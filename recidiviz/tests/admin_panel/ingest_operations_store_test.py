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
from typing import Optional
from unittest import mock
from unittest.case import TestCase

import pytest
from google.cloud import tasks_v2
from mock import create_autospec

from recidiviz.admin_panel.ingest_operations_store import IngestOperationsStore
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManagerImpl,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_instance_status_manager import (
    PostgresDirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.tools.postgres import local_postgres_helpers
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
        local_postgres_helpers.use_on_disk_postgresql_database(self.operations_key)

        self.state_code_list_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_operations_store.get_direct_ingest_states_launched_in_env",
            return_value=[StateCode.US_XX, StateCode.US_YY],
        )

        self.state_code_list_patcher.start()

        self.fs = FakeGCSFileSystem()
        self.fs_patcher = mock.patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()

        self.task_manager_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_operations_store.DirectIngestCloudTaskManagerImpl",
            return_value=create_autospec(DirectIngestCloudTaskManagerImpl),
        )
        self.task_manager_patcher.start()

        self.cloud_task_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_operations_store.tasks_v2.CloudTasksClient",
            return_value=create_autospec(tasks_v2.CloudTasksClient),
        )
        self.cloud_task_patcher.start()

        self.operations_store = IngestOperationsStore()

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.operations_key)
        self.state_code_list_patcher.stop()
        self.fs_patcher.stop()
        self.task_manager_patcher.stop()
        self.cloud_task_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )


class IngestOperationsStoreGetAllCurrentIngestInstanceStatusesTest(
    IngestOperationsStoreTestBase
):
    """Implements tests for get_all_current_ingest_instance_statuses."""

    def setUp(self) -> None:
        super().setUp()

        self.us_xx_primary_status_manager = PostgresDirectIngestInstanceStatusManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )

        self.us_xx_secondary_status_manager = PostgresDirectIngestInstanceStatusManager(
            StateCode.US_XX.value,
            DirectIngestInstance.SECONDARY,
        )
        self.us_yy_primary_status_manager = PostgresDirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.PRIMARY,
        )

        self.us_yy_secondary_status_manager = PostgresDirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.SECONDARY,
        )

    # Arrange
    # ... set up state so that you can test what you want to test ...
    # Act
    # Assert
    def test_no_statuses(self) -> None:
        """
        Assert that when there are no statuses, a dictionary is created with None values
        """

        result = self.operations_store.get_all_current_ingest_instance_statuses()

        expected = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: None,
                DirectIngestInstance.SECONDARY: None,
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: None,
                DirectIngestInstance.SECONDARY: None,
            },
        }
        self.assertEqual(expected, result)

    def test_all_different_statuses(self) -> None:
        """
        Assert that the correct dictionary exists when all primary and secondary statuses
        are differnt
        """
        self.us_xx_primary_status_manager.add_instance_status(
            DirectIngestStatus.STANDARD_RERUN_STARTED
        )
        self.us_xx_secondary_status_manager.add_instance_status(
            DirectIngestStatus.UP_TO_DATE
        )
        self.us_yy_primary_status_manager.add_instance_status(
            DirectIngestStatus.FLASH_IN_PROGRESS
        )
        self.us_yy_secondary_status_manager.add_instance_status(
            DirectIngestStatus.FLASH_COMPLETED
        )

        dif_statuses = self.operations_store.get_all_current_ingest_instance_statuses()

        expected = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestInstance.SECONDARY: DirectIngestStatus.UP_TO_DATE,
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: DirectIngestStatus.FLASH_IN_PROGRESS,
                DirectIngestInstance.SECONDARY: DirectIngestStatus.FLASH_COMPLETED,
            },
        }
        self.assertEqual(expected, dif_statuses)

    def test_primary_status_set_no_secondary(self) -> None:
        """
        Assert that no secondary status exists when only primary is set
        """
        self.us_xx_primary_status_manager.add_instance_status(
            DirectIngestStatus.STANDARD_RERUN_STARTED
        )
        self.us_yy_primary_status_manager.add_instance_status(
            DirectIngestStatus.FLASH_IN_PROGRESS
        )

        primary_statuses = (
            self.operations_store.get_all_current_ingest_instance_statuses()
        )

        expected = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestInstance.SECONDARY: None,
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: DirectIngestStatus.FLASH_IN_PROGRESS,
                DirectIngestInstance.SECONDARY: None,
            },
        }
        self.assertEqual(expected, primary_statuses)

    def test_secondary_status_set_no_primary(self) -> None:
        """
        Assert that no secondary status exists when only primary is set
        """
        self.us_xx_secondary_status_manager.add_instance_status(
            DirectIngestStatus.STANDARD_RERUN_STARTED
        )
        self.us_yy_secondary_status_manager.add_instance_status(
            DirectIngestStatus.FLASH_IN_PROGRESS
        )

        secondary_statuses = (
            self.operations_store.get_all_current_ingest_instance_statuses()
        )

        expected = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: None,
                DirectIngestInstance.SECONDARY: DirectIngestStatus.STANDARD_RERUN_STARTED,
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: None,
                DirectIngestInstance.SECONDARY: DirectIngestStatus.FLASH_IN_PROGRESS,
            },
        }
        self.assertEqual(expected, secondary_statuses)

    def test_this_state_not_that_state(self) -> None:
        """
        Assert that when one state is set no other state is set
        """
        self.us_yy_primary_status_manager.add_instance_status(
            DirectIngestStatus.READY_TO_FLASH
        )
        self.us_yy_secondary_status_manager.add_instance_status(
            DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS
        )

        one_state = self.operations_store.get_all_current_ingest_instance_statuses()

        expected = {
            StateCode.US_XX: {
                DirectIngestInstance.PRIMARY: None,
                DirectIngestInstance.SECONDARY: None,
            },
            StateCode.US_YY: {
                DirectIngestInstance.PRIMARY: DirectIngestStatus.READY_TO_FLASH,
                DirectIngestInstance.SECONDARY: DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
            },
        }
        self.assertEqual(expected, one_state)


class IngestOperationsStoreRawFileProcessingStatusTest(IngestOperationsStoreTestBase):
    """Implements tests for IngestOperationsStore get_ingest_raw_file_processing_status."""

    def setUp(self) -> None:
        super().setUp()

        self.us_xx_expected_file_tags = DirectIngestRegionRawFileConfig(
            region_code=StateCode.US_XX.value,
            region_module=fake_regions,
        ).raw_file_tags

        self.region_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_operations_store.get_region",
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
        manager = PostgresDirectIngestRawFileMetadataManager(
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
                break

    def test_get_ingest_file_processing_status_returns_processed_list(self) -> None:
        manager = PostgresDirectIngestRawFileMetadataManager(
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
                break

    def test_get_ingest_file_processing_status_returns_list_with_files_in_bucket(
        self,
    ) -> None:
        manager = PostgresDirectIngestRawFileMetadataManager(
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
                break

    def test_get_ingest_file_processing_status_returns_list_multiple_file_tags(
        self,
    ) -> None:
        manager = PostgresDirectIngestRawFileMetadataManager(
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
