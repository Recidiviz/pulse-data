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
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManagerImpl,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class IngestOperationsStoreTest(TestCase):
    """Implements tests for IngestOperationsStoreTest."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.operations_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_postgres_helpers.use_on_disk_postgresql_database(self.operations_key)

        self.us_xx_primary_status_manager = DirectIngestInstanceStatusManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )

        self.us_xx_secondary_status_manager = DirectIngestInstanceStatusManager(
            StateCode.US_XX.value,
            DirectIngestInstance.SECONDARY,
        )
        self.us_yy_primary_status_manager = DirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.PRIMARY,
        )

        self.us_yy_secondary_status_manager = DirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.SECONDARY,
        )

        self.state_code_list_patcher = mock.patch(
            "recidiviz.admin_panel.ingest_operations_store.get_direct_ingest_states_launched_in_env",
            return_value=[StateCode.US_XX, StateCode.US_YY],
        )

        self.state_code_list_patcher.start()

        self.fs_patcher = mock.patch.object(
            GcsfsFactory, "build", return_value=FakeGCSFileSystem()
        )
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
