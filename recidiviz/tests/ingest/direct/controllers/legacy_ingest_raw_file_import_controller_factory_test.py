# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for the LegacyIngestRawFileImportControllerFactory."""
import unittest

import pytest
from mock import Mock, patch

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct import templates
from recidiviz.ingest.direct.controllers import (
    legacy_ingest_raw_file_import_controller_factory,
)
from recidiviz.ingest.direct.controllers.legacy_ingest_raw_file_import_controller import (
    LegacyIngestRawFileImportController,
)
from recidiviz.ingest.direct.controllers.legacy_ingest_raw_file_import_controller_factory import (
    LegacyIngestRawFileImportControllerFactory,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_region_codes,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestGatingError,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers

CONTROLLER_FACTORY_PACKAGE_NAME = (
    legacy_ingest_raw_file_import_controller_factory.__name__
)


@pytest.mark.uses_db
class TestLegacyIngestRawFileImportControllerFactory(unittest.TestCase):
    """Tests for the LegacyIngestRawFileImportControllerFactory."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.operations_database_key = SQLAlchemyDatabaseKey.for_schema(
            SchemaType.OPERATIONS
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.operations_database_key
        )

        self.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value="recidiviz-456"
        )
        self.bq_client_patcher = patch("google.cloud.bigquery.Client")
        self.storage_client_patcher = patch("google.cloud.storage.Client")
        self.task_client_patcher = patch("google.cloud.tasks_v2.CloudTasksClient")

        self.raw_data_dag_patchers = [
            patch(path, Mock(return_value=False))
            for path in [
                f"{CONTROLLER_FACTORY_PACKAGE_NAME}.is_raw_data_import_dag_enabled",
                "recidiviz.ingest.direct.controllers.legacy_ingest_raw_file_import_controller.is_raw_data_import_dag_enabled",
            ]
        ]
        self.raw_data_dag_enabled_mock = [
            patcher.start() for patcher in self.raw_data_dag_patchers
        ]

        def mock_build_fs() -> FakeGCSFileSystem:
            return FakeGCSFileSystem()

        self.fs_patcher = patch.object(GcsfsFactory, "build", new=mock_build_fs)

        self.project_id_patcher.start()
        self.bq_client_patcher.start()
        self.storage_client_patcher.start()
        self.task_client_patcher.start()
        self.fs_patcher.start()

        # Seed the DB with initial statuses for all regions
        for region_code in get_existing_region_codes():
            for ingest_instance in DirectIngestInstance:
                DirectIngestInstanceStatusManager(
                    region_code=region_code,
                    ingest_instance=ingest_instance,
                ).add_initial_status()

    def tearDown(self) -> None:
        for patcher in self.raw_data_dag_patchers:
            patcher.stop()
        self.project_id_patcher.stop()
        self.bq_client_patcher.stop()
        self.storage_client_patcher.stop()
        self.task_client_patcher.stop()
        self.fs_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.operations_database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_build_gcsfs_ingest_controller_all_regions(self) -> None:
        for region_code in get_existing_region_codes():
            for ingest_instance in DirectIngestInstance:
                controller = LegacyIngestRawFileImportControllerFactory.build(
                    region_code=region_code,
                    ingest_instance=ingest_instance,
                    allow_unlaunched=False,
                )

                self.assertIsNotNone(controller)
                self.assertIsInstance(controller, LegacyIngestRawFileImportController)
                self.assertEqual(ingest_instance, controller.ingest_instance)

    def test_build_gcsfs_ingest_controller_all_regions_raw_import_secondary(
        self,
    ) -> None:
        for region_code in get_existing_region_codes():
            # Update so a reimport has now started in SECONDARY
            status_manager = DirectIngestInstanceStatusManager(
                region_code=region_code,
                ingest_instance=DirectIngestInstance.SECONDARY,
            )

            status_manager.add_instance_status(
                DirectIngestStatus.RAW_DATA_REIMPORT_STARTED
            )

            controller = LegacyIngestRawFileImportControllerFactory.build(
                region_code=region_code,
                ingest_instance=DirectIngestInstance.SECONDARY,
                allow_unlaunched=False,
            )

            self.assertIsNotNone(controller)
            self.assertIsInstance(controller, LegacyIngestRawFileImportController)
            self.assertEqual(DirectIngestInstance.SECONDARY, controller.ingest_instance)

    def test_build_gcsfs_ingest_controller_all_regions_do_not_allow_launched(
        self,
    ) -> None:
        for region_code in get_existing_region_codes():
            for ingest_instance in DirectIngestInstance:
                controller = LegacyIngestRawFileImportControllerFactory.build(
                    region_code=region_code,
                    ingest_instance=ingest_instance,
                    allow_unlaunched=True,
                )

                # Should still succeed for all controllers in the test environment
                self.assertIsNotNone(controller)
                self.assertIsInstance(controller, LegacyIngestRawFileImportController)
                self.assertEqual(ingest_instance, controller.ingest_instance)

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    @patch(
        f"{CONTROLLER_FACTORY_PACKAGE_NAME}.get_direct_ingest_states_existing_in_env",
        Mock(return_value=[StateCode.US_XX]),
    )
    def test_build_throws_in_prod_region_only_launched_in_staging(
        self,
    ) -> None:
        mock_region = fake_region(
            region_code="us_xx",
            environment="staging",
            region_module=templates,
        )
        with patch(
            "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region",
            Mock(return_value=mock_region),
        ):
            with self.assertRaisesRegex(
                DirectIngestError,
                r"^Bad environment \[production\] for region \[us_xx\].$",
            ):
                _ = LegacyIngestRawFileImportControllerFactory.build(
                    region_code=mock_region.region_code,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                    allow_unlaunched=False,
                )

    @patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="staging"),
    )
    @patch(
        f"{CONTROLLER_FACTORY_PACKAGE_NAME}.get_direct_ingest_states_existing_in_env",
        Mock(return_value=[StateCode.US_XX]),
    )
    def test_build_succeeds_in_staging_region_launched_in_prod(self) -> None:
        mock_region = fake_region(
            region_code="us_xx",
            environment="production",
            region_module=templates,
        )
        with patch(
            "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region",
            Mock(return_value=mock_region),
        ):
            controller = LegacyIngestRawFileImportControllerFactory.build(
                region_code=mock_region.region_code,
                ingest_instance=DirectIngestInstance.PRIMARY,
                allow_unlaunched=False,
            )
            self.assertIsNotNone(controller)
            self.assertIsInstance(controller, LegacyIngestRawFileImportController)
            self.assertEqual(DirectIngestInstance.PRIMARY, controller.ingest_instance)

    def test_build_for_unsupported_region_throws(self) -> None:
        with self.assertRaisesRegex(
            DirectIngestError,
            r"^Unsupported direct ingest region \[US_XX\] in project \[recidiviz-456\]$",
        ):
            _ = LegacyIngestRawFileImportControllerFactory.build(
                region_code="us_xx",
                ingest_instance=DirectIngestInstance.PRIMARY,
                allow_unlaunched=False,
            )

    @patch(
        f"{CONTROLLER_FACTORY_PACKAGE_NAME}.get_direct_ingest_states_existing_in_env",
        Mock(return_value=[StateCode.US_XX]),
    )
    def test_build_for_raw_data_dag_enabled(self) -> None:
        for mocked in self.raw_data_dag_enabled_mock:
            mocked.return_value = True

        with self.assertRaisesRegex(
            DirectIngestGatingError,
            r"^LegacyIngestRawFileImportControllerFactory for region \[US_XX\] and instance \[PRIMARY\] should not need to be called once raw data import DAG is active$",
        ):
            _ = LegacyIngestRawFileImportControllerFactory.build(
                region_code="us_xx",
                ingest_instance=DirectIngestInstance.PRIMARY,
                allow_unlaunched=False,
            )
