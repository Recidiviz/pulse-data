#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================

"""Tests for operations/dao.py."""

import datetime
from datetime import timedelta
from typing import Optional
from unittest import TestCase
from unittest.mock import patch

import pytest
import pytz
from freezegun import freeze_time

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import dao, schema
from recidiviz.persistence.database.schema.operations.dao import (
    stale_secondary_raw_data,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.big_query.fakes.fake_direct_ingest_instance_status_manager import (
    FakeDirectIngestInstanceStatusManager,
)
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class TestDao(TestCase):
    """Test that the methods in dao.py correctly read from the SQL database."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(
            SchemaType.OPERATIONS
        )
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    # TODO(#14198): move operations/dao.py functionality into DirectIngestRawFileMetadataManager and migrate tests
    # to postgres_direct_ingest_file_metadata_manager_test.

    def test_get_raw_file_metadata_for_file_id(self) -> None:
        self.add_raw_file_metadata(
            file_id=10,
            region_code="US_XX",
            file_tag="file_tag_1",
            normalized_file_name="normalized_file_tag_1",
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            # If this call doesn't raise an exception, it means that the correct metadata row was retrieved.
            metadata = dao.get_raw_file_metadata_for_file_id(
                session=session,
                region_code="US_XX",
                file_id=10,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
            self.assertEqual(metadata.file_id, 10)

    def test_raw_file_metadata_mark_as_invalidated(self) -> None:
        self.add_raw_file_metadata(
            file_id=10,
            region_code="US_XX",
            file_tag="file_tag_1",
            normalized_file_name="normalized_file_tag_1",
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            dao.mark_raw_file_as_invalidated(
                session=session,
                region_code="US_XX",
                file_id=10,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
            metadata = dao.get_raw_file_metadata_for_file_id(
                session=session,
                region_code="US_XX",
                file_id=10,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
            self.assertEqual(metadata.is_invalidated, True)

    def add_raw_file_metadata(
        self, file_id: int, region_code: str, file_tag: str, normalized_file_name: str
    ) -> None:
        """Used for testing purposes. Add a new row in the direct_ingest_raw_file_metadata table using the
        parameters."""
        with SessionFactory.using_database(self.database_key) as session:
            now = datetime.datetime.now(tz=pytz.UTC)
            session.add(
                schema.DirectIngestRawFileMetadata(
                    region_code=region_code,
                    file_tag=file_tag,
                    file_id=file_id,
                    normalized_file_name=normalized_file_name,
                    file_discovery_time=now,
                    file_processed_time=None,
                    update_datetime=now,
                    raw_data_instance=DirectIngestInstance.PRIMARY.value,
                    is_invalidated=False,
                )
            )

    def test_stale_secondary_raw_data_no_primary_file_after_frozen_discovery_date(
        self,
    ) -> None:
        discovery_date = datetime.datetime(2022, 7, 1, 1, 2, 3, 0, tzinfo=pytz.UTC)
        with freeze_time(discovery_date):
            with patch(
                "recidiviz.persistence.database.schema.operations.dao.PostgresDirectIngestInstanceStatusManager",
            ) as instance_status_manager_cls:
                instance_status_manager_cls.return_value = (
                    FakeDirectIngestInstanceStatusManager(
                        region_code="us_xx",
                        ingest_instance=DirectIngestInstance.PRIMARY,
                        initial_statuses=[DirectIngestStatus.STANDARD_RERUN_STARTED],
                    )
                )

                normalized_path_str = to_normalized_unprocessed_raw_file_path(
                    original_file_path="bucket/file_tag.csv", dt=discovery_date
                )
                raw_unprocessed_path_1 = GcsfsFilePath.from_absolute_path(
                    normalized_path_str
                )

                raw_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
                    region_code="us_xx",
                    raw_data_instance=DirectIngestInstance.PRIMARY,
                )

                # Mark the file as discovered in PRIMARY on frozen `discovery_date`
                raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)

                # Assert that secondary raw data is not stale, because no file in PRIMARY is found after
                # `discovery_date`
                self.assertFalse(stale_secondary_raw_data("us_xx"))

    def test_stale_secondary_raw_data_rerun_start_before_discovery_present_in_both(
        self,
    ) -> None:
        discovery_date = datetime.datetime(2022, 7, 1, 1, 2, 3, 0, tzinfo=pytz.UTC)
        rerun_start_before_discovery_date = discovery_date - timedelta(hours=1)

        with freeze_time(discovery_date):
            # Discover the same file_tag in both PRIMARY and SECONDARY at the `discovery_date`
            normalized_path_str = to_normalized_unprocessed_raw_file_path(
                original_file_path="bucket/file_tag.csv", dt=discovery_date
            )
            raw_unprocessed_path_1 = GcsfsFilePath.from_absolute_path(
                normalized_path_str
            )

            raw_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
                region_code="us_xx",
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
            raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)

            secondary_normalized_path_str = to_normalized_unprocessed_raw_file_path(
                original_file_path="secondary_bucket/file_tag.csv", dt=discovery_date
            )
            secondary_raw_unprocessed_path_1 = GcsfsFilePath.from_absolute_path(
                secondary_normalized_path_str
            )

            secondary_raw_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
                region_code="us_xx",
                raw_data_instance=DirectIngestInstance.SECONDARY,
            )
            secondary_raw_metadata_manager.mark_raw_file_as_discovered(
                secondary_raw_unprocessed_path_1
            )

        with freeze_time(rerun_start_before_discovery_date):
            # Start a secondary raw data import rerun BEFORE the discovery_date
            fake_manager = FakeDirectIngestInstanceStatusManager(
                region_code="us_xx",
                ingest_instance=DirectIngestInstance.SECONDARY,
                initial_statuses=[
                    DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED
                ],
            )
            with patch(
                "recidiviz.persistence.database.schema.operations.dao.PostgresDirectIngestInstanceStatusManager",
                return_value=fake_manager,
            ):
                # Assert that raw data is not considered stale because the same file_tag is present and non-invalidated
                # in both instances, and the rerun in secondary started BEFORE the files were discovered in both
                # PRIMARY and SECONDARY
                self.assertFalse(stale_secondary_raw_data("us_xx"))

    def test_stale_secondary_raw_data_standard_secondary_rerun(
        self,
    ) -> None:
        discovery_date = datetime.datetime(2022, 7, 1, 1, 2, 3, 0)
        rerun_start_before_discovery_date = discovery_date - timedelta(hours=1)

        with freeze_time(discovery_date):
            # Discover a file_tag in PRIMARY
            normalized_path_str = to_normalized_unprocessed_raw_file_path(
                original_file_path="bucket/file_tag.csv", dt=discovery_date
            )
            raw_unprocessed_path_1 = GcsfsFilePath.from_absolute_path(
                normalized_path_str
            )

            raw_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
                region_code="us_xx",
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
            raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)

        with freeze_time(rerun_start_before_discovery_date):
            # Start a secondary standard BEFORE the discovery_date
            fake_manager = FakeDirectIngestInstanceStatusManager(
                region_code="us_xx",
                ingest_instance=DirectIngestInstance.SECONDARY,
                initial_statuses=[DirectIngestStatus.STANDARD_RERUN_STARTED],
            )
            with patch(
                "recidiviz.persistence.database.schema.operations.dao.PostgresDirectIngestInstanceStatusManager",
                return_value=fake_manager,
            ):
                # Assert that raw data is not considered stale because there isn't a secondary raw data import in
                # progress in SECONDARY
                self.assertFalse(stale_secondary_raw_data("us_xx"))

    def test_stale_secondary_raw_data_rerun_start_before_discovery_present_in_both_invalidated_in_secondary(
        self,
    ) -> None:
        discovery_date = datetime.datetime(2022, 7, 1, tzinfo=pytz.UTC)
        rerun_start_before_discovery_date = discovery_date - timedelta(hours=1)

        with freeze_time(discovery_date):
            # Discover the same file_tag in both PRIMARY and SECONDARY at the same time
            normalized_path_str = to_normalized_unprocessed_raw_file_path(
                original_file_path="bucket/file_tag.csv", dt=discovery_date
            )
            raw_unprocessed_path_1 = GcsfsFilePath.from_absolute_path(
                normalized_path_str
            )

            raw_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
                region_code="us_xx",
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
            raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)

            secondary_normalized_path_str = to_normalized_unprocessed_raw_file_path(
                original_file_path="secondary_bucket/file_tag.csv", dt=discovery_date
            )
            secondary_raw_unprocessed_path_1 = GcsfsFilePath.from_absolute_path(
                secondary_normalized_path_str
            )

            secondary_raw_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
                region_code="us_xx",
                raw_data_instance=DirectIngestInstance.SECONDARY,
            )
            secondary_raw_metadata_manager.mark_raw_file_as_discovered(
                secondary_raw_unprocessed_path_1
            )

            # Mark secondary raw file tag as invalidated
            secondary_raw_metadata_manager.mark_file_as_invalidated(
                secondary_raw_unprocessed_path_1
            )

        with freeze_time(rerun_start_before_discovery_date):
            fake_manager = FakeDirectIngestInstanceStatusManager(
                region_code="us_xx",
                ingest_instance=DirectIngestInstance.SECONDARY,
                initial_statuses=[
                    DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED
                ],
            )
            with patch(
                "recidiviz.persistence.database.schema.operations.dao.PostgresDirectIngestInstanceStatusManager",
                return_value=fake_manager,
            ):
                # Assert that raw data is considered stale because the same file_tag is present in both, but is
                # invalidated in SECONDARY
                self.assertTrue(stale_secondary_raw_data("us_xx"))
