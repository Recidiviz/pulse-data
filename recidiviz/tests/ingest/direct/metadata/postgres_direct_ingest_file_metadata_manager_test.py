# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for classes in postgres_direct_ingest_file_metadata_manager.py."""
import datetime
import unittest
from typing import Optional, Type

import pytest
import pytz
import sqlalchemy
from freezegun import freeze_time
from mock import patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.metadata.direct_ingest_file_metadata_manager import (
    DirectIngestRawFileMetadataSummary,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import DirectIngestInstanceError
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity, entity_graph_eq
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.tools.postgres import local_postgres_helpers


def _fake_eq(e1: Entity, e2: Entity) -> bool:
    def _should_ignore_field_cb(_: Type, field_name: str) -> bool:
        return field_name == "file_id"

    return entity_graph_eq(e1, e2, _should_ignore_field_cb)


def _make_unprocessed_raw_data_path(
    path_str: str,
    dt: datetime.datetime = datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
) -> GcsfsFilePath:
    normalized_path_str = to_normalized_unprocessed_raw_file_path(
        original_file_path=path_str, dt=dt
    )
    return GcsfsFilePath.from_absolute_path(normalized_path_str)


def _make_processed_raw_data_path(path_str: str) -> GcsfsFilePath:
    path = _make_unprocessed_raw_data_path(path_str)
    # pylint:disable=protected-access
    return DirectIngestGCSFileSystem._to_processed_file_path(path)


@pytest.mark.uses_db
class PostgresDirectIngestRawFileMetadataManagerTest(unittest.TestCase):
    """Tests for PostgresDirectIngestRawFileMetadataManager."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        self.raw_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
            region_code="us_xx",
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )
        self.raw_metadata_manager_secondary = (
            PostgresDirectIngestRawFileMetadataManager(
                region_code="us_xx",
                raw_data_instance=DirectIngestInstance.SECONDARY,
            )
        )

        self.raw_metadata_manager_other_region = (
            PostgresDirectIngestRawFileMetadataManager(
                region_code="us_yy",
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
        )

        self.entity_eq_patcher = patch(
            "recidiviz.persistence.entity.base_entity.Entity.__eq__",
            _fake_eq,
        )
        self.entity_eq_patcher.start()

    def tearDown(self) -> None:
        self.entity_eq_patcher.stop()
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_register_processed_path_crashes(self) -> None:
        raw_processed_path = _make_processed_raw_data_path("bucket/file_tag.csv")

        with self.assertRaises(ValueError):
            self.raw_metadata_manager.mark_raw_file_as_discovered(raw_processed_path)

    def test_get_raw_file_metadata_when_not_yet_registered(self) -> None:
        # Arrange
        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        with self.assertRaises(ValueError):
            self.raw_metadata_manager.get_raw_file_metadata(raw_unprocessed_path)

    @freeze_time("2015-01-02T03:04:06")
    def test_get_raw_file_metadata(self) -> None:
        # Arrange
        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path)
        metadata = self.raw_metadata_manager.get_raw_file_metadata(raw_unprocessed_path)

        # Assert
        expected_metadata = DirectIngestRawFileMetadata.new_with_defaults(
            region_code="US_XX",
            file_tag="file_tag",
            file_discovery_time=datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=pytz.UTC),
            normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
            file_processed_time=None,
            update_datetime=datetime.datetime(2015, 1, 2, 3, 3, 3, 3, tzinfo=pytz.UTC),
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        self.assertIsInstance(metadata, DirectIngestRawFileMetadata)
        self.assertIsNotNone(metadata.file_id)
        self.assertEqual(expected_metadata, metadata)

    @freeze_time("2015-01-02T03:04:06")
    def test_get_raw_file_metadata_unique_to_state(self) -> None:
        # Arrange
        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        self.raw_metadata_manager_other_region.mark_raw_file_as_discovered(
            raw_unprocessed_path
        )

        # Act
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path)
        metadata = self.raw_metadata_manager.get_raw_file_metadata(raw_unprocessed_path)

        # Assert
        expected_metadata = DirectIngestRawFileMetadata.new_with_defaults(
            region_code=self.raw_metadata_manager.region_code,
            file_tag="file_tag",
            file_discovery_time=datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=pytz.UTC),
            normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
            file_processed_time=None,
            update_datetime=datetime.datetime(2015, 1, 2, 3, 3, 3, 3, tzinfo=pytz.UTC),
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        self.assertIsInstance(metadata, DirectIngestRawFileMetadata)
        self.assertIsNotNone(metadata.file_id)

        self.assertEqual(expected_metadata, metadata)

    def test_has_raw_file_been_discovered(self) -> None:
        # Arrange
        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path)

        # Assert
        self.assertTrue(
            self.raw_metadata_manager.has_raw_file_been_discovered(raw_unprocessed_path)
        )
        self.assertTrue(
            self.raw_metadata_manager.has_raw_file_been_discovered(raw_unprocessed_path)
        )
        # The file was only discovered in PRIMARY so assert that it was not discovered in SECONDARY.
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_file_been_discovered(
                raw_unprocessed_path
            )
        )

    def test_has_raw_file_been_discovered_returns_false_for_no_rows(self) -> None:
        # Arrange
        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Assert
        self.assertFalse(
            self.raw_metadata_manager.has_raw_file_been_discovered(raw_unprocessed_path)
        )
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_file_been_discovered(
                raw_unprocessed_path
            )
        )

    def test_has_raw_file_been_processed(self) -> None:
        # Arrange
        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path)
        self.raw_metadata_manager.mark_raw_file_as_processed(raw_unprocessed_path)

        # Assert
        self.assertTrue(
            self.raw_metadata_manager.has_raw_file_been_discovered(raw_unprocessed_path)
        )
        self.assertTrue(
            self.raw_metadata_manager.has_raw_file_been_processed(raw_unprocessed_path)
        )
        # The raw file was only processed in PRIMARY, so assert that it was not processed in SECONDARY.
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_file_been_processed(
                raw_unprocessed_path
            )
        )

    def test_has_raw_file_been_processed_returns_false_for_no_rows(self) -> None:
        # Arrange
        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Assert
        self.assertFalse(
            self.raw_metadata_manager.has_raw_file_been_processed(raw_unprocessed_path)
        )
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_file_been_processed(
                raw_unprocessed_path
            )
        )

    def test_has_raw_file_been_processed_returns_false_for_no_processed_time(
        self,
    ) -> None:
        # Arrange
        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path)

        # Assert
        self.assertFalse(
            self.raw_metadata_manager.has_raw_file_been_processed(raw_unprocessed_path)
        )
        self.assertFalse(
            self.raw_metadata_manager.has_raw_file_been_processed(raw_unprocessed_path)
        )
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_file_been_processed(
                raw_unprocessed_path
            )
        )

    @freeze_time("2015-01-02T03:05:06.000007")
    def test_mark_raw_file_as_processed(self) -> None:
        # Arrange
        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path)
        self.raw_metadata_manager.mark_raw_file_as_processed(raw_unprocessed_path)

        # Assert
        metadata = self.raw_metadata_manager.get_raw_file_metadata(raw_unprocessed_path)

        self.assertEqual(
            datetime.datetime(2015, 1, 2, 3, 5, 6, 7, tzinfo=pytz.UTC),
            metadata.file_processed_time,
        )

    def test_get_metadata_for_raw_files_discovered_after_datetime_empty(self) -> None:
        self.assertEqual(
            [],
            self.raw_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime(
                "any_tag", discovery_time_lower_bound_exclusive=None
            ),
        )

    def test_get_metadata_for_raw_files_discovered_after_datetime(self) -> None:
        with freeze_time("2015-01-02T03:05:05"):
            raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
                "bucket/file_tag.csv",
                dt=datetime.datetime.now(tz=pytz.UTC),
            )
            self.raw_metadata_manager.mark_raw_file_as_discovered(
                raw_unprocessed_path_1
            )
            self.raw_metadata_manager_other_region.mark_raw_file_as_discovered(
                raw_unprocessed_path_1
            )

        with freeze_time("2015-01-02T03:06:06"):
            raw_unprocessed_path_2 = _make_unprocessed_raw_data_path(
                "bucket/other_tag.csv",
                dt=datetime.datetime.now(tz=pytz.UTC),
            )
            self.raw_metadata_manager.mark_raw_file_as_discovered(
                raw_unprocessed_path_2
            )

        with freeze_time("2015-01-02T03:07:07"):
            raw_unprocessed_path_3 = _make_unprocessed_raw_data_path(
                "bucket/file_tag.csv",
                dt=datetime.datetime.now(tz=pytz.UTC),
            )
            self.raw_metadata_manager.mark_raw_file_as_discovered(
                raw_unprocessed_path_3
            )

        expected_list = [
            DirectIngestRawFileMetadata.new_with_defaults(
                region_code=self.raw_metadata_manager.region_code,
                file_tag="file_tag",
                file_discovery_time=datetime.datetime(
                    2015, 1, 2, 3, 5, 5, tzinfo=pytz.UTC
                ),
                normalized_file_name="unprocessed_2015-01-02T03:05:05:000000_raw_file_tag.csv",
                update_datetime=datetime.datetime(2015, 1, 2, 3, 5, 5, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY,
            ),
            DirectIngestRawFileMetadata.new_with_defaults(
                region_code=self.raw_metadata_manager.region_code,
                file_tag="file_tag",
                file_discovery_time=datetime.datetime(
                    2015, 1, 2, 3, 7, 7, tzinfo=pytz.UTC
                ),
                normalized_file_name="unprocessed_2015-01-02T03:07:07:000000_raw_file_tag.csv",
                update_datetime=datetime.datetime(2015, 1, 2, 3, 7, 7, tzinfo=pytz.UTC),
                raw_data_instance=DirectIngestInstance.PRIMARY,
            ),
        ]

        self.assertEqual(
            expected_list,
            self.raw_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime(
                "file_tag", discovery_time_lower_bound_exclusive=None
            ),
        )

        expected_list = expected_list[-1:]

        self.assertEqual(
            expected_list,
            self.raw_metadata_manager.get_metadata_for_raw_files_discovered_after_datetime(
                "file_tag",
                discovery_time_lower_bound_exclusive=datetime.datetime(
                    2015, 1, 2, 3, 7, 0
                ),
            ),
        )

    def test_get_metadata_for_all_raw_files_in_region_empty(self) -> None:
        self.assertEqual(
            [],
            self.raw_metadata_manager.get_metadata_for_all_raw_files_in_region(),
        )

    def test_get_metadata_for_all_raw_files_in_region(self) -> None:
        with freeze_time(datetime.datetime(2015, 1, 2, 3, 5, 5, tzinfo=pytz.UTC)):
            raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
                "bucket/file_tag.csv",
                dt=datetime.datetime.now(tz=pytz.UTC),
            )
            self.raw_metadata_manager.mark_raw_file_as_discovered(
                raw_unprocessed_path_1
            )
            self.raw_metadata_manager.mark_raw_file_as_processed(raw_unprocessed_path_1)

        with freeze_time(datetime.datetime(2015, 1, 2, 3, 6, 6, tzinfo=pytz.UTC)):
            raw_unprocessed_path_2 = _make_unprocessed_raw_data_path(
                "bucket/other_tag.csv",
                dt=datetime.datetime.now(tz=pytz.UTC),
            )
            self.raw_metadata_manager.mark_raw_file_as_discovered(
                raw_unprocessed_path_2
            )

        with freeze_time(datetime.datetime(2015, 1, 2, 3, 7, 7, tzinfo=pytz.UTC)):
            raw_unprocessed_path_3 = _make_unprocessed_raw_data_path(
                "bucket/file_tag.csv",
                dt=datetime.datetime.now(tz=pytz.UTC),
            )
            self.raw_metadata_manager.mark_raw_file_as_discovered(
                raw_unprocessed_path_3
            )

        expected_list = [
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag",
                num_unprocessed_files=1,
                num_processed_files=1,
                latest_processed_time=datetime.datetime(
                    2015, 1, 2, 3, 5, 5, tzinfo=pytz.UTC
                ),
                latest_discovery_time=datetime.datetime(
                    2015, 1, 2, 3, 7, 7, tzinfo=pytz.UTC
                ),
                latest_update_datetime=datetime.datetime(
                    2015, 1, 2, 3, 5, 5, tzinfo=pytz.UTC
                ),
            ),
            DirectIngestRawFileMetadataSummary(
                file_tag="other_tag",
                num_unprocessed_files=1,
                num_processed_files=0,
                latest_processed_time=None,
                latest_discovery_time=datetime.datetime(
                    2015, 1, 2, 3, 6, 6, tzinfo=pytz.UTC
                ),
                latest_update_datetime=None,
            ),
        ]

        self.assertEqual(
            expected_list,
            self.raw_metadata_manager.get_metadata_for_all_raw_files_in_region(),
        )

    @freeze_time("2015-01-02T03:04:06")
    def test_get_unprocessed_raw_files_when_no_files(self) -> None:
        # Assert
        self.assertEqual([], self.raw_metadata_manager.get_unprocessed_raw_files())

    def test_get_non_invalidated_raw_files_when_no_files(self) -> None:
        # Assert
        self.assertEqual([], self.raw_metadata_manager.get_non_invalidated_files())

    def test_get_non_invalidated_raw_files(self) -> None:
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=pytz.UTC),
        )
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)

        # Assert
        self.assertEqual(1, len(self.raw_metadata_manager.get_non_invalidated_files()))

        self.raw_metadata_manager.mark_file_as_invalidated(raw_unprocessed_path_1)

        # Assert
        self.assertEqual([], self.raw_metadata_manager.get_non_invalidated_files())

    def test_get_unprocessed_raw_files_when_secondary_db(self) -> None:
        # Arrange
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
        )

        # Act
        self.raw_metadata_manager_secondary.mark_raw_file_as_discovered(
            raw_unprocessed_path_1
        )

        # Assert
        with self.assertRaises(DirectIngestInstanceError):
            self.raw_metadata_manager_secondary.get_unprocessed_raw_files()

    @freeze_time(datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=pytz.UTC))
    def test_transfer_metadata_to_new_instance_secondary_to_primary(self) -> None:
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=pytz.UTC),
        )
        self.raw_metadata_manager_secondary.mark_raw_file_as_discovered(
            raw_unprocessed_path_1
        )

        expected_metadata = DirectIngestRawFileMetadata.new_with_defaults(
            file_id=1,
            region_code="US_XX",
            file_tag="file_tag",
            file_discovery_time=datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=pytz.UTC),
            normalized_file_name="unprocessed_2015-01-02T03:04:06:000000_raw_file_tag.csv",
            file_processed_time=None,
            update_datetime=datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=pytz.UTC),
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        # Act
        self.raw_metadata_manager_secondary.transfer_metadata_to_new_instance(
            self.raw_metadata_manager
        )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestRawFileMetadata)
                .filter_by(
                    region_code=self.raw_metadata_manager.region_code.upper(),
                    raw_data_instance=self.raw_metadata_manager.raw_data_instance.value,
                )
                .one()
            )
            # Check here that found_metadata has expected items and all instances are marked primary
            self.assertEqual(
                expected_metadata, convert_schema_object_to_entity(metadata)
            )

            # Assert that secondary instance was moved to primary instance, thus secondary no longer exists
            no_row_found_regex = r"No row was found when one was required"
            with self.assertRaisesRegex(
                sqlalchemy.exc.NoResultFound, no_row_found_regex
            ):
                _ = (
                    session.query(schema.DirectIngestRawFileMetadata)
                    .filter_by(
                        region_code=self.raw_metadata_manager_secondary.region_code.upper(),
                        raw_data_instance=self.raw_metadata_manager_secondary.raw_data_instance.value,
                    )
                    .one()
                )

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_primary_to_secondary(self) -> None:
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=pytz.UTC),
        )
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)

        expected_metadata = DirectIngestRawFileMetadata.new_with_defaults(
            file_id=1,
            region_code="US_XX",
            file_tag="file_tag",
            file_discovery_time=datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=pytz.UTC),
            normalized_file_name="unprocessed_2015-01-02T03:04:06:000000_raw_file_tag.csv",
            file_processed_time=None,
            update_datetime=datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=pytz.UTC),
            raw_data_instance=DirectIngestInstance.SECONDARY,
        )

        # Act
        self.raw_metadata_manager.transfer_metadata_to_new_instance(
            self.raw_metadata_manager_secondary
        )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestRawFileMetadata)
                .filter_by(
                    region_code=self.raw_metadata_manager_secondary.region_code.upper(),
                    raw_data_instance=self.raw_metadata_manager_secondary.raw_data_instance.value,
                )
                .one()
            )
            # Check here that found_metadata has expected items and all instances are marked primary
            self.assertEqual(
                expected_metadata, convert_schema_object_to_entity(metadata)
            )

            # Assert that secondary instance was moved to primary instance, thus secondary no longer exists
            no_row_found_regex = r"No row was found when one was required"
            with self.assertRaisesRegex(
                sqlalchemy.exc.NoResultFound, no_row_found_regex
            ):
                _ = (
                    session.query(schema.DirectIngestRawFileMetadata)
                    .filter_by(
                        region_code=self.raw_metadata_manager.region_code.upper(),
                        raw_data_instance=self.raw_metadata_manager.raw_data_instance.value,
                    )
                    .one()
                )

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_primary_to_primary(self) -> None:
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=pytz.UTC),
        )
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)

        same_instance = (
            r"Either state codes are not the same or new instance is same as origin"
        )
        with self.assertRaisesRegex(ValueError, same_instance):
            self.raw_metadata_manager.transfer_metadata_to_new_instance(
                self.raw_metadata_manager
            )

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_secondary_to_secondary(self) -> None:
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=pytz.UTC),
        )
        self.raw_metadata_manager_secondary.mark_raw_file_as_discovered(
            raw_unprocessed_path_1
        )

        same_instance = (
            r"Either state codes are not the same or new instance is same as origin"
        )
        with self.assertRaisesRegex(ValueError, same_instance):
            self.raw_metadata_manager_secondary.transfer_metadata_to_new_instance(
                self.raw_metadata_manager_secondary
            )

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_different_states(self) -> None:
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=pytz.UTC),
        )
        self.raw_metadata_manager_secondary.mark_raw_file_as_discovered(
            raw_unprocessed_path_1
        )

        self.raw_metadata_manager_dif_state = (
            PostgresDirectIngestRawFileMetadataManager(
                region_code="us_yy",
                raw_data_instance=DirectIngestInstance.SECONDARY,
            )
        )

        dif_state = (
            r"Either state codes are not the same or new instance is same as origin"
        )
        with self.assertRaisesRegex(ValueError, dif_state):
            self.raw_metadata_manager_secondary.transfer_metadata_to_new_instance(
                self.raw_metadata_manager_dif_state
            )

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_existing_raw_data(self) -> None:
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=pytz.UTC),
        )
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)

        dif_state = (
            r"Destination instance should not have any valid raw file metadata rows."
        )
        with self.assertRaisesRegex(ValueError, dif_state):
            self.raw_metadata_manager_secondary.transfer_metadata_to_new_instance(
                self.raw_metadata_manager
            )

    def test_mark_instance_as_invalidated(self) -> None:
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=pytz.UTC),
        )
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)
        self.assertEqual(1, len(self.raw_metadata_manager.get_non_invalidated_files()))

        self.raw_metadata_manager.mark_instance_data_invalidated()
        self.assertEqual(0, len(self.raw_metadata_manager.get_non_invalidated_files()))
