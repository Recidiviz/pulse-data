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
from typing import Type

import pytz
from freezegun import freeze_time
from mock import patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
    PostgresDirectIngestSftpFileMetadataManager,
)
from recidiviz.ingest.direct.types.errors import DirectIngestInstanceError
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity, entity_graph_eq
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawFileMetadata,
    DirectIngestSftpFileMetadata,
)
from recidiviz.tests.utils import fakes


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


class PostgresDirectIngestRawFileMetadataManagerTest(unittest.TestCase):
    """Tests for PostgresDirectIngestRawFileMetadataManager."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        fakes.use_in_memory_sqlite_database(self.database_key)

        self.raw_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
            region_code="us_xx", ingest_database_name="us_xx_ingest_database_name"
        )
        self.raw_metadata_manager_secondary = (
            PostgresDirectIngestRawFileMetadataManager(
                region_code="us_xx",
                ingest_database_name="us_xx_ingest_database_name_secondary",
            )
        )

        self.raw_metadata_manager_other_region = (
            PostgresDirectIngestRawFileMetadataManager(
                region_code="us_yy", ingest_database_name="us_yy_ingest_database_name"
            )
        )

        self.entity_eq_patcher = patch(
            "recidiviz.persistence.entity.operations.entities.OperationsEntity.__eq__",
            _fake_eq,
        )
        self.entity_eq_patcher.start()

    def tearDown(self) -> None:
        self.entity_eq_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

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
        metadata_secondary = self.raw_metadata_manager_secondary.get_raw_file_metadata(
            raw_unprocessed_path
        )

        # Assert
        expected_metadata = DirectIngestRawFileMetadata.new_with_defaults(
            region_code="US_XX",
            file_tag="file_tag",
            discovery_time=datetime.datetime(2015, 1, 2, 3, 4, 6),
            normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
            processed_time=None,
            datetimes_contained_upper_bound_inclusive=datetime.datetime(
                2015, 1, 2, 3, 3, 3, 3
            ),
        )

        self.assertIsInstance(metadata, DirectIngestRawFileMetadata)
        self.assertIsNotNone(metadata.file_id)
        self.assertEqual(expected_metadata, metadata)

        # Raw file metadata is not impacted by which ingest database the metadata
        # manager is associated with.
        self.assertIsInstance(metadata_secondary, DirectIngestRawFileMetadata)
        self.assertIsNotNone(metadata_secondary.file_id)
        self.assertEqual(expected_metadata, metadata_secondary)

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
            discovery_time=datetime.datetime(2015, 1, 2, 3, 4, 6),
            normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
            processed_time=None,
            datetimes_contained_upper_bound_inclusive=datetime.datetime(
                2015, 1, 2, 3, 3, 3, 3
            ),
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
        self.assertTrue(
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
        self.assertTrue(
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
            datetime.datetime(2015, 1, 2, 3, 5, 6, 7), metadata.processed_time
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
                discovery_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
                normalized_file_name="unprocessed_2015-01-02T03:05:05:000000_raw_file_tag.csv",
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2015, 1, 2, 3, 5, 5
                ),
            ),
            DirectIngestRawFileMetadata.new_with_defaults(
                region_code=self.raw_metadata_manager.region_code,
                file_tag="file_tag",
                discovery_time=datetime.datetime(2015, 1, 2, 3, 7, 7),
                normalized_file_name="unprocessed_2015-01-02T03:07:07:000000_raw_file_tag.csv",
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2015, 1, 2, 3, 7, 7
                ),
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

    @freeze_time("2015-01-02T03:04:06")
    def test_get_num_unprocessed_raw_files(self) -> None:
        # Arrange
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
        )

        raw_unprocessed_path_2 = _make_unprocessed_raw_data_path(
            "bucket/file_tag2.csv",
        )

        # Act
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_2)

        unprocessed_paths = [raw_unprocessed_path_1, raw_unprocessed_path_2]

        # Assert
        self.assertEqual(
            len(unprocessed_paths),
            self.raw_metadata_manager.get_num_unprocessed_raw_files(),
        )
        self.assertEqual(0, self.raw_metadata_manager.get_num_processed_raw_files())

        # Act
        self.raw_metadata_manager.mark_raw_file_as_processed(raw_unprocessed_path_1)

        # Assert
        self.assertEqual(1, self.raw_metadata_manager.get_num_unprocessed_raw_files())
        self.assertEqual(1, self.raw_metadata_manager.get_num_processed_raw_files())

    @freeze_time("2015-01-02T03:04:06")
    def test_get_num_unprocessed_raw_files_multi_region(self) -> None:
        # Arrange
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
        )

        raw_unprocessed_path_2 = _make_unprocessed_raw_data_path(
            "bucket/file_tag2.csv",
        )

        # Act
        self.raw_metadata_manager.mark_raw_file_as_discovered(raw_unprocessed_path_1)
        self.raw_metadata_manager_other_region.mark_raw_file_as_discovered(
            raw_unprocessed_path_2
        )

        # Assert
        self.assertEqual(
            1,
            self.raw_metadata_manager.get_num_unprocessed_raw_files(),
        )
        self.assertEqual(0, self.raw_metadata_manager.get_num_processed_raw_files())

        # Act
        self.raw_metadata_manager.mark_raw_file_as_processed(raw_unprocessed_path_1)

        # Assert
        self.assertEqual(0, self.raw_metadata_manager.get_num_unprocessed_raw_files())
        self.assertEqual(1, self.raw_metadata_manager.get_num_processed_raw_files())

    @freeze_time("2015-01-02T03:04:06")
    def test_get_num_unprocessed_raw_files_when_no_files(self) -> None:
        # Assert
        self.assertEqual(0, self.raw_metadata_manager.get_num_unprocessed_raw_files())

    def test_get_num_unprocessed_raw_files_when_secondary_db(self) -> None:
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
            self.raw_metadata_manager_secondary.get_num_unprocessed_raw_files()


class PostgresDirectIngestSftpFileMetadataManagerTest(unittest.TestCase):
    """Tests for PostgresDirectIngestSftpFileMetadataManager."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        fakes.use_in_memory_sqlite_database(self.database_key)
        self.sftp_metadata_manager = PostgresDirectIngestSftpFileMetadataManager(
            region_code="us_xx", ingest_database_name="us_xx_ingest_database_name"
        )

        self.sftp_metadata_manager_secondary = (
            PostgresDirectIngestSftpFileMetadataManager(
                region_code="us_xx",
                ingest_database_name="us_xx_ingest_database_name_secondary",
            )
        )

        self.sftp_metadata_manager_other_region = (
            PostgresDirectIngestSftpFileMetadataManager(
                region_code="us_yy", ingest_database_name="us_yy_ingest_database_name"
            )
        )

        self.entity_eq_patcher = patch(
            "recidiviz.persistence.entity.operations.entities.OperationsEntity.__eq__",
            _fake_eq,
        )
        self.entity_eq_patcher.start()

    def tearDown(self) -> None:
        self.entity_eq_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

    def test_get_sftp_file_metadata_when_not_yet_registered(self) -> None:
        with self.assertRaises(ValueError):
            self.sftp_metadata_manager.get_sftp_file_metadata("nonexistent.csv")

    @freeze_time("2015-01-02T03:04:06")
    def test_get_sftp_file_metadata(self) -> None:
        # Arrange
        sftp_path = "Recidiviz20150102/somefile.csv"

        # Act
        self.sftp_metadata_manager.mark_sftp_file_as_discovered(sftp_path)
        metadata = self.sftp_metadata_manager.get_sftp_file_metadata(sftp_path)
        metadata_secondary = (
            self.sftp_metadata_manager_secondary.get_sftp_file_metadata(sftp_path)
        )

        # Assert
        expected_metadata = DirectIngestSftpFileMetadata.new_with_defaults(
            region_code="US_XX",
            discovery_time=datetime.datetime(2015, 1, 2, 3, 4, 6),
            remote_file_path=sftp_path,
            processed_time=None,
        )

        self.assertIsInstance(metadata, DirectIngestSftpFileMetadata)
        self.assertIsNotNone(metadata.file_id)
        self.assertEqual(expected_metadata, metadata)

        # Raw file metadata is not impacted by which ingest database the metadata
        # manager is associated with.
        self.assertIsInstance(metadata_secondary, DirectIngestSftpFileMetadata)
        self.assertIsNotNone(metadata_secondary.file_id)
        self.assertEqual(expected_metadata, metadata_secondary)

    @freeze_time("2015-01-02T03:04:06")
    def test_get_sftp_file_metadata_unique_to_state(self) -> None:
        # Arrange
        sftp_path = "Recidiviz20150102/somefile.csv"

        self.sftp_metadata_manager_other_region.mark_sftp_file_as_discovered(sftp_path)

        # Act
        self.sftp_metadata_manager.mark_sftp_file_as_discovered(sftp_path)
        metadata = self.sftp_metadata_manager.get_sftp_file_metadata(sftp_path)

        # Assert
        expected_metadata = DirectIngestSftpFileMetadata.new_with_defaults(
            region_code=self.sftp_metadata_manager.region_code,
            discovery_time=datetime.datetime(2015, 1, 2, 3, 4, 6),
            remote_file_path=sftp_path,
            processed_time=None,
        )

        self.assertIsInstance(metadata, DirectIngestSftpFileMetadata)
        self.assertIsNotNone(metadata.file_id)

        self.assertEqual(expected_metadata, metadata)

    def test_has_sftp_file_been_discovered(self) -> None:
        # Arrange
        sftp_path = "Recidiviz20150102/somefile.csv"

        # Act
        self.sftp_metadata_manager.mark_sftp_file_as_discovered(sftp_path)

        # Assert
        self.assertTrue(
            self.sftp_metadata_manager.has_sftp_file_been_discovered(sftp_path)
        )
        self.assertTrue(
            self.sftp_metadata_manager_secondary.has_sftp_file_been_discovered(
                sftp_path
            )
        )

    def test_has_sftp_file_been_discovered_returns_false_for_no_rows(self) -> None:
        # Arrange
        sftp_path = "Recidiviz20150102/somefile.csv"

        # Assert
        self.assertFalse(
            self.sftp_metadata_manager.has_sftp_file_been_discovered(sftp_path)
        )
        self.assertFalse(
            self.sftp_metadata_manager_secondary.has_sftp_file_been_discovered(
                sftp_path
            )
        )

    def test_has_sftp_file_been_processed(self) -> None:
        # Arrange
        sftp_path = "Recidiviz20150102/somefile.csv"

        # Act
        self.sftp_metadata_manager.mark_sftp_file_as_discovered(sftp_path)
        self.sftp_metadata_manager.mark_sftp_file_as_processed(sftp_path)

        # Assert
        self.assertTrue(
            self.sftp_metadata_manager.has_sftp_file_been_processed(sftp_path)
        )
        self.assertTrue(
            self.sftp_metadata_manager_secondary.has_sftp_file_been_processed(sftp_path)
        )

    def test_has_sftp_file_been_processed_returns_false_for_no_rows(self) -> None:
        # Arrange
        sftp_path = "Recidiviz20150102/somefile.csv"

        # Assert
        self.assertFalse(
            self.sftp_metadata_manager.has_sftp_file_been_processed(sftp_path)
        )
        self.assertFalse(
            self.sftp_metadata_manager_secondary.has_sftp_file_been_processed(sftp_path)
        )

    def test_has_sftp_file_been_processed_returns_false_for_no_processed_time(
        self,
    ) -> None:
        # Arrange
        sftp_path = "Recidiviz20150102/somefile.csv"

        # Act
        self.sftp_metadata_manager.mark_sftp_file_as_discovered(sftp_path)

        # Assert
        self.assertFalse(
            self.sftp_metadata_manager.has_sftp_file_been_processed(sftp_path)
        )
        self.assertFalse(
            self.sftp_metadata_manager_secondary.has_sftp_file_been_processed(sftp_path)
        )

    @freeze_time("2015-01-02T03:05:06.000007")
    def test_mark_sftp_file_as_processed(self) -> None:
        # Arrange
        sftp_path = "Recidiviz20150102/somefile.csv"

        # Act
        self.sftp_metadata_manager.mark_sftp_file_as_discovered(sftp_path)
        self.sftp_metadata_manager.mark_sftp_file_as_processed(sftp_path)

        # Assert
        metadata = self.sftp_metadata_manager.get_sftp_file_metadata(sftp_path)

        self.assertEqual(
            datetime.datetime(2015, 1, 2, 3, 5, 6, 7), metadata.processed_time
        )
