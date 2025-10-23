# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for classes in direct_ingest_raw_file_metadata_manager"""
import datetime
import unittest
from datetime import timedelta
from typing import Dict, List, Type

import pytest
import sqlalchemy
from freezegun import freeze_time
from mock import patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
    DirectIngestRawFileMetadataSummary,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity, entity_graph_eq
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawBigQueryFileMetadata,
    DirectIngestRawGCSFileMetadata,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult
from recidiviz.utils.types import assert_type


def _fake_eq(e1: Entity, e2: Entity) -> bool:
    def _should_ignore_field_cb(_: Type, field_name: str) -> bool:
        return field_name in ("file_id", "gcs_file_id", "bq_file", "gcs_files")

    return entity_graph_eq(e1, e2, _should_ignore_field_cb)


def make_unprocessed_raw_data_path(
    path_str: str,
    dt: datetime.datetime = datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
) -> GcsfsFilePath:
    normalized_path_str = to_normalized_unprocessed_raw_file_path(
        original_file_path=path_str, dt=dt
    )
    return GcsfsFilePath.from_absolute_path(normalized_path_str)


def make_processed_raw_data_path(path_str: str) -> GcsfsFilePath:
    path = make_unprocessed_raw_data_path(path_str)
    # pylint:disable=protected-access
    return DirectIngestGCSFileSystem._to_processed_file_path(path)


@pytest.mark.uses_db
class DirectIngestRawFileMetadataManagerTest(unittest.TestCase):
    """Tests for DirectIngestRawFileMetadataManager."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

        self.raw_metadata_manager = DirectIngestRawFileMetadataManager(
            region_code="us_xx",
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )
        self.raw_metadata_manager_secondary = DirectIngestRawFileMetadataManager(
            region_code="us_xx",
            raw_data_instance=DirectIngestInstance.SECONDARY,
        )

        self.raw_metadata_manager_other_region = DirectIngestRawFileMetadataManager(
            region_code="us_yy",
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        self.entity_eq_patcher = patch(
            "recidiviz.persistence.entity.base_entity.Entity.__eq__",
            _fake_eq,
        )
        self.entity_eq_patcher.start()

    def tearDown(self) -> None:
        self.entity_eq_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_register_processed_path_crashes(self) -> None:
        raw_processed_path = make_processed_raw_data_path("bucket/file_tag.csv")

        with self.assertRaises(ValueError):
            self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                raw_processed_path
            )

    def test_get_raw_file_metadata_when_not_yet_registered(self) -> None:
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")
        with self.assertRaises(ValueError):
            self.raw_metadata_manager.get_raw_gcs_file_metadata(raw_unprocessed_path)

    @freeze_time("2015-01-02T03:04:06")
    def test_get_raw_gcs_file_metadata(self) -> None:
        # Arrange
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(raw_unprocessed_path)
        metadata = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            raw_unprocessed_path
        )

        # Assert
        expected_metadata = DirectIngestRawGCSFileMetadata.new_with_defaults(
            gcs_file_id=1,
            region_code="US_XX",
            file_tag="file_tag",
            file_discovery_time=datetime.datetime(
                2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC
            ),
            update_datetime=datetime.datetime(
                2015, 1, 2, 3, 3, 3, 3, tzinfo=datetime.UTC
            ),
            normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            is_invalidated=False,
        )

        self.assertIsInstance(metadata, DirectIngestRawGCSFileMetadata)
        self.assertIsNotNone(metadata.gcs_file_id)
        self.assertEqual(expected_metadata, metadata)
        self.assertIsNotNone(metadata.file_id)

    @freeze_time("2015-01-02T03:04:06")
    def test_get_raw_gcs_file_metadata_unique_to_state(self) -> None:
        # Arrange
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        self.raw_metadata_manager_other_region.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        # Act
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(raw_unprocessed_path)
        metadata = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            raw_unprocessed_path
        )

        # Assert
        expected_metadata = DirectIngestRawGCSFileMetadata.new_with_defaults(
            gcs_file_id=1,
            region_code=self.raw_metadata_manager.region_code,
            file_tag="file_tag",
            file_discovery_time=datetime.datetime(
                2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC
            ),
            normalized_file_name="unprocessed_2015-01-02T03:03:03:000003_raw_file_tag.csv",
            update_datetime=datetime.datetime(
                2015, 1, 2, 3, 3, 3, 3, tzinfo=datetime.UTC
            ),
            raw_data_instance=DirectIngestInstance.PRIMARY,
            is_invalidated=False,
        )

        self.assertIsInstance(metadata, DirectIngestRawGCSFileMetadata)
        self.assertIsNotNone(metadata.gcs_file_id)
        self.assertEqual(expected_metadata, metadata)

    def test_has_raw_gcs_file_been_discovered(self) -> None:
        # Arrange
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        metadata = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        # Assert
        self.assertTrue(
            self.raw_metadata_manager.has_raw_gcs_file_been_discovered(
                raw_unprocessed_path
            )
        )
        # The file was only discovered in PRIMARY so assert that it was not discovered in SECONDARY.
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_gcs_file_been_discovered(
                raw_unprocessed_path
            )
        )
        self.assertIsNotNone(metadata.file_id)

    def test_has_raw_gcs_file_been_discovered_chunked_single(self) -> None:
        fixed_datetime = datetime.datetime(2121, 2, 1, 2, 1, 2, tzinfo=datetime.UTC)

        # discover all chunks
        file_paths_of_same_file_tag: List[GcsfsFilePath] = []
        for i in range(7):
            file_path = make_unprocessed_raw_data_path(
                path_str="bucket/file_tag.csv",
                dt=fixed_datetime + timedelta(hours=i),
            )
            metadata = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                file_path, is_chunked_file=True
            )
            self.assertIsNone(metadata.file_id)
            file_paths_of_same_file_tag.append(file_path)

        # make sure they persisted
        for file_path in file_paths_of_same_file_tag:
            self.assertTrue(
                self.raw_metadata_manager.has_raw_gcs_file_been_discovered(file_path)
            )

        # also make a file path of a different file tag
        file_path_diff_tag = make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_two_point_oh.csv",
            dt=fixed_datetime,
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            file_path_diff_tag, is_chunked_file=True
        )

        # if we throw in another file tag, this should fail
        with self.assertRaisesRegex(
            ValueError,
            r"Found multiple file tags \[file_tag, file_tag_two_point_oh\], "
            r"but only expected one.",
        ):
            _ = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
                [*file_paths_of_same_file_tag, file_path_diff_tag]
            )

        bq_metadata = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
            file_paths_of_same_file_tag[:4]
        )

        self.assertEqual(
            bq_metadata.update_datetime, fixed_datetime + timedelta(hours=3)
        )

        for file_path in file_paths_of_same_file_tag[:4]:
            self.assertEqual(
                self.raw_metadata_manager.get_raw_gcs_file_metadata(file_path).file_id,
                bq_metadata.file_id,
            )

        # if some of this batch are already registered, we should error too
        with self.assertRaisesRegex(
            ValueError,
            r"Found unexpected number of paths: expected \[\d\] but found \[\d\]",
        ):
            _ = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
                file_paths_of_same_file_tag
            )

        bq_metadata_2 = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
            file_paths_of_same_file_tag[4:]
        )

        self.assertEqual(
            bq_metadata_2.update_datetime, fixed_datetime + timedelta(hours=6)
        )

        for file_path in file_paths_of_same_file_tag[4:]:
            self.assertEqual(
                self.raw_metadata_manager.get_raw_gcs_file_metadata(file_path).file_id,
                bq_metadata_2.file_id,
            )

    def test_has_raw_gcs_file_been_discovered_chunked_multiple(self) -> None:
        # Arrange
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        metadata = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path, is_chunked_file=True
        )

        # Assert
        self.assertTrue(
            self.raw_metadata_manager.has_raw_gcs_file_been_discovered(
                raw_unprocessed_path
            )
        )
        # The file was only discovered in PRIMARY so assert that it was not discovered in SECONDARY.
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_gcs_file_been_discovered(
                raw_unprocessed_path
            )
        )
        self.assertIsNone(metadata.file_id)

        bq_metadata = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
            [raw_unprocessed_path]
        )

        gcs_metadata = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            raw_unprocessed_path
        )

        self.assertIsNotNone(gcs_metadata.file_id)
        self.assertEqual(bq_metadata.file_id, gcs_metadata.file_id)

    def test_has_raw_gcs_file_been_discovered_returns_false_for_no_rows(self) -> None:
        # Arrange
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Assert
        self.assertFalse(
            self.raw_metadata_manager.has_raw_gcs_file_been_discovered(
                raw_unprocessed_path
            )
        )
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_gcs_file_been_discovered(
                raw_unprocessed_path
            )
        )

    def test_has_raw_gcs_file_been_processed(self) -> None:
        # Arrange
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        metadata = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert metadata.file_id is not None

        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(metadata.file_id)

        # Assert
        self.assertTrue(
            self.raw_metadata_manager.has_raw_gcs_file_been_discovered(
                raw_unprocessed_path
            )
        )
        self.assertTrue(
            self.raw_metadata_manager.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )
        # The raw file was only processed in PRIMARY, so assert that it was not processed in SECONDARY.
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )

    def test_has_raw_gcs_file_been_processed_chunked(self) -> None:
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        self.assertFalse(
            self.raw_metadata_manager.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )

        _ = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path, is_chunked_file=True
        )

        self.assertFalse(
            self.raw_metadata_manager.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )

        metadata = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
            [raw_unprocessed_path]
        )

        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(metadata.file_id)

        self.assertTrue(
            self.raw_metadata_manager.has_raw_gcs_file_been_discovered(
                raw_unprocessed_path
            )
        )
        self.assertTrue(
            self.raw_metadata_manager.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )

        self.assertTrue(
            self.raw_metadata_manager.has_raw_biq_query_file_been_processed(
                metadata.file_id
            )
        )
        # The raw file was only processed in PRIMARY, so assert that it was not processed in SECONDARY.
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )

    def test_has_raw_file_been_processed_returns_false_for_no_rows(self) -> None:
        # Arrange
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Assert
        self.assertFalse(
            self.raw_metadata_manager.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )

    def test_has_raw_file_been_processed_returns_false_for_no_processed_time(
        self,
    ) -> None:
        # Arrange
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(raw_unprocessed_path)

        # Assert
        self.assertFalse(
            self.raw_metadata_manager.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )
        self.assertFalse(
            self.raw_metadata_manager.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )
        self.assertFalse(
            self.raw_metadata_manager_secondary.has_raw_gcs_file_been_processed(
                raw_unprocessed_path
            )
        )

    def test_mark_raw_file_as_processed(self) -> None:
        # Arrange
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        # Act
        metadata = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert metadata.file_id is not None

        processed_time = datetime.datetime(2015, 1, 2, 3, 5, 5, tzinfo=datetime.UTC)
        with freeze_time(processed_time):
            self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                metadata.file_id
            )

    def test_mark_raw_file_as_processed_but_is_invalidated(self) -> None:
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        metadata = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )
        assert metadata.file_id is not None
        with SessionFactory.using_database(self.database_key) as session:
            self.raw_metadata_manager.mark_file_as_invalidated_by_file_id_with_session(
                session=session,
                file_id=metadata.file_id,
            )
        with self.assertRaisesRegex(
            ValueError, r"Cannot mark \[\d+\] as processed as the file is invalidated"
        ):
            self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                metadata.file_id
            )

    def test_get_raw_file_metadata_for_file_id(self) -> None:

        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        old_metadata = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert old_metadata.file_id is not None

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            # If this call doesn't raise an exception, it means that the correct metadata row was retrieved.
            metadata = self.raw_metadata_manager._get_raw_big_query_file_metadata_for_file_id(  # pylint: disable=protected-access
                session=session, file_id=old_metadata.file_id
            )
            self.assertEqual(metadata.file_id, old_metadata.file_id)

    def test_raw_file_metadata_mark_as_invalidated(self) -> None:
        raw_unprocessed_path = make_unprocessed_raw_data_path("bucket/file_tag.csv")

        old_metadata = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert old_metadata.file_id is not None

        with SessionFactory.using_database(self.database_key) as session:
            self.raw_metadata_manager.mark_file_as_invalidated_by_file_id_with_session(
                session=session,
                file_id=old_metadata.file_id,
            )

        metadata = self.raw_metadata_manager.get_raw_big_query_file_metadata(
            old_metadata.file_id
        )

        assert metadata.is_invalidated is True

        gcs_metadata = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            raw_unprocessed_path
        )
        assert gcs_metadata.is_invalidated is True

    @freeze_time("2015-01-02T03:04:06")
    def test_get_unprocessed_raw_files_eligible_for_import_when_no_files(self) -> None:
        # Assert
        self.assertEqual(
            {},
            self.raw_metadata_manager.get_unprocessed_raw_big_query_files_eligible_for_import(),
        )

    def test_get_non_invalidated_raw_files_when_no_files(self) -> None:
        # Assert
        self.assertEqual(
            [], self.raw_metadata_manager.get_non_invalidated_raw_big_query_files()
        )

    def test_get_non_invalidated_raw_files(self) -> None:
        raw_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )
        metadata = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )

        assert metadata.file_id is not None

        # Assert
        self.assertEqual(
            1, len(self.raw_metadata_manager.get_non_invalidated_raw_big_query_files())
        )

        with SessionFactory.using_database(self.database_key) as session:
            self.raw_metadata_manager.mark_file_as_invalidated_by_file_id_with_session(
                session, metadata.file_id
            )

        # Assert
        self.assertEqual(
            [], self.raw_metadata_manager.get_non_invalidated_raw_big_query_files()
        )

    @freeze_time(datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC))
    def test_transfer_metadata_to_new_instance_secondary_to_primary(self) -> None:
        raw_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )
        self.raw_metadata_manager_secondary.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )

        with SessionFactory.using_database(self.database_key) as session:
            pruning_metadata_secondary = schema.DirectIngestRawDataPruningMetadata(
                region_code=self.raw_metadata_manager_secondary.region_code,
                raw_data_instance=self.raw_metadata_manager_secondary.raw_data_instance.value,
                file_tag="test_file_tag",
                updated_at=datetime.datetime.now(tz=datetime.UTC),
                automatic_pruning_enabled=True,
                raw_file_primary_keys="col1,col2",
                raw_files_contain_full_historical_lookback=False,
            )
            session.add(pruning_metadata_secondary)

        expected_gcs_metadata = DirectIngestRawGCSFileMetadata.new_with_defaults(
            gcs_file_id=1,
            region_code="US_XX",
            file_tag="file_tag",
            file_discovery_time=datetime.datetime(
                2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC
            ),
            normalized_file_name="unprocessed_2015-01-02T03:04:06:000000_raw_file_tag.csv",
            update_datetime=datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC),
            raw_data_instance=DirectIngestInstance.PRIMARY,
            is_invalidated=False,
        )

        expected_bq_metadata = DirectIngestRawBigQueryFileMetadata.new_with_defaults(
            file_id=1,
            region_code="US_XX",
            file_tag="file_tag",
            file_processed_time=None,
            is_invalidated=False,
            update_datetime=datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC),
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            self.raw_metadata_manager_secondary.transfer_metadata_to_new_instance(
                self.raw_metadata_manager, session
            )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestRawGCSFileMetadata)
                .filter_by(
                    region_code=self.raw_metadata_manager.region_code.upper(),
                    raw_data_instance=self.raw_metadata_manager.raw_data_instance.value,
                )
                .one()
            )
            # Check here that found_metadata has expected items and all instances are marked primary
            self.assertEqual(
                expected_gcs_metadata,
                convert_schema_object_to_entity(
                    metadata,
                    DirectIngestRawGCSFileMetadata,
                    populate_direct_back_edges=False,
                ),
            )

            metadata = (
                session.query(schema.DirectIngestRawBigQueryFileMetadata)
                .filter_by(
                    region_code=self.raw_metadata_manager.region_code.upper(),
                    raw_data_instance=self.raw_metadata_manager.raw_data_instance.value,
                )
                .one()
            )
            # Check here that found_metadata has expected items and all instances are marked primary
            self.assertEqual(
                expected_bq_metadata,
                convert_schema_object_to_entity(
                    metadata,
                    DirectIngestRawBigQueryFileMetadata,
                    populate_direct_back_edges=False,
                ),
            )

            # Assert that secondary instance was moved to primary instance, thus secondary no longer exists
            no_row_found_regex = r"No row was found when one was required"
            with self.assertRaisesRegex(
                sqlalchemy.exc.NoResultFound, no_row_found_regex
            ):
                _ = (
                    session.query(schema.DirectIngestRawGCSFileMetadata)
                    .filter_by(
                        region_code=self.raw_metadata_manager_secondary.region_code.upper(),
                        raw_data_instance=self.raw_metadata_manager_secondary.raw_data_instance.value,
                    )
                    .one()
                )

            no_row_found_regex = r"No row was found when one was required"
            with self.assertRaisesRegex(
                sqlalchemy.exc.NoResultFound, no_row_found_regex
            ):
                _ = (
                    session.query(schema.DirectIngestRawBigQueryFileMetadata)
                    .filter_by(
                        region_code=self.raw_metadata_manager_secondary.region_code.upper(),
                        raw_data_instance=self.raw_metadata_manager_secondary.raw_data_instance.value,
                    )
                    .one()
                )

            primary_pruning_metadata = (
                session.query(schema.DirectIngestRawDataPruningMetadata)
                .filter_by(
                    region_code=self.raw_metadata_manager.region_code.upper(),
                    raw_data_instance=self.raw_metadata_manager.raw_data_instance.value,
                )
                .all()
            )
            self.assertEqual(len(primary_pruning_metadata), 1)
            self.assertEqual(primary_pruning_metadata[0].file_tag, "test_file_tag")
            self.assertEqual(
                primary_pruning_metadata[0].automatic_pruning_enabled, True
            )
            self.assertEqual(
                primary_pruning_metadata[0].raw_file_primary_keys, "col1,col2"
            )

            secondary_pruning_metadata = (
                session.query(schema.DirectIngestRawDataPruningMetadata)
                .filter_by(
                    region_code=self.raw_metadata_manager_secondary.region_code.upper(),
                    raw_data_instance=self.raw_metadata_manager_secondary.raw_data_instance.value,
                )
                .all()
            )
            self.assertEqual(len(secondary_pruning_metadata), 0)

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_primary_to_secondary(self) -> None:
        expected_gcs: List[DirectIngestRawGCSFileMetadata] = []
        expected_bq: List[DirectIngestRawBigQueryFileMetadata] = []
        for i in range(0, 3):
            raw_unprocessed_path = make_unprocessed_raw_data_path(
                "bucket/file_tag.csv",
                dt=datetime.datetime.now(tz=datetime.UTC) + timedelta(hours=i),
            )
            self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                raw_unprocessed_path
            )

            expected_gcs.append(
                DirectIngestRawGCSFileMetadata.new_with_defaults(
                    gcs_file_id=1,
                    region_code="US_XX",
                    file_tag="file_tag",
                    file_discovery_time=datetime.datetime(
                        2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC
                    ),
                    normalized_file_name=raw_unprocessed_path.file_name,
                    update_datetime=datetime.datetime(
                        2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC
                    )
                    + timedelta(hours=i),
                    raw_data_instance=DirectIngestInstance.SECONDARY,
                    is_invalidated=False,
                )
            )

            expected_bq.append(
                DirectIngestRawBigQueryFileMetadata.new_with_defaults(
                    file_id=1,
                    region_code="US_XX",
                    file_tag="file_tag",
                    file_processed_time=None,
                    is_invalidated=False,
                    update_datetime=datetime.datetime(
                        2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC
                    )
                    + timedelta(hours=i),
                    raw_data_instance=DirectIngestInstance.SECONDARY,
                )
            )

        # Act
        with SessionFactory.using_database(self.database_key) as session:
            self.raw_metadata_manager.transfer_metadata_to_new_instance(
                self.raw_metadata_manager_secondary, session
            )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestRawGCSFileMetadata)
                .filter_by(
                    region_code=self.raw_metadata_manager_secondary.region_code.upper(),
                    raw_data_instance=self.raw_metadata_manager_secondary.raw_data_instance.value,
                )
                .all()
            )
            # Check here that found_metadata has expected items and all instances are marked primary
            self.assertEqual(
                expected_gcs,
                [
                    convert_schema_object_to_entity(
                        file,
                        DirectIngestRawGCSFileMetadata,
                        populate_direct_back_edges=False,
                    )
                    for file in metadata
                ],
            )

            metadata = (
                session.query(schema.DirectIngestRawBigQueryFileMetadata)
                .filter_by(
                    region_code=self.raw_metadata_manager_secondary.region_code.upper(),
                    raw_data_instance=self.raw_metadata_manager_secondary.raw_data_instance.value,
                )
                .all()
            )
            # Check here that found_metadata has expected items and all instances are marked primary
            self.assertEqual(
                expected_bq,
                [
                    convert_schema_object_to_entity(
                        file,
                        DirectIngestRawBigQueryFileMetadata,
                        populate_direct_back_edges=False,
                    )
                    for file in metadata
                ],
            )

            # Assert that secondary instance was moved to primary instance, thus secondary no longer exists
            no_row_found_regex = r"No row was found when one was required"
            with self.assertRaisesRegex(
                sqlalchemy.exc.NoResultFound, no_row_found_regex
            ):
                _ = (
                    session.query(schema.DirectIngestRawGCSFileMetadata)
                    .filter_by(
                        region_code=self.raw_metadata_manager.region_code.upper(),
                        raw_data_instance=self.raw_metadata_manager.raw_data_instance.value,
                    )
                    .one()
                )

            no_row_found_regex = r"No row was found when one was required"
            with self.assertRaisesRegex(
                sqlalchemy.exc.NoResultFound, no_row_found_regex
            ):
                _ = (
                    session.query(schema.DirectIngestRawBigQueryFileMetadata)
                    .filter_by(
                        region_code=self.raw_metadata_manager.region_code.upper(),
                        raw_data_instance=self.raw_metadata_manager.raw_data_instance.value,
                    )
                    .one()
                )

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_primary_to_primary(self) -> None:
        raw_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )

        same_instance = (
            r"Either state codes are not the same or new instance is same as origin"
        )
        with self.assertRaisesRegex(ValueError, same_instance):
            with SessionFactory.using_database(self.database_key) as session:
                self.raw_metadata_manager.transfer_metadata_to_new_instance(
                    self.raw_metadata_manager, session
                )

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_secondary_to_secondary(self) -> None:
        raw_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )
        self.raw_metadata_manager_secondary.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )

        same_instance = (
            r"Either state codes are not the same or new instance is same as origin"
        )
        with self.assertRaisesRegex(ValueError, same_instance):
            with SessionFactory.using_database(self.database_key) as session:
                self.raw_metadata_manager_secondary.transfer_metadata_to_new_instance(
                    self.raw_metadata_manager_secondary, session
                )

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_different_states(self) -> None:
        raw_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )
        self.raw_metadata_manager_secondary.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )

        self.raw_metadata_manager_dif_state = DirectIngestRawFileMetadataManager(
            region_code="us_yy",
            raw_data_instance=DirectIngestInstance.SECONDARY,
        )

        dif_state = (
            r"Either state codes are not the same or new instance is same as origin"
        )
        with self.assertRaisesRegex(ValueError, dif_state):
            with SessionFactory.using_database(self.database_key) as session:
                self.raw_metadata_manager_secondary.transfer_metadata_to_new_instance(
                    self.raw_metadata_manager_dif_state, session
                )

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_existing_raw_data(self) -> None:
        raw_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )

        dif_state = (
            r"Destination instance should not have any valid raw file metadata rows."
        )
        with self.assertRaisesRegex(ValueError, dif_state):
            with SessionFactory.using_database(self.database_key) as session:
                self.raw_metadata_manager_secondary.transfer_metadata_to_new_instance(
                    self.raw_metadata_manager, session
                )

    @freeze_time("2015-01-02T03:04:06")
    def test_transfer_metadata_to_new_instance_existing_pruning_metadata_fails(
        self,
    ) -> None:
        raw_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )
        self.raw_metadata_manager_secondary.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )

        with SessionFactory.using_database(self.database_key) as session:
            pruning_metadata_primary = schema.DirectIngestRawDataPruningMetadata(
                region_code=self.raw_metadata_manager.region_code,
                raw_data_instance=self.raw_metadata_manager.raw_data_instance.value,
                file_tag="test_file_tag",
                updated_at=datetime.datetime.now(tz=datetime.UTC),
                automatic_pruning_enabled=True,
                raw_file_primary_keys="col1,col2",
                raw_files_contain_full_historical_lookback=False,
            )
            session.add(pruning_metadata_primary)

        expected_error = (
            r"Destination instance should not have any raw data pruning metadata rows."
        )
        with self.assertRaisesRegex(ValueError, expected_error):
            with SessionFactory.using_database(self.database_key) as session:
                self.raw_metadata_manager_secondary.transfer_metadata_to_new_instance(
                    self.raw_metadata_manager, session
                )

    def test_mark_instance_as_invalidated(self) -> None:
        raw_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )
        self.assertEqual(
            1, len(self.raw_metadata_manager.get_non_invalidated_raw_big_query_files())
        )

        with SessionFactory.using_database(self.database_key) as session:
            pruning_metadata = schema.DirectIngestRawDataPruningMetadata(
                region_code=self.raw_metadata_manager.region_code,
                raw_data_instance=self.raw_metadata_manager.raw_data_instance.value,
                file_tag="test_file_tag",
                updated_at=datetime.datetime.now(tz=datetime.UTC),
                automatic_pruning_enabled=True,
                raw_file_primary_keys="col1,col2",
                raw_files_contain_full_historical_lookback=False,
            )
            session.add(pruning_metadata)

        self.raw_metadata_manager.mark_instance_data_invalidated()
        self.assertEqual(
            0, len(self.raw_metadata_manager.get_non_invalidated_raw_big_query_files())
        )
        assert (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                raw_unprocessed_path_1
            ).is_invalidated
            is True
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            pruning_metadata_after = (
                session.query(schema.DirectIngestRawDataPruningMetadata)
                .filter_by(
                    region_code=self.raw_metadata_manager.region_code.upper(),
                    raw_data_instance=self.raw_metadata_manager.raw_data_instance.value,
                )
                .all()
            )
            self.assertEqual(len(pruning_metadata_after), 0)

    def test_mark_instance_as_invalidated_ungrouped(self) -> None:
        raw_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )
        raw_ungrouped_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_ungrouped_unprocessed_path_1, is_chunked_file=True
        )
        self.assertEqual(
            1, len(self.raw_metadata_manager.get_non_invalidated_raw_big_query_files())
        )

        self.raw_metadata_manager.mark_instance_data_invalidated()
        self.assertEqual(
            0, len(self.raw_metadata_manager.get_non_invalidated_raw_big_query_files())
        )
        assert (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                raw_unprocessed_path_1
            ).is_invalidated
            is True
        )
        assert (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                raw_ungrouped_unprocessed_path_1
            ).is_invalidated
            is True
        )

    def test_get_unprocessed_raw_files_eligible_for_import_when_secondary_db(
        self,
    ) -> None:
        enabled_secondary_import_manager = DirectIngestRawFileMetadataManager(
            region_code="us_va", raw_data_instance=DirectIngestInstance.SECONDARY
        )
        # Arrange
        raw_unprocessed_path_1 = make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
        )

        # Act
        enabled_secondary_import_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )

        self.assertEqual(
            1,
            len(
                enabled_secondary_import_manager.get_unprocessed_raw_big_query_files_eligible_for_import()
            ),
        )

    def test_get_unprocessed_raw_files_eligible_for_import_multiple_pending_files_with_same_file_tag(
        self,
    ) -> None:
        fixed_datetime = datetime.datetime(2022, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)

        # Generate four files of the same file_tag and mark each as discovered
        file_ids: List[int] = []
        for i in range(0, 4):
            file = make_unprocessed_raw_data_path(
                path_str="bucket/file_tag.csv",
                dt=fixed_datetime + timedelta(hours=i),
            )
            # Discover every file
            obj = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(file)
            assert obj.file_id is not None
            file_ids.append(obj.file_id)

        results: Dict[str, List[DirectIngestRawBigQueryFileMetadata]] = {}
        for i in range(1, len(file_ids) + 1):
            # mark as processed
            self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                file_ids[i - 1]
            )
            # get new eligible files
            results = (
                self.raw_metadata_manager.get_unprocessed_raw_big_query_files_eligible_for_import()
            )
            # make sure things match up
            self.assertEqual(1 if i != 4 else 0, len(results))
            self.assertEqual(4 - i, len(results["file_tag"]))
            for j in range(0, 4 - i):
                self.assertEqual(
                    fixed_datetime + timedelta(hours=j + i),
                    results["file_tag"][j].update_datetime,
                )

    def test_get_unprocessed_raw_files_eligible_for_import_multiple_pending_files_with_multiple_file_tags(
        self,
    ) -> None:
        fixed_datetime = datetime.datetime(2022, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)

        # Generate a number of files for two different file_tags at different steps of discovery and processing
        for i in range(1, 5):
            file_tag_1_path = make_unprocessed_raw_data_path(
                path_str="bucket/file_tag_1.csv",
                dt=fixed_datetime + timedelta(hours=i),
            )

            file_tag_2_path = make_unprocessed_raw_data_path(
                path_str="bucket/file_tag_2.csv",
                dt=fixed_datetime + timedelta(hours=i),
            )

            # Discover every file
            metadata_tag_1 = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                file_tag_1_path
            )
            metadata_tag_2 = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                file_tag_2_path
            )

            # Mark only the first file (of each file_tag) as already processed
            if i == 1:
                assert metadata_tag_1.file_id is not None
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    metadata_tag_1.file_id
                )
                assert metadata_tag_2.file_id is not None
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    metadata_tag_2.file_id
                )

        results: Dict[
            str, List[DirectIngestRawBigQueryFileMetadata]
        ] = (
            self.raw_metadata_manager.get_unprocessed_raw_big_query_files_eligible_for_import()
        )

        self.assertEqual(2, len(results))
        # Assert that the update_datetime for both file_tags is that second files of each
        for result in results.values():
            self.assertEqual(
                fixed_datetime + timedelta(hours=2), result[0].update_datetime
            )

    def test_get_max_update_datetimes(self) -> None:
        day_1 = datetime.datetime(2022, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)
        day_2 = datetime.datetime(2022, 10, 2, 0, 0, 0, tzinfo=datetime.UTC)
        day_3 = datetime.datetime(2022, 10, 3, 0, 0, 0, tzinfo=datetime.UTC)

        file_tag_1_path = make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_1.csv",
            dt=day_1,
        )
        file_tag_1_path_2 = make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_1.csv", dt=day_3
        )
        file_tag_2_path = make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_2.csv",
            dt=day_2,
        )
        file_tag_3_path = make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_3.csv",
            dt=day_3,
        )

        file_1_path_1 = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            file_tag_1_path
        )
        file_1_path_2 = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            file_tag_1_path_2
        )
        file_2_path_1 = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            file_tag_2_path
        )
        file_3_path_1 = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            file_tag_3_path
        )

        assert file_1_path_1.file_id is not None
        assert file_1_path_2.file_id is not None
        assert file_2_path_1.file_id is not None
        assert file_3_path_1.file_id is not None

        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            file_1_path_1.file_id
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            file_1_path_2.file_id
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            file_2_path_1.file_id
        )

        with SessionFactory.using_database(self.database_key) as session:
            results: Dict[
                str, datetime.datetime
            ] = self.raw_metadata_manager.get_max_update_datetimes(session)

        self.assertDictEqual(
            {
                "file_tag_1": day_3,
                "file_tag_2": day_2,
            },
            results,
        )

    def test_get_metadata_for_all_raw_files_in_region_empty(self) -> None:
        self.assertListEqual(
            [],
            self.raw_metadata_manager.get_metadata_for_all_raw_files_in_region(),
        )

    def test_get_metadata_for_all_raw_files_in_region(self) -> None:

        dt = datetime.datetime(2022, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)

        # case: all processed, no unprocessed
        for day in range(0, 3):
            update_dt = dt + timedelta(days=day)
            file = make_unprocessed_raw_data_path(
                path_str="bucket/file_tag_1.csv",
                dt=update_dt,
            )
            with freeze_time(update_dt + timedelta(hours=1)):
                gcs_file = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    file
                )
            with freeze_time(update_dt + timedelta(hours=2)):
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    assert_type(gcs_file.file_id, int)
                )

        # case: some processed, some unprocessed
        for day in range(0, 3):
            update_dt = dt + timedelta(days=day)
            file = make_unprocessed_raw_data_path(
                path_str="bucket/file_tag_2.csv",
                dt=update_dt,
            )
            with freeze_time(update_dt + timedelta(hours=1)):
                gcs_file = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    file
                )
            if day == 2:
                continue
            with freeze_time(update_dt + timedelta(hours=2)):
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    assert_type(gcs_file.file_id, int)
                )

        # case: none processed, some unprocessed
        for day in range(0, 3):
            update_dt = dt + timedelta(days=day)
            file = make_unprocessed_raw_data_path(
                path_str="bucket/file_tag_3.csv",
                dt=update_dt,
            )
            with freeze_time(update_dt + timedelta(hours=1)):
                gcs_file = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    file
                )

        # case: some processed, some unprocessed, but max values are invalidated
        for day in range(0, 4):
            update_dt = dt + timedelta(days=day)
            file = make_unprocessed_raw_data_path(
                path_str="bucket/file_tag_4.csv",
                dt=update_dt,
            )
            with freeze_time(update_dt + timedelta(hours=1)):
                gcs_file = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    file
                )

            if day == 3:
                continue
            with freeze_time(update_dt + timedelta(hours=2)):
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    assert_type(gcs_file.file_id, int)
                )
            if day == 2:
                with SessionFactory.using_database(self.database_key) as session:
                    self.raw_metadata_manager.mark_file_as_invalidated_by_file_id_with_session(
                        session, assert_type(gcs_file.file_id, int)
                    )

        expected_list = [
            # case: all processed, no unprocessed
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag_1",
                num_unprocessed_files=0,
                num_processed_files=3,
                num_ungrouped_files=0,
                latest_processed_time=datetime.datetime(
                    2022, 10, 3, 2, 0, 0, tzinfo=datetime.UTC
                ),
                latest_discovery_time=datetime.datetime(
                    2022, 10, 3, 1, 0, 0, tzinfo=datetime.UTC
                ),
                latest_update_datetime=datetime.datetime(
                    2022, 10, 3, 0, 0, 0, tzinfo=datetime.UTC
                ),
            ),
            # case: some processed, some unprocessed
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag_2",
                num_unprocessed_files=1,
                num_processed_files=2,
                num_ungrouped_files=0,
                latest_processed_time=datetime.datetime(
                    2022, 10, 2, 2, 0, 0, tzinfo=datetime.UTC
                ),
                latest_discovery_time=datetime.datetime(
                    2022, 10, 3, 1, 0, 0, tzinfo=datetime.UTC
                ),
                latest_update_datetime=datetime.datetime(
                    2022, 10, 2, 0, 0, 0, tzinfo=datetime.UTC
                ),
            ),
            # case: none processed, some unprocessed
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag_3",
                num_unprocessed_files=3,
                num_processed_files=0,
                num_ungrouped_files=0,
                latest_processed_time=None,
                latest_discovery_time=datetime.datetime(
                    2022, 10, 3, 1, 0, 0, tzinfo=datetime.UTC
                ),
                latest_update_datetime=None,
            ),
            # case: some processed, some unprocessed, but max values are invalidated
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag_4",
                num_unprocessed_files=1,
                num_processed_files=2,
                num_ungrouped_files=0,
                latest_processed_time=datetime.datetime(
                    2022, 10, 2, 2, 0, 0, tzinfo=datetime.UTC
                ),
                latest_discovery_time=datetime.datetime(
                    2022, 10, 4, 1, 0, 0, tzinfo=datetime.UTC
                ),
                latest_update_datetime=datetime.datetime(
                    2022, 10, 2, 0, 0, 0, tzinfo=datetime.UTC
                ),
            ),
        ]

        self.assertListEqual(
            expected_list,
            self.raw_metadata_manager.get_metadata_for_all_raw_files_in_region(),
        )

    def test_get_metadata_for_all_raw_files_in_region_chunked(self) -> None:

        dt = datetime.datetime(2022, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)

        # case: not chunked, all processed, no unprocessed
        for day in range(0, 3):
            update_dt = dt + timedelta(days=day)
            file = make_unprocessed_raw_data_path(
                path_str="bucket/file_tag_0.csv",
                dt=update_dt,
            )
            with freeze_time(update_dt + timedelta(hours=1)):
                gcs_file = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    file
                )
            with freeze_time(update_dt + timedelta(hours=10)):
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    assert_type(gcs_file.file_id, int)
                )

        num_chunks = 3

        # case: chunked, all processed, no unprocessed
        for day in range(0, 3):
            update_dt = dt + timedelta(days=day)

            paths = []
            for chunk_num in range(num_chunks):
                file = make_unprocessed_raw_data_path(
                    path_str=f"bucket/file_tag_1-{chunk_num}.csv",
                    dt=update_dt,
                )
                with freeze_time(update_dt + timedelta(hours=1 + chunk_num)):
                    gcs_file = (
                        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                            file, is_chunked_file=True
                        )
                    )
                    paths.append(file)

            bq_file = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
                paths
            )

            with freeze_time(update_dt + timedelta(hours=10)):
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    bq_file.file_id
                )

        # case: chunked, some processed, some unprocessed
        for day in range(0, 3):
            update_dt = dt + timedelta(days=day)

            paths = []
            for chunk_num in range(num_chunks):
                file = make_unprocessed_raw_data_path(
                    path_str=f"bucket/file_tag_2-{chunk_num}.csv",
                    dt=update_dt,
                )
                with freeze_time(update_dt + timedelta(hours=1 + chunk_num)):
                    gcs_file = (
                        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                            file, is_chunked_file=True
                        )
                    )
                    paths.append(file)

            bq_file = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
                paths
            )

            if day == 2:
                continue
            with freeze_time(update_dt + timedelta(hours=10)):
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    bq_file.file_id
                )

        # case: chunked, none processed, some unprocessed
        for day in range(0, 3):
            update_dt = dt + timedelta(days=day)

            paths = []
            for chunk_num in range(num_chunks):
                file = make_unprocessed_raw_data_path(
                    path_str=f"bucket/file_tag_3-{chunk_num}.csv",
                    dt=update_dt,
                )
                with freeze_time(update_dt + timedelta(hours=1 + chunk_num)):
                    gcs_file = (
                        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                            file, is_chunked_file=True
                        )
                    )
                    paths.append(file)

            self.raw_metadata_manager.register_raw_big_query_file_for_paths(paths)

        # case: chunked some processed, some unprocessed, but max values are invalidated
        for day in range(0, 4):
            update_dt = dt + timedelta(days=day)

            paths = []
            for chunk_num in range(num_chunks):
                file = make_unprocessed_raw_data_path(
                    path_str=f"bucket/file_tag_4-{chunk_num}.csv",
                    dt=update_dt,
                )
                with freeze_time(update_dt + timedelta(hours=1 + chunk_num)):
                    gcs_file = (
                        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                            file, is_chunked_file=True
                        )
                    )
                    paths.append(file)

            bq_file = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
                paths
            )

            if day == 3:
                continue
            with freeze_time(update_dt + timedelta(hours=10)):
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    bq_file.file_id
                )

            if day == 2:
                with SessionFactory.using_database(self.database_key) as session:
                    self.raw_metadata_manager.mark_file_as_invalidated_by_file_id_with_session(
                        session, bq_file.file_id
                    )

        # case: chunked not grouped, some processed, some not
        for day in range(0, 4):
            update_dt = dt + timedelta(days=day)

            paths = []
            for chunk_num in range(num_chunks):
                file = make_unprocessed_raw_data_path(
                    path_str=f"bucket/file_tag_5-{chunk_num}.csv",
                    dt=update_dt,
                )
                with freeze_time(update_dt + timedelta(hours=1 + chunk_num)):
                    gcs_file = (
                        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                            file, is_chunked_file=True
                        )
                    )
                    paths.append(file)

            if day == 2:
                continue

            bq_file = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
                paths
            )

            if day == 3:
                continue
            with freeze_time(update_dt + timedelta(hours=10)):
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    bq_file.file_id
                )

        expected_list = [
            # case: not chunked, all processed, no unprocessed
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag_0",
                num_unprocessed_files=0,
                num_processed_files=3,
                num_ungrouped_files=0,
                latest_processed_time=datetime.datetime(
                    2022, 10, 3, 10, 0, 0, tzinfo=datetime.UTC
                ),
                latest_discovery_time=datetime.datetime(
                    2022, 10, 3, 1, 0, 0, tzinfo=datetime.UTC
                ),
                latest_update_datetime=datetime.datetime(
                    2022, 10, 3, 0, 0, 0, tzinfo=datetime.UTC
                ),
            ),
            # case: chunked, all processed, no unprocessed
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag_1",
                num_unprocessed_files=0,
                num_processed_files=3,
                num_ungrouped_files=0,
                latest_processed_time=datetime.datetime(
                    2022, 10, 3, 10, 0, 0, tzinfo=datetime.UTC
                ),
                latest_discovery_time=datetime.datetime(
                    2022, 10, 3, 3, 0, 0, tzinfo=datetime.UTC
                ),
                latest_update_datetime=datetime.datetime(
                    2022, 10, 3, 0, 0, 0, tzinfo=datetime.UTC
                ),
            ),
            # case: chunked, some processed, some unprocessed
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag_2",
                num_unprocessed_files=1,
                num_processed_files=2,
                num_ungrouped_files=0,
                latest_processed_time=datetime.datetime(
                    2022, 10, 2, 10, 0, 0, tzinfo=datetime.UTC
                ),
                latest_discovery_time=datetime.datetime(
                    2022, 10, 3, 3, 0, 0, tzinfo=datetime.UTC
                ),
                latest_update_datetime=datetime.datetime(
                    2022, 10, 2, 0, 0, 0, tzinfo=datetime.UTC
                ),
            ),
            # case: chunked, none processed, some unprocessed
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag_3",
                num_unprocessed_files=3,
                num_processed_files=0,
                num_ungrouped_files=0,
                latest_processed_time=None,
                latest_discovery_time=datetime.datetime(
                    2022, 10, 3, 3, 0, 0, tzinfo=datetime.UTC
                ),
                latest_update_datetime=None,
            ),
            # case: chunked some processed, some unprocessed, but max values are invalidated
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag_4",
                num_unprocessed_files=1,
                num_processed_files=2,
                num_ungrouped_files=0,
                latest_processed_time=datetime.datetime(
                    2022, 10, 2, 10, 0, 0, tzinfo=datetime.UTC
                ),
                latest_discovery_time=datetime.datetime(
                    2022, 10, 4, 3, 0, 0, tzinfo=datetime.UTC
                ),
                latest_update_datetime=datetime.datetime(
                    2022, 10, 2, 0, 0, 0, tzinfo=datetime.UTC
                ),
            ),
            DirectIngestRawFileMetadataSummary(
                file_tag="file_tag_5",
                num_unprocessed_files=1,
                num_processed_files=2,
                num_ungrouped_files=3,
                latest_processed_time=datetime.datetime(
                    2022, 10, 2, 10, 0, 0, tzinfo=datetime.UTC
                ),
                latest_discovery_time=datetime.datetime(
                    2022, 10, 4, 3, 0, 0, tzinfo=datetime.UTC
                ),
                latest_update_datetime=datetime.datetime(
                    2022, 10, 2, 0, 0, 0, tzinfo=datetime.UTC
                ),
            ),
        ]

        self.assertListEqual(
            expected_list,
            self.raw_metadata_manager.get_metadata_for_all_raw_files_in_region(),
        )

    def test_get_metadata_for_all_raw_files_in_region_chunked_nulls_not_included_in_distinct(
        self,
    ) -> None:

        curr_dt = datetime.datetime(2022, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)
        yesterday_processed = None
        yesterday_update = None
        num_chunks = 3

        for day in range(3):

            # chunked not grouped, make sure they don't show up in anywhere else
            paths = []
            for chunk_num in range(num_chunks):
                file = make_unprocessed_raw_data_path(
                    path_str=f"bucket/file_tag_0-{chunk_num}.csv",
                    dt=curr_dt,
                )
                with freeze_time(curr_dt + timedelta(hours=1 + chunk_num)):
                    self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                        file, is_chunked_file=True
                    )

                    paths.append(file)

            expected_list = [
                DirectIngestRawFileMetadataSummary(
                    file_tag="file_tag_0",
                    num_unprocessed_files=0,
                    num_processed_files=day,
                    num_ungrouped_files=3,
                    latest_processed_time=yesterday_processed,
                    latest_discovery_time=curr_dt + timedelta(hours=3),
                    latest_update_datetime=yesterday_update,
                ),
            ]

            self.assertListEqual(
                expected_list,
                self.raw_metadata_manager.get_metadata_for_all_raw_files_in_region(),
            )

            # chunked and grouped

            bq_file = self.raw_metadata_manager.register_raw_big_query_file_for_paths(
                paths
            )

            expected_list_after_grouping = [
                DirectIngestRawFileMetadataSummary(
                    file_tag="file_tag_0",
                    num_unprocessed_files=1,
                    num_processed_files=day,
                    num_ungrouped_files=0,
                    latest_processed_time=yesterday_processed,
                    latest_discovery_time=curr_dt + timedelta(hours=3),
                    latest_update_datetime=yesterday_update,
                ),
            ]

            self.assertListEqual(
                expected_list_after_grouping,
                self.raw_metadata_manager.get_metadata_for_all_raw_files_in_region(),
            )

            with freeze_time(curr_dt + timedelta(hours=10)):
                self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
                    bq_file.file_id
                )

            # chunked and grouped and processed
            latest_processed = curr_dt + timedelta(hours=10)

            expected_list_after_processed = [
                DirectIngestRawFileMetadataSummary(
                    file_tag="file_tag_0",
                    num_unprocessed_files=0,
                    num_processed_files=day + 1,
                    num_ungrouped_files=0,
                    latest_processed_time=latest_processed,
                    latest_discovery_time=curr_dt + timedelta(hours=3),
                    latest_update_datetime=curr_dt,
                ),
            ]

            self.assertListEqual(
                expected_list_after_processed,
                self.raw_metadata_manager.get_metadata_for_all_raw_files_in_region(),
            )

            yesterday_processed = latest_processed
            yesterday_update = curr_dt
            curr_dt = curr_dt + timedelta(days=1)

    def test_stale_secondary_raw_data(self) -> None:
        dt = datetime.datetime(2022, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)

        self.raw_metadata_manager_secondary.stale_secondary_raw_data()

        file = make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_0.csv",
            dt=dt,
        )
        with freeze_time(dt):
            gcs_file = (
                self.raw_metadata_manager_secondary.mark_raw_gcs_file_as_discovered(
                    file
                )
            )

            self.raw_metadata_manager_secondary.mark_file_as_invalidated_by_file_id(
                assert_type(gcs_file.file_id, int)
            )

        file_ii = make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_0.csv",
            dt=dt + timedelta(hours=1),
        )

        with freeze_time(dt + timedelta(hours=1)):
            _ = self.raw_metadata_manager_secondary.mark_raw_gcs_file_as_discovered(
                file_ii
            )

        missing_files_i = self.raw_metadata_manager_secondary.stale_secondary_raw_data()

        self.assertEqual(missing_files_i, [])

        # is invalidated in secondary but is before timestamp
        file_iii = make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_0.csv", dt=dt
        )

        with freeze_time(dt):
            gcs_file = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                file_iii
            )

        missing_files_ii = (
            self.raw_metadata_manager_secondary.stale_secondary_raw_data()
        )

        self.assertEqual(missing_files_ii, [])

        file_iv = make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_0.csv", dt=dt + timedelta(hours=4)
        )

        with freeze_time(dt + timedelta(hours=4)):
            gcs_file = self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                file_iv
            )

        missing_files_iii = (
            self.raw_metadata_manager_secondary.stale_secondary_raw_data()
        )

        self.assertEqual(missing_files_iii, [file_iv.blob_name])

        self.raw_metadata_manager.mark_file_as_invalidated_by_file_id(
            assert_type(gcs_file.file_id, int)
        )

        missing_files_iv = (
            self.raw_metadata_manager_secondary.stale_secondary_raw_data()
        )

        self.assertEqual(missing_files_iv, [])
