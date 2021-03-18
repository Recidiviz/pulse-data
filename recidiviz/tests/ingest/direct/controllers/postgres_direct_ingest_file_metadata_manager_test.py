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
"""Tests for PostgresDirectIngestFileMetadataManagerTest."""
import datetime
import unittest
from sqlite3 import IntegrityError
from typing import Type, List, Tuple

from freezegun import freeze_time
from mock import patch
from more_itertools import one

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
    to_normalized_unprocessed_file_path,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    GcsfsIngestViewExportArgs,
)
from recidiviz.ingest.direct.controllers.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestFileMetadataManager,
)
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SQLAlchemyDatabaseKey,
    DEFAULT_DB_NAME,
)
from recidiviz.persistence.entity.base_entity import entity_graph_eq, Entity
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawFileMetadata,
    DirectIngestIngestFileMetadata,
)
from recidiviz.tests.utils import fakes


class PostgresDirectIngestFileMetadataManagerTest(unittest.TestCase):
    """Tests for PostgresDirectIngestFileMetadataManagerTest."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        fakes.use_in_memory_sqlite_database(self.database_key)
        self.metadata_manager = PostgresDirectIngestFileMetadataManager(
            region_code="us_xx"
        )
        self.metadata_manager_other_region = PostgresDirectIngestFileMetadataManager(
            region_code="us_yy"
        )

        def fake_eq(e1: Entity, e2: Entity) -> bool:
            def _should_ignore_field_cb(_: Type, field_name: str) -> bool:
                return field_name == "file_id"

            return entity_graph_eq(e1, e2, _should_ignore_field_cb)

        self.entity_eq_patcher = patch(
            "recidiviz.persistence.entity.operations.entities.OperationsEntity.__eq__",
            fake_eq,
        )
        self.entity_eq_patcher.start()

    def tearDown(self) -> None:
        self.entity_eq_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

    @staticmethod
    def _make_unprocessed_path(
        path_str: str,
        file_type: GcsfsDirectIngestFileType,
        dt: datetime.datetime = datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
    ) -> GcsfsFilePath:
        normalized_path_str = to_normalized_unprocessed_file_path(
            original_file_path=path_str, file_type=file_type, dt=dt
        )
        return GcsfsFilePath.from_absolute_path(normalized_path_str)

    def _make_processed_path(
        self, path_str: str, file_type: GcsfsDirectIngestFileType
    ) -> GcsfsFilePath:
        path = self._make_unprocessed_path(path_str, file_type)
        # pylint:disable=protected-access
        return DirectIngestGCSFileSystem._to_processed_file_path(path)

    def test_register_processed_path_crashes(self) -> None:
        raw_processed_path = self._make_processed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.RAW_DATA
        )

        with self.assertRaises(ValueError):
            self.metadata_manager.mark_file_as_discovered(raw_processed_path)

        ingest_view_processed_path = self._make_processed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )

        with self.assertRaises(ValueError):
            self.metadata_manager.mark_file_as_discovered(ingest_view_processed_path)

    def test_get_raw_file_metadata_when_not_yet_registered(self) -> None:
        # Arrange
        raw_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.RAW_DATA
        )

        # Act
        with self.assertRaises(ValueError):
            self.metadata_manager.get_file_metadata(raw_unprocessed_path)

    @freeze_time("2015-01-02T03:04:06")
    def test_get_raw_file_metadata(self) -> None:
        # Arrange
        raw_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.RAW_DATA
        )

        # Act
        self.metadata_manager.mark_file_as_discovered(raw_unprocessed_path)
        metadata = self.metadata_manager.get_file_metadata(raw_unprocessed_path)

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

    @freeze_time("2015-01-02T03:04:06")
    def test_get_raw_file_metadata_unique_to_state(self) -> None:
        # Arrange
        raw_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.RAW_DATA
        )

        self.metadata_manager_other_region.mark_file_as_discovered(raw_unprocessed_path)

        # Act
        self.metadata_manager.mark_file_as_discovered(raw_unprocessed_path)
        metadata = self.metadata_manager.get_file_metadata(raw_unprocessed_path)

        # Assert
        expected_metadata = DirectIngestRawFileMetadata.new_with_defaults(
            region_code=self.metadata_manager.region_code,
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

    @freeze_time("2015-01-02T03:05:06.000007")
    def test_mark_raw_file_as_processed(self) -> None:
        # Arrange
        raw_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.RAW_DATA
        )

        # Act
        self.metadata_manager.mark_file_as_discovered(raw_unprocessed_path)
        self.metadata_manager.mark_file_as_processed(raw_unprocessed_path)

        # Assert
        metadata = self.metadata_manager.get_file_metadata(raw_unprocessed_path)

        self.assertEqual(
            datetime.datetime(2015, 1, 2, 3, 5, 6, 7), metadata.processed_time
        )

    def test_get_metadata_for_raw_files_discovered_after_datetime_empty(self) -> None:
        self.assertEqual(
            [],
            self.metadata_manager.get_metadata_for_raw_files_discovered_after_datetime(
                "any_tag", discovery_time_lower_bound_exclusive=None
            ),
        )

    def test_get_metadata_for_raw_files_discovered_after_datetime(self) -> None:
        with freeze_time("2015-01-02T03:05:05"):
            raw_unprocessed_path_1 = self._make_unprocessed_path(
                "bucket/file_tag.csv",
                GcsfsDirectIngestFileType.RAW_DATA,
                dt=datetime.datetime.utcnow(),
            )
            self.metadata_manager.mark_file_as_discovered(raw_unprocessed_path_1)
            self.metadata_manager_other_region.mark_file_as_discovered(
                raw_unprocessed_path_1
            )

        with freeze_time("2015-01-02T03:06:06"):
            raw_unprocessed_path_2 = self._make_unprocessed_path(
                "bucket/other_tag.csv",
                GcsfsDirectIngestFileType.RAW_DATA,
                dt=datetime.datetime.utcnow(),
            )
            self.metadata_manager.mark_file_as_discovered(raw_unprocessed_path_2)

        with freeze_time("2015-01-02T03:07:07"):
            raw_unprocessed_path_3 = self._make_unprocessed_path(
                "bucket/file_tag.csv",
                GcsfsDirectIngestFileType.RAW_DATA,
                dt=datetime.datetime.utcnow(),
            )
            self.metadata_manager.mark_file_as_discovered(raw_unprocessed_path_3)

        expected_list = [
            DirectIngestRawFileMetadata.new_with_defaults(
                region_code=self.metadata_manager.region_code,
                file_tag="file_tag",
                discovery_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
                normalized_file_name="unprocessed_2015-01-02T03:05:05:000000_raw_file_tag.csv",
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2015, 1, 2, 3, 5, 5
                ),
            ),
            DirectIngestRawFileMetadata.new_with_defaults(
                region_code=self.metadata_manager.region_code,
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
            self.metadata_manager.get_metadata_for_raw_files_discovered_after_datetime(
                "file_tag", discovery_time_lower_bound_exclusive=None
            ),
        )

        expected_list = expected_list[-1:]

        self.assertEqual(
            expected_list,
            self.metadata_manager.get_metadata_for_raw_files_discovered_after_datetime(
                "file_tag",
                discovery_time_lower_bound_exclusive=datetime.datetime(
                    2015, 1, 2, 3, 7, 0
                ),
            ),
        )

    def test_ingest_view_file_progression(self) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        ingest_view_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        self.run_ingest_view_file_progression(
            args, self.metadata_manager, ingest_view_unprocessed_path
        )

    def test_ingest_view_file_progression_discovery_before_export_recorded(
        self,
    ) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        ingest_view_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        self.run_ingest_view_file_progression(
            args,
            self.metadata_manager,
            ingest_view_unprocessed_path,
            discovery_before_export_recorded=True,
        )

    def test_ingest_view_file_progression_two_regions(self) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        ingest_view_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        self.run_ingest_view_file_progression(
            args, self.metadata_manager, ingest_view_unprocessed_path
        )
        self.run_ingest_view_file_progression(
            args, self.metadata_manager_other_region, ingest_view_unprocessed_path
        )

    def test_ingest_view_file_progression_same_args_twice_throws(self) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        ingest_view_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        self.run_ingest_view_file_progression(
            args, self.metadata_manager, ingest_view_unprocessed_path
        )

        with self.assertRaises(IntegrityError):
            ingest_view_unprocessed_path = self._make_unprocessed_path(
                "bucket/file_tag.csv",
                GcsfsDirectIngestFileType.INGEST_VIEW,
                dt=datetime.datetime.now(),
            )
            self.run_ingest_view_file_progression(
                args, self.metadata_manager, ingest_view_unprocessed_path
            )

        session = SessionFactory.for_database(self.database_key)
        results = session.query(schema.DirectIngestIngestFileMetadata).all()
        self.assertEqual(1, len(results))

    def test_ingest_view_file_same_args_after_invalidation(self) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        ingest_view_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        self.run_ingest_view_file_progression(
            args, self.metadata_manager, ingest_view_unprocessed_path
        )

        # Invalidate the previous row
        session = SessionFactory.for_database(self.database_key)
        results = session.query(schema.DirectIngestIngestFileMetadata).all()
        result = one(results)
        result.is_invalidated = True
        session.commit()

        # Now we can rerun with the same args
        ingest_view_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv",
            GcsfsDirectIngestFileType.INGEST_VIEW,
            dt=datetime.datetime.now(),
        )
        self.run_ingest_view_file_progression(
            args, self.metadata_manager, ingest_view_unprocessed_path
        )

    def test_ingest_view_file_progression_two_files_same_tag(self) -> None:

        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        ingest_view_unprocessed_path_1 = self._make_unprocessed_path(
            "bucket/file_tag.csv",
            GcsfsDirectIngestFileType.INGEST_VIEW,
            dt=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )
        self.run_ingest_view_file_progression(
            args, self.metadata_manager, ingest_view_unprocessed_path_1
        )

        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 3, 3, 3, 3, 3),
        )

        ingest_view_unprocessed_path_2 = self._make_unprocessed_path(
            "bucket/file_tag.csv",
            GcsfsDirectIngestFileType.INGEST_VIEW,
            dt=datetime.datetime(2015, 1, 3, 3, 3, 3, 3),
        )
        self.run_ingest_view_file_progression(
            args, self.metadata_manager, ingest_view_unprocessed_path_2
        )

        session = SessionFactory.for_database(self.database_key)
        results = session.query(schema.DirectIngestIngestFileMetadata).all()

        self.assertEqual(
            {
                ingest_view_unprocessed_path_1.file_name,
                ingest_view_unprocessed_path_2.file_name,
            },
            {r.normalized_file_name for r in results},
        )
        for r in results:
            self.assertTrue(r.export_time)
            self.assertTrue(r.processed_time)

    def run_ingest_view_file_progression(
        self,
        export_args: GcsfsIngestViewExportArgs,
        metadata_manager: PostgresDirectIngestFileMetadataManager,
        ingest_view_unprocessed_path: GcsfsFilePath,
        discovery_before_export_recorded: bool = False,
        split_file: bool = False,
    ) -> None:
        """Runs through the full progression of operations we expect to run on an individual ingest view file."""
        with freeze_time("2015-01-02T03:05:05"):
            metadata_manager.register_ingest_file_export_job(export_args)

        ingest_file_metadata = metadata_manager.get_ingest_view_metadata_for_export_job(
            export_args
        )

        expected_metadata = DirectIngestIngestFileMetadata.new_with_defaults(
            region_code=metadata_manager.region_code,
            file_tag=export_args.ingest_view_name,
            is_invalidated=False,
            is_file_split=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
            datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export,
            normalized_file_name=None,
            export_time=None,
            discovery_time=None,
            processed_time=None,
            ingest_database_name=DEFAULT_DB_NAME,
        )

        self.assertEqual(expected_metadata, ingest_file_metadata)

        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.register_ingest_view_export_file_name(
                ingest_file_metadata, ingest_view_unprocessed_path
            )
        expected_metadata.normalized_file_name = ingest_view_unprocessed_path.file_name

        metadata = metadata_manager.get_file_metadata(ingest_view_unprocessed_path)
        self.assertEqual(expected_metadata, metadata)

        # ... export actually performed in here
        if discovery_before_export_recorded:
            with freeze_time("2015-01-02T03:06:07"):
                metadata_manager.mark_file_as_discovered(ingest_view_unprocessed_path)

            expected_metadata.discovery_time = datetime.datetime(2015, 1, 2, 3, 6, 7)
            expected_metadata.export_time = datetime.datetime(2015, 1, 2, 3, 6, 7)

            metadata = metadata_manager.get_file_metadata(ingest_view_unprocessed_path)
            self.assertEqual(expected_metadata, metadata)

        with freeze_time("2015-01-02T03:06:08"):
            self.metadata_manager.mark_ingest_view_exported(ingest_file_metadata)

        expected_metadata.export_time = datetime.datetime(2015, 1, 2, 3, 6, 8)

        metadata = metadata_manager.get_file_metadata(ingest_view_unprocessed_path)
        self.assertEqual(expected_metadata, metadata)

        if not discovery_before_export_recorded:
            with freeze_time("2015-01-02T03:07:07"):
                metadata_manager.mark_file_as_discovered(ingest_view_unprocessed_path)

            expected_metadata.discovery_time = datetime.datetime(2015, 1, 2, 3, 7, 7)
            metadata = metadata_manager.get_file_metadata(ingest_view_unprocessed_path)
            self.assertEqual(expected_metadata, metadata)

        split_file_paths_and_metadata: List[
            Tuple[GcsfsFilePath, DirectIngestIngestFileMetadata]
        ] = []
        if split_file:
            metadata = metadata_manager.get_file_metadata(ingest_view_unprocessed_path)
            if not isinstance(metadata, DirectIngestIngestFileMetadata):
                self.fail(f"Unexpected metadata type {type(metadata)}")

            for i in range(2):
                split_file_path = self._make_unprocessed_path(
                    f"bucket/split{i}.csv", GcsfsDirectIngestFileType.INGEST_VIEW
                )
                self.run_split_ingest_file_progression_pre_processing(
                    metadata_manager,
                    metadata,
                    split_file_path,
                    discovery_before_export_recorded,
                )

                split_file_metadata = metadata_manager.get_file_metadata(
                    split_file_path
                )

                if not isinstance(split_file_metadata, DirectIngestIngestFileMetadata):
                    self.fail(
                        f"Unexpected split_file_metadata type [{split_file_metadata}]."
                    )

                split_file_paths_and_metadata.append(
                    (split_file_path, split_file_metadata)
                )

        with freeze_time("2015-01-02T03:08:08"):
            metadata_manager.mark_file_as_processed(ingest_view_unprocessed_path)

        expected_metadata.processed_time = datetime.datetime(2015, 1, 2, 3, 8, 8)

        metadata = metadata_manager.get_file_metadata(ingest_view_unprocessed_path)

        self.assertEqual(expected_metadata, metadata)

        for split_file_path, split_file_metadata in split_file_paths_and_metadata:
            expected_metadata = split_file_metadata
            with freeze_time("2015-01-02T03:09:09"):
                metadata_manager.mark_file_as_processed(split_file_path)

            expected_metadata.processed_time = datetime.datetime(2015, 1, 2, 3, 9, 9)

            metadata = metadata_manager.get_file_metadata(split_file_path)

            self.assertEqual(expected_metadata, metadata)

    def run_split_ingest_file_progression_pre_processing(
        self,
        metadata_manager: PostgresDirectIngestFileMetadataManager,
        original_file_metadata: DirectIngestIngestFileMetadata,
        split_file_path: GcsfsFilePath,
        discovery_before_export_recorded: bool = False,
    ) -> None:
        """Runs through the full progression of operations we expect to run on
        a split ingest file, up until processing."""
        expected_metadata = DirectIngestIngestFileMetadata.new_with_defaults(
            region_code=metadata_manager.region_code,
            file_tag=original_file_metadata.file_tag,
            is_invalidated=False,
            is_file_split=True,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            datetimes_contained_lower_bound_exclusive=original_file_metadata.datetimes_contained_lower_bound_exclusive,
            datetimes_contained_upper_bound_inclusive=original_file_metadata.datetimes_contained_upper_bound_inclusive,
            normalized_file_name=split_file_path.file_name,
            export_time=None,
            discovery_time=None,
            processed_time=None,
            ingest_database_name=DEFAULT_DB_NAME,
        )

        with freeze_time("2015-01-02T03:05:05"):
            split_file_metadata = self.metadata_manager.register_ingest_file_split(
                original_file_metadata, split_file_path
            )

        self.assertEqual(expected_metadata, split_file_metadata)
        metadata = metadata_manager.get_file_metadata(split_file_path)
        self.assertEqual(expected_metadata, metadata)

        # ... export actually performed in here
        if discovery_before_export_recorded:
            with freeze_time("2015-01-02T03:06:07"):
                metadata_manager.mark_file_as_discovered(split_file_path)

            expected_metadata.discovery_time = datetime.datetime(2015, 1, 2, 3, 6, 7)
            expected_metadata.export_time = datetime.datetime(2015, 1, 2, 3, 6, 7)

            metadata = metadata_manager.get_file_metadata(split_file_path)
            self.assertEqual(expected_metadata, metadata)

        with freeze_time("2015-01-02T03:06:08"):
            self.metadata_manager.mark_ingest_view_exported(split_file_metadata)

        expected_metadata.export_time = datetime.datetime(2015, 1, 2, 3, 6, 8)

        metadata = metadata_manager.get_file_metadata(split_file_path)
        self.assertEqual(expected_metadata, metadata)

        if not discovery_before_export_recorded:
            with freeze_time("2015-01-02T03:07:07"):
                metadata_manager.mark_file_as_discovered(split_file_path)

            expected_metadata.discovery_time = datetime.datetime(2015, 1, 2, 3, 7, 7)
            metadata = metadata_manager.get_file_metadata(split_file_path)
            self.assertEqual(expected_metadata, metadata)

    def test_ingest_then_split_progression(self) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        ingest_view_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        self.run_ingest_view_file_progression(
            args, self.metadata_manager, ingest_view_unprocessed_path, split_file=True
        )

    def test_ingest_then_split_progression_discovery_before_export_recorded(
        self,
    ) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        ingest_view_unprocessed_path = self._make_unprocessed_path(
            "bucket/file_tag.csv", GcsfsDirectIngestFileType.INGEST_VIEW
        )
        self.run_ingest_view_file_progression(
            args,
            self.metadata_manager,
            ingest_view_unprocessed_path,
            discovery_before_export_recorded=True,
            split_file=True,
        )

    def test_get_ingest_view_metadata_for_most_recent_valid_job_no_jobs(self) -> None:
        self.assertIsNone(
            self.metadata_manager.get_ingest_view_metadata_for_most_recent_valid_job(
                "any_tag"
            )
        )

    def test_get_ingest_view_metadata_for_most_recent_valid_job(self) -> None:
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager.register_ingest_file_export_job(
                GcsfsIngestViewExportArgs(
                    ingest_view_name="file_tag",
                    upper_bound_datetime_prev=None,
                    upper_bound_datetime_to_export=datetime.datetime(
                        2015, 1, 2, 2, 2, 2, 2
                    ),
                )
            )

        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.register_ingest_file_export_job(
                GcsfsIngestViewExportArgs(
                    ingest_view_name="file_tag",
                    upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
                    upper_bound_datetime_to_export=datetime.datetime(
                        2015, 1, 2, 3, 3, 3, 3
                    ),
                )
            )

        with freeze_time("2015-01-02T03:07:07"):
            self.metadata_manager.register_ingest_file_export_job(
                GcsfsIngestViewExportArgs(
                    ingest_view_name="another_tag",
                    upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
                    upper_bound_datetime_to_export=datetime.datetime(
                        2015, 1, 2, 3, 4, 4, 4
                    ),
                )
            )

        most_recent_valid_job = (
            self.metadata_manager.get_ingest_view_metadata_for_most_recent_valid_job(
                "file_tag"
            )
        )

        self.assertIsNotNone(most_recent_valid_job)
        if most_recent_valid_job is None:
            self.fail("most_recent_valid_job is unexpectedly None")

        self.assertEqual("file_tag", most_recent_valid_job.file_tag)
        self.assertEqual(
            datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            most_recent_valid_job.datetimes_contained_lower_bound_exclusive,
        )
        self.assertEqual(
            datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
            most_recent_valid_job.datetimes_contained_upper_bound_inclusive,
        )

        # Invalidate the row that was just returned
        session = SessionFactory.for_database(self.database_key)
        results = (
            session.query(schema.DirectIngestIngestFileMetadata)
            .filter_by(file_id=most_recent_valid_job.file_id)
            .all()
        )
        result = one(results)
        result.is_invalidated = True
        session.commit()

        most_recent_valid_job = (
            self.metadata_manager.get_ingest_view_metadata_for_most_recent_valid_job(
                "file_tag"
            )
        )
        if most_recent_valid_job is None:
            self.fail("most_recent_valid_job is unexpectedly None")
        self.assertEqual("file_tag", most_recent_valid_job.file_tag)
        self.assertEqual(
            None, most_recent_valid_job.datetimes_contained_lower_bound_exclusive
        )
        self.assertEqual(
            datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            most_recent_valid_job.datetimes_contained_upper_bound_inclusive,
        )

    def test_get_ingest_view_metadata_pending_export_empty(self) -> None:
        self.assertEqual(
            [], self.metadata_manager.get_ingest_view_metadata_pending_export()
        )

    def test_get_ingest_view_metadata_pending_export_basic(self) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.register_ingest_file_export_job(args)

        expected_list = [
            DirectIngestIngestFileMetadata.new_with_defaults(
                region_code="US_XX",
                file_tag="file_tag",
                is_invalidated=False,
                is_file_split=False,
                job_creation_time=datetime.datetime(2015, 1, 2, 3, 6, 6),
                datetimes_contained_lower_bound_exclusive=datetime.datetime(
                    2015, 1, 2, 2, 2, 2, 2
                ),
                datetimes_contained_upper_bound_inclusive=datetime.datetime(
                    2015, 1, 2, 3, 3, 3, 3
                ),
                ingest_database_name=DEFAULT_DB_NAME,
            )
        ]

        self.assertEqual(
            expected_list,
            self.metadata_manager.get_ingest_view_metadata_pending_export(),
        )

    def test_get_ingest_view_metadata_pending_export_all_exported(self) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.register_ingest_file_export_job(args)

        with freeze_time("2015-01-02T03:07:07"):
            path = self._make_unprocessed_path(
                "bucket/file_tag.csv",
                file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
                dt=datetime.datetime.utcnow(),
            )
            metadata = self.metadata_manager.get_ingest_view_metadata_for_export_job(
                args
            )
            self.metadata_manager.register_ingest_view_export_file_name(metadata, path)
            # ... export actually performed in here
            self.metadata_manager.mark_ingest_view_exported(metadata)

        self.assertEqual(
            [], self.metadata_manager.get_ingest_view_metadata_pending_export()
        )

    def test_get_ingest_view_metadata_pending_export_all_exported_in_region(
        self,
    ) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        args_other_region = GcsfsIngestViewExportArgs(
            ingest_view_name="other_file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.register_ingest_file_export_job(args)
            self.metadata_manager_other_region.register_ingest_file_export_job(
                args_other_region
            )

        with freeze_time("2015-01-02T03:07:07"):
            path = self._make_unprocessed_path(
                "bucket/file_tag.csv",
                file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
                dt=datetime.datetime.utcnow(),
            )
            metadata = self.metadata_manager.get_ingest_view_metadata_for_export_job(
                args
            )
            self.metadata_manager.register_ingest_view_export_file_name(metadata, path)
            # ... export actually performed in here
            self.metadata_manager.mark_ingest_view_exported(metadata)

        self.assertEqual(
            [], self.metadata_manager.get_ingest_view_metadata_pending_export()
        )

    def test_register_ingest_view_export_file_name_already_exists_raises(self) -> None:
        args = GcsfsIngestViewExportArgs(
            ingest_view_name="file_tag",
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        metadata_entity = self.metadata_manager.register_ingest_file_export_job(args)
        self.metadata_manager.register_ingest_view_export_file_name(
            metadata_entity,
            self._make_unprocessed_path(
                "bucket/file_tag.csv", GcsfsDirectIngestFileType.INGEST_VIEW
            ),
        )

        with self.assertRaises(ValueError):
            self.metadata_manager.register_ingest_view_export_file_name(
                metadata_entity,
                self._make_unprocessed_path(
                    "bucket/file_tag.csv", GcsfsDirectIngestFileType.INGEST_VIEW
                ),
            )
