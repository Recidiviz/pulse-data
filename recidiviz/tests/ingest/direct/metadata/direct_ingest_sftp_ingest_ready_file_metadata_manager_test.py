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
"""Tests for DirectIngestSftpIngestReadyFileMetadataManager"""
import datetime
import unittest
from typing import Type
from unittest.mock import patch

import sqlalchemy
from freezegun import freeze_time

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.metadata.direct_ingest_sftp_ingest_ready_file_metadata_manager import (
    DirectIngestSftpIngestReadyFileMetadataManager,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity, entity_graph_eq
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestSftpIngestReadyFileMetadata,
)
from recidiviz.tests.utils import fakes


def _fake_eq(e1: Entity, e2: Entity) -> bool:
    def _should_ignore_field_cb(_: Type, field_name: str) -> bool:
        return field_name == "file_id"

    return entity_graph_eq(e1, e2, _should_ignore_field_cb)


@freeze_time("2020-01-01T01:01:01")
class TestDirectIngestSftpIngestReadyFileMetadataManager(unittest.TestCase):
    """Tests for DirectIngestSftpIngestReadyFileMetadataManager"""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        fakes.use_in_memory_sqlite_database(self.database_key)
        self.metadata_manager = DirectIngestSftpIngestReadyFileMetadataManager(
            region_code="us_xx"
        )
        self.entity_eq_patcher = patch(
            "recidiviz.persistence.entity.base_entity.Entity.__eq__",
            _fake_eq,
        )
        self.entity_eq_patcher.start()

    def tearDown(self) -> None:
        self.entity_eq_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

    def test_get_ingest_ready_file_metadata_not_yet_existing(self) -> None:
        with self.assertRaises(sqlalchemy.exc.NoResultFound):
            self.metadata_manager.get_ingest_ready_file_metadata(
                GcsfsFilePath("bucket", "not_found"), "remote_file"
            )

    def test_get_ingest_ready_file_metadata(self) -> None:
        processed_file = GcsfsFilePath("bucket", "processed_file")
        self.metadata_manager.mark_ingest_ready_file_as_discovered(
            processed_file, "remote_file"
        )
        metadata = self.metadata_manager.get_ingest_ready_file_metadata(
            processed_file, "remote_file"
        )

        expected_metadata = DirectIngestSftpIngestReadyFileMetadata.new_with_defaults(
            region_code=self.metadata_manager.region_code,
            post_processed_normalized_file_path=processed_file.file_name,
            remote_file_path="remote_file",
            file_discovery_time=datetime.datetime(2020, 1, 1, 1, 1, 1),
            file_upload_time=None,
        )

        self.assertIsInstance(metadata, DirectIngestSftpIngestReadyFileMetadata)
        self.assertIsNotNone(metadata.file_id)

        self.assertEqual(expected_metadata, metadata)

    def test_has_ingest_ready_file_been_discovered(self) -> None:
        discovered_file = GcsfsFilePath("bucket", "discovered")
        self.metadata_manager.mark_ingest_ready_file_as_discovered(
            discovered_file, "remote_file"
        )
        self.assertTrue(
            self.metadata_manager.has_ingest_ready_file_been_discovered(
                discovered_file, "remote_file"
            )
        )

    def test_has_ingest_ready_file_been_discovered_false_for_no_rows(self) -> None:
        self.assertFalse(
            self.metadata_manager.has_ingest_ready_file_been_discovered(
                GcsfsFilePath("bucket", "no_file"), "no_remote_file"
            )
        )

    def test_has_ingest_ready_file_been_uploaded(self) -> None:
        uploaded_file = GcsfsFilePath("bucket", "uploaded_file")
        self.metadata_manager.mark_ingest_ready_file_as_discovered(
            uploaded_file, "remote_file"
        )
        self.metadata_manager.mark_ingest_ready_file_as_uploaded(
            uploaded_file, "remote_file"
        )

        self.assertTrue(
            self.metadata_manager.has_ingest_ready_file_been_discovered(
                uploaded_file, "remote_file"
            )
        )
        self.assertTrue(
            self.metadata_manager.has_ingest_ready_file_been_uploaded(
                uploaded_file, "remote_file"
            )
        )

    def test_has_ingest_ready_file_been_uploaded_false_for_no_rows(self) -> None:
        self.assertFalse(
            self.metadata_manager.has_ingest_ready_file_been_uploaded(
                GcsfsFilePath("bucket", "no_file"), "no_remote_file"
            )
        )

    def test_has_ingest_ready_file_been_uploaded_false_for_no_upload_time(
        self,
    ) -> None:
        self.metadata_manager.mark_ingest_ready_file_as_discovered(
            GcsfsFilePath("bucket", "not_uploaded"), "remote_file"
        )

        self.assertFalse(
            self.metadata_manager.has_ingest_ready_file_been_uploaded(
                GcsfsFilePath("bucket", "not_uploaded"), "remote_file"
            )
        )

    def test_mark_ingest_ready_file_as_uploaded(self) -> None:
        uploaded_file = GcsfsFilePath("bucket", "uploaded_file")
        self.metadata_manager.mark_ingest_ready_file_as_discovered(
            uploaded_file, "remote_file"
        )
        self.metadata_manager.mark_ingest_ready_file_as_uploaded(
            uploaded_file, "remote_file"
        )

        metadata = self.metadata_manager.get_ingest_ready_file_metadata(
            uploaded_file, "remote_file"
        )

        self.assertEqual(
            datetime.datetime(2020, 1, 1, 1, 1, 1), metadata.file_upload_time
        )

    def test_get_not_uploaded_ingest_ready_files(self) -> None:
        discovered_not_uploaded = GcsfsFilePath("bucket", "discovered_not_uploaded")
        discovered_and_uploaded = GcsfsFilePath("bucket", "discovered_and_uploaded")
        self.metadata_manager.mark_ingest_ready_file_as_discovered(
            discovered_not_uploaded, "remote_file"
        )
        self.metadata_manager.mark_ingest_ready_file_as_discovered(
            discovered_and_uploaded, "remote_file"
        )
        self.metadata_manager.mark_ingest_ready_file_as_uploaded(
            discovered_and_uploaded, "remote_file"
        )

        metadatas = self.metadata_manager.get_not_uploaded_ingest_ready_files()
        self.assertListEqual(
            metadatas,
            [
                DirectIngestSftpIngestReadyFileMetadata.new_with_defaults(
                    region_code=self.metadata_manager.region_code,
                    post_processed_normalized_file_path=discovered_not_uploaded.file_name,
                    remote_file_path="remote_file",
                    file_discovery_time=datetime.datetime(2020, 1, 1, 1, 1, 1),
                    file_upload_time=None,
                )
            ],
        )
