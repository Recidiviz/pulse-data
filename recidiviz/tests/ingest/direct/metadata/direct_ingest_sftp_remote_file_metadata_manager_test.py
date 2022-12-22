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
"""Tests for DirectIngestSftpRemoteFileMetadataManager"""
import datetime
import unittest
from typing import Type
from unittest.mock import patch

import pytz
import sqlalchemy
from freezegun import freeze_time

from recidiviz.ingest.direct.metadata.direct_ingest_sftp_remote_file_metadata_manager import (
    DirectIngestSftpRemoteFileMetadataManager,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity, entity_graph_eq
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestSftpRemoteFileMetadata,
)
from recidiviz.tests.utils import fakes


def _fake_eq(e1: Entity, e2: Entity) -> bool:
    def _should_ignore_field_cb(_: Type, field_name: str) -> bool:
        return field_name == "file_id"

    return entity_graph_eq(e1, e2, _should_ignore_field_cb)


@freeze_time("2020-01-01T01:01:01")
class TestDirectIngestSftpRemoteFileMetadataManager(unittest.TestCase):
    """Tests for DirectIngestSftpRemoteFileMetadataManager"""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        fakes.use_in_memory_sqlite_database(self.database_key)
        self.metadata_manager = DirectIngestSftpRemoteFileMetadataManager(
            region_code="us_xx"
        )
        self.entity_eq_patcher = patch(
            "recidiviz.persistence.entity.base_entity.Entity.__eq__",
            _fake_eq,
        )
        self.entity_eq_patcher.start()

        self.timestamp = datetime.datetime.now(tz=pytz.UTC).timestamp()

    def tearDown(self) -> None:
        self.entity_eq_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

    def test_get_remote_file_metadata_not_yet_existing(self) -> None:
        with self.assertRaises(sqlalchemy.exc.NoResultFound):
            self.metadata_manager.get_remote_file_metadata(
                "not_yet_existing.txt", self.timestamp
            )

    def test_get_remote_file_metadata(self) -> None:
        self.metadata_manager.mark_remote_file_as_discovered("file", self.timestamp)
        metadata = self.metadata_manager.get_remote_file_metadata(
            "file", self.timestamp
        )

        expected_metadata = DirectIngestSftpRemoteFileMetadata.new_with_defaults(
            region_code=self.metadata_manager.region_code,
            remote_file_path="file",
            sftp_timestamp=self.timestamp,
            file_discovery_time=datetime.datetime(2020, 1, 1, 1, 1, 1),
            file_download_time=None,
        )

        self.assertIsInstance(metadata, DirectIngestSftpRemoteFileMetadata)
        self.assertIsNotNone(metadata.file_id)

        self.assertEqual(expected_metadata, metadata)

    def test_has_remote_file_been_discovered(self) -> None:
        self.metadata_manager.mark_remote_file_as_discovered("file", self.timestamp)
        self.assertTrue(
            self.metadata_manager.has_remote_file_been_discovered(
                "file", self.timestamp
            )
        )

    def test_has_remote_file_been_discovered_false_for_no_rows(self) -> None:
        self.assertFalse(
            self.metadata_manager.has_remote_file_been_discovered(
                "no_row", self.timestamp
            )
        )

    def test_has_remote_file_been_downloaded(self) -> None:
        self.metadata_manager.mark_remote_file_as_discovered("file", self.timestamp)
        self.metadata_manager.mark_remote_file_as_downloaded("file", self.timestamp)

        self.assertTrue(
            self.metadata_manager.has_remote_file_been_discovered(
                "file", self.timestamp
            )
        )
        self.assertTrue(
            self.metadata_manager.has_remote_file_been_downloaded(
                "file", self.timestamp
            )
        )

    def test_has_remote_file_been_downloaded_false_for_no_rows(self) -> None:
        self.assertFalse(
            self.metadata_manager.has_remote_file_been_downloaded(
                "no_row", self.timestamp
            )
        )

    def test_has_remote_file_been_downloaded_false_for_no_download_time(self) -> None:
        self.metadata_manager.mark_remote_file_as_discovered("file", self.timestamp)

        self.assertFalse(
            self.metadata_manager.has_remote_file_been_downloaded(
                "file", self.timestamp
            )
        )

    def test_mark_remote_file_as_downloaded(self) -> None:
        self.metadata_manager.mark_remote_file_as_discovered("file", self.timestamp)
        self.metadata_manager.mark_remote_file_as_downloaded("file", self.timestamp)

        metadata = self.metadata_manager.get_remote_file_metadata(
            "file", self.timestamp
        )

        self.assertEqual(
            datetime.datetime(2020, 1, 1, 1, 1, 1), metadata.file_download_time
        )

    def test_get_not_downloaded_remote_files(self) -> None:
        self.metadata_manager.mark_remote_file_as_discovered(
            "discovered_not_downloaded", self.timestamp
        )
        self.metadata_manager.mark_remote_file_as_discovered(
            "discovered_and_downloaded", self.timestamp
        )
        self.metadata_manager.mark_remote_file_as_downloaded(
            "discovered_and_downloaded", self.timestamp
        )

        metadatas = self.metadata_manager.get_not_downloaded_remote_files()
        self.assertListEqual(
            metadatas,
            [
                DirectIngestSftpRemoteFileMetadata.new_with_defaults(
                    region_code=self.metadata_manager.region_code,
                    remote_file_path="discovered_not_downloaded",
                    sftp_timestamp=self.timestamp,
                    file_discovery_time=datetime.datetime(2020, 1, 1, 1, 1, 1),
                    file_download_time=None,
                )
            ],
        )
