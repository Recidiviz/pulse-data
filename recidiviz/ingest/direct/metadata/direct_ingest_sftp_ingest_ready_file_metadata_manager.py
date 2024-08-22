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
"""An interface for a class that handles writing metadata about pending and completed
SFTP ingest ready file uploads."""
import datetime
from typing import List

import pytz
import sqlalchemy

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestSftpIngestReadyFileMetadata,
)


class DirectIngestSftpIngestReadyFileMetadataManager:
    """An interface for a class that handles writing metadata about pending and completed
    SFTP ingest ready file uploads. Note: an ingest ready file is not necessarily the same
    as the remote file, because a remote file may have some post-processing to undergo
    to produce ingest ready files (like a zip file being unzipped)."""

    def __init__(self, region_code: str):
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def has_ingest_ready_file_been_discovered(
        self, post_processed_normalized_file: GcsfsFilePath, remote_file_path: str
    ) -> bool:
        """Checks whether the file at this post_processed_normalized_file has already
        been marked as discovered."""
        try:
            _ = self.get_ingest_ready_file_metadata(
                post_processed_normalized_file, remote_file_path
            )
        except sqlalchemy.exc.NoResultFound:
            return False
        return True

    def has_ingest_ready_file_been_uploaded(
        self, post_processed_normalized_file: GcsfsFilePath, remote_file_path: str
    ) -> bool:
        """Checks whether the file at this post_processed_normalized_file has already
        been marked as uploaded to the ingest bucket."""
        try:
            metadata = self.get_ingest_ready_file_metadata(
                post_processed_normalized_file, remote_file_path
            )
        except sqlalchemy.exc.NoResultFound:
            # For SFTP files, if a file's metadata is not present in the database,
            # then it is assumed to be not uploaded, as it is seen as not existing.
            return False

        return metadata.file_upload_time is not None

    def mark_ingest_ready_file_as_discovered(
        self, post_processed_normalized_file: GcsfsFilePath, remote_file_path: str
    ) -> None:
        """Writes a new row to the appropriate metadata table for a new, not uploaded post
        processed file at this post_processed_normalized_file with the appropriate file
        discovery time."""
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.DirectIngestSftpIngestReadyFileMetadata(
                    region_code=self.region_code,
                    post_processed_normalized_file_path=post_processed_normalized_file.blob_name,
                    remote_file_path=remote_file_path,
                    file_discovery_time=datetime.datetime.now(tz=pytz.UTC),
                )
            )

    def mark_ingest_ready_file_as_uploaded(
        self, post_processed_normalized_file: GcsfsFilePath, remote_file_path: str
    ) -> None:
        """Marks the file represented by the post_processed_normalized_file and remote_file_path
        as uploaded to the ingest bucket with the appropriate file uploaded time."""
        with SessionFactory.using_database(self.database_key) as session:
            metadata = (
                session.query(schema.DirectIngestSftpIngestReadyFileMetadata)
                .filter_by(
                    region_code=self.region_code,
                    post_processed_normalized_file_path=post_processed_normalized_file.blob_name,
                    remote_file_path=remote_file_path,
                )
                .one()
            )
            metadata.file_upload_time = datetime.datetime.now(tz=pytz.UTC)

    def get_ingest_ready_file_metadata(
        self, post_processed_normalized_file: GcsfsFilePath, remote_file_path: str
    ) -> DirectIngestSftpIngestReadyFileMetadata:
        """Returns metadata information for the provided post processed file and timestamp."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestSftpIngestReadyFileMetadata)
                .filter_by(
                    region_code=self.region_code,
                    post_processed_normalized_file_path=post_processed_normalized_file.blob_name,
                    remote_file_path=remote_file_path,
                )
                .one()
            )
            return convert_schema_object_to_entity(
                metadata,
                DirectIngestSftpIngestReadyFileMetadata,
                populate_direct_back_edges=False,
            )

    def get_not_uploaded_ingest_ready_files(
        self,
    ) -> List[DirectIngestSftpIngestReadyFileMetadata]:
        """Returns metadata for the remote files for this region that have not been uploaded
        to the ingest bucket."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            results = session.query(
                schema.DirectIngestSftpIngestReadyFileMetadata
            ).filter_by(region_code=self.region_code, file_upload_time=None)
            return [
                convert_schema_object_to_entity(
                    result,
                    DirectIngestSftpIngestReadyFileMetadata,
                    populate_direct_back_edges=False,
                )
                for result in results
            ]
