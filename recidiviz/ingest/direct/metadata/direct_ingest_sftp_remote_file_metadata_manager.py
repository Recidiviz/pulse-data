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
SFTP remote data downloads."""
import datetime
from typing import List

import pytz
import sqlalchemy

from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestSftpRemoteFileMetadata,
)


class DirectIngestSftpRemoteFileMetadataManager:
    """An interface for a class that handles writing metadata about pending and completed
    SFTP remote data downloads."""

    def __init__(self, region_code: str):
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    @staticmethod
    def _schema_object_to_entity(
        schema_metadata: schema.DirectIngestSftpRemoteFileMetadata,
    ) -> DirectIngestSftpRemoteFileMetadata:
        entity_metadata = convert_schema_object_to_entity(
            schema_metadata,
            DirectIngestSftpRemoteFileMetadata,
            populate_direct_back_edges=False,
        )

        if not isinstance(entity_metadata, DirectIngestSftpRemoteFileMetadata):
            raise ValueError(f"Unexpected metadata type: {type(entity_metadata)}")

        return entity_metadata

    def get_remote_file_metadata(
        self, remote_file_path: str, sftp_timestamp: float
    ) -> DirectIngestSftpRemoteFileMetadata:
        """Returns metadata information for the provided remote file and timestamp."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestSftpRemoteFileMetadata)
                .filter_by(
                    region_code=self.region_code,
                    remote_file_path=remote_file_path,
                    sftp_timestamp=sftp_timestamp,
                )
                .one()
            )
            return convert_schema_object_to_entity(
                metadata,
                DirectIngestSftpRemoteFileMetadata,
                populate_direct_back_edges=False,
            )

    def has_remote_file_been_discovered(
        self, remote_file_path: str, sftp_timestamp: float
    ) -> bool:
        """Checks whether the file at this timestamp has already been marked as discovered."""
        try:
            _ = self.get_remote_file_metadata(remote_file_path, sftp_timestamp)
        except sqlalchemy.exc.NoResultFound:
            return False
        return True

    def has_remote_file_been_downloaded(
        self, remote_file_path: str, sftp_timestamp: float
    ) -> bool:
        """Checks whether the file at this timestamp has already been marked as downloaded."""
        try:
            metadata = self.get_remote_file_metadata(remote_file_path, sftp_timestamp)
        except sqlalchemy.exc.NoResultFound:
            # For SFTP files, if a file's metadata is not present in the database,
            # then it is assumed to be not downloaded, as it is seen as not existing.
            return False

        return metadata.file_download_time is not None

    def mark_remote_file_as_discovered(
        self, remote_file_path: str, sftp_timestamp: float
    ) -> None:
        """Writes a new row to the appropriate metadata table for a new, not downloaded remote
        file at this timestamp with the appropriate file discovery time."""
        with SessionFactory.using_database(self.database_key) as session:
            session.add(
                schema.DirectIngestSftpRemoteFileMetadata(
                    region_code=self.region_code,
                    remote_file_path=remote_file_path,
                    sftp_timestamp=sftp_timestamp,
                    file_discovery_time=datetime.datetime.now(tz=pytz.UTC),
                )
            )

    def mark_remote_file_as_downloaded(
        self, remote_file_path: str, sftp_timestamp: float
    ) -> None:
        """Marks the file represented by the remote_file and timestamp as downloaded with
        the appropriate file downloaded time."""
        with SessionFactory.using_database(self.database_key) as session:
            metadata = (
                session.query(schema.DirectIngestSftpRemoteFileMetadata)
                .filter_by(
                    region_code=self.region_code,
                    remote_file_path=remote_file_path,
                    sftp_timestamp=sftp_timestamp,
                )
                .one()
            )
            metadata.file_download_time = datetime.datetime.now(tz=pytz.UTC)

    def get_not_downloaded_remote_files(
        self,
    ) -> List[DirectIngestSftpRemoteFileMetadata]:
        """Returns metadata for the remote files for this region that have not been downloaded."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            results = session.query(
                schema.DirectIngestSftpRemoteFileMetadata
            ).filter_by(region_code=self.region_code, file_download_time=None)
            return [
                convert_schema_object_to_entity(
                    result,
                    DirectIngestSftpRemoteFileMetadata,
                    populate_direct_back_edges=False,
                )
                for result in results
            ]
