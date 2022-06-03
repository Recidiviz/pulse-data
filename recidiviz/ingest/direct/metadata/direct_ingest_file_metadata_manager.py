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
"""A class that handles writing metadata about each direct ingest file to disk."""
import abc
import datetime
from typing import List, Optional

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawFileMetadata,
    DirectIngestSftpFileMetadata,
)


class DirectIngestSftpFileMetadataManager:
    """An abstract interface for a class that handles writing metadata about SFTP
    files to disk.
    """

    @abc.abstractmethod
    def has_sftp_file_been_discovered(self, remote_file_path: str) -> bool:
        """Checks whether the file at this path has already been marked as discovered."""

    @abc.abstractmethod
    def mark_sftp_file_as_discovered(self, remote_file_path: str) -> None:
        """Writes a new row to the appropriate metadata table for a new, unprocessed sftp
        file."""

    @abc.abstractmethod
    def has_sftp_file_been_processed(self, remote_file_path: str) -> bool:
        """Checks whether the file at this path has already been marked as processed."""

    @abc.abstractmethod
    def mark_sftp_file_as_processed(self, remote_file_path: str) -> None:
        """Marks the file represented by the |remote_file_path| as processed in the appropriate
        metadata table."""

    @abc.abstractmethod
    def get_sftp_file_metadata(
        self, remote_file_path: str
    ) -> DirectIngestSftpFileMetadata:
        """Returns metadata information for the provided path. If the file has not yet been registered in the
        appropriate metadata table, this function will generate a file_id to return with the metadata.
        """


class DirectIngestRawFileMetadataManager:
    """An abstract interface for a class that handles writing metadata about raw data
    files to disk.
    """

    @abc.abstractmethod
    def has_raw_file_been_discovered(self, path: GcsfsFilePath) -> bool:
        """Checks whether the file at this path has already been marked as discovered."""

    @abc.abstractmethod
    def mark_raw_file_as_discovered(self, path: GcsfsFilePath) -> None:
        """Writes a new row to the appropriate metadata table for a new, unprocessed raw file, or updates the existing
        metadata row for this path with the appropriate file discovery time."""

    @abc.abstractmethod
    def get_unprocessed_raw_files(self) -> List[DirectIngestRawFileMetadata]:
        """Returns metadata for the unprocessed raw files in the operations table for
        this region.
        """

    @abc.abstractmethod
    def get_raw_file_metadata(self, path: GcsfsFilePath) -> DirectIngestRawFileMetadata:
        """Returns metadata information for the provided path. If the file has not yet been registered in the
        appropriate metadata table, this function will generate a file_id to return with the metadata.
        """

    @abc.abstractmethod
    def has_raw_file_been_processed(self, path: GcsfsFilePath) -> bool:
        """Checks whether the file at this path has already been marked as processed."""

    @abc.abstractmethod
    def mark_raw_file_as_processed(self, path: GcsfsFilePath) -> None:
        """Marks the file represented by the |metadata| as processed in the appropriate metadata table."""

    @abc.abstractmethod
    def get_metadata_for_raw_files_discovered_after_datetime(
        self,
        raw_file_tag: str,
        discovery_time_lower_bound_exclusive: Optional[datetime.datetime],
    ) -> List[DirectIngestRawFileMetadata]:
        """Returns metadata for all raw files with a given tag that have been updated after the provided date."""

    @abc.abstractmethod
    def get_num_unprocessed_raw_files(self) -> int:
        """Returns the number of unprocessed raw files in the operations table for this region"""
