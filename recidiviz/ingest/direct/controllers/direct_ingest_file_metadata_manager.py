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
from typing import Optional, List

from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsIngestViewExportArgs
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.entity.operations.entities import DirectIngestFileMetadata, DirectIngestIngestFileMetadata, \
    DirectIngestRawFileMetadata


class DirectIngestFileMetadataManager:
    """An abstract interface for a class that handles writing metadata about each direct ingest file to disk."""

    @abc.abstractmethod
    def register_ingest_file_export_job(self,
                                        ingest_view_job_args: GcsfsIngestViewExportArgs) -> None:
        """Writes a new row to the ingest view metadata table with the expected path once the job completes."""

    @abc.abstractmethod
    def register_new_file(self, path: GcsfsFilePath) -> None:
        """Writes a new row to the appropriate metadata table for a new, unprocessed file, or updates the existing
        metadata row for this path with the appropriate file discovery time."""

    @abc.abstractmethod
    def get_file_metadata(self,
                          path: GcsfsFilePath) -> DirectIngestFileMetadata:
        """Returns metadata information for the provided path. If the file has not yet been registered in the
        appropriate metadata table, this function will generate a file_id to return with the metadata.
        """

    @abc.abstractmethod
    def mark_file_as_processed(self,
                               path: GcsfsFilePath,
                               # TODO(3020): Once we write rows to postgres immediately when we encounter a new file, we
                               #  shouldn't need this arg - just query for the appropriate row to get the file id.
                               metadata: DirectIngestFileMetadata) -> None:
        """Marks the file represented by the |metadata| as processed in the appropriate metadata table."""

    @abc.abstractmethod
    def get_ingest_view_metadata_for_job(
            self, ingest_view_job_args: GcsfsIngestViewExportArgs) -> DirectIngestIngestFileMetadata:
        """Returns the ingest file metadata row corresponding to the export job with the provided args. Throws if such a
        row does not exist.
        """

    @abc.abstractmethod
    def mark_ingest_view_exported(self,
                                  metadata: DirectIngestIngestFileMetadata,
                                  exported_path: GcsfsFilePath) -> None:
        """Commits the current time as the export_time for the given ingest file, recording also the path that was
        exported.
        """

    @abc.abstractmethod
    def get_ingest_view_metadata_for_most_recent_valid_job(
            self,
            ingest_view_tag: str) -> Optional[DirectIngestIngestFileMetadata]:
        """Returns most recently created export metadata row where is_invalidated is False, or None if there are no
        metadata rows for this file tag for this manager's region."""

    @abc.abstractmethod
    def get_ingest_view_metadata_pending_export(self) -> List[DirectIngestIngestFileMetadata]:
        """Returns metadata for all ingest files have not yet been exported."""

    @abc.abstractmethod
    def get_metadata_for_raw_files_discovered_after_datetime(
            self,
            raw_file_tag: str,
            discovery_time_lower_bound_exclusive: Optional[datetime.datetime]
    ) -> List[DirectIngestRawFileMetadata]:
        """Returns metadata for all raw files with a given tag that have been updated after the provided date."""
