# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Class for handling chunked raw data files"""
import attr

from recidiviz.common import attr_validators
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata_history import (
    RawFileChunkingMetadataHistory,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)


@attr.define
class RawDataImportChunkedFileHandler:
    """Coalesces gcs files into conceptual bigquery files based on the provided
    chunking metadata history.

    Attributes:
        chunking_metadata_by_file_tag: Dictionary mapping file tags to their
            chunking metadata history. If a file tag is not present, that file
            type is assumed not to have chunking. May be None for states with
            no chunked files.
    """

    chunking_metadata_by_file_tag: dict[
        str, RawFileChunkingMetadataHistory
    ] | None = attr.ib(default=None, validator=attr_validators.is_opt_dict)

    def coalesce_chunked_files(
        self, file_tag: str, gcs_files: list[RawGCSFileMetadata]
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """Given a |file_tag|, either groups the chunked GCS files into a single
        "conceptual" BigQuery files, or returns a RawDataFilesSkippedError indicating
        that we were unable to group the files together."""
        if (
            self.chunking_metadata_by_file_tag is None
            or file_tag not in self.chunking_metadata_by_file_tag
        ):
            return skipped_error_for_unrecognized_file_tag_for_chunked_files(
                file_tag=file_tag, gcs_files=gcs_files
            )

        utc_upload_date = min(
            gcs_file.parts.utc_upload_datetime for gcs_file in gcs_files
        ).date()

        chunking_metadata = self.chunking_metadata_by_file_tag[
            file_tag
        ].get_metadata_for_date(utc_upload_date)

        return chunking_metadata.coalesce_files(file_tag=file_tag, gcs_files=gcs_files)


def skipped_error_for_unrecognized_file_tag_for_chunked_files(
    *, file_tag: str, gcs_files: list[RawGCSFileMetadata]
) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
    """Utility method for building a response when a file tag is not recognized or is
    not a known chunked file.
    """
    return [], [
        RawDataFilesSkippedError.from_gcs_files_and_message(
            skipped_message=f"No known way of coalescing files for {file_tag}",
            gcs_files=gcs_files,
            file_tag=file_tag,
        )
    ]
