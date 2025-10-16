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
"""Logic for handling non-chunked raw data files"""
import logging

from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)


class NonChunkedFileMixin:
    """Mixin class for handling non-chunked raw data files. This should only used in contexts
    where we are importing a legacy file that used to only be a single file with no suffixes,
    but now it is classified as a chunked file for new files going forward. This should not be
    used for files that have never been marked as chunked."""

    @classmethod
    def group_single_non_chunked_file(
        cls,
        *,
        gcs_files: list[RawGCSFileMetadata],
        file_tag: str,
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """Validates that |gcs_files| contains exactly one file (non-chunked) and
        creates a single RawBigQueryFileMetadata from it.

        This is useful for handling files that were historically sent as single files
        but may have transitioned to chunked files at a later date.
        """
        if len(gcs_files) != 1:
            return [], [
                RawDataFilesSkippedError.from_gcs_files_and_message(
                    skipped_message=(
                        f"Skipping grouping for [{file_tag}]; Expected exactly 1 "
                        f"GCS file, but found [{len(gcs_files)}]"
                    ),
                    gcs_files=gcs_files,
                    file_tag=file_tag,
                )
            ]

        logging.info(
            "Found single GCS file for [%s] on [%s]",
            file_tag,
            gcs_files[0].parts.utc_upload_datetime.date(),
        )
        return [RawBigQueryFileMetadata.from_gcs_files(gcs_files=gcs_files)], []
