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
"""Raw data import download delegate for US_TN"""
import datetime

from recidiviz.ingest.direct.raw_data.base_raw_data_import_delegate import (
    BaseRawDataImportDelegate,
    skipped_error_for_unrecognized_file_tag_for_chunked_files,
)
from recidiviz.ingest.direct.raw_data.mixins.sequential_chunked_file_mixin import (
    SequentialChunkedFileMixin,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)


class UsTnRawDataImportDelegate(BaseRawDataImportDelegate, SequentialChunkedFileMixin):
    def coalesce_chunked_files(
        self, file_tag: str, gcs_files: list[RawGCSFileMetadata]
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """ContactNoteComment is the only chunked file in Tennessee."""

        if file_tag == "ContactNoteComment":
            # on & before 2025-02-14, ContactNoteComment always had 19 chunks
            if min(
                gcs_file.parts.utc_upload_datetime for gcs_file in gcs_files
            ).date() <= datetime.date(2025, 2, 14):
                return self.group_n_files_with_sequential_suffixes(
                    n=19, gcs_files=gcs_files, file_tag=file_tag
                )

            return self.group_n_files_with_sequential_suffixes(
                n=20, gcs_files=gcs_files, file_tag=file_tag
            )

        return skipped_error_for_unrecognized_file_tag_for_chunked_files(
            file_tag=file_tag, gcs_files=gcs_files
        )
