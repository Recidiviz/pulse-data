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
"""Raw data import download delegate for US_UT"""
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


class UsUtRawDataImportDelegate(BaseRawDataImportDelegate, SequentialChunkedFileMixin):
    def coalesce_chunked_files(
        self, file_tag: str, gcs_files: list[RawGCSFileMetadata]
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """Logic for handling chunked files in Utah."""

        if file_tag == "sprvsn_cntc" and min(
            gcs_file.parts.utc_upload_datetime for gcs_file in gcs_files
        ).date() > datetime.date(2025, 2, 13):
            return SequentialChunkedFileMixin.group_files_with_sequential_suffixes(
                file_tag=file_tag, gcs_files=gcs_files, zero_indexed=True
            )

        if file_tag == "tst_qstn_rspns":
            return SequentialChunkedFileMixin.group_files_with_sequential_suffixes(
                file_tag=file_tag, gcs_files=gcs_files, zero_indexed=True
            )

        return skipped_error_for_unrecognized_file_tag_for_chunked_files(
            file_tag=file_tag, gcs_files=gcs_files
        )
