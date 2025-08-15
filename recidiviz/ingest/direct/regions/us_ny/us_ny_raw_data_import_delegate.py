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
"""Raw data import download delegate for US_NY"""

from recidiviz.ingest.direct.raw_data.base_raw_data_import_delegate import (
    BaseRawDataImportDelegate,
    skipped_error_for_unrecognized_file_tag_for_chunked_files,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)


class UsNyRawDataImportDelegate(BaseRawDataImportDelegate):
    def coalesce_chunked_files(
        self, file_tag: str, gcs_files: list[RawGCSFileMetadata]
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """As of 3/6/2025, there are no chunked files in US_NY"""
        return skipped_error_for_unrecognized_file_tag_for_chunked_files(
            file_tag=file_tag, gcs_files=gcs_files
        )
