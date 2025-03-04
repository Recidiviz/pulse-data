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
"""Base delegate class for handling state-specific raw data download logic"""

import abc

from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)


class BaseRawDataImportDelegate:
    """Base delegate class for handling state-specific raw data download logic"""

    @abc.abstractmethod
    def coalesce_chunked_files(
        self, file_tag: str, gcs_files: list[RawGCSFileMetadata]
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """Given a |file_tag|, either groups the chunked GCS files into a single
        "conceptual" BigQuery files, or returns a RawDataFilesSkippedError indicating
        that we were unable to group the files together.
        """
