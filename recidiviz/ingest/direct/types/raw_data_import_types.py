# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Types associated with raw data imports"""
from enum import Enum, auto
from typing import Optional

import attr

from recidiviz.big_query.big_query_utils import (
    is_big_query_valid_delimiter,
    is_big_query_valid_encoding,
    is_big_query_valid_line_terminator,
)
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.common.constants.csv import DEFAULT_CSV_ENCODING
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig


class PreImportNormalizationType(Enum):
    """The type of normalization needed to make a CSV conform to BigQuery's load job
    standards
    """

    # The encoding, delimiter, and/or line terminator must be updated to make this CSV BigQuery import compatible
    ENCODING_DELIMITER_AND_TERMINATOR_UPDATE = auto()

    # Only the encoding must be updated to make this CSV BQ import compatible
    ENCODING_UPDATE_ONLY = auto()

    @staticmethod
    def required_pre_import_normalization_type(
        config: DirectIngestRawFileConfig,
    ) -> Optional["PreImportNormalizationType"]:
        """Given a raw data |config|, returns the pre-import normalization type required
        to make this CSV conform to BigQuery's load job standards. If no normalization
        is needed, will return None."""

        requires_encoding_translation = not is_big_query_valid_encoding(config.encoding)
        final_encoding = (
            DEFAULT_CSV_ENCODING if requires_encoding_translation else config.encoding
        )

        if not is_big_query_valid_line_terminator(
            config.line_terminator
        ) or not is_big_query_valid_delimiter(config.separator, final_encoding):
            return PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE

        if requires_encoding_translation:
            return PreImportNormalizationType.ENCODING_UPDATE_ONLY

        return None


@attr.define
class RequiresPreImportNormalizationFileChunk:
    """Encapsulates the path, the chunk boundary and the type of normalization required
    for the CSV to conform to BigQuery's load job standards."""

    path: str
    normalization_type: PreImportNormalizationType
    chunk_boundary: CsvChunkBoundary
    # TODO(#28586) determine how we want to to do this -- i'm thinking we read the headers
    # in during the accounting pass step?
    headers: str


@attr.define
class NormalizedCsvChunkResult:
    """Encapsulates the output path, chunk boundary and checksum of the CSV chunk. This
    should relate 1-1 to a RequiresPreImportNormalizationFileChunk."""

    path: str
    chunk_boundary: CsvChunkBoundary
    crc32c: str
