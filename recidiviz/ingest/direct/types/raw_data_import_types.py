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
import json
from enum import Enum, auto
from typing import List, Optional

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
    normalization_type: Optional[PreImportNormalizationType]
    chunk_boundary: CsvChunkBoundary
    headers: str

    def serialize(self) -> str:
        return json.dumps(
            {
                "path": self.path,
                "normalization_type": (
                    self.normalization_type.value if self.normalization_type else ""
                ),
                "chunk_boundary": self.chunk_boundary.serialize(),
                "headers": self.headers,
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "RequiresPreImportNormalizationFileChunk":
        data = json.loads(json_str)
        return RequiresPreImportNormalizationFileChunk(
            path=data["path"],
            normalization_type=PreImportNormalizationType(data["normalization_type"]),
            chunk_boundary=CsvChunkBoundary.deserialize(data["chunk_boundary"]),
            headers=data["headers"],
        )


@attr.define
class RequiresPreImportNormalizationFile:
    """Encapsulates the path, the headers, the chunk boundaries for the file
    and the type of normalization required for the CSV to conform to
    BigQuery's load job standards."""

    path: str
    normalization_type: Optional[PreImportNormalizationType]
    chunk_boundaries: List[CsvChunkBoundary]
    headers: str

    def serialize(self) -> str:
        return json.dumps(
            {
                "path": self.path,
                "normalization_type": (
                    self.normalization_type.value if self.normalization_type else ""
                ),
                "chunk_boundaries": [
                    boundary.serialize() for boundary in self.chunk_boundaries
                ],
                "headers": self.headers,
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "RequiresPreImportNormalizationFile":
        data = json.loads(json_str)
        return RequiresPreImportNormalizationFile(
            path=data["path"],
            normalization_type=PreImportNormalizationType(data["normalization_type"]),
            chunk_boundaries=[
                CsvChunkBoundary.deserialize(boundary)
                for boundary in data["chunk_boundaries"]
            ],
            headers=data["headers"],
        )

    def to_file_chunks(
        self,
    ) -> List[RequiresPreImportNormalizationFileChunk]:
        """
        Extract individual RequiresPreImportNormalizationFileChunk objects from list of CSVBoundary objects.
        """
        individual_chunks = []
        for chunk_boundary in self.chunk_boundaries:
            chunk = RequiresPreImportNormalizationFileChunk(
                path=self.path,
                normalization_type=self.normalization_type,
                chunk_boundary=chunk_boundary,
                headers=self.headers,
            )
            individual_chunks.append(chunk)
        return individual_chunks


@attr.define
class NormalizedCsvChunkResult:
    """Encapsulates the output path, chunk boundary and checksum of the CSV chunk. This
    should relate 1-1 to a RequiresPreImportNormalizationFileChunk."""

    input_file_path: str
    output_file_path: str
    chunk_boundary: CsvChunkBoundary
    crc32c: int

    def get_chunk_boundary_size(self) -> int:
        return self.chunk_boundary.get_chunk_size()

    def serialize(self) -> str:
        result_dict = {
            "input_file_path": self.input_file_path,
            "output_file_path": self.output_file_path,
            "chunk_boundary": self.chunk_boundary.serialize(),
            "crc32c": self.crc32c,
        }
        return json.dumps(result_dict)

    @staticmethod
    def deserialize(json_str: str) -> "NormalizedCsvChunkResult":
        data = json.loads(json_str)
        return NormalizedCsvChunkResult(
            input_file_path=data["input_file_path"],
            output_file_path=data["output_file_path"],
            chunk_boundary=CsvChunkBoundary.deserialize(data["chunk_boundary"]),
            crc32c=data["crc32c"],
        )


@attr.define
class RequiresNormalizationFile:
    file_path: str
    file_tag: str


@attr.define
class ImportReadyNormalizedFile:
    input_file_path: str
    output_file_paths: List[str]


@attr.define
class LoadPrepSummary:
    """Summary from DirectIngestRawFileLoadManager.load_and_prep_paths step that will
    be combined with AppendSummary to build a row in direct_ingest_raw_data_import_session


    append_ready_table_address (str): temp BQ address of loaded, transformed and migrated
        raw data
    raw_rows_count (int): number of raw rows loaded from raw file paths before any
        transformations or filtering occured
    """

    append_ready_table_address: str
    raw_rows_count: int
