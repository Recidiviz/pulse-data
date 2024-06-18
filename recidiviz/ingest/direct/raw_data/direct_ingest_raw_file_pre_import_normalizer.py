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
"""Code for normalizing any csv file into a BigQuery-readable file."""
import csv
import io
from types import ModuleType
from typing import IO, Optional, Union

from recidiviz.cloud_storage.bytes_chunk_reader import BytesChunkReader
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.read_only_csv_normalizing_stream import (
    ReadOnlyCsvNormalizingStream,
)
from recidiviz.cloud_storage.verifiable_bytes_reader import VerifiableBytesReader
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_temporary_output_directory_path,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.types.raw_data_import_types import (
    NormalizedCsvChunkResult,
    PreImportNormalizationType,
    RequiresPreImportNormalizationFileChunk,
)

# See #28586 for bandwidth testing that lead to this default
DEFAULT_READ_CHUNK_SIZE = 32 * 1024 * 1024  # 32 mb


class DirectIngestRawFilePreImportNormalizer:
    """Class for coordinating the normalization of a csv dialect. Given a filesystem
    (knowledge about how to read/write chunks) and a state code (knowledge about file
    metadata via raw file configs), this class can deteremine the correct transformations
    needed to normalize the csv dialect to a BigQuery-readable format.
    """

    def __init__(
        self,
        fs: GCSFileSystem,
        state_code: StateCode,
        region_module_override: Optional[ModuleType] = None,
        read_chunk_size: int = DEFAULT_READ_CHUNK_SIZE,
    ) -> None:
        self._fs = fs
        self._state_code = state_code
        # TODO(#29013) allow for sandbox name here
        self._temp_output_dir = gcsfs_direct_ingest_temporary_output_directory_path()
        self._region_config = get_region_raw_file_config(
            self._state_code.value.lower(), region_module_override
        )
        self._read_chunk_size = read_chunk_size

    def _get_output_path(self, file_tag: str, chunk_num: int) -> GcsfsFilePath:
        """Given a |file_tag| and a |chunk_num|, return the output file path in
        a Google Cloud Storage bucket.
        """
        filename = f"temp_{file_tag}_{chunk_num}.csv"
        return GcsfsFilePath.from_directory_and_file_name(
            self._temp_output_dir, filename
        )

    @staticmethod
    def _requires_prepended_headers(
        chunk_num: int, infer_columns_from_config: bool
    ) -> bool:
        """We will prepend headers for all chunks for files that have
        |infer_columns_from_config| as True. For all other files, we will want to
        preprend headers for all but the first chunk (chunk_num = 0).
        """
        return infer_columns_from_config or chunk_num != 0

    def normalize_chunk_for_import(
        self, chunk: RequiresPreImportNormalizationFileChunk
    ) -> NormalizedCsvChunkResult:
        """Given a |chunk|, normalize the chunk according to it's normalization_type so
        it can be imported to BQ and write the result out to a temp Google Cloud Storage
        path.
        """

        if chunk.normalization_type is None:
            raise ValueError("No pass is required for this chunk")

        # first, get all the info we need about the chunk

        path = GcsfsFilePath.from_absolute_path(chunk.path)
        path_parts = filename_parts_from_path(path)
        config = self._region_config.raw_file_configs[path_parts.file_tag]
        output_path = self._get_output_path(
            path_parts.file_tag, chunk.chunk_boundary.chunk_num
        )

        # then, execute the actual normalization step
        with self._fs.open(
            path, mode="rb", chunk_size=self._read_chunk_size, verifiable=False
        ) as f:

            offset_reader = BytesChunkReader(
                f,
                read_start_inclusive=chunk.chunk_boundary.start_inclusive,
                read_end_exclusive=chunk.chunk_boundary.end_exclusive,
                name=path_parts.file_tag,
            )

            verifiable_reader = VerifiableBytesReader(
                offset_reader, name=path_parts.file_tag
            )

            text_reader = io.TextIOWrapper(verifiable_reader, encoding=config.encoding)

            output_reader: Union[IO, ReadOnlyCsvNormalizingStream]

            if (
                chunk.normalization_type
                == PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE
            ):
                output_reader = ReadOnlyCsvNormalizingStream(
                    text_reader,
                    config.separator,
                    config.line_terminator,
                    csv.QUOTE_NONE if config.ignore_quotes else csv.QUOTE_MINIMAL,
                )
            else:
                output_reader = text_reader

            decoded_output = output_reader.read()

            if self._requires_prepended_headers(
                chunk.chunk_boundary.chunk_num, config.infer_columns_from_config
            ):
                decoded_output = chunk.headers + decoded_output

            # lastly, upload the output
            # upload_from_string does the encoding for us. when we pass it a string, it
            # will encode it as utf-8 and then call blob.upload_from_file
            # note: if a blob w/ the same name exists, it will just overwrite it
            self._fs.upload_from_string(output_path, decoded_output, "text/csv")

        return NormalizedCsvChunkResult(
            path=output_path.abs_path(),
            chunk_boundary=chunk.chunk_boundary,
            crc32c=verifiable_reader.get_crc32c(),
        )
