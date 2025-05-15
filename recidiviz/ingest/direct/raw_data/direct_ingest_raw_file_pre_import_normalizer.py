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
import logging
from types import ModuleType
from typing import IO, Callable, Optional, Union

from recidiviz.cloud_storage.bytes_chunk_reader import BytesChunkReader
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.cloud_storage.read_only_csv_normalizing_stream import (
    ReadOnlyCsvNormalizingStream,
)
from recidiviz.cloud_storage.verifiable_bytes_reader import VerifiableBytesReader
from recidiviz.common.constants.states import StateCode
from recidiviz.common.io.codec_error_handler import LimitedErrorReplacementHandler
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_temporary_output_directory_path,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.types.raw_data_import_types import (
    PreImportNormalizationType,
    PreImportNormalizedCsvChunkResult,
    RequiresPreImportNormalizationFileChunk,
)
from recidiviz.utils.encoding import register_unique_error_handler

# See #28586 for bandwidth testing that lead to this default
DEFAULT_READ_CHUNK_SIZE = 32 * 1024 * 1024  # 32 mb


class DirectIngestRawFilePreImportNormalizer:
    """Class for coordinating the normalization of a csv dialect. Given a filesystem
    (knowledge about how to read/write chunks) and a state code (knowledge about file
    metadata via raw file configs), this class can determine the correct transformations
    needed to normalize the csv dialect to a BigQuery-readable format.
    """

    def __init__(
        self,
        fs: GCSFileSystem,
        state_code: StateCode,
        region_module_override: Optional[ModuleType] = None,
        read_chunk_size: int = DEFAULT_READ_CHUNK_SIZE,
        temp_output_dir: Optional[GcsfsDirectoryPath] = None,
    ) -> None:
        self._fs = fs
        self._state_code = state_code
        self._temp_output_dir = (
            temp_output_dir or gcsfs_direct_ingest_temporary_output_directory_path()
        )
        self._region_config = get_region_raw_file_config(
            self._state_code.value.lower(), region_module_override
        )
        self._read_chunk_size = read_chunk_size

    @staticmethod
    def _should_strip_headers(chunk_num: int, infer_columns_from_config: bool) -> bool:
        """Return True if |chunk_num| is 0 and |infer_columns_from_config| is False (the
        raw csv contains a header line)."""
        return not infer_columns_from_config and chunk_num == 0

    @staticmethod
    def strip_first_line(decoded_output: str) -> str:
        """Find the first newline in the decoded_output and return the substring starting
        after this newline. If no newline is found, return the original string.
        We do not expect any newline to appear within the text of a header, so we don't
        handle escaping here."""
        return (
            decoded_output[i + 1 :]
            if (i := decoded_output.find("\n")) != -1
            else decoded_output
        )

    @staticmethod
    def _get_errors_mode_and_counter(
        max_unparseable_bytes: int | None,
        chunk: RequiresPreImportNormalizationFileChunk,
    ) -> tuple[str, Callable[[], list[str]]]:
        if max_unparseable_bytes is None:
            return "strict", lambda: []

        handler_name = f"{chunk.path.uri()}--{chunk.chunk_boundary.chunk_num}"
        handler = LimitedErrorReplacementHandler(
            max_number_of_errors=max_unparseable_bytes
        )

        register_unique_error_handler(name=handler_name, handler=handler)

        return handler_name, handler.get_exceptions

    def output_path_for_chunk(
        self, chunk: RequiresPreImportNormalizationFileChunk
    ) -> GcsfsFilePath:
        """Returns the intended output path for the provided |chunk|"""
        file_name = (
            f"temp_{chunk.path.base_file_name}_{chunk.chunk_boundary.chunk_num}.csv"
        )
        return GcsfsFilePath.from_directory_and_file_name(
            self._temp_output_dir, file_name
        )

    def normalize_chunk_for_import(
        self, chunk: RequiresPreImportNormalizationFileChunk
    ) -> PreImportNormalizedCsvChunkResult:
        """Given |chunk|, normalize the chunk according to it's normalization_type so
        it can be imported to BQ and write the result out to a temp Google Cloud Storage
        path. If the chunk is the first chunk and the raw file has a header line, the header
        line will be stripped from the output.
        """

        if chunk.pre_import_normalization_type is None:
            raise ValueError("No pass is required for this chunk")

        # first, get all the info we need about the chunk
        path_parts = filename_parts_from_path(chunk.path)
        config = self._region_config.raw_file_configs[path_parts.file_tag]
        output_path = self.output_path_for_chunk(chunk)
        errors_mode, error_counter = self._get_errors_mode_and_counter(
            config.max_num_unparseable_bytes, chunk
        )

        logging.info(
            "normalizing [%s]: chunk [%s] (%s -> %s)",
            chunk.path.file_name,
            chunk.chunk_boundary.chunk_num,
            chunk.chunk_boundary.start_inclusive,
            chunk.chunk_boundary.end_exclusive,
        )

        # then, execute the actual normalization step
        with self._fs.open(
            chunk.path, mode="rb", chunk_size=self._read_chunk_size, verifiable=False
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

            text_reader = io.TextIOWrapper(
                verifiable_reader,
                encoding=config.encoding,
                errors=errors_mode,
            )

            output_reader: Union[IO, ReadOnlyCsvNormalizingStream]

            if (
                chunk.pre_import_normalization_type
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
            if self._should_strip_headers(
                chunk.chunk_boundary.chunk_num, config.infer_columns_from_config
            ):
                decoded_output = self.strip_first_line(decoded_output)

            # lastly, upload the output
            # upload_from_string does the encoding for us. when we pass it a string, it
            # will encode it as utf-8 and then call blob.upload_from_file
            # note: if a blob w/ the same name exists, it will just overwrite it
            self._fs.upload_from_string(output_path, decoded_output, "text/csv")

        return PreImportNormalizedCsvChunkResult(
            input_file_path=chunk.path,
            output_file_path=output_path,
            chunk_boundary=chunk.chunk_boundary,
            crc32c=verifiable_reader.get_crc32c_as_int(),
            byte_decoding_errors=error_counter(),
        )
