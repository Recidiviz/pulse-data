# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Entrypoint for RawDataFileChunkingEntrypoint"""
import argparse
import concurrent.futures
import traceback
from typing import List

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import (
    CsvChunkBoundary,
    GcsfsCsvChunkBoundaryFinder,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.entrypoints.entrypoint_utils import save_to_xcom
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.raw_data.read_raw_file_column_headers import (
    DirectIngestRawFileHeaderReader,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    BatchedTaskInstanceOutput,
    RawFileProcessingError,
    RequiresNormalizationFile,
    RequiresPreImportNormalizationFile,
)

MAX_THREADS = 8  # TODO(#29946) determine reasonable default
FILE_LIST_DELIMITER = "^"


def extract_file_chunks_concurrently(
    requires_normalization_files: List[str], state_code: StateCode
) -> str:
    fs = GcsfsFactory.build()
    region_raw_file_config = DirectIngestRegionRawFileConfig(state_code.value)
    deserialized_files = [
        RequiresNormalizationFile.deserialize(f) for f in requires_normalization_files
    ]

    chunking_result = _process_files_concurrently(
        fs, deserialized_files, region_raw_file_config
    )
    return chunking_result.serialize()


def _process_files_concurrently(
    fs: GCSFileSystem,
    requires_normalization_files: List[RequiresNormalizationFile],
    region_raw_file_config: DirectIngestRegionRawFileConfig,
) -> BatchedTaskInstanceOutput[RequiresPreImportNormalizationFile]:
    """Finds row safe chunk boundaries and headers for each file"""
    results: List[RequiresPreImportNormalizationFile] = []
    errors: List[RawFileProcessingError] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {
            executor.submit(
                _extract_file_chunks,
                fs,
                requires_normalization_file,
                region_raw_file_config,
            ): requires_normalization_file
            for requires_normalization_file in requires_normalization_files
        }
        for future in concurrent.futures.as_completed(futures):
            requires_normalization_file = futures[future]
            try:
                results.append(future.result())
            except Exception as e:
                errors.append(
                    RawFileProcessingError(
                        file_path=requires_normalization_file.path,
                        error_msg=f"{requires_normalization_file.path}: {str(e)}\n{traceback.format_exc()}",
                    )
                )

    return BatchedTaskInstanceOutput[RequiresPreImportNormalizationFile](
        results=results, errors=errors
    )


def _extract_file_chunks(
    fs: GCSFileSystem,
    requires_normalization_file: RequiresNormalizationFile,
    region_raw_file_config: DirectIngestRegionRawFileConfig,
) -> RequiresPreImportNormalizationFile:
    gcs_path = GcsfsFilePath.from_absolute_path(requires_normalization_file.path)
    raw_file_config = _get_raw_file_config(region_raw_file_config, gcs_path)
    headers = _get_file_headers(fs, gcs_path, raw_file_config)

    chunker = GcsfsCsvChunkBoundaryFinder(fs)
    chunks = chunker.get_chunks_for_gcs_path(gcs_path)

    return _serialize_chunks(requires_normalization_file, chunks, headers)


def _serialize_chunks(
    requires_normalization_file: RequiresNormalizationFile,
    chunks: List[CsvChunkBoundary],
    headers: List[str],
) -> RequiresPreImportNormalizationFile:
    return RequiresPreImportNormalizationFile(
        path=requires_normalization_file.path,
        normalization_type=requires_normalization_file.normalization_type,
        chunk_boundaries=chunks,
        headers=headers,
    )


def _get_file_headers(
    fs: GCSFileSystem,
    input_gcs_path: GcsfsFilePath,
    raw_file_config: DirectIngestRawFileConfig,
) -> List[str]:
    file_reader = DirectIngestRawFileHeaderReader(fs, input_gcs_path, raw_file_config)
    return file_reader.read_and_validate_column_headers()


def _get_raw_file_config(
    region_raw_file_config: DirectIngestRegionRawFileConfig,
    input_gcs_path: GcsfsFilePath,
) -> DirectIngestRawFileConfig:
    parts = filename_parts_from_path(input_gcs_path)
    raw_file_config = region_raw_file_config.raw_file_configs[parts.file_tag]
    return raw_file_config


class RawDataFileChunkingEntrypoint(EntrypointInterface):
    """Entrypoint for dividing raw data file into row safe chunks"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--requires_normalization_files",
            type=lambda s: s.split(FILE_LIST_DELIMITER),
            required=True,
            help="Caret-separated list of file paths",
        )
        parser.add_argument(
            "--state_code",
            help="The state code the raw data belongs to",
            type=StateCode,
            choices=list(StateCode),
            required=True,
        )

        return parser

    @staticmethod
    def run_entrypoint(args: argparse.Namespace) -> None:
        file_paths = args.requires_normalization_files
        state_code = args.state_code

        results = extract_file_chunks_concurrently(file_paths, state_code)

        save_to_xcom(results)
