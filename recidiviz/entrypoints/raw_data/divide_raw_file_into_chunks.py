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
from types import ModuleType
from typing import List, Optional

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import (
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
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.raw_data.read_raw_file_column_headers import (
    DirectIngestRawFileHeaderReader,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    PreImportNormalizationType,
    RawFileProcessingError,
    RequiresPreImportNormalizationFile,
)
from recidiviz.utils.airflow_types import BatchedTaskInstanceOutput
from recidiviz.utils.types import assert_type

MAX_THREADS = 8  # TODO(#29946) determine reasonable default
FILE_LIST_DELIMITER = "^"


def extract_file_chunks_concurrently(
    serialized_requires_pre_import_normalization_file_paths: List[str],
    state_code: StateCode,
    region_module_override: Optional[ModuleType] = None,
) -> str:
    fs = GcsfsFactory.build()
    region_raw_file_config = get_region_raw_file_config(
        state_code.value, region_module=region_module_override
    )
    requires_pre_import_normalization_file_paths = [
        GcsfsFilePath.from_absolute_path(serialized_path)
        for serialized_path in serialized_requires_pre_import_normalization_file_paths
    ]

    chunking_result = _process_files_concurrently(
        fs, requires_pre_import_normalization_file_paths, region_raw_file_config
    )
    return chunking_result.serialize()


def _process_files_concurrently(
    fs: GCSFileSystem,
    requires_pre_import_normalization_file_paths: List[GcsfsFilePath],
    region_raw_file_config: DirectIngestRegionRawFileConfig,
) -> BatchedTaskInstanceOutput[
    RequiresPreImportNormalizationFile, RawFileProcessingError
]:
    """Finds row safe chunk boundaries and headers for each file"""
    results: List[RequiresPreImportNormalizationFile] = []
    errors: List[RawFileProcessingError] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {
            executor.submit(
                _extract_file_chunks,
                fs,
                requires_pre_import_normalization_file_path,
                region_raw_file_config,
            ): requires_pre_import_normalization_file_path
            for requires_pre_import_normalization_file_path in requires_pre_import_normalization_file_paths
        }
        for future in concurrent.futures.as_completed(futures):
            requires_pre_import_normalization_file_path = futures[future]
            try:
                results.append(future.result())
            except Exception as e:
                errors.append(
                    RawFileProcessingError(
                        original_file_path=requires_pre_import_normalization_file_path,
                        temporary_file_paths=None,
                        error_msg=f"{requires_pre_import_normalization_file_path.abs_path()}: {str(e)}\n{traceback.format_exc()}",
                    )
                )

    return BatchedTaskInstanceOutput[
        RequiresPreImportNormalizationFile, RawFileProcessingError
    ](results=results, errors=errors)


def _extract_file_chunks(
    fs: GCSFileSystem,
    requires_pre_import_normalization_file_path: GcsfsFilePath,
    region_raw_file_config: DirectIngestRegionRawFileConfig,
) -> RequiresPreImportNormalizationFile:
    raw_file_config = _get_raw_file_config(
        region_raw_file_config, requires_pre_import_normalization_file_path
    )
    headers = _get_file_headers(
        fs, requires_pre_import_normalization_file_path, raw_file_config
    )

    chunker = GcsfsCsvChunkBoundaryFinder(fs)
    chunks = chunker.get_chunks_for_gcs_path(
        requires_pre_import_normalization_file_path
    )

    return RequiresPreImportNormalizationFile(
        path=requires_pre_import_normalization_file_path,
        pre_import_normalization_type=assert_type(
            PreImportNormalizationType.required_pre_import_normalization_type(
                raw_file_config
            ),
            PreImportNormalizationType,
        ),
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
