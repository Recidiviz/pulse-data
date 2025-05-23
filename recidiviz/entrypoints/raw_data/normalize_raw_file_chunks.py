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
"""Entrypoint for normalizing raw data file chunks for import"""
import argparse
import traceback
from typing import List

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.entrypoints.entrypoint_utils import save_to_gcs_xcom
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_pre_import_normalizer import (
    DirectIngestRawFilePreImportNormalizer,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    PreImportNormalizedCsvChunkResult,
    RawFileProcessingError,
    RequiresPreImportNormalizationFileChunk,
)
from recidiviz.utils.airflow_types import BatchedTaskInstanceOutput

# Delimit the list of file chunks with a caret
# to avoid parsing issues with serialized chunk objects
FILE_CHUNK_LIST_DELIMITER = "^"


def _normalize_chunks(
    normalizer: DirectIngestRawFilePreImportNormalizer,
    chunks: List[RequiresPreImportNormalizationFileChunk],
) -> BatchedTaskInstanceOutput[
    PreImportNormalizedCsvChunkResult, RawFileProcessingError
]:
    results: List[PreImportNormalizedCsvChunkResult] = []
    errors: List[RawFileProcessingError] = []

    for chunk in chunks:
        try:
            results.append(normalizer.normalize_chunk_for_import(chunk))
        except Exception as e:
            errors.append(
                RawFileProcessingError(
                    original_file_path=chunk.path,
                    # even though this file might not exist, in case there was a partial
                    # upload, let's provide this path just to make sure it gets cleaned
                    # up
                    temporary_file_paths=[normalizer.output_path_for_chunk(chunk)],
                    error_msg=f"Error for file {chunk.path} chunk {chunk.chunk_boundary}: {str(e)}\n{traceback.format_exc()}",
                )
            )

    return BatchedTaskInstanceOutput[
        PreImportNormalizedCsvChunkResult, RawFileProcessingError
    ](results=results, errors=errors)


def normalize_raw_file_chunks(
    fs: GCSFileSystem, serialized_chunks: List[str], state_code: StateCode
) -> str:
    normalizer = DirectIngestRawFilePreImportNormalizer(fs, state_code)
    deserialized_chunks = [
        RequiresPreImportNormalizationFileChunk.deserialize(chunk)
        for chunk in serialized_chunks
    ]

    normalized_chunks_result = _normalize_chunks(normalizer, deserialized_chunks)
    return normalized_chunks_result.serialize()


class RawDataChunkNormalizationEntrypoint(EntrypointInterface):
    """Entrypoint for normalizing raw data file chunks for import"""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--file_chunks",
            type=lambda s: s.split(FILE_CHUNK_LIST_DELIMITER),
            required=True,
            help="Caret-separated list of serialized RequiresPreImportNormalizationFileChunk",
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
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        chunks = args.file_chunks
        state_code = args.state_code

        fs = GcsfsFactory.build()

        results = normalize_raw_file_chunks(fs, chunks, state_code)

        save_to_gcs_xcom(fs=fs, output_str=results)
