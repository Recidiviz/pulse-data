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
"""GCS file processing tasks"""

import base64
import concurrent.futures
import heapq
from collections import defaultdict
from typing import Dict, List, Tuple

from airflow.decorators import task
from more_itertools import distribute

from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    ENTRYPOINT_ARGUMENTS,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.entrypoints.raw_data.normalize_raw_file_chunks import (
    FILE_CHUNK_LIST_DELIMITER,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    BatchedTaskInstanceOutput,
    ImportReadyNormalizedFile,
    MappedBatchedTaskOutput,
    NormalizedCsvChunkResult,
    RawFileProcessingError,
    RequiresPreImportNormalizationFile,
    RequiresPreImportNormalizationFileChunk,
)
from recidiviz.utils.crc32c import digest_ordered_checksum_and_size_pairs

MAX_THREADS = 16  # TODO(#29946) determine reasonable default


@task
def create_file_batches(file_paths: List[str], num_batches: int) -> List[List[str]]:
    fs = GcsfsFactory.build()
    return batch_files_by_size(fs, file_paths, num_batches)


def batch_files_by_size(
    fs: GCSFileSystem, file_paths: List[str], num_batches: int
) -> List[List[str]]:
    """Divide files into batches with approximately equal cumulative size"""
    # If get_file_size returns None, set size to 0 and don't worry about sorting correctly
    # If the file doesn't exist we'll return an error downstream
    files_with_sizes = _get_files_with_sizes_concurrently(fs, file_paths)
    files_with_sizes.sort(key=lambda x: x[1], reverse=True)

    num_batches = len(file_paths) if len(file_paths) < num_batches else num_batches
    batches: List[List[str]] = [[] for _ in range(num_batches)]
    heap = [(0, batch_index) for batch_index in range(num_batches)]
    heapq.heapify(heap)

    for file_path, file_size in files_with_sizes:
        batch_size, batch_index = heapq.heappop(heap)
        batches[batch_index].append(file_path)
        heapq.heappush(heap, (batch_size + file_size, batch_index))

    return batches


def _get_files_with_sizes_concurrently(
    fs: GCSFileSystem, file_paths: List[str]
) -> List[Tuple[str, int]]:
    files_with_sizes: List[Tuple[str, int]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_file_path = {
            executor.submit(_get_file_size, fs, file_path): file_path
            for file_path in file_paths
        }

        for future in concurrent.futures.as_completed(future_to_file_path):
            file_path = future_to_file_path[future]
            try:
                size = future.result()
                files_with_sizes.append((file_path, size))
            except Exception:
                files_with_sizes.append((file_path, 0))

    return files_with_sizes


def _get_file_size(fs: GCSFileSystem, file_path: str) -> int:
    return fs.get_file_size(GcsfsFilePath.from_absolute_path(file_path)) or 0


@task
def generate_chunk_processing_pod_arguments(
    state_code: str, file_chunks: List[str], num_batches: int
) -> List[List[str]]:
    return [
        [
            *ENTRYPOINT_ARGUMENTS,
            "--entrypoint=RawDataChunkNormalizationEntrypoint",
            f"--state_code={state_code}",
            f"--file_chunks={FILE_CHUNK_LIST_DELIMITER.join(batch)}",
        ]
        for batch in _divide_file_chunks_into_batches(file_chunks, num_batches)
    ]


def _divide_file_chunks_into_batches(
    file_chunking_result: List[str], num_batches: int
) -> List[List[str]]:
    # Each file chunking task returns a list of serialized file chunks
    # So we need to flatten the result list and deserialize each chunk
    all_results: List[
        RequiresPreImportNormalizationFile
    ] = MappedBatchedTaskOutput.deserialize(
        file_chunking_result, result_cls=RequiresPreImportNormalizationFile
    ).flatten_results()

    batches = create_chunk_batches(all_results, num_batches)
    serialized_batches = [[chunk.serialize() for chunk in batch] for batch in batches]

    return serialized_batches


def create_chunk_batches(
    file_chunks: List[RequiresPreImportNormalizationFile], num_batches: int
) -> List[List[RequiresPreImportNormalizationFileChunk]]:
    """Distribute file chunks into a specified number of batches in a round-robin fashion.

    This function takes a list of file chunks, each potentially containing multiple chunk boundaries,
    and distributes these chunks into the specified number of batches. If the number of batches
    exceeds the number of chunks, the number of batches is reduced to match the number of chunks.
    """
    all_chunks = _create_individual_chunk_objects_list(file_chunks)
    num_batches = min(len(all_chunks), num_batches)

    batches = distribute(num_batches, all_chunks)
    return [list(batch) for batch in batches]


def _create_individual_chunk_objects_list(
    file_chunks: List[RequiresPreImportNormalizationFile],
) -> List[RequiresPreImportNormalizationFileChunk]:
    individual_chunks = []
    for file_chunk in file_chunks:
        individual_chunks.extend(file_chunk.to_file_chunks())
    return individual_chunks


@task
def regroup_and_verify_file_chunks(
    normalized_chunks_result: List[str],
) -> str:
    """Task organizes normalized chunks by file and compares their collective checksum
    against the full file checksum to ensure all file bytes were read correctly"""
    mapped_task_output = MappedBatchedTaskOutput.deserialize(
        normalized_chunks_result, result_cls=NormalizedCsvChunkResult
    )
    all_results: List[NormalizedCsvChunkResult] = mapped_task_output.flatten_results()
    all_errors: List[RawFileProcessingError] = mapped_task_output.flatten_errors()
    file_to_normalized_chunks = regroup_normalized_file_chunks(all_results, all_errors)

    results = verify_file_checksums_and_build_import_ready_file(
        file_to_normalized_chunks
    )
    # Return all checksum mismatches and all errors seen in previous normalization task
    results.errors.extend(all_errors)

    return results.serialize()


def verify_file_checksums_and_build_import_ready_file(
    file_to_normalized_chunks: Dict[str, List[NormalizedCsvChunkResult]],
) -> BatchedTaskInstanceOutput[ImportReadyNormalizedFile]:
    """Verify the checksum of the normalized file chunks against the full file checksum.
    if they match return the ImportReadyNormalizedFile"""

    fs = GcsfsFactory.build()
    normalized_files: List[ImportReadyNormalizedFile] = []
    errors: List[RawFileProcessingError] = []

    for file_path, chunks in file_to_normalized_chunks.items():
        chunk_checksums_and_sizes = [
            (chunk.crc32c, chunk.get_chunk_boundary_size()) for chunk in chunks
        ]
        chunk_combined_digest = digest_ordered_checksum_and_size_pairs(
            chunk_checksums_and_sizes
        )
        chunk_combined_checksum = base64.b64encode(chunk_combined_digest).decode(
            "utf-8"
        )

        full_file_checksum = fs.get_crc32c(GcsfsFilePath.from_absolute_path(file_path))

        if chunk_combined_checksum != full_file_checksum:
            errors.append(
                RawFileProcessingError(
                    file_path=file_path,
                    error_msg=f"Checksum mismatch for {file_path}: {chunk_combined_checksum} != {full_file_checksum}",
                )
            )
        else:
            normalized_files.append(
                ImportReadyNormalizedFile(
                    input_file_path=file_path,
                    output_file_paths=[chunk.output_file_path for chunk in chunks],
                )
            )

    return BatchedTaskInstanceOutput[ImportReadyNormalizedFile](
        results=normalized_files, errors=errors
    )


def regroup_normalized_file_chunks(
    normalized_chunks_result: List[NormalizedCsvChunkResult],
    normalized_chunks_errors: List[RawFileProcessingError],
) -> Dict[str, List[NormalizedCsvChunkResult]]:
    """Returns dictionary of filepath: [normalized_chunk_results]"""
    file_to_normalized_chunks = defaultdict(list)
    files_with_previous_errors = set(
        error.file_path for error in normalized_chunks_errors
    )

    for chunk in normalized_chunks_result:
        # If we encountered an error in the normalization of any of the file chunks
        # then the checksum verification will fail
        # don't include those chunks so we find only true checksum mismatches
        if chunk.input_file_path in files_with_previous_errors:
            continue
        file_to_normalized_chunks[chunk.input_file_path].append(chunk)

    # Chunks need to be in order for checksum validation
    for chunks in file_to_normalized_chunks.values():
        chunks.sort(key=lambda x: x.chunk_boundary.chunk_num)

    return file_to_normalized_chunks
