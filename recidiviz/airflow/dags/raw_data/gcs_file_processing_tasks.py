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
import logging
import traceback
from collections import defaultdict
from itertools import groupby
from typing import Any, Dict, List, Tuple

from airflow.decorators import task
from airflow.exceptions import AirflowException

from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    ENTRYPOINT_ARGUMENTS,
)
from recidiviz.airflow.dags.raw_data.concurrency_utils import (
    MAX_GCS_FILE_SIZE_REQUEST_THREADS,
)
from recidiviz.airflow.dags.raw_data.filtering_tasks import (
    filter_header_results_by_processing_errors,
)
from recidiviz.airflow.dags.raw_data.metadata import (
    FILE_IDS_TO_HEADERS,
    HEADER_VERIFICATION_ERRORS,
)
from recidiviz.airflow.dags.raw_data.utils import (
    evenly_weighted_buckets_with_max,
    get_direct_ingest_region_raw_config,
    get_est_number_of_chunks_concurrently,
    max_number_of_buckets_with_target,
)
from recidiviz.airflow.dags.utils.constants import (
    RAISE_CHUNK_NORMALIZATION_ERRORS,
    RAISE_FILE_CHUNKING_ERRORS,
    RAISE_HEADER_VERIFICATION_ERRORS,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_header_reader import (
    DirectIngestRawFileHeaderReader,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    ImportReadyFile,
    PreImportNormalizedCsvChunkResult,
    RawBigQueryFileMetadata,
    RawFileBigQueryLoadConfig,
    RawFileProcessingError,
    RequiresPreImportNormalizationFile,
    RequiresPreImportNormalizationFileChunk,
)
from recidiviz.utils.airflow_types import (
    BatchedTaskInstanceOutput,
    MappedBatchedTaskOutput,
)
from recidiviz.utils.crc32c import digest_ordered_checksum_and_size_pairs
from recidiviz.utils.types import assert_type

ENTRYPOINT_ARG_LIST_DELIMITER = "^"
BYTES_IN_MB = 1024 * 1024


@task
def generate_file_chunking_pod_arguments(
    *,
    region_code: str,
    serialized_requires_pre_import_normalization_file_paths: List[str],
    target_num_chunking_airflow_tasks: int,
    max_chunks_per_airflow_task: int,
) -> List[List[str]]:
    return [
        [
            *ENTRYPOINT_ARGUMENTS,
            "--entrypoint=RawDataFileChunkingEntrypoint",
            f"--state_code={region_code}",
            f"--requires_normalization_files={ENTRYPOINT_ARG_LIST_DELIMITER.join(batch)}",
        ]
        for batch in create_file_batches(
            serialized_requires_pre_import_normalization_file_paths=serialized_requires_pre_import_normalization_file_paths,
            target_num_chunking_airflow_tasks=target_num_chunking_airflow_tasks,
            max_chunks_per_airflow_task=max_chunks_per_airflow_task,
        )
    ]


def create_file_batches(
    *,
    serialized_requires_pre_import_normalization_file_paths: List[str],
    target_num_chunking_airflow_tasks: int,
    max_chunks_per_airflow_task: int,
) -> List[List[str]]:
    fs = GcsfsFactory.build()
    requires_pre_import_normalization_file_paths = [
        GcsfsFilePath.from_absolute_path(path)
        for path in serialized_requires_pre_import_normalization_file_paths
    ]

    batches = _batch_files_by_size(
        fs,
        requires_pre_import_normalization_file_paths,
        target_num_chunking_airflow_tasks=target_num_chunking_airflow_tasks,
        max_chunks_per_airflow_task=max_chunks_per_airflow_task,
    )
    serialized_batches = [[path.abs_path() for path in batch] for batch in batches]

    return serialized_batches


def _batch_files_by_size(
    fs: GCSFileSystem,
    requires_pre_import_normalization_file_paths: List[GcsfsFilePath],
    *,
    target_num_chunking_airflow_tasks: int,
    max_chunks_per_airflow_task: int,
) -> List[List[GcsfsFilePath]]:
    """Divide files into batches with approximately equal cumulative size"""
    # TODO(#35694): remove the need for these calls by moving the est number of chunks
    # onto the RawBigQueryFileMetadata class?
    files_with_sizes = get_est_number_of_chunks_concurrently(
        fs, requires_pre_import_normalization_file_paths
    )
    return evenly_weighted_buckets_with_max(
        files_with_sizes,
        target_n=target_num_chunking_airflow_tasks,
        max_weight_per_bucket=max_chunks_per_airflow_task,
    )


@task
def generate_chunk_processing_pod_arguments(
    *,
    region_code: str,
    serialized_pre_import_files: List[str],
    target_num_normalization_airflow_tasks: int,
    max_file_chunks_per_airflow_task: int,
) -> List[List[str]]:
    """Builds file chunk normalization kubernetes pod arguments by expanding |serialized_pre_import_files|
    into file chunks, batching no more than |max_file_chunks_per_airflow_task| file chunks
    for independent airflow task consumption.
    """
    all_pre_import_files = [
        RequiresPreImportNormalizationFile.deserialize(file)
        for file in serialized_pre_import_files
    ]

    return [
        [
            *ENTRYPOINT_ARGUMENTS,
            "--entrypoint=RawDataChunkNormalizationEntrypoint",
            f"--state_code={region_code}",
            f"--file_chunks={ENTRYPOINT_ARG_LIST_DELIMITER.join(batch)}",
        ]
        for batch in divide_file_chunks_into_batches(
            all_pre_import_files=all_pre_import_files,
            target_num_normalization_airflow_tasks=target_num_normalization_airflow_tasks,
            max_file_chunks_per_airflow_task=max_file_chunks_per_airflow_task,
        )
    ]


def divide_file_chunks_into_batches(
    *,
    all_pre_import_files: List[RequiresPreImportNormalizationFile],
    target_num_normalization_airflow_tasks: int,
    max_file_chunks_per_airflow_task: int,
) -> List[List[str]]:
    """Distribute files into file chunk batches for consumption by airflow normalization
    tasks.

    This function will target |target_num_normalization_airflow_tasks| concurrent airflow tasks;
    however, if we have more than |max_file_chunks_per_airflow_task| file chunks per batch, we will
    expand to however many tasks are necessary to assign |max_file_chunks_per_airflow_task| chunks
    per task. From anecdotal testing, any more than |max_file_chunks_per_airflow_task| exceeds python's
    max command length.
    """
    all_pre_import_file_chunks = _create_individual_chunk_objects_list(
        all_pre_import_files
    )
    batches = max_number_of_buckets_with_target(
        items=all_pre_import_file_chunks,
        target_number_of_buckets=target_num_normalization_airflow_tasks,
        max_per_bucket=max_file_chunks_per_airflow_task,
    )
    serialized_batches = [[chunk.serialize() for chunk in batch] for batch in batches]

    return serialized_batches


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
    serialized_requires_pre_import_normalization_files_bq_metadata: List[str],
    serialized_requires_pre_import_normalization_files_bq_schema: Dict[str, str],
) -> str:
    """Task organizes normalized chunks by file and compares their collective checksum
    against the full file checksum to ensure all file bytes were read correctly.
    """
    mapped_task_output = MappedBatchedTaskOutput.deserialize(
        normalized_chunks_result,
        result_cls=PreImportNormalizedCsvChunkResult,
        error_cls=RawFileProcessingError,
    )
    all_results: List[
        PreImportNormalizedCsvChunkResult
    ] = mapped_task_output.flatten_results()
    upstream_errors: List[RawFileProcessingError] = mapped_task_output.flatten_errors()

    (
        input_path_incomplete_errors,
        file_path_to_normalized_chunks,
    ) = regroup_normalized_file_chunks(all_results, upstream_errors)

    requires_pre_import_normalization_files_bq_metadata = [
        RawBigQueryFileMetadata.deserialize(
            serialized_requires_normalization_file_bq_metadata
        )
        for serialized_requires_normalization_file_bq_metadata in serialized_requires_pre_import_normalization_files_bq_metadata
    ]
    requires_pre_import_normalization_files_bq_schema = {
        int(file_id): RawFileBigQueryLoadConfig.deserialize(schema)
        for file_id, schema in serialized_requires_pre_import_normalization_files_bq_schema.items()
    }

    checksum_errors, filtered_file_path_to_normalized_chunks = verify_file_checksums(
        file_path_to_normalized_chunks
    )

    conceptual_file_incomplete_errors, import_ready_files = build_import_ready_files(
        filtered_file_path_to_normalized_chunks,
        requires_pre_import_normalization_files_bq_metadata,
        requires_pre_import_normalization_files_bq_schema,
    )

    all_errors = [
        *upstream_errors,
        *checksum_errors,
        *input_path_incomplete_errors,
        *conceptual_file_incomplete_errors,
    ]

    (
        non_blocked_import_ready_files,
        skipped_errors,
    ) = filter_normalization_results_based_on_errors(import_ready_files, all_errors)

    return BatchedTaskInstanceOutput[ImportReadyFile, RawFileProcessingError](
        errors=[*all_errors, *skipped_errors],
        results=non_blocked_import_ready_files,
    ).serialize()


def filter_normalization_results_based_on_errors(
    import_ready_files: List[ImportReadyFile],
    all_processing_errors: List[RawFileProcessingError],
) -> Tuple[List[ImportReadyFile], List[RawFileProcessingError]]:
    """Filters |import_ready_files| by removing all elements wholes file_tag has an
    error in |all_processing_errors| with an older update_datetime.
    """
    non_blocked_import_ready_files: List[ImportReadyFile] = []
    skipped_files: List[RawFileProcessingError] = []

    blocking_failed_files_by_file_tag: Dict[str, RawFileProcessingError] = {
        file_tag: min(group, key=lambda x: x.parts.utc_upload_datetime)
        for file_tag, group in groupby(
            sorted(all_processing_errors, key=lambda x: x.parts.file_tag),
            lambda x: x.parts.file_tag,
        )
    }

    for import_ready_file in import_ready_files:
        if (
            import_ready_file.file_tag in blocking_failed_files_by_file_tag
            and blocking_failed_files_by_file_tag[
                import_ready_file.file_tag
            ].parts.utc_upload_datetime
            < import_ready_file.update_datetime
        ):
            blocking_error = blocking_failed_files_by_file_tag[
                import_ready_file.file_tag
            ]
            skipped_files.append(
                RawFileProcessingError(
                    # this single file path will link back to file_id downstream
                    original_file_path=import_ready_file.original_file_paths[0],
                    temporary_file_paths=import_ready_file.pre_import_normalized_file_paths,
                    error_type=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                    error_msg=f"Blocked Import: failed due to import-blocking failure from {blocking_error.original_file_path}",
                )
            )
            continue
        non_blocked_import_ready_files.append(import_ready_file)

    return non_blocked_import_ready_files, skipped_files


def verify_file_checksums(
    file_path_to_normalized_chunks: Dict[
        GcsfsFilePath, List[PreImportNormalizedCsvChunkResult]
    ],
) -> Tuple[
    List[RawFileProcessingError],
    Dict[GcsfsFilePath, List[PreImportNormalizedCsvChunkResult]],
]:
    """Verifies the checksum of the normalized file chunks against the full file checksum
    and returns the list errors from non-matching files and a mapping of input file paths
    to their corresponding PreImportNormalizedCsvChunkResults.
    """

    fs = GcsfsFactory.build()
    errors: List[RawFileProcessingError] = []
    filtered_file_path_to_normalized_chunks: Dict[
        GcsfsFilePath, List[PreImportNormalizedCsvChunkResult]
    ] = {}

    for file_path, chunks in file_path_to_normalized_chunks.items():
        chunk_checksums_and_sizes = [
            (chunk.crc32c, chunk.get_chunk_boundary_size()) for chunk in chunks
        ]
        chunk_combined_digest = digest_ordered_checksum_and_size_pairs(
            chunk_checksums_and_sizes
        )
        chunk_combined_checksum = base64.b64encode(chunk_combined_digest).decode(
            "utf-8"
        )

        full_file_checksum = fs.get_crc32c(file_path)

        if chunk_combined_checksum != full_file_checksum:
            errors.append(
                RawFileProcessingError(
                    original_file_path=file_path,
                    temporary_file_paths=[chunk.output_file_path for chunk in chunks],
                    error_msg=f"Checksum mismatch for {file_path.abs_path()}: {chunk_combined_checksum} != {full_file_checksum}",
                )
            )
        else:
            filtered_file_path_to_normalized_chunks[file_path] = chunks

    return errors, filtered_file_path_to_normalized_chunks


def regroup_normalized_file_chunks(
    normalized_chunks_result: List[PreImportNormalizedCsvChunkResult],
    normalized_chunks_errors: List[RawFileProcessingError],
) -> Tuple[
    List[RawFileProcessingError],
    Dict[GcsfsFilePath, List[PreImportNormalizedCsvChunkResult]],
]:
    """Groups normalized chunk results by their input file path and returns a map of
    input file path to all associated normalized chunks, as well as successful chunks
    that were skipped due to an error encountered normalizing a different chunk.
    """
    file_path_to_normalized_chunks: Dict[
        GcsfsFilePath, List[PreImportNormalizedCsvChunkResult]
    ] = defaultdict(list)

    input_path_incomplete_errors: List[RawFileProcessingError] = []

    files_with_previous_errors = set(
        error.original_file_path for error in normalized_chunks_errors
    )

    for chunk in normalized_chunks_result:
        # If we encountered an error in the normalization of any of the file chunks
        # then the checksum verification will fail so don't include those chunks so we
        # find only true checksum mismatches
        if chunk.input_file_path in files_with_previous_errors:
            input_path_incomplete_errors.append(
                RawFileProcessingError(
                    original_file_path=chunk.input_file_path,
                    temporary_file_paths=[chunk.output_file_path],
                    error_type=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                    error_msg=f"Chunk [{chunk.chunk_boundary.chunk_num}] of [{chunk.input_file_path.abs_path()}] skipped due to error encountered with a different chunk with the same input path",
                )
            )
            continue
        file_path_to_normalized_chunks[chunk.input_file_path].append(chunk)

    # Chunks need to be in order for checksum validation
    for chunks in file_path_to_normalized_chunks.values():
        chunks.sort(key=lambda x: x.chunk_boundary.chunk_num)

    return input_path_incomplete_errors, file_path_to_normalized_chunks


def build_import_ready_files(
    filtered_file_path_to_normalized_chunks: Dict[
        GcsfsFilePath, List[PreImportNormalizedCsvChunkResult]
    ],
    requires_pre_import_normalization_files_bq_metadata: List[RawBigQueryFileMetadata],
    requires_pre_import_normalization_files_bq_schema: Dict[
        int, RawFileBigQueryLoadConfig
    ],
) -> Tuple[List[RawFileProcessingError], List[ImportReadyFile]]:
    """Uses |filtered_file_path_to_normalized_chunks| to determine which candidates from
    |requires_pre_import_normalization_files_bq_metadata| will be used to create import
    ready files, skipping any entries that are missing file paths.
    """

    # --- first, create mapping of original file path to associated bq metadata --------

    path_to_requires_normalization_files_bq_metadata: Dict[
        GcsfsFilePath, RawBigQueryFileMetadata
    ] = {
        gcs_metadata.path: bq_metadata
        for bq_metadata in requires_pre_import_normalization_files_bq_metadata
        for gcs_metadata in bq_metadata.gcs_files
    }

    # --- next, group input files by file_id -------------------------------------------

    file_id_to_input_paths: Dict[int, List[GcsfsFilePath]] = defaultdict(list)
    for valid_input_file in filtered_file_path_to_normalized_chunks:
        bq_metadata = path_to_requires_normalization_files_bq_metadata[valid_input_file]
        file_id_to_input_paths[assert_type(bq_metadata.file_id, int)].append(
            valid_input_file
        )

    # --- next, actually build new import ready files ----------------------------------

    new_import_ready_files: List[ImportReadyFile] = []
    conceptual_file_incomplete_errors: List[RawFileProcessingError] = []
    for bq_metadata in requires_pre_import_normalization_files_bq_metadata:
        successful_input_paths = set(
            file_id_to_input_paths[assert_type(bq_metadata.file_id, int)]
        )
        all_input_paths = {gcs_file.path for gcs_file in bq_metadata.gcs_files}

        if missing_paths := all_input_paths - successful_input_paths:
            logging.error(
                "Skipping import for [%s] w/ update_datetime [%s] and file_id [%s] due "
                "to failed pre-import normalization step for [%s]",
                bq_metadata.file_tag,
                bq_metadata.update_datetime.isoformat(),
                bq_metadata.file_id,
                missing_paths,
            )
            # we add errors for all input paths that are not missing to ensure that
            # we properly clean up the temporary file paths that were created
            for input_path in all_input_paths - missing_paths:
                conceptual_file_incomplete_errors.append(
                    RawFileProcessingError(
                        original_file_path=input_path,
                        temporary_file_paths=[
                            chunk.output_file_path
                            for chunk in filtered_file_path_to_normalized_chunks[
                                input_path
                            ]
                        ],
                        error_msg=f"Missing [{''.join(p.abs_path() for p in missing_paths)}] paths so could not build full conceptual file",
                    )
                )
            continue

        new_import_ready_files.append(
            ImportReadyFile.from_bq_metadata_load_config_and_normalized_chunk_result(
                bq_metadata,
                requires_pre_import_normalization_files_bq_schema[
                    assert_type(bq_metadata.file_id, int)
                ],
                {
                    gcs_file.path: filtered_file_path_to_normalized_chunks[
                        gcs_file.path
                    ]
                    for gcs_file in bq_metadata.gcs_files
                },
            )
        )

    return conceptual_file_incomplete_errors, new_import_ready_files


def _read_and_validate_headers(
    fs: GCSFileSystem,
    raw_file_config: DirectIngestRawFileConfig,
    gcs_file: GcsfsFilePath,
) -> List[str]:
    """For the given |gcs_file_path| --
    - If the raw file config has infer_columns_from_config=False, read the first row of the file
      and verify that the headers match the expected headers from the raw file config.
    - If the raw file config has infer_columns_from_config=True, return the column headers found
      in the raw file config and verify that there is no unexpected header row in the file.
    """
    file_reader = DirectIngestRawFileHeaderReader(fs, raw_file_config)

    return file_reader.read_and_validate_column_headers(gcs_file)


def read_and_verify_column_headers_concurrently(
    fs: GCSFileSystem,
    region_raw_file_config: DirectIngestRegionRawFileConfig,
    bq_metadata: List[RawBigQueryFileMetadata],
) -> Tuple[Dict[int, List[str]], List[RawFileProcessingError]]:
    """Reads and validates the headers of the files in the provided bq metadata concurrently."""

    results: Dict[str, List[str]] = {}
    errors: Dict[str, RawFileProcessingError] = {}
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_GCS_FILE_SIZE_REQUEST_THREADS
    ) as executor:
        futures = {
            executor.submit(
                _read_and_validate_headers,
                fs,
                region_raw_file_config.raw_file_configs[gcs_file.parts.file_tag],
                gcs_file.path,
            ): gcs_file.path
            for metadata in bq_metadata
            for gcs_file in metadata.gcs_files
        }
        for future in concurrent.futures.as_completed(futures):
            file_path = futures[future]
            try:
                results[file_path.abs_path()] = future.result()
            except Exception as e:
                errors[file_path.abs_path()] = RawFileProcessingError(
                    original_file_path=file_path,
                    temporary_file_paths=None,
                    error_msg=f"{file_path.abs_path()}: {str(e)}\n{traceback.format_exc()}",
                )

    file_id_to_headers: Dict[int, List[str]] = {}
    for metadata in bq_metadata:
        if any(errors.get(gcs_file.abs_path) for gcs_file in metadata.gcs_files):
            continue

        base_file = metadata.gcs_files[0]
        base_headers = results[base_file.abs_path]

        if all(
            base_headers == results[gcs_file.abs_path]
            for gcs_file in metadata.gcs_files[1:]
        ):
            file_id_to_headers[assert_type(metadata.file_id, int)] = base_headers
        else:
            for gcs_file in metadata.gcs_files:
                current_headers = results[gcs_file.abs_path]
                if base_headers != current_headers:
                    errors[gcs_file.abs_path] = RawFileProcessingError(
                        original_file_path=gcs_file.path,
                        temporary_file_paths=None,
                        error_msg=(
                            f"Raw file headers found in [{base_file.abs_path}]: [{base_headers}] "
                            f"do not match headers found in [{gcs_file.abs_path}]: [{current_headers}]"
                        ),
                    )
    return file_id_to_headers, list(errors.values())


@task
def read_and_verify_column_headers(
    region_code: str, serialized_bq_metadata: List[str]
) -> Dict[str, Any]:
    """For each file path in the provided bq metadata:
    If the raw file config has infer_columns_from_config=False, read the first row of the file
    and verify that the headers match the expected headers from the raw file config.
    If the raw file config has infer_columns_from_config=True, return the column headers found
    in the raw file config and verify that there is no unexpected header row in the file.

    Returns a dictionary with the following:
    - FILE_PATHS_TO_HEADERS: a mapping of file paths to a str list of column headers in the order they were found
    in the raw file or in the raw file config if the raw file did not have a header row
    - HEADER_VERIFICATION_ERRORS: a list of serialized RawFileProcessingError objects containing any errors that occurred
    during header verification"""
    bq_metadata = [
        RawBigQueryFileMetadata.deserialize(serialized_metadata)
        for serialized_metadata in serialized_bq_metadata
    ]

    fs = GcsfsFactory.build()
    region_raw_file_config = get_direct_ingest_region_raw_config(region_code)

    results, errors = read_and_verify_column_headers_concurrently(
        fs, region_raw_file_config, bq_metadata
    )

    filtered_file_ids, skipped_files = filter_header_results_by_processing_errors(
        bq_metadata, results, errors
    )

    return {
        FILE_IDS_TO_HEADERS: filtered_file_ids,
        HEADER_VERIFICATION_ERRORS: [
            error.serialize() for error in [*errors, *skipped_files]
        ],
    }


@task(task_id=RAISE_HEADER_VERIFICATION_ERRORS)
def raise_header_verification_errors(header_verification_errors: List[str]) -> None:
    errors = [
        RawFileProcessingError.deserialize(error)
        for error in header_verification_errors
    ]

    _raise_task_errors(errors)


@task(task_id=RAISE_FILE_CHUNKING_ERRORS)
def raise_file_chunking_errors(serialized_chunking_errors: List[str]) -> None:
    chunking_errors = [
        RawFileProcessingError.deserialize(error)
        for error in serialized_chunking_errors
    ]
    _raise_task_errors(chunking_errors)


@task(task_id=RAISE_CHUNK_NORMALIZATION_ERRORS)
def raise_chunk_normalization_errors(chunk_normalization_output: str) -> None:
    errors = BatchedTaskInstanceOutput.deserialize(
        chunk_normalization_output,
        result_cls=ImportReadyFile,
        error_cls=RawFileProcessingError,
    ).errors
    _raise_task_errors(errors)


def _raise_task_errors(task_errors: List[RawFileProcessingError]) -> None:
    if task_errors:
        error_msg = "\n\n".join([str(error) for error in task_errors])
        raise AirflowException(f"Error(s) occurred in file processing:\n{error_msg}")
