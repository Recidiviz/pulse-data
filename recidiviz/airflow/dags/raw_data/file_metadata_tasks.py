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
"""Python logic for managing and handling raw file metadata"""
import concurrent.futures
import logging
from collections import deque
from itertools import groupby
from typing import Any, Deque, Dict, List, Optional, Tuple

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from more_itertools import distribute

from recidiviz.airflow.dags.raw_data.concurrency_utils import (
    MAX_BQ_LOAD_AIRFLOW_TASKS,
    MAX_GCS_FILE_SIZE_REQUEST_THREADS,
    MAX_NUMBER_OF_CHUNKS_FOR_SECONDARY_IMPORT,
)
from recidiviz.airflow.dags.raw_data.metadata import (
    APPEND_READY_FILE_BATCHES,
    BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS,
    BQ_METADATA_TO_IMPORT_THIS_RUN,
    FILE_IMPORTS,
    HAS_FILE_IMPORT_ERRORS,
    IMPORT_READY_FILES,
    PROCESSED_PATHS_TO_RENAME,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_SCHEMA,
    SKIPPED_FILE_ERRORS,
)
from recidiviz.airflow.dags.raw_data.utils import (
    get_direct_ingest_region_raw_config,
    get_est_number_of_chunks_for_paths,
)
from recidiviz.airflow.dags.utils.constants import RAISE_OPERATIONS_REGISTRATION_ERRORS
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
    RawDataPruningStatus,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    AppendReadyFile,
    AppendReadyFileBatch,
    AppendSummary,
    ImportReadyFile,
    PreImportNormalizationType,
    RawBigQueryFileMetadata,
    RawDataAppendImportError,
    RawDataFilesSkippedError,
    RawFileBigQueryLoadConfig,
    RawFileImport,
    RawFileLoadAndPrepError,
    RawFileProcessingError,
)
from recidiviz.utils.airflow_types import (
    BatchedTaskInstanceOutput,
    MappedBatchedTaskOutput,
)
from recidiviz.utils.types import assert_type


@task
def split_by_pre_import_normalization_type(
    region_code: str,
    serialized_bq_metadata: List[str],
    file_ids_to_headers: Dict[str, List[str]],
) -> Dict[str, Any]:
    """Determines the pre-import normalization needs of each element of
    |serialized_bq_metadata|, returning a dictionary with three keys:
        - import_ready_files: list of serialized ImportReadyFile that will skip the
            pre-import normalization steps
        - requires_pre_normalization_files: list of GcsfsFilePath objects
            to be processed in parallel by pre-import normalization step
        - requires_normalization_files_big_query_metadata: list of serialized
            RawBigQueryFileMetadata that will be combined downstream with the
            resultant ImportReadyNormalizedFile objects to to build ImportReadyFile
            objects
        - requires_normalization_files_big_query_schema: dictionary mapping file_id
            to serialized RawFileBigQueryLoadConfig objects that will be combined
            downstream with the resultant ImportReadyNormalizedFile objects to build
            ImportReadyFile objects
    """

    bq_metadata = [
        RawBigQueryFileMetadata.deserialize(serialized_metadata)
        for serialized_metadata in serialized_bq_metadata
    ]

    region_config = get_direct_ingest_region_raw_config(region_code=region_code)

    import_ready_files: List[ImportReadyFile] = []
    pre_import_normalization_required_big_query_metadata: List[
        RawBigQueryFileMetadata
    ] = []
    pre_import_normalization_required_big_query_schema: Dict[
        int, RawFileBigQueryLoadConfig
    ] = {}
    pre_import_normalization_required_files: List[GcsfsFilePath] = []

    for metadata in bq_metadata:
        pre_import_normalization_type = (
            PreImportNormalizationType.required_pre_import_normalization_type(
                region_config.raw_file_configs[metadata.file_tag]
            )
        )

        if (
            file_headers := file_ids_to_headers.get(
                str(assert_type(metadata.file_id, int))
            )
        ) is None:
            logging.info(
                "Skipping import for file_id [%s] as raw file header verification was not successful",
                metadata.file_id,
            )
            continue

        bq_schema = RawFileBigQueryLoadConfig.from_headers_and_raw_file_config(
            file_headers=file_headers,
            raw_file_config=region_config.raw_file_configs[metadata.file_tag],
        )

        if pre_import_normalization_type:
            pre_import_normalization_required_big_query_metadata.append(metadata)
            pre_import_normalization_required_big_query_schema[
                assert_type(metadata.file_id, int)
            ] = bq_schema
            for gcs_file in metadata.gcs_files:
                pre_import_normalization_required_files.append(gcs_file.path)
        else:
            import_ready_files.append(
                ImportReadyFile.from_bq_metadata_and_load_config(metadata, bq_schema)
            )

    return {
        IMPORT_READY_FILES: [file.serialize() for file in import_ready_files],
        REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA: [
            metadata.serialize()
            for metadata in pre_import_normalization_required_big_query_metadata
        ],
        REQUIRES_PRE_IMPORT_NORMALIZATION_FILES: [
            file.abs_path() for file in pre_import_normalization_required_files
        ],
        REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_SCHEMA: {
            file_id: schema.serialize()
            for file_id, schema in pre_import_normalization_required_big_query_schema.items()
        },
    }


@task
def get_files_to_import_this_run(
    *, raw_data_instance: DirectIngestInstance, serialized_bq_metadata: List[str]
) -> Dict[str, List[str]]:
    """Limits the number of files we are going to import in a single run of the raw
    data import DAG, due to the scale of full secondary re-imports.
    """

    serialized_metadata_to_run: list[str] = []
    serialized_metadata_to_defer: list[str] = []
    if raw_data_instance == DirectIngestInstance.PRIMARY:
        serialized_metadata_to_run = serialized_bq_metadata
    else:
        metadata_to_run, metadata_to_defer = _limit_files_by_est_chunks(
            serialized_bq_metadata=serialized_bq_metadata,
            est_number_of_chunks=MAX_NUMBER_OF_CHUNKS_FOR_SECONDARY_IMPORT,
        )

        serialized_metadata_to_run = [
            metadata.serialize() for metadata in metadata_to_run
        ]
        serialized_metadata_to_defer = [
            metadata.serialize() for metadata in metadata_to_defer
        ]

    return {
        BQ_METADATA_TO_IMPORT_THIS_RUN: serialized_metadata_to_run,
        BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS: serialized_metadata_to_defer,
    }


def _limit_files_by_est_chunks(
    *, serialized_bq_metadata: list[str], est_number_of_chunks: int
) -> tuple[list[RawBigQueryFileMetadata], list[RawBigQueryFileMetadata]]:
    """Bifurcates |serialized_bq_metadata| into two buckets: the first has |est_number_of_chunks|
    chunks of the files with the oldest update_datetime, while the second has all the
    remaining files. The number of chunks in each bucket is an estimate as we will not
    know the exact number of chunks until after the chunking step, as we look for up to
    2 mb past the "expected" chunk boundary to find a row-safe chunk boundary (so
    ostensibly a file that is 100 mb + 1 byte would get an estimate of 2 chunks but in
    reality only be 1 chunk).
    """

    fs = GcsfsFactory.build()
    candidate_bq_metadata_queue: Deque[RawBigQueryFileMetadata] = deque()

    # we add metadata to FIFO candidate queue in ascending order to ensure that we
    # always import the oldest files first.
    for metadata in sorted(
        (
            RawBigQueryFileMetadata.deserialize(serialized_bq)
            for serialized_bq in serialized_bq_metadata
        ),
        key=lambda x: (x.update_datetime, x.file_tag),
    ):
        candidate_bq_metadata_queue.append(metadata)

    metadata_to_run: list[RawBigQueryFileMetadata] = []
    metadata_to_run_cumulative_chunks = 0
    metadata_to_defer: list[RawBigQueryFileMetadata] = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_GCS_FILE_SIZE_REQUEST_THREADS
    ) as executor:

        # while we still have more chunk headroom and there are still files that are
        # yet to be processed
        while (
            candidate_bq_metadata_queue
            and metadata_to_run_cumulative_chunks < est_number_of_chunks
        ):
            file_size_futures: dict[
                concurrent.futures.Future, RawBigQueryFileMetadata
            ] = {}

            # queue a limited number of files at a time so we don't send an overwhelming
            # number of requests if the number of files to process is large (the batch
            # size is largely arbitrary).
            batch_size = min(
                MAX_GCS_FILE_SIZE_REQUEST_THREADS * 6, len(candidate_bq_metadata_queue)
            )
            for _ in range(batch_size):
                next_bq_metadata = candidate_bq_metadata_queue.popleft()
                file_size_futures[
                    executor.submit(
                        get_est_number_of_chunks_for_paths,
                        fs,
                        [
                            gcs_metadata.path
                            for gcs_metadata in next_bq_metadata.gcs_files
                        ],
                    )
                ] = next_bq_metadata

            # fetches results in the order we QUEUED them, not the order that we finished
            # since we want to ensure that we are always adding to |metadata_to_run| in
            # strict update_datetime order
            for future, bq_metadata in file_size_futures.items():
                try:
                    size = future.result()
                except Exception:
                    # If get_file_size returns None, set number of chunks to 1; if the file
                    # doesn't exist we'll return an error downstream
                    size = 1

                if metadata_to_run_cumulative_chunks >= est_number_of_chunks:
                    # if we have reached out max number of files for this import, mark
                    # as deferred
                    metadata_to_defer.append(bq_metadata)
                else:
                    metadata_to_run.append(bq_metadata)
                    metadata_to_run_cumulative_chunks += size

    if len(candidate_bq_metadata_queue) > 0:
        metadata_to_defer.extend(candidate_bq_metadata_queue)

    return (
        metadata_to_run,
        metadata_to_defer,
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def coalesce_import_ready_files(
    maybe_serialized_import_ready_files_no_normalization: Optional[List[str]],
    serialized_pre_import_normalization_result: Optional[str],
) -> List[List[str]]:
    """Combines import ready file objects from pre-import normalization and files that
    did not need pre-import normalization into equally sized batches.
    """
    if maybe_serialized_import_ready_files_no_normalization is None:
        raise ValueError(
            "If there are no import ready files, we expect an empty list; however since"
            "we found None, that means the task failed to run upstream so let's fail "
            "loud."
        )

    pre_import_normalization_results = (
        BatchedTaskInstanceOutput.deserialize(
            serialized_pre_import_normalization_result,
            ImportReadyFile,
            RawFileProcessingError,
        ).results
        if serialized_pre_import_normalization_result is not None
        else []
    )

    all_import_ready_files = [
        *maybe_serialized_import_ready_files_no_normalization,
        *[result.serialize() for result in pre_import_normalization_results],
    ]

    if not all_import_ready_files:
        return []

    num_batches = min(len(all_import_ready_files), MAX_BQ_LOAD_AIRFLOW_TASKS)

    return [list(batch) for batch in distribute(num_batches, all_import_ready_files)]


@task(trigger_rule=TriggerRule.ALL_DONE)
def coalesce_results_and_errors(
    *,
    region_code: str,
    raw_data_instance: DirectIngestInstance,
    serialized_bq_metadata: Optional[List[str]],
    serialized_header_verification_errors: Optional[List[str]],
    serialized_chunking_errors: Optional[List[str]],
    serialized_pre_import_normalization_result: Optional[str],
    serialized_load_prep_results: Optional[List[str]],
    serialized_append_batches: Optional[Dict[str, List[str]]],
    serialized_append_result: Optional[List[str]],
) -> Dict[str, List[str] | bool]:
    """Reconciles RawBigQueryFileMetadata objects against the results and errors
    of the pre-import normalization and big query load steps, returning a dictionary
    with four keys:
        - file_imports: a list of RawFileImport objects to be persisted to the
            operations database
        - processed_paths_to_rename: a list of GcsfsFilePath objects that need their
            processed state renamed from unprocessed to processed after successfully
            being imported
    """

    (
        bq_metadata,
        import_results,
        import_errors,
    ) = _deserialize_coalesce_results_and_errors_inputs(
        serialized_bq_metadata,
        serialized_header_verification_errors,
        serialized_chunking_errors,
        serialized_pre_import_normalization_result,
        serialized_load_prep_results,
        serialized_append_batches,
        serialized_append_result,
    )

    (
        successful_file_imports_for_file_id,
        successfully_imported_paths,
    ) = _build_file_imports_for_results(*import_results)

    region_raw_config = get_direct_ingest_region_raw_config(region_code)

    failed_file_imports_for_file_id = _build_file_imports_for_errors(
        region_raw_config, raw_data_instance, bq_metadata, *import_errors
    )

    missing_file_imports = _reconcile_file_imports_and_bq_metadata(
        region_raw_config,
        raw_data_instance,
        bq_metadata,
        successful_file_imports_for_file_id,
        failed_file_imports_for_file_id,
    )

    return {
        FILE_IMPORTS: [
            summary.serialize()
            for summary in [
                *successful_file_imports_for_file_id.values(),
                *failed_file_imports_for_file_id.values(),
                *missing_file_imports,
            ]
        ],
        HAS_FILE_IMPORT_ERRORS: bool(
            failed_file_imports_for_file_id or missing_file_imports
        ),
        PROCESSED_PATHS_TO_RENAME: [
            path.abs_path() for path in successfully_imported_paths
        ],
    }


def _reconcile_file_imports_and_bq_metadata(
    raw_region_config: DirectIngestRegionRawFileConfig,
    raw_data_instance: DirectIngestInstance,
    bq_metadata: List[RawBigQueryFileMetadata],
    successful_file_imports_for_file_id: Dict[int, RawFileImport],
    failed_file_imports_for_file_id: Dict[int, RawFileImport],
) -> List[RawFileImport]:
    """Validates that for every file_id in |bq_metadata|, there is either a
    corresponding success in |successful_file_imports_for_file_id| or failure
    in |failed_file_imports_for_file_id|; in theory this should never happen
    but let's just backstop against it in case an upstream task failed without writing
    its results to xcom. For all "missing" file_ids, returns an RawFileImportSummary
    object with an import_status of FAILED_UNKNOWN.
    """

    missing_file_ids: List[RawFileImport] = []
    for metadata in bq_metadata:
        if (
            metadata.file_id not in successful_file_imports_for_file_id
            and metadata.file_id not in failed_file_imports_for_file_id
        ):
            logging.error(
                "Found bq metadata entry with file id [%s] without a corresponding "
                "successful result or error",
                metadata.file_id,
            )
            missing_file_ids.append(
                RawFileImport(
                    file_id=assert_type(metadata.file_id, int),
                    import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
                    historical_diffs_active=_historical_diffs_active_for_tag(
                        raw_region_config, raw_data_instance, metadata.file_tag
                    ),
                    error_message=(
                        f"Could not locate a success or failure for this file_id "
                        f"[{metadata.file_id}] despite it being marked for import. This"
                        f"is likely indicative that a DAG-level failure occurred during"
                        f"this import run."
                    ),
                )
            )

    return missing_file_ids


def _historical_diffs_active_for_tag(
    raw_region_config: DirectIngestRegionRawFileConfig,
    raw_data_instance: DirectIngestInstance,
    file_tag: str,
) -> bool:
    """Boolean return for if the raw file_tag will have historical diffs active"""
    return (
        raw_region_config.raw_file_configs[file_tag].get_pruning_status(
            raw_data_instance
        )
        == RawDataPruningStatus.AUTOMATIC
    )


def _build_file_imports_for_errors(
    raw_region_config: DirectIngestRegionRawFileConfig,
    raw_data_instance: DirectIngestInstance,
    bq_metadata: List[RawBigQueryFileMetadata],
    processing_errors: List[RawFileProcessingError],
    load_and_prep_errors: List[RawFileLoadAndPrepError],
    append_errors: List[RawDataAppendImportError],
) -> Dict[int, RawFileImport]:
    """Builds RawFileImport objects for all errors seen during the import process"""

    failed_file_imports: Dict[int, RawFileImport] = {}

    # --- simple case: append_errors and load_and_prep_errors have file_id -------------

    for append_error in append_errors:
        failed_file_imports[append_error.file_id] = RawFileImport(
            file_id=append_error.file_id,
            import_status=append_error.error_type,
            historical_diffs_active=_historical_diffs_active_for_tag(
                raw_region_config, raw_data_instance, append_error.file_tag
            ),
            error_message=append_error.error_msg,
        )

    for load_and_prep_error in load_and_prep_errors:
        failed_file_imports[load_and_prep_error.file_id] = RawFileImport(
            file_id=load_and_prep_error.file_id,
            import_status=load_and_prep_error.error_type,
            historical_diffs_active=_historical_diffs_active_for_tag(
                raw_region_config, raw_data_instance, load_and_prep_error.file_tag
            ),
            error_message=load_and_prep_error.error_msg,
        )

    # --- complex case: map file path back to file_id ----------------------------------

    path_to_bq_metadata: Dict[GcsfsFilePath, RawBigQueryFileMetadata] = {
        gcs_metadata.path: metadata
        for metadata in bq_metadata
        for gcs_metadata in metadata.gcs_files
    }

    # because we split files up during the pre-import normalization stage, it's possible
    # that we could have more than one error per input path
    for failing_file_path, processing_errors_for_path_iter in groupby(
        sorted(processing_errors, key=lambda x: x.original_file_path),
        key=lambda x: x.original_file_path,
    ):
        processing_errors_for_path = list(processing_errors_for_path_iter)
        bq_metadata_for_error = path_to_bq_metadata[failing_file_path]
        file_id = assert_type(bq_metadata_for_error.file_id, int)
        failed_file_imports[file_id] = RawFileImport(
            file_id=file_id,
            import_status=processing_errors_for_path[0].error_type,
            historical_diffs_active=_historical_diffs_active_for_tag(
                raw_region_config,
                raw_data_instance,
                processing_errors_for_path[0].parts.file_tag,
            ),
            error_message="\n\n".join(
                error.error_msg for error in processing_errors_for_path
            ),
        )

    return failed_file_imports


def _build_file_imports_for_results(
    append_summaries: List[AppendSummary],
    append_ready_file_batches: List[AppendReadyFileBatch],
) -> Tuple[Dict[int, RawFileImport], List[GcsfsFilePath]]:
    """Groups |append_summaries| and |append_ready_file_batches| by file_id and builds
    RawFileImportSummary objects for file_ids that have results from both stages
    of big query load step.
    """

    successful_file_imports: Dict[int, RawFileImport] = {}
    successfully_imported_paths: List[GcsfsFilePath] = []

    append_summary_by_file_id = {
        append_summary.file_id: append_summary for append_summary in append_summaries
    }

    for append_ready_file_batch in append_ready_file_batches:
        for (
            append_ready_files_for_tag
        ) in append_ready_file_batch.append_ready_files_by_tag.values():
            for append_ready_file in append_ready_files_for_tag:
                if append_summary := append_summary_by_file_id.get(
                    append_ready_file.import_ready_file.file_id
                ):
                    successful_file_imports[
                        append_summary.file_id
                    ] = RawFileImport.from_load_results(
                        append_ready_file, append_summary
                    )
                    successfully_imported_paths.extend(
                        append_ready_file.import_ready_file.original_file_paths
                    )

    return successful_file_imports, successfully_imported_paths


def _deserialize_coalesce_results_and_errors_inputs(
    serialized_bq_metadata: Optional[List[str]],
    serialized_header_verification_errors: Optional[List[str]],
    serialized_chunking_errors: Optional[List[str]],
    serialized_pre_import_normalization_result: Optional[str],
    serialized_load_prep_results: Optional[List[str]],
    serialized_append_batches: Optional[Dict[str, List[str]]],
    serialized_append_result: Optional[List[str]],
) -> Tuple[
    List[RawBigQueryFileMetadata],
    Tuple[List[AppendSummary], List[AppendReadyFileBatch]],
    Tuple[
        List[RawFileProcessingError],
        List[RawFileLoadAndPrepError],
        List[RawDataAppendImportError],
    ],
]:
    """Deserialize and group inputs of coalesce_results_and_errors"""

    bq_metadata = (
        [
            RawBigQueryFileMetadata.deserialize(serialized_bq_metadata)
            for serialized_bq_metadata in serialized_bq_metadata
        ]
        if serialized_bq_metadata
        else []
    )

    header_verification_errors = (
        [
            RawFileProcessingError.deserialize(serialized_error)
            for serialized_error in serialized_header_verification_errors
        ]
        if serialized_header_verification_errors
        else []
    )

    chunking_errors = (
        [
            RawFileProcessingError.deserialize(file)
            for file in serialized_chunking_errors
        ]
        if serialized_chunking_errors
        else []
    )

    pre_import_normalization_step_errors = (
        BatchedTaskInstanceOutput.deserialize(
            serialized_pre_import_normalization_result,
            result_cls=ImportReadyFile,
            error_cls=RawFileProcessingError,
        ).errors
        if serialized_pre_import_normalization_result
        else []
    )

    load_and_prep_step_output = MappedBatchedTaskOutput.deserialize(
        serialized_load_prep_results or [],
        result_cls=AppendReadyFile,
        error_cls=RawFileLoadAndPrepError,
    )

    append_batches_step_results = (
        [
            AppendReadyFileBatch.deserialize(serialized_append_ready_file_batch)
            for serialized_append_ready_file_batch in serialized_append_batches[
                APPEND_READY_FILE_BATCHES
            ]
        ]
        if serialized_append_batches
        else []
    )
    append_batches_skipped_file_errors = (
        [
            RawFileLoadAndPrepError.deserialize(serialized_skipped_file_errors)
            for serialized_skipped_file_errors in serialized_append_batches[
                SKIPPED_FILE_ERRORS
            ]
        ]
        if serialized_append_batches
        else []
    )

    append_step_output = MappedBatchedTaskOutput.deserialize(
        serialized_append_result or [],
        result_cls=AppendSummary,
        error_cls=RawDataAppendImportError,
    )

    return (
        bq_metadata,
        (
            append_step_output.flatten_results(),
            append_batches_step_results,
        ),
        (
            [
                *header_verification_errors,
                *chunking_errors,
                *pre_import_normalization_step_errors,
            ],
            [
                *load_and_prep_step_output.flatten_errors(),
                *append_batches_skipped_file_errors,
            ],
            append_step_output.flatten_errors(),
        ),
    )


@task(task_id=RAISE_OPERATIONS_REGISTRATION_ERRORS)
def raise_operations_registration_errors(
    serialized_bq_metadata_skipped_file_errors: List[str],
    serialized_bq_postgres_alignment_skipped_file_errors: List[str],
    serialized_pruning_metadata_skipped_file_errors: List[str],
) -> None:
    skipped_file_errors = [
        RawDataFilesSkippedError.deserialize(serialized_skipped_file_error)
        for serialized_skipped_file_error in serialized_bq_metadata_skipped_file_errors
        + serialized_bq_postgres_alignment_skipped_file_errors
        + serialized_pruning_metadata_skipped_file_errors
    ]

    if skipped_file_errors:
        error_msg = "\n\n".join([str(error) for error in skipped_file_errors])
        raise AirflowException(f"Error(s) occurred in file registration:\n{error_msg}")
