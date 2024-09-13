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
import logging
from typing import Any, Dict, List, Optional, Tuple

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from more_itertools import distribute

from recidiviz.airflow.dags.raw_data.metadata import (
    APPEND_READY_FILE_BATCHES,
    FILE_IMPORTS,
    IMPORT_READY_FILES,
    PROCESSED_PATHS_TO_RENAME,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_SCHEMA,
    SKIPPED_FILE_ERRORS,
    TEMPORARY_PATHS_TO_CLEAN,
    TEMPORARY_TABLES_TO_CLEAN,
)
from recidiviz.airflow.dags.raw_data.utils import get_direct_ingest_region_raw_config
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import (
    raw_data_pruning_enabled_in_state_and_instance,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
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
    RawFileBigQueryLoadConfig,
    RawFileImport,
    RawFileLoadAndPrepError,
    RawFileProcessingError,
    RequiresPreImportNormalizationFile,
)
from recidiviz.utils.airflow_types import (
    BatchedTaskInstanceOutput,
    MappedBatchedTaskOutput,
)
from recidiviz.utils.types import assert_type

MAX_BQ_LOAD_JOBS = 8  # TODO(#29946) determine reasonable default


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


@task(trigger_rule=TriggerRule.ALL_DONE)
def coalesce_import_ready_files(
    serialized_input_ready_files_no_normalization: Optional[List[str]],
    serialized_pre_import_normalization_result: Optional[str],
) -> List[List[str]]:
    """Combines import ready file objects from pre-import normalization and files that
    did not need pre-import normalization into equally sized batches.
    """
    if serialized_input_ready_files_no_normalization is None:
        raise AirflowSkipException(
            "Import ready files that dont require pre-import normalization is None; skipping as we cannot proceed!"
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
        *serialized_input_ready_files_no_normalization,
        *[result.serialize() for result in pre_import_normalization_results],
    ]

    if not all_import_ready_files:
        return []

    num_batches = min(len(all_import_ready_files), MAX_BQ_LOAD_JOBS)

    return [list(batch) for batch in distribute(num_batches, all_import_ready_files)]


@task(trigger_rule=TriggerRule.ALL_DONE)
def coalesce_results_and_errors(
    region_code: str,
    raw_data_instance: DirectIngestInstance,
    serialized_bq_metadata: List[str],
    serialized_divide_files_into_chunks: List[str],
    serialized_pre_import_normalization_result: str,
    serialized_load_prep_results: List[str],
    serialized_append_batches: Dict[str, List[str]],
    serialized_append_result: List[str],
) -> Dict[str, List[str]]:
    """Reconciles RawBigQueryFileMetadata objects against the results and errors
    of the pre-import normalization and big query load steps, returning a dictionary
    with four keys:
        - file_imports: a list of RawFileImport objects to be persisted to the
            operations database
        - processed_paths_to_rename: a list of GcsfsFilePath objects that need their
            processed state renamed from unprocessed to processed after successfully
            being imported
        - temporary_paths_to_clean: a list of GcsfsFilePath objects that need to be
            deleted
        - temporary_tables_to_clean: a list of BigQueryAddress objects that need to be
            deleted
    """

    (
        bq_metadata,
        import_results,
        import_errors,
    ) = _deserialize_coalesce_results_and_errors_inputs(
        serialized_bq_metadata,
        serialized_divide_files_into_chunks,
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

    (
        failed_file_imports_for_file_id,
        temporary_file_paths_to_clean,
        temporary_tables_to_clean,
    ) = _build_file_imports_for_errors(
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
        PROCESSED_PATHS_TO_RENAME: [
            path.abs_path() for path in successfully_imported_paths
        ],
        TEMPORARY_PATHS_TO_CLEAN: [
            path.abs_path() for path in temporary_file_paths_to_clean
        ],
        TEMPORARY_TABLES_TO_CLEAN: [
            address.to_str() for address in temporary_tables_to_clean
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
                )
            )

    return missing_file_ids


# TODO(#12390): Delete once raw data pruning is live.
def _historical_diffs_active_for_tag(
    raw_region_config: DirectIngestRegionRawFileConfig,
    raw_data_instance: DirectIngestInstance,
    file_tag: str,
) -> bool:
    """Boolean return for if the raw file_tag will have historical diffs active"""
    raw_data_pruning_enabled = raw_data_pruning_enabled_in_state_and_instance(
        StateCode(raw_region_config.region_code.upper()), raw_data_instance
    )
    if not raw_data_pruning_enabled:
        return False

    return not raw_region_config.raw_file_configs[
        file_tag
    ].is_exempt_from_raw_data_pruning()


def _build_file_imports_for_errors(
    raw_region_config: DirectIngestRegionRawFileConfig,
    raw_data_instance: DirectIngestInstance,
    bq_metadata: List[RawBigQueryFileMetadata],
    processing_errors: List[RawFileProcessingError],
    load_and_prep_errors: List[RawFileLoadAndPrepError],
    append_errors: List[RawDataAppendImportError],
) -> Tuple[Dict[int, RawFileImport], List[GcsfsFilePath], List[BigQueryAddress],]:
    """Builds RawFileImport objects for all errors seen during the import process,
    additionally returning temporary file paths and temporary big query tables that we
    want to make sure are cleaned up
    """

    failed_file_imports: Dict[int, RawFileImport] = {}
    temporary_file_paths_to_clean: List[GcsfsFilePath] = []
    temporary_tables_to_clean: List[BigQueryAddress] = []

    # --- simple case: append_errors and load_and_prep_errors have file_id -------------

    for append_error in append_errors:
        failed_file_imports[append_error.file_id] = RawFileImport(
            file_id=append_error.file_id,
            import_status=append_error.error_type,
            historical_diffs_active=_historical_diffs_active_for_tag(
                raw_region_config, raw_data_instance, append_error.file_tag
            ),
        )
        temporary_tables_to_clean.append(append_error.raw_temp_table)

    for load_and_prep_error in load_and_prep_errors:
        failed_file_imports[load_and_prep_error.file_id] = RawFileImport(
            file_id=load_and_prep_error.file_id,
            import_status=load_and_prep_error.error_type,
            historical_diffs_active=_historical_diffs_active_for_tag(
                raw_region_config, raw_data_instance, load_and_prep_error.file_tag
            ),
        )
        if load_and_prep_error.pre_import_normalized_file_paths:
            temporary_file_paths_to_clean.extend(
                load_and_prep_error.pre_import_normalized_file_paths
            )
        if load_and_prep_error.temp_table:
            temporary_tables_to_clean.append(load_and_prep_error.temp_table)

    # --- complex case: map file path back to file_id ----------------------------------

    path_to_bq_metadata: Dict[GcsfsFilePath, RawBigQueryFileMetadata] = {
        gcs_metadata.path: metadata
        for metadata in bq_metadata
        for gcs_metadata in metadata.gcs_files
    }

    for processing_error in processing_errors:
        bq_metadata_for_error = path_to_bq_metadata[processing_error.original_file_path]
        file_id = assert_type(bq_metadata_for_error.file_id, int)
        failed_file_imports[file_id] = RawFileImport(
            file_id=file_id,
            import_status=processing_error.error_type,
            historical_diffs_active=_historical_diffs_active_for_tag(
                raw_region_config,
                raw_data_instance,
                processing_error.file_tag,
            ),
        )
        if processing_error.temporary_file_paths:
            temporary_file_paths_to_clean.extend(processing_error.temporary_file_paths)

    return (
        failed_file_imports,
        temporary_file_paths_to_clean,
        temporary_tables_to_clean,
    )


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
    serialized_divide_files_into_chunks: Optional[List[str]],
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
    """Deserialize and group inputs of coalesce_results_and_errors.

    If |serialized_bq_metadata| is empty, we don't have the starting list of files
    that we intended to import, so we will assume this is an empty branch.
    """
    if serialized_bq_metadata is None:
        raise AirflowSkipException(
            "Found no bq metadata, so assuming that this is an empty branch"
        )

    bq_metadata = [
        RawBigQueryFileMetadata.deserialize(serialized_bq_metadata)
        for serialized_bq_metadata in serialized_bq_metadata
    ]

    divide_files_step_output = MappedBatchedTaskOutput.deserialize(
        serialized_divide_files_into_chunks or [],
        result_cls=RequiresPreImportNormalizationFile,
        error_cls=RawFileProcessingError,
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
                *divide_files_step_output.flatten_errors(),
                *pre_import_normalization_step_errors,
            ],
            [
                *load_and_prep_step_output.flatten_errors(),
                *append_batches_skipped_file_errors,
            ],
            append_step_output.flatten_errors(),
        ),
    )
