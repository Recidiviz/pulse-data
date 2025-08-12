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
"""Airflow tasks for biq query load step of the raw data import dag"""
import concurrent.futures
import logging
import traceback
from collections import defaultdict
from itertools import groupby
from typing import Dict, List, Tuple

from airflow.decorators import task
from airflow.exceptions import AirflowException

from recidiviz.airflow.dags.raw_data.concurrency_utils import (
    MAX_BQ_APPEND_AIRFLOW_TASKS,
    MAX_BQ_APPEND_CLIENT_THREADS_PER_TASK,
    MAX_BQ_LOAD_CLIENT_THREADS_PER_TASK,
)
from recidiviz.airflow.dags.raw_data.metadata import (
    APPEND_READY_FILE_BATCHES,
    IMPORT_RUN_ID,
    SKIPPED_FILE_ERRORS,
)
from recidiviz.airflow.dags.raw_data.utils import (
    get_direct_ingest_region_raw_config,
    n_evenly_weighted_buckets,
)
from recidiviz.airflow.dags.utils.constants import (
    RAISE_APPEND_ERRORS,
    RAISE_LOAD_PREP_ERRORS,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_resources.platform_resource_labels import (
    IngestInstanceResourceLabel,
    PlatformOrchestrationResourceLabel,
    StateCodeResourceLabel,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_load_manager import (
    DirectIngestRawFileLoadManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationError,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    AppendReadyFile,
    AppendReadyFileBatch,
    AppendSummary,
    ImportReadyFile,
    RawDataAppendImportError,
    RawDataImportError,
    RawFileLoadAndPrepError,
)
from recidiviz.utils.airflow_types import (
    BatchedTaskInstanceOutput,
    MappedBatchedTaskOutput,
)


@task
def load_and_prep_paths_for_batch(
    region_code: str,
    raw_data_instance: DirectIngestInstance,
    import_run_id_dict: dict[str, int],
    serialized_import_ready_files: List[str],
) -> str:
    """Given a batch of |serialized_import_ready_files|, asynchronously loads
    each file in parallel into it's own table, applying pre-migration
    transformations and raw data migrations.

    Batches files together within a single process to restrain parallelism across
    shared airflow workers; see go/raw-data-batches for more detailed info.

    The |import_run_id_dict| arg is the serialized xcom output of the "write_import_start"
    task.
    """

    import_ready_files = [
        ImportReadyFile.deserialize(serialized_file)
        for serialized_file in serialized_import_ready_files
    ]

    if not import_run_id_dict or not isinstance(
        import_run_id := import_run_id_dict.get(IMPORT_RUN_ID), int
    ):
        raise ValueError(
            f"Could not retrieve import_run_id from upstream; found: [{import_run_id_dict}]"
        )

    fs = GcsfsFactory.build()
    bq_client = BigQueryClientImpl(
        default_job_labels=[
            StateCodeResourceLabel(value=region_code.lower()),
            IngestInstanceResourceLabel(value=raw_data_instance.value.lower()),
            PlatformOrchestrationResourceLabel.RAW_DATA_IMPORT_DAG.value,
        ]
    )

    manager = DirectIngestRawFileLoadManager(
        raw_data_instance=raw_data_instance,
        region_raw_file_config=get_direct_ingest_region_raw_config(
            region_code=region_code
        ),
        fs=fs,
        big_query_client=bq_client,
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_BQ_LOAD_CLIENT_THREADS_PER_TASK
    ) as executor:
        future_to_metadata = {
            executor.submit(
                manager.load_and_prep_paths,
                import_ready_file,
                temp_table_prefix=str(import_run_id),
            ): import_ready_file
            for import_ready_file in import_ready_files
        }

        succeeded_loads: List[AppendReadyFile] = []
        failed_loads: List[RawFileLoadAndPrepError] = []

        for future in concurrent.futures.as_completed(future_to_metadata):
            try:
                succeeded_loads.append(future.result())
            except RawDataImportBlockingValidationError as e:
                failed_loads.append(
                    RawFileLoadAndPrepError(
                        file_id=future_to_metadata[future].file_id,
                        original_file_paths=future_to_metadata[
                            future
                        ].original_file_paths,
                        pre_import_normalized_file_paths=future_to_metadata[
                            future
                        ].pre_import_normalized_file_paths,
                        file_tag=future_to_metadata[future].file_tag,
                        update_datetime=future_to_metadata[future].update_datetime,
                        error_msg=str(e),
                        temp_table=None,
                        error_type=DirectIngestRawFileImportStatus.FAILED_VALIDATION_STEP,
                    )
                )
            except Exception as e:
                failed_loads.append(
                    RawFileLoadAndPrepError(
                        file_id=future_to_metadata[future].file_id,
                        original_file_paths=future_to_metadata[
                            future
                        ].original_file_paths,
                        pre_import_normalized_file_paths=future_to_metadata[
                            future
                        ].pre_import_normalized_file_paths,
                        file_tag=future_to_metadata[future].file_tag,
                        update_datetime=future_to_metadata[future].update_datetime,
                        error_msg=f"{str(e)}\n{traceback.format_exc()}",
                        temp_table=None,
                    )
                )

    return BatchedTaskInstanceOutput[AppendReadyFile, RawFileLoadAndPrepError](
        results=succeeded_loads, errors=failed_loads
    ).serialize()


@task(task_id=RAISE_LOAD_PREP_ERRORS)
def raise_load_prep_errors(
    serialized_batched_task_instance_output: List[str],
    serialized_skipped_file_errors: List[str],
) -> None:
    """Combines and raises errors from load and prep step, as well as files that were
    skipped due to import-blocking failures as identified in generate_append_batches.
    """
    load_errors = MappedBatchedTaskOutput.deserialize(
        serialized_batched_task_instance_output,
        result_cls=AppendReadyFile,
        error_cls=RawFileLoadAndPrepError,
    ).flatten_errors()
    block_errors = [
        RawFileLoadAndPrepError.deserialize(error)
        for error in serialized_skipped_file_errors
    ]

    _raise_task_errors([*load_errors, *block_errors])


@task(multiple_outputs=True)
def generate_append_batches(
    serialized_batched_task_instance_output: List[str],
) -> Dict[str, List[str]]:
    """Given the collected results from all `load_and_prep_paths_batch` tasks in
    |serialized_batched_task_instance_output|
        1) Determines which files (data now stored in prepared form in temporary BQ
        tables) can proceed with the append step. Any failed file imports will
        "block" files with the same file_tag which have a more recent update_datetime
        value.
        2)  Groups all "append ready" files by file tag, then batches those groups into
        roughly equally sized buckets.

    Returns information about skipped files and the append-ready file batches.
    """
    load_step_results = MappedBatchedTaskOutput.deserialize(
        serialized_batched_task_instance_output,
        result_cls=AppendReadyFile,
        error_cls=RawFileLoadAndPrepError,
    )

    append_ready_files, skipped_file_errors = _filter_load_results_based_on_errors(
        load_step_results.flatten_results(), load_step_results.flatten_errors()
    )

    append_ready_files_by_file_tag: Dict[str, List[AppendReadyFile]] = defaultdict(
        list[AppendReadyFile]
    )
    for append_ready_file in append_ready_files:
        append_ready_files_by_file_tag[
            append_ready_file.import_ready_file.file_tag
        ].append(append_ready_file)

    append_ready_files_batches = n_evenly_weighted_buckets(
        [
            ((file_tag, append_ready_files_for_tag), len(append_ready_files_for_tag))
            for file_tag, append_ready_files_for_tag in append_ready_files_by_file_tag.items()
        ],
        MAX_BQ_APPEND_AIRFLOW_TASKS,
    )

    append_ready_files_by_file_tag_batches: List[Dict[str, List[AppendReadyFile]]] = [
        dict(append_ready_files_batch)
        for append_ready_files_batch in append_ready_files_batches
    ]

    return {
        SKIPPED_FILE_ERRORS: [skip.serialize() for skip in skipped_file_errors],
        APPEND_READY_FILE_BATCHES: [
            AppendReadyFileBatch(
                append_ready_files_by_tag=append_ready_files_by_tag
            ).serialize()
            for append_ready_files_by_tag in append_ready_files_by_file_tag_batches
        ],
    }


def _filter_load_results_based_on_errors(
    successful_loads: List[AppendReadyFile], failed_loads: List[RawFileLoadAndPrepError]
) -> Tuple[List[AppendReadyFile], List[RawFileLoadAndPrepError]]:
    """Filters |successful_loads| by removing all elements whose file_tag has an error
    in |failed_loads| with a older update_datetime.
    """

    append_ready_files: List[AppendReadyFile] = []
    skipped_files: List[RawFileLoadAndPrepError] = []

    # for all files w/in a file_tag that failed to properly import, let's determine the
    # oldest update_datetime so we can skip all files that have a more recent
    # update_datetime that import successfully
    blocking_failed_files_by_file_tag: Dict[str, RawFileLoadAndPrepError] = {
        file_tag: min(group, key=lambda x: x.update_datetime)
        for file_tag, group in groupby(
            sorted(failed_loads, key=lambda x: x.file_tag),
            lambda x: x.file_tag,
        )
    }

    for successful_load in successful_loads:
        if (
            successful_load.import_ready_file.file_tag
            in blocking_failed_files_by_file_tag
            and blocking_failed_files_by_file_tag[
                successful_load.import_ready_file.file_tag
            ].update_datetime
            < successful_load.import_ready_file.update_datetime
        ):
            logging.error(
                "Skipping append of [%s] due to import-blocking error",
                successful_load.append_ready_table_address.to_str(),
            )
            blocking_error = blocking_failed_files_by_file_tag[
                successful_load.import_ready_file.file_tag
            ]
            skipped_files.append(
                RawFileLoadAndPrepError(
                    file_id=successful_load.import_ready_file.file_id,
                    temp_table=successful_load.append_ready_table_address,
                    original_file_paths=successful_load.import_ready_file.original_file_paths,
                    pre_import_normalized_file_paths=successful_load.import_ready_file.pre_import_normalized_file_paths,
                    update_datetime=successful_load.import_ready_file.update_datetime,
                    file_tag=successful_load.import_ready_file.file_tag,
                    error_msg=f"Blocked Import: failed due to import-blocking failure from {blocking_error.original_file_paths}",
                    error_type=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                )
            )
        else:
            append_ready_files.append(successful_load)

    return append_ready_files, skipped_files


def _raise_task_errors(task_errors: List[RawDataImportError]) -> None:
    if task_errors:
        error_msg = "\n\n".join([str(error) for error in task_errors])
        raise AirflowException(f"Error(s) occurred in file processing:\n{error_msg}")


@task
def append_to_raw_data_table_for_batch(
    region_code: str,
    raw_data_instance: DirectIngestInstance,
    serialized_append_ready_file_batch: str,
) -> str:
    """Uses |serialized_append_ready_file_batch|, which contains a list of AppendReadyFile
    objects grouped by file_tag, to sequentially append data from temp tables into the
    corresponding raw data table, optionally applying historical diffs.

    For all failed file appends, all files with the same file_tag that have a more
    recent update_datetime will be skipped.

    Batches file tags together within a single process to restrain parallelism across
    shared airflow workers; see go/raw-data-batches for more detailed info.
    """

    append_ready_file_batch = AppendReadyFileBatch.deserialize(
        serialized_append_ready_file_batch
    )

    fs = GcsfsFactory.build()
    bq_client = BigQueryClientImpl(
        default_job_labels=[
            StateCodeResourceLabel(value=region_code.lower()),
            IngestInstanceResourceLabel(value=raw_data_instance.value.lower()),
            PlatformOrchestrationResourceLabel.RAW_DATA_IMPORT_DAG.value,
        ]
    )

    manager = DirectIngestRawFileLoadManager(
        raw_data_instance=raw_data_instance,
        region_raw_file_config=get_direct_ingest_region_raw_config(region_code),
        fs=fs,
        big_query_client=bq_client,
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_BQ_APPEND_CLIENT_THREADS_PER_TASK
    ) as executor:
        append_futures = [
            executor.submit(
                _append_to_raw_data_table_for_file_tag,
                manager,
                append_ready_files_for_tag,
            )
            for _, append_ready_files_for_tag in append_ready_file_batch.append_ready_files_by_tag.items()
        ]

        succeeded_loads = []
        failed_loads = []

        for future in concurrent.futures.as_completed(append_futures):
            batch_results = future.result()
            succeeded_loads.extend(batch_results.results)
            failed_loads.extend(batch_results.errors)

    return BatchedTaskInstanceOutput[AppendSummary, RawDataAppendImportError](
        results=succeeded_loads, errors=failed_loads
    ).serialize()


def _append_to_raw_data_table_for_file_tag(
    manager: DirectIngestRawFileLoadManager,
    append_ready_files_for_tag: List[AppendReadyFile],
) -> BatchedTaskInstanceOutput[AppendSummary, RawDataAppendImportError]:
    """Sequentially appends data from temp tables into the corresponding raw data table,
    optionally applying a historical diff to always historical exports. Data is appended
    in order of update_datetime, ascending.

    If a file fails to append, all subsequent files with more recent update_datetimes
    will be skipped and marked as failed.
    """

    results: List[AppendSummary] = []
    failures: List[RawDataAppendImportError] = []

    for append_ready_file in sorted(
        append_ready_files_for_tag, key=lambda x: x.import_ready_file.update_datetime
    ):
        if failures:
            logging.error(
                "Skipping import of [%s] due to import-blocking failure",
                append_ready_file.append_ready_table_address.to_str(),
            )
            failures.append(
                RawDataAppendImportError(
                    file_id=append_ready_file.import_ready_file.file_id,
                    raw_temp_table=append_ready_file.append_ready_table_address,
                    error_type=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                    error_msg=f"Blocked Import: failed due to import-blocking failure from {failures[0].raw_temp_table.to_str()}",
                )
            )
            continue
        try:
            results.append(manager.append_to_raw_data_table(append_ready_file))
        except Exception as e:
            failures.append(
                RawDataAppendImportError(
                    file_id=append_ready_file.import_ready_file.file_id,
                    raw_temp_table=append_ready_file.append_ready_table_address,
                    error_msg=f"{str(e)}\n{traceback.format_exc()}",
                )
            )

    return BatchedTaskInstanceOutput[AppendSummary, RawDataAppendImportError](
        results=results, errors=failures
    )


@task(task_id=RAISE_APPEND_ERRORS)
def raise_append_errors(
    append_tasks_output: List[str],
) -> None:
    """Raises errors from append_to_raw_data_table_for_region step"""
    append_errors = MappedBatchedTaskOutput.deserialize(
        append_tasks_output,
        result_cls=AppendSummary,
        error_cls=RawDataAppendImportError,
    ).flatten_errors()

    _raise_task_errors(append_errors)


@task
def append_ready_file_batches_from_generate_append_batches(
    append_batches_output: Dict,
) -> List[str]:
    return append_batches_output[APPEND_READY_FILE_BATCHES]
