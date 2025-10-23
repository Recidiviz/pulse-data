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
"""DAG configuration to run raw data imports"""
from typing import List

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.monitoring.dag_registry import get_raw_data_import_dag_id
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.operators.raw_data.direct_ingest_list_files_operator import (
    DirectIngestListNormalizedUnprocessedFilesOperator,
)
from recidiviz.airflow.dags.raw_data.acquire_resource_lock_sql_query_generator import (
    AcquireRawDataResourceLockSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.bq_load_tasks import (
    append_ready_file_batches_from_generate_append_batches,
    append_to_raw_data_table_for_batch,
    generate_append_batches,
    load_and_prep_paths_for_batch,
    raise_append_errors,
    raise_load_prep_errors,
)
from recidiviz.airflow.dags.raw_data.clean_up_tasks import (
    move_successfully_imported_paths_to_storage,
)
from recidiviz.airflow.dags.raw_data.concurrency_utils import (
    CHUNKING_MAX_CONCURRENT_TASKS,
    DEFAULT_CHUNKING_NUM_TASKS,
    DEFAULT_NORMALIZATION_NUM_TASKS,
    MAX_CHUNKS_PER_CHUNKING_TASK,
    MAX_FILE_CHUNKS_PER_NORMALIZATION_TASK,
    NORMALIZATION_MAX_CONCURRENT_TASKS,
)
from recidiviz.airflow.dags.raw_data.file_metadata_tasks import (
    coalesce_import_ready_files,
    coalesce_results_and_errors,
    get_files_to_import_this_run,
    raise_operations_registration_errors,
    split_by_pre_import_normalization_type,
)
from recidiviz.airflow.dags.raw_data.filtering_tasks import (
    filter_chunking_results_by_errors,
)
from recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks import (
    generate_chunk_processing_pod_arguments,
    generate_file_chunking_pod_arguments,
    raise_chunk_normalization_errors,
    raise_file_chunking_errors,
    raise_header_verification_errors,
    read_and_verify_column_headers,
    regroup_and_verify_file_chunks,
)
from recidiviz.airflow.dags.raw_data.get_all_unprocessed_bq_file_metadata_sql_query_generator import (
    GetAllUnprocessedBQFileMetadataSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.get_all_unprocessed_gcs_file_metadata_sql_query_generator import (
    GetAllUnprocessedGCSFileMetadataSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.initialize_raw_data_dag_group import (
    initialize_raw_data_dag_group,
)
from recidiviz.airflow.dags.raw_data.metadata import (
    BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS,
    BQ_METADATA_TO_IMPORT_THIS_RUN,
    CHUNKING_ERRORS,
    CHUNKING_RESULTS,
    FILE_IDS_TO_HEADERS,
    HAS_FILE_IMPORT_ERRORS,
    HEADER_VERIFICATION_ERRORS,
    IMPORT_READY_FILES,
    PROCESSED_PATHS_TO_RENAME,
    RAW_DATA_BRANCHING,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA,
    REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_SCHEMA,
    RESOURCE_LOCK_ACQUISITION_DESCRIPTION,
    RESOURCE_LOCKS_NEEDED,
    SKIPPED_FILE_ERRORS,
    get_resource_lock_ttl,
)
from recidiviz.airflow.dags.raw_data.raw_data_branching import (
    create_raw_data_branch_map,
    get_raw_data_branch_filter,
    get_raw_data_import_branch_key,
)
from recidiviz.airflow.dags.raw_data.release_resource_lock_sql_query_generator import (
    ReleaseRawDataResourceLockSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.sequencing_tasks import (
    has_files_to_import,
    maybe_trigger_dag_rerun,
    successfully_acquired_all_locks,
)
from recidiviz.airflow.dags.raw_data.verify_big_query_postgres_alignment_sql_query_generator import (
    VerifyBigQueryPostgresAlignmentSQLQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.verify_raw_data_pruning_metadata_sql_query_generator import (
    VerifyRawDataPruningMetadataSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.write_file_import_start_sql_query_generator import (
    WriteImportStartCloudSqlGenerator,
)
from recidiviz.airflow.dags.raw_data.write_file_processed_time_to_bq_file_metadata_sql_query_generator import (
    WriteFileProcessedTimeToBQFileMetadataSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.write_import_completions_query_generator import (
    WriteImportCompletionsSqlQueryGenerator,
)
from recidiviz.airflow.dags.utils.branching_by_key import (
    DAGNode,
    create_branching_by_key,
)
from recidiviz.airflow.dags.utils.cloud_sql import cloud_sql_conn_id_for_schema_type
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.airflow.dags.utils.kubernetes_pod_operator_task_groups import (
    kubernetes_pod_operator_mapped_task_with_output,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a "disable expression-not-assigned" because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


def create_single_state_code_ingest_instance_raw_data_import_branch(
    state_code: StateCode,
    raw_data_instance: DirectIngestInstance,
) -> List[DAGNode]:
    """Given a |state_code| and |raw_data_instance|, creates a task group that
    executes the necessary steps to import all relevant files in the ingest bucket into
    BigQuery.
    """
    with TaskGroup(
        get_raw_data_import_branch_key(state_code, raw_data_instance)
    ) as raw_data_branch:

        # --- step 1: resource lock & file discovery -----------------------------------
        # inputs: (state_code, raw_data_instance)
        # execution layer: celery
        # outputs: [ GcsfsFilePath ]

        operations_cloud_sql_conn_id = cloud_sql_conn_id_for_schema_type(
            SchemaType.OPERATIONS
        )

        acquire_locks = CloudSqlQueryOperator(
            task_id="acquire_raw_data_resource_locks",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=AcquireRawDataResourceLockSqlQueryGenerator(
                region_code=state_code.value,
                raw_data_instance=raw_data_instance,
                resources=RESOURCE_LOCKS_NEEDED,
                lock_description=RESOURCE_LOCK_ACQUISITION_DESCRIPTION,
                lock_ttl_seconds=get_resource_lock_ttl(raw_data_instance),
            ),
        )

        # ensure_locks_acquired is upstream of (set further down in the dag):
        #   - list_normalized_unprocessed_gcs_file_paths
        #   - serialized_import_ready_files
        #   - clean_and_storage_jobs
        #   - ensure_release_resource_locks_release_if_acquired
        # TriggerRule is ALL_DONE
        ensure_locks_acquired = successfully_acquired_all_locks(
            maybe_serialized_acquired_locks=acquire_locks.output,
            resources_needed=RESOURCE_LOCKS_NEEDED,
        )

        # Lists all unprocessed files for the given state and instance
        # that we received through SFTP or direct upload.
        list_normalized_unprocessed_gcs_file_paths = (
            DirectIngestListNormalizedUnprocessedFilesOperator(
                task_id="list_normalized_unprocessed_gcs_file_paths",
                bucket=gcsfs_direct_ingest_bucket_for_state(
                    project_id=get_project_id(),
                    region_code=state_code.value,
                    ingest_instance=raw_data_instance,
                ).bucket_name,
            )
        )

        acquire_locks >> [
            ensure_locks_acquired,
            list_normalized_unprocessed_gcs_file_paths,
        ]

        # ------------------------------------------------------------------------------

        # --- step 2: processing logic & metadata management ---------------------------
        # inputs: [ GcsfsFilePath ]
        # execution layer: celery
        # outputs: [ ImportReadyFile ], [ RequiresPreImportNormalizationFile ]

        get_all_unprocessed_gcs_file_metadata = CloudSqlQueryOperator(
            task_id="get_all_unprocessed_gcs_file_metadata",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=GetAllUnprocessedGCSFileMetadataSqlQueryGenerator(
                region_code=state_code.value,
                raw_data_instance=raw_data_instance,
                list_normalized_unprocessed_gcs_file_paths_task_id=list_normalized_unprocessed_gcs_file_paths.task_id,
            ),
        )

        get_all_unprocessed_bq_file_metadata = CloudSqlQueryOperator(
            task_id="get_all_unprocessed_bq_file_metadata",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=GetAllUnprocessedBQFileMetadataSqlQueryGenerator(
                region_code=state_code.value,
                raw_data_instance=raw_data_instance,
                get_all_unprocessed_gcs_file_metadata_task_id=get_all_unprocessed_gcs_file_metadata.task_id,
            ),
        )

        get_all_unprocessed_bq_file_metadata_with_congruous_raw_data = CloudSqlQueryOperator(
            task_id="verify_big_query_postgres_alignment",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=VerifyBigQueryPostgresAlignmentSQLQueryGenerator(
                state_code=state_code,
                raw_data_instance=raw_data_instance,
                project_id=get_project_id(),
                get_all_unprocessed_bq_file_metadata_task_id=get_all_unprocessed_bq_file_metadata.task_id,
            ),
        )

        get_all_unprocessed_bq_file_metadata_with_valid_pruning_config = CloudSqlQueryOperator(
            task_id="verify_raw_data_pruning_metadata",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=VerifyRawDataPruningMetadataSqlQueryGenerator(
                state_code=state_code,
                raw_data_instance=raw_data_instance,
                verify_big_query_postgres_alignment_task_id=get_all_unprocessed_bq_file_metadata_with_congruous_raw_data.task_id,
            ),
        )
        # should_run_import is upstream of (set further down in the dag):
        #   - files_to_import_this_run
        #   - serialized_import_ready_files
        #   - write_import_completions
        should_run_import = has_files_to_import(
            get_all_unprocessed_bq_file_metadata_with_valid_pruning_config.output
        )

        skipped_file_errors = raise_operations_registration_errors(
            serialized_bq_metadata_skipped_file_errors=get_all_unprocessed_bq_file_metadata.output[
                SKIPPED_FILE_ERRORS
            ],
            serialized_bq_postgres_alignment_skipped_file_errors=get_all_unprocessed_bq_file_metadata_with_congruous_raw_data.output[
                SKIPPED_FILE_ERRORS
            ],
            serialized_pruning_metadata_skipped_file_errors=get_all_unprocessed_bq_file_metadata_with_valid_pruning_config.output[
                SKIPPED_FILE_ERRORS
            ],
        )

        # here, we bifurcate between files we ARE importing and files we are deferring
        files_to_import_this_run = get_files_to_import_this_run(
            raw_data_instance=raw_data_instance,
            serialized_bq_metadata=get_all_unprocessed_bq_file_metadata_with_valid_pruning_config.output,
        )

        get_all_unprocessed_bq_file_metadata_with_valid_pruning_config >> [
            should_run_import,
            skipped_file_errors,
            files_to_import_this_run,
        ]

        write_import_start = CloudSqlQueryOperator(
            task_id="write_import_start",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=WriteImportStartCloudSqlGenerator(
                region_code=state_code.value,
                raw_data_instance=raw_data_instance,
                files_to_import_this_run_task_id=files_to_import_this_run.operator.task_id,
            ),
        )

        file_headers = read_and_verify_column_headers(
            region_code=state_code.value,
            serialized_bq_metadata=files_to_import_this_run[
                BQ_METADATA_TO_IMPORT_THIS_RUN
            ],
        )

        header_errors = raise_header_verification_errors(
            header_verification_errors=file_headers[HEADER_VERIFICATION_ERRORS]
        )

        files_to_process = split_by_pre_import_normalization_type(
            region_code=state_code.value,
            serialized_bq_metadata=files_to_import_this_run[
                BQ_METADATA_TO_IMPORT_THIS_RUN
            ],
            file_ids_to_headers=file_headers[FILE_IDS_TO_HEADERS],
        )

        # sequencing if we have files to import
        (
            list_normalized_unprocessed_gcs_file_paths
            >> get_all_unprocessed_gcs_file_metadata
            >> get_all_unprocessed_bq_file_metadata
            >> get_all_unprocessed_bq_file_metadata_with_congruous_raw_data
            >> get_all_unprocessed_bq_file_metadata_with_valid_pruning_config
            >> files_to_import_this_run
            >> write_import_start
            >> file_headers
            >> [
                header_errors,
                files_to_process,
            ]
        )

        # ------------------------------------------------------------------------------

        # --- step 3: pre-import normalization -----------------------------------------
        # inputs: [ RequiresNormalizationFile ]
        # execution layer: k8s
        # outputs: [ ImportReadyFile ]

        with TaskGroup("pre_import_normalization") as pre_import_normalization:
            chunking_output = kubernetes_pod_operator_mapped_task_with_output(
                task_id="raw_data_file_chunking",
                expand_arguments=generate_file_chunking_pod_arguments(
                    region_code=state_code.value,
                    serialized_requires_pre_import_normalization_file_paths=files_to_process[
                        REQUIRES_PRE_IMPORT_NORMALIZATION_FILES
                    ],
                    target_num_chunking_airflow_tasks=DEFAULT_CHUNKING_NUM_TASKS,
                    max_chunks_per_airflow_task=MAX_CHUNKS_PER_CHUNKING_TASK,
                ),
                max_active_tis_per_dag=CHUNKING_MAX_CONCURRENT_TASKS,
            )

            filtered_chunks = filter_chunking_results_by_errors(chunking_output)

            raise_file_chunking_errors(filtered_chunks[CHUNKING_ERRORS])

            normalization_output = kubernetes_pod_operator_mapped_task_with_output(
                task_id="raw_data_chunk_normalization",
                expand_arguments=generate_chunk_processing_pod_arguments(
                    region_code=state_code.value,
                    serialized_pre_import_files=filtered_chunks[CHUNKING_RESULTS],
                    target_num_normalization_airflow_tasks=DEFAULT_NORMALIZATION_NUM_TASKS,
                    max_file_chunks_per_airflow_task=MAX_FILE_CHUNKS_PER_NORMALIZATION_TASK,
                ),
                max_active_tis_per_dag=NORMALIZATION_MAX_CONCURRENT_TASKS,
            )

            pre_import_normalization_result = regroup_and_verify_file_chunks(
                normalization_output,
                files_to_process[REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_METADATA],
                files_to_process[REQUIRES_PRE_IMPORT_NORMALIZATION_FILES_BQ_SCHEMA],
            )
            raise_chunk_normalization_errors(pre_import_normalization_result)

        files_to_process >> pre_import_normalization

        # ------------------------------------------------------------------------------

        # --- step 4: big-query upload -------------------------------------------------
        # inputs: [ ImportReadyFile ]
        # execution layer: celery
        # outputs: [ AppendReadyFile ], [ AppendSummary ]

        serialized_import_ready_files = coalesce_import_ready_files(
            files_to_process[IMPORT_READY_FILES],
            pre_import_normalization_result,
        )

        with TaskGroup("big_query_load") as big_query_load:
            # load paths into temp table
            load_and_prep_results = load_and_prep_paths_for_batch.partial(
                raw_data_instance=raw_data_instance,
                region_code=state_code.value,
                import_run_id_dict=write_import_start.output,
            ).expand(serialized_import_ready_files=serialized_import_ready_files)

            # batch tasks for next step and raise errors
            append_batches_output = generate_append_batches(load_and_prep_results)

            raise_load_prep_errors(
                load_and_prep_results, append_batches_output[SKIPPED_FILE_ERRORS]
            )

            # append temp tables to raw data table
            append_results = append_to_raw_data_table_for_batch.partial(
                raw_data_instance=raw_data_instance, region_code=state_code.value
            ).expand(
                serialized_append_ready_file_batch=append_ready_file_batches_from_generate_append_batches(
                    append_batches_output
                )
            )

            raise_append_errors(append_results)

        pre_import_normalization >> serialized_import_ready_files >> big_query_load

        # ------------------------------------------------------------------------------

        # --- step 5: cleanup & storage ------------------------------------------------
        # inputs: [ AppendReadyFile ], [ AppendSummary ], [ RawBigQueryFileMetadataSummary ]
        # execution layer: celery
        # outputs:

        with TaskGroup("cleanup_and_storage") as cleanup_and_storage:

            # trigger rule is ALL_DONE
            clean_and_storage_jobs = coalesce_results_and_errors(
                region_code=state_code.value,
                raw_data_instance=raw_data_instance,
                serialized_bq_metadata=files_to_import_this_run[
                    BQ_METADATA_TO_IMPORT_THIS_RUN
                ],
                serialized_header_verification_errors=file_headers[
                    HEADER_VERIFICATION_ERRORS
                ],
                serialized_chunking_errors=filtered_chunks[CHUNKING_ERRORS],
                serialized_pre_import_normalization_result=pre_import_normalization_result,
                serialized_load_prep_results=load_and_prep_results,
                serialized_append_batches=append_batches_output,
                serialized_append_result=append_results,
            )

            write_import_completions = CloudSqlQueryOperator(
                task_id="write_import_completions",
                cloud_sql_conn_id=operations_cloud_sql_conn_id,
                query_generator=WriteImportCompletionsSqlQueryGenerator(
                    region_code=state_code.value,
                    raw_data_instance=raw_data_instance,
                    coalesce_results_and_errors_task_id=clean_and_storage_jobs.operator.task_id,
                    write_import_start_task_id=write_import_start.task_id,
                ),
            )

            write_file_processed_times = CloudSqlQueryOperator(
                task_id="write_file_processed_time",
                cloud_sql_conn_id=operations_cloud_sql_conn_id,
                query_generator=WriteFileProcessedTimeToBQFileMetadataSqlQueryGenerator(
                    write_import_run_task_id=write_import_completions.task_id
                ),
            )

            # Moves successfully imported files from the "ingest bucket"
            # to the "storage bucket" for the given state and instance.
            # Recall that each state and instance has an "ingest bucket"
            # while each instance has a "storage bucket" with a subdir
            # for each state.
            renamed_imported_paths = move_successfully_imported_paths_to_storage(
                state_code.value,
                raw_data_instance,
                clean_and_storage_jobs[PROCESSED_PATHS_TO_RENAME],
            )

            clean_and_storage_jobs >> [
                write_import_completions,
                renamed_imported_paths,
            ]

            write_import_start >> write_import_completions

            write_import_completions >> write_file_processed_times

        [
            big_query_load,
            header_errors,
            skipped_file_errors,
        ] >> cleanup_and_storage

        ensure_release_resource_locks_release_if_acquired = EmptyOperator(
            task_id="ensure_release_resource_locks_release_if_acquired",
            trigger_rule=TriggerRule.ALL_DONE,
        )

        release_locks = CloudSqlQueryOperator(
            task_id="release_raw_data_resource_locks",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=ReleaseRawDataResourceLockSqlQueryGenerator(
                region_code=state_code.value,
                raw_data_instance=raw_data_instance,
                acquire_resource_lock_task_id=acquire_locks.task_id,
            ),
            trigger_rule=TriggerRule.NONE_SKIPPED,
        )

        cleanup_and_storage >> ensure_release_resource_locks_release_if_acquired

        [
            ensure_release_resource_locks_release_if_acquired,
            acquire_locks,
        ] >> release_locks

        # if we didn't have files to import, let's cascade our skip down through ALL_SUCCESS
        # trigger rules and have the short circuit override (skip without respecting) the ALL_DONE trigger rules
        should_run_import >> [
            files_to_import_this_run,  # if we short circuit, will cascade skip through ALL_SUCCESS trigger rules
            serialized_import_ready_files,  # if we short circuit, will skip task despite ALL_DONE trigger rule
            write_import_completions,  # if we short circuit, will skip task despite ALL_DONE trigger rule of clean_and_storage_jobs
        ]

        # if we didn't acquire resource locks, let's cascade our skip down through ALL_SUCCESS
        # and have the short circuit override (skip without respecting) the ALL_DONE trigger rules
        ensure_locks_acquired >> [
            list_normalized_unprocessed_gcs_file_paths,  # if we short circuit, will cascade skip through ALL_SUCCESS trigger rules
            serialized_import_ready_files,  # if we short circuit, will skip task despite ALL_DONE trigger rule
            clean_and_storage_jobs,  # if we short circuit, will skip task despite ALL_DONE trigger rule
            ensure_release_resource_locks_release_if_acquired,  # if we short circuit, will skip task despite ALL_DONE trigger rule
        ]

        # ------------------------------------------------------------------------------

        maybe_new_dag_rerun = maybe_trigger_dag_rerun(
            region_code=state_code.value,
            raw_data_instance=raw_data_instance,
            deferred_files=files_to_import_this_run[
                BQ_METADATA_TO_IMPORT_IN_FUTURE_RUNS
            ],
            has_file_import_errors=clean_and_storage_jobs[HAS_FILE_IMPORT_ERRORS],
        )

        release_locks >> maybe_new_dag_rerun

    return [
        raw_data_branch,
        ensure_release_resource_locks_release_if_acquired,
        serialized_import_ready_files,
        ensure_locks_acquired,
        clean_and_storage_jobs,
    ]


@dag(
    dag_id=get_raw_data_import_dag_id(get_project_id()),
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def create_raw_data_import_dag() -> None:
    """DAG configuration to run raw data imports"""

    # --- step 0: pipeline initialization ---------------------------------------------
    # inputs: dag parameters
    # execution layer: celery
    # outputs: selects pipeline branches for each selected (state_code, ingest_instance)
    # pair.

    # branches are created for all launched states for both primary and secondary.
    # by default, we run w/ only primary branches selected. when state code and ingest
    # instance filters are both applied, only the specified branch will be selected

    with TaskGroup(RAW_DATA_BRANCHING) as raw_data_branching:
        create_branching_by_key(
            create_raw_data_branch_map(
                create_single_state_code_ingest_instance_raw_data_import_branch
            ),
            get_raw_data_branch_filter,
        )

    initialize_raw_data_dag_group() >> raw_data_branching

    # ---------------------------------------------------------------------------------


raw_data_import_dag = create_raw_data_import_dag()
