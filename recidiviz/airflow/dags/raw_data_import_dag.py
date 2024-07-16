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
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.monitoring.dag_registry import get_raw_data_import_dag_id
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.operators.raw_data.direct_ingest_list_files_operator import (
    DirectIngestListNormalizedUnprocessedFilesOperator,
)
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    RecidivizKubernetesPodOperator,
    get_kubernetes_pod_kwargs,
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
from recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks import (
    generate_chunk_processing_pod_arguments,
    generate_file_chunking_pod_arguments,
    raise_chunk_normalization_errors,
    raise_file_chunking_errors,
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
    RESOURCE_LOCK_AQUISITION_DESCRIPTION,
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
from recidiviz.airflow.dags.utils.branching_by_key import create_branching_by_key
from recidiviz.airflow.dags.utils.cloud_sql import cloud_sql_conn_id_for_schema_type
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.environment import get_project_id
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

NUM_BATCHES = 5  # TODO(#29946) determine reasonable default


def create_single_state_code_ingest_instance_raw_data_import_branch(
    state_code: StateCode,
    raw_data_instance: DirectIngestInstance,
) -> TaskGroup:
    """Given a |state_code| and |raw_data_instance|, will create a task group that
    executes the necessary steps to import all relevant files in the ingest bucket into
    BigQuery.
    """
    with TaskGroup(
        get_raw_data_import_branch_key(state_code.value, raw_data_instance.value)
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
                lock_description=RESOURCE_LOCK_AQUISITION_DESCRIPTION,
                lock_ttl_seconds=get_resource_lock_ttl(raw_data_instance),
            ),
        )

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

        acquire_locks >> list_normalized_unprocessed_gcs_file_paths

        # ------------------------------------------------------------------------------

        # --- step 2: processing logic & metadata management ---------------------------
        # inputs: [ GcsfsFilePath ]
        # execution layer: celery
        # outputs: [ ImportReadyOriginalFile ], [ RequiresPreImportNormalizationFile ]

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

        # TODO(#30170) implement splitting into RequiresNormalizationFile, ImportReadyOriginalFile

        # ------------------------------------------------------------------------------

        # --- step 3: pre-import normalization -----------------------------------------
        # inputs: [ RequiresNormalizationFile ]
        # execution layer: k8s
        # outputs: [ ImportReadyNormalizedFile ]

        serialized_requires_normalization_files: List[str] = []
        with TaskGroup("pre_import_normalization") as pre_import_normalization:
            divide_files_into_chunks = RecidivizKubernetesPodOperator.partial(
                **get_kubernetes_pod_kwargs(
                    task_id="raw_data_file_chunking",
                    do_xcom_push=True,
                )
            ).expand(
                arguments=generate_file_chunking_pod_arguments(
                    state_code.value,
                    serialized_requires_normalization_files,
                    num_batches=NUM_BATCHES,
                )
            )
            raise_file_chunking_errors(
                divide_files_into_chunks.output,
            )
            normalized_chunks = RecidivizKubernetesPodOperator.partial(
                **get_kubernetes_pod_kwargs(
                    task_id="raw_data_chunk_normalization",
                    do_xcom_push=True,
                )
            ).expand(
                arguments=generate_chunk_processing_pod_arguments(
                    state_code.value,
                    file_chunks=divide_files_into_chunks.output,
                    num_batches=NUM_BATCHES,
                )
            )
            verify_file_results = regroup_and_verify_file_chunks(
                normalized_chunks.output
            )
            raise_chunk_normalization_errors(verify_file_results)

        (
            list_normalized_unprocessed_gcs_file_paths
            >> get_all_unprocessed_gcs_file_metadata
            >> get_all_unprocessed_bq_file_metadata
            >> pre_import_normalization
        )

        # ------------------------------------------------------------------------------

        # --- step 4: big-query upload -------------------------------------------------
        # inputs: [ *[ ImportReadyOriginalFile ], *[ ImportReadyNormalizedFile ] ]
        # execution layer: celery
        # outputs: [ ImportSessionInfo ]

        # TODO(#30168) implement coalesce of results from steps above

        with TaskGroup("biq_query_load") as big_query_load:
            all_files: List[str] = []

            # load paths into temp table
            load_and_prep_results = load_and_prep_paths_for_batch.partial(
                raw_data_instance=raw_data_instance, region_code=state_code.value
            ).expand(serialized_import_ready_files=all_files)

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

        pre_import_normalization >> big_query_load

        # ------------------------------------------------------------------------------

        # --- step 5: cleanup & storage ----------------------------------------------
        # inputs: [ ImportSessionInfo ], [ RequiresCleanupFile ]
        # execution layer: celery
        # outputs:

        # TODO(#30169) implement writes to file metadata & import sessions, as well as
        # file cleanup

        release_locks = CloudSqlQueryOperator(
            task_id="release_raw_data_resource_locks",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=ReleaseRawDataResourceLockSqlQueryGenerator(
                region_code=state_code.value,
                raw_data_instance=raw_data_instance,
                acquire_resource_lock_task_id=acquire_locks.task_id,
            ),
        )

        big_query_load >> release_locks

        # ------------------------------------------------------------------------------

    return raw_data_branch


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

    # branches are created for all enabled states for both primary and secondary.
    # by default, we run w/ only primary branches selected. when state code and ingest
    # instance filters are both applied, only the specified branch will be selected

    with TaskGroup("raw_data_branching") as raw_data_branching:
        create_branching_by_key(
            create_raw_data_branch_map(
                create_single_state_code_ingest_instance_raw_data_import_branch
            ),
            get_raw_data_branch_filter,
        )

    initialize_raw_data_dag_group() >> raw_data_branching

    # ---------------------------------------------------------------------------------


raw_data_import_dag = create_raw_data_import_dag()
