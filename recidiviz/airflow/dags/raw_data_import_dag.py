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

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup

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
from recidiviz.airflow.dags.raw_data.initialize_raw_data_dag_group import (
    initialize_raw_data_dag_group,
)
from recidiviz.airflow.dags.raw_data.metadata import (
    RESOURCE_LOCK_AQUISITION_DESCRIPTION,
    RESOURCE_LOCKS_NEEDED,
    get_resource_lock_ttl,
)
from recidiviz.airflow.dags.raw_data.raw_data_branching import (
    create_raw_data_branch_map,
    get_raw_data_branch_filter,
    get_raw_data_import_branch_key,
)
from recidiviz.airflow.dags.raw_data.register_raw_gcs_file_metadata_sql_query_generator import (
    RegisterRawGCSFileMetadataSqlQueryGenerator,
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

        unprocessed_paths = DirectIngestListNormalizedUnprocessedFilesOperator(
            task_id="list_normalized_unprocessed_files",
            bucket=gcsfs_direct_ingest_bucket_for_state(
                project_id=get_project_id(),
                region_code=state_code.value,
                ingest_instance=raw_data_instance,
            ).bucket_name,
        )

        acquire_locks >> unprocessed_paths

        # ------------------------------------------------------------------------------

        # --- step 2: processing logic & metadata management ---------------------------
        # inputs: [ GcsfsFilePath ]
        # execution layer: celery
        # outputs: [ ImportReadyOriginalFile ], [ RequiresPreImportNormalizationFile ]

        register_gcs_file_metadata = CloudSqlQueryOperator(
            task_id="register_raw_gcs_file_metadata",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=RegisterRawGCSFileMetadataSqlQueryGenerator(
                region_code=state_code.value,
                raw_data_instance=raw_data_instance,
                list_unprocessed_files_task_id=unprocessed_paths.task_id,
            ),
        )

        # TODO(#30170) implement splitting into RequiresNormalizationFile, ImportReadyOriginalFile

        unprocessed_paths >> register_gcs_file_metadata

        # ------------------------------------------------------------------------------

        # --- step 3: pre-import normalization -----------------------------------------
        # inputs: [ RequiresNormalizationFile ]
        # execution layer: k8s
        # outputs: [ ImportReadyNormalizedFile ]

        # TODO(#29946) implement pre-import file boundary finder
        # TODO(#30167) implement pre-import normalization

        # ------------------------------------------------------------------------------

        # --- step 4: big-query upload -------------------------------------------------
        # inputs: [ *[ ImportReadyOriginalFile ], *[ ImportReadyNormalizedFile ] ]
        # execution layer: k8s
        # outputs: [ ImportSessionInfo ]

        # TODO(#30168) implement bq load step

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

        register_gcs_file_metadata >> release_locks

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
