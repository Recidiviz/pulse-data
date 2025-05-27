# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""The DAG configuration for downloading files from SFTP."""
import logging
from datetime import timedelta
from typing import List, Optional, Union

from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, ShortCircuitOperator
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.retry import Retry

from recidiviz.airflow.dags.monitoring.dag_registry import get_sftp_dag_id
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.operators.sftp.filter_invalid_gcs_files import (
    FilterInvalidGcsFilesOperator,
)
from recidiviz.airflow.dags.operators.sftp.find_sftp_files_operator import (
    FindSftpFilesOperator,
)
from recidiviz.airflow.dags.operators.sftp.gcs_to_gcs_operator import (
    SFTPGcsToGcsOperator,
)
from recidiviz.airflow.dags.operators.sftp.gcs_transform_file_operator import (
    RecidivizGcsFileTransformOperator,
)
from recidiviz.airflow.dags.operators.sftp.sftp_to_gcs_operator import (
    RecidivizSftpToGcsOperator,
)
from recidiviz.airflow.dags.raw_data.acquire_resource_lock_sql_query_generator import (
    AcquireRawDataResourceLockSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.release_resource_lock_sql_query_generator import (
    ReleaseRawDataResourceLockSqlQueryGenerator,
)
from recidiviz.airflow.dags.sftp.filter_downloaded_files_sql_query_generator import (
    FilterDownloadedFilesSqlQueryGenerator,
)
from recidiviz.airflow.dags.sftp.gather_discovered_ingest_ready_files_sql_query_generator import (
    GatherDiscoveredIngestReadyFilesSqlQueryGenerator,
)
from recidiviz.airflow.dags.sftp.gather_discovered_remote_files_sql_query_generator import (
    GatherDiscoveredRemoteFilesSqlQueryGenerator,
)
from recidiviz.airflow.dags.sftp.mark_ingest_ready_files_discovered_sql_query_generator import (
    MarkIngestReadyFilesDiscoveredSqlQueryGenerator,
)
from recidiviz.airflow.dags.sftp.mark_ingest_ready_files_uploaded_sql_query_generator import (
    MarkIngestReadyFilesUploadedSqlQueryGenerator,
)
from recidiviz.airflow.dags.sftp.mark_remote_files_discovered_sql_query_generator import (
    MarkRemoteFilesDiscoveredSqlQueryGenerator,
)
from recidiviz.airflow.dags.sftp.mark_remote_files_downloaded_sql_query_generator import (
    MarkRemoteFilesDownloadedSqlQueryGenerator,
)
from recidiviz.airflow.dags.sftp.metadata import (
    END_SFTP,
    SFTP_ENABLED_YAML_CONFIG,
    SFTP_EXCLUDED_PATHS_YAML_CONFIG,
    SFTP_REQUIRED_RESOURCES,
    SFTP_RESOURCE_LOCK_DESCRIPTION,
    SFTP_RESOURCE_LOCK_TTL_SECONDS,
    START_SFTP,
    TASK_RETRIES,
    get_configs_bucket,
)
from recidiviz.airflow.dags.utils.cloud_sql import cloud_sql_conn_id_for_schema_type
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.airflow.dags.utils.gcsfs_utils import read_yaml_config
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import RawDataResourceLock
from recidiviz.persistence.database.schema_type import SchemaType

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

retry: Retry = Retry(predicate=lambda _: False)

# This is the maximum number of tasks to run in parallel when they are dynamically
# generated. This prevents the scheduler and all workers from being overloaded.
MAX_TASKS_TO_RUN_IN_PARALLEL = 15


def sftp_enabled_states(project_id: str) -> List[StateCode]:
    """Returns a list of state codes that have the necessary SFTP infrastructure in
    pulse-data enabled for |project_id|.
    """
    enabled_states: List[StateCode] = []
    for state_code in StateCode:
        try:
            delegate = SftpDownloadDelegateFactory.build(region_code=state_code.value)
            if project_id in delegate.supported_environments():
                enabled_states.append(state_code)
        except ValueError:
            logging.info(
                "%s does not have a configured SFTP delegate.", state_code.value
            )
            continue
    return enabled_states


def is_enabled_in_config(state_code_str: str) -> bool:
    """Validates with our external config file that we should run SFTP for the provided
    |state_code_str|.
    """
    state_code = StateCode(state_code_str.upper())
    config = read_yaml_config(
        GcsfsFilePath.from_directory_and_file_name(
            dir_path=get_configs_bucket(get_project_id()),
            file_name=SFTP_ENABLED_YAML_CONFIG,
        )
    )
    enabled_states = {
        StateCode(enabled_state_code_str.upper())
        for enabled_state_code_str in config.pop_list("states", str)
    }

    return state_code in enabled_states


@task.short_circuit(
    ignore_downstream_trigger_rules=True, trigger_rule=TriggerRule.ALL_DONE
)
def successfully_acquired_all_locks(
    maybe_serialized_acquired_locks: Optional[List[str]],
    resources_needed: List[DirectIngestRawDataResourceLockResource],
) -> bool:
    """If we were not able to acquire raw data resource locks, let's skip all
    downstream tasks to make sure no work is done.
    """
    if maybe_serialized_acquired_locks is None:
        return False

    acquired_locks = [
        RawDataResourceLock.deserialize(serialized_lock)
        for serialized_lock in maybe_serialized_acquired_locks
    ]
    resources_acquired = {
        lock.lock_resource for lock in acquired_locks if lock.released is False
    }

    return all(resource in resources_acquired for resource in resources_needed)


def xcom_output_is_non_empty_list(
    xcom_output: Optional[List],
    task_id_if_non_empty: Union[str, List[str]],
    task_id_if_empty: Union[str, List[str]],
) -> Union[str, List[str]]:
    if xcom_output is not None and len(xcom_output) > 0:
        return task_id_if_non_empty
    return task_id_if_empty


def discovered_files_lists_are_equal_and_non_empty(
    xcom_prior_discovered_files: Optional[List],
    xcom_discovered_files: Optional[List],
    task_ids_if_equal: List[str],
    task_id_if_not_equal_or_empty: str,
) -> Union[str, List[str]]:
    if xcom_prior_discovered_files is None:
        raise ValueError("Expected to have a non-null previously discovered files list")
    if xcom_discovered_files is None:
        raise ValueError("Expected to have a non-null discovered files list")
    if (
        len(xcom_prior_discovered_files) != len(xcom_discovered_files)
        or len(xcom_discovered_files) == 0
    ):
        return task_id_if_not_equal_or_empty
    return task_ids_if_equal


@task
def remove_queued_up_dags(dag_run: Optional[DagRun] = None) -> None:
    if not dag_run:
        raise ValueError(
            "Dag run not passed to task. Should be automatically set due to function "
            "being a task."
        )
    with create_session() as session:
        session.query(DagRun).filter(
            DagRun.dag_id == dag_run.dag_id, DagRun.state == State.QUEUED
        ).delete()


@dag(
    dag_id=get_sftp_dag_id(project=get_project_id()),
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
)
def sftp_dag() -> None:
    """This executes operations to handle files downloaded from SFTP servers."""

    project_id = get_project_id()

    start_sftp = EmptyOperator(task_id=START_SFTP)
    rm_dags = remove_queued_up_dags()
    start_sftp >> rm_dags
    end_sftp = EmptyOperator(task_id=END_SFTP, trigger_rule=TriggerRule.ALL_DONE)
    for state_code in sftp_enabled_states(project_id):
        with TaskGroup(group_id=state_code.value) as state_specific_task_group:
            # We want to make sure that SFTP is enabled for the state, otherwise we skip
            # everything for the state.
            check_config = ShortCircuitOperator(
                task_id="check_config",
                python_callable=is_enabled_in_config,
                op_kwargs={"state_code_str": state_code.value},
                ignore_downstream_trigger_rules=True,
            )

            operations_cloud_sql_conn_id = cloud_sql_conn_id_for_schema_type(
                SchemaType.OPERATIONS
            )
            # Remote File Discovery
            with TaskGroup("remote_file_discovery") as remote_file_discovery:
                find_sftp_files_from_server = FindSftpFilesOperator(
                    task_id="find_sftp_files_to_download",
                    state_code=state_code.value,
                    excluded_remote_files_config_path=(
                        GcsfsFilePath.from_directory_and_file_name(
                            dir_path=get_configs_bucket(project_id),
                            file_name=SFTP_EXCLUDED_PATHS_YAML_CONFIG,
                        )
                    ),
                )
                filter_downloaded_files = CloudSqlQueryOperator(
                    task_id="filter_downloaded_files",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=FilterDownloadedFilesSqlQueryGenerator(
                        region_code=state_code.value,
                        find_sftp_files_task_id=find_sftp_files_from_server.task_id,
                    ),
                )
                mark_remote_files_discovered = CloudSqlQueryOperator(
                    task_id="mark_remote_files_discovered",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=MarkRemoteFilesDiscoveredSqlQueryGenerator(
                        region_code=state_code.value,
                        filter_downloaded_files_task_id=filter_downloaded_files.task_id,
                    ),
                )
                gather_discovered_remote_files = CloudSqlQueryOperator(
                    task_id="gather_discovered_remote_files",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=GatherDiscoveredRemoteFilesSqlQueryGenerator(
                        region_code=state_code.value
                    ),
                    # This step will always execute regardless of success of the previous
                    # steps. This is to ensure that prior failed to download files are
                    # rediscovered.
                    trigger_rule=TriggerRule.ALL_DONE,
                )

                (
                    find_sftp_files_from_server
                    >> filter_downloaded_files
                    >> mark_remote_files_discovered
                    >> gather_discovered_remote_files
                )

            # Remote File Download flow
            with TaskGroup("remote_file_download") as remote_file_download:
                download_sftp_files = RecidivizSftpToGcsOperator.partial(
                    task_id="download_sftp_files",
                    project_id=project_id,
                    region_code=state_code.value,
                    max_active_tis_per_dag=MAX_TASKS_TO_RUN_IN_PARALLEL,
                    execution_timeout=timedelta(hours=8),
                    retries=TASK_RETRIES,
                ).expand_kwargs(gather_discovered_remote_files.output)
                post_process_downloaded_files = RecidivizGcsFileTransformOperator.partial(
                    task_id="post_process_downloaded_files",
                    project_id=project_id,
                    region_code=state_code.value,
                    max_active_tis_per_dag=MAX_TASKS_TO_RUN_IN_PARALLEL,
                    # These tasks will always trigger no matter the status of the prior
                    # tasks. We need to wait until downloads are finished in order
                    # to decide what to post process.
                    trigger_rule=TriggerRule.ALL_DONE,
                    retries=TASK_RETRIES,
                ).expand_kwargs(
                    download_sftp_files.output
                )
                mark_remote_files_downloaded = CloudSqlQueryOperator(
                    task_id="mark_remote_files_downloaded",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=MarkRemoteFilesDownloadedSqlQueryGenerator(
                        region_code=state_code.value,
                        post_process_sftp_files_task_id=post_process_downloaded_files.task_id,
                    ),
                )
                do_not_mark_remote_files_downloaded = EmptyOperator(
                    task_id="do_not_mark_remote_files_downloaded"
                )
                end_remote_file_download = EmptyOperator(
                    task_id="end_remote_file_download",
                    trigger_rule=TriggerRule.ALL_DONE,
                )
                check_remote_files_downloaded_and_post_processed = BranchPythonOperator(
                    task_id="check_remote_files_downloaded_and_post_processed",
                    python_callable=xcom_output_is_non_empty_list,
                    op_kwargs={
                        "xcom_output": XComArg(post_process_downloaded_files),
                        "task_id_if_non_empty": mark_remote_files_downloaded.task_id,
                        "task_id_if_empty": do_not_mark_remote_files_downloaded.task_id,
                    },
                    # This task will always trigger no matter the status of the prior tasks.
                    trigger_rule=TriggerRule.ALL_DONE,
                )
                (
                    gather_discovered_remote_files
                    >> download_sftp_files
                    >> post_process_downloaded_files
                    >> check_remote_files_downloaded_and_post_processed
                    >> [
                        mark_remote_files_downloaded,
                        do_not_mark_remote_files_downloaded,
                    ]
                    >> end_remote_file_download
                )

            gather_prior_discovered_ingest_ready_files = CloudSqlQueryOperator(
                task_id="gather_prior_discovered_ingest_ready_files",
                cloud_sql_conn_id=operations_cloud_sql_conn_id,
                query_generator=GatherDiscoveredIngestReadyFilesSqlQueryGenerator(
                    region_code=state_code.value
                ),
                # This step will always execute regardless of success of the previous
                # steps.
                trigger_rule=TriggerRule.ALL_DONE,
            )

            do_not_upload_ingest_ready_files = EmptyOperator(
                task_id="do_not_upload_ingest_ready_files"
            )

            # Ingest Ready File Discovery
            with TaskGroup(
                "ingest_ready_file_discovery"
            ) as ingest_ready_file_discovery:
                # Some files are initially downloaded as ZIP files that may contain improper
                # files for ingest, therefore we need to filter them out before uploading.
                filter_invalid_files_downloaded = FilterInvalidGcsFilesOperator(
                    task_id="filter_invalid_files_downloaded_from_sftp",
                    collect_all_post_processed_files_task_id=mark_remote_files_downloaded.task_id,
                )
                mark_ingest_ready_files_discovered = CloudSqlQueryOperator(
                    task_id="mark_ingest_ready_files_discovered",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=MarkIngestReadyFilesDiscoveredSqlQueryGenerator(
                        region_code=state_code.value,
                        filter_invalid_gcs_files_task_id=filter_invalid_files_downloaded.task_id,
                    ),
                )
                gather_discovered_ingest_ready_files = CloudSqlQueryOperator(
                    task_id="gather_discovered_ingest_ready_files",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=GatherDiscoveredIngestReadyFilesSqlQueryGenerator(
                        region_code=state_code.value
                    ),
                    # This step will always execute regardless of success of the previous
                    # steps.
                    trigger_rule=TriggerRule.ALL_DONE,
                )
                (
                    filter_invalid_files_downloaded
                    >> mark_ingest_ready_files_discovered
                    >> gather_discovered_ingest_ready_files
                )

            # Ingest Ready File Upload
            with TaskGroup("ingest_ready_file_upload") as ingest_ready_file_upload:
                with TaskGroup(
                    "acquire_permission_for_ingest_file_upload"
                ) as acquire_permission_for_ingest_file_upload:
                    # --- raw data resource lock acquisition -------------------------------
                    acquire_locks = CloudSqlQueryOperator(
                        task_id="acquire_raw_data_resource_locks",
                        cloud_sql_conn_id=operations_cloud_sql_conn_id,
                        query_generator=AcquireRawDataResourceLockSqlQueryGenerator(
                            region_code=state_code.value,
                            raw_data_instance=DirectIngestInstance.PRIMARY,
                            resources=SFTP_REQUIRED_RESOURCES,
                            lock_description=SFTP_RESOURCE_LOCK_DESCRIPTION,
                            lock_ttl_seconds=SFTP_RESOURCE_LOCK_TTL_SECONDS,
                        ),
                    )

                    ensure_acquired_locks = successfully_acquired_all_locks(
                        acquire_locks.output, SFTP_REQUIRED_RESOURCES
                    )

                    acquire_locks >> ensure_acquired_locks

                upload_files_to_ingest_bucket = SFTPGcsToGcsOperator.partial(
                    task_id="upload_files_to_ingest_bucket",
                    project_id=project_id,
                    region_code=state_code.value,
                    max_active_tis_per_dag=MAX_TASKS_TO_RUN_IN_PARALLEL,
                    retries=TASK_RETRIES,
                ).expand_kwargs(gather_discovered_ingest_ready_files.output)
                mark_ingest_ready_files_uploaded = CloudSqlQueryOperator(
                    task_id="mark_ingest_ready_files_uploaded",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=MarkIngestReadyFilesUploadedSqlQueryGenerator(
                        region_code=state_code.value,
                        upload_files_to_ingest_bucket_task_id=upload_files_to_ingest_bucket.task_id,
                    ),
                )
                do_not_mark_ingest_ready_files_uploaded = EmptyOperator(
                    task_id="do_not_mark_ingest_ready_files_uploaded"
                )
                check_ingest_ready_files_uploaded = BranchPythonOperator(
                    task_id="check_ingest_ready_files_uploaded",
                    python_callable=xcom_output_is_non_empty_list,
                    op_kwargs={
                        "xcom_output": XComArg(upload_files_to_ingest_bucket),
                        "task_id_if_non_empty": mark_ingest_ready_files_uploaded.task_id,
                        "task_id_if_empty": do_not_mark_ingest_ready_files_uploaded.task_id,
                    },
                )
                end_ingest_ready_files_uploaded = EmptyOperator(
                    task_id="end_ingest_ready_files_uploaded",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                )

                with TaskGroup(
                    "release_permission_for_ingest_file_upload"
                ) as release_permission_for_ingest_file_upload:

                    # --- release raw data resource locks ----------------------------------

                    release_locks = CloudSqlQueryOperator(
                        task_id="release_raw_data_resource_locks",
                        cloud_sql_conn_id=operations_cloud_sql_conn_id,
                        query_generator=ReleaseRawDataResourceLockSqlQueryGenerator(
                            region_code=state_code.value,
                            raw_data_instance=DirectIngestInstance.PRIMARY,
                            acquire_resource_lock_task_id=acquire_locks.task_id,
                        ),
                    )

                    acquire_locks >> release_locks

                (
                    acquire_permission_for_ingest_file_upload
                    >> upload_files_to_ingest_bucket
                    >> check_ingest_ready_files_uploaded
                    >> [
                        mark_ingest_ready_files_uploaded,
                        do_not_mark_ingest_ready_files_uploaded,
                    ]
                    >> end_ingest_ready_files_uploaded
                    >> release_permission_for_ingest_file_upload
                )

            check_if_ingest_ready_files_have_stabilized = BranchPythonOperator(
                task_id="check_if_ingest_ready_files_have_stabilized",
                python_callable=discovered_files_lists_are_equal_and_non_empty,
                op_kwargs={
                    "xcom_prior_discovered_files": XComArg(
                        gather_prior_discovered_ingest_ready_files
                    ),
                    "xcom_discovered_files": XComArg(
                        gather_discovered_ingest_ready_files
                    ),
                    "task_ids_if_equal": [
                        acquire_locks.task_id,
                        ensure_acquired_locks.operator.task_id,
                        release_locks.task_id,
                    ],
                    "task_id_if_not_equal_or_empty": do_not_upload_ingest_ready_files.task_id,
                },
                # This task will always trigger no matter the status of the prior tasks.
                trigger_rule=TriggerRule.ALL_DONE,
            )

            (
                check_config
                >> remote_file_discovery
                >> remote_file_download
                >> gather_prior_discovered_ingest_ready_files
                >> ingest_ready_file_discovery
                >> check_if_ingest_ready_files_have_stabilized
                >> [
                    ingest_ready_file_upload,
                    do_not_upload_ingest_ready_files,
                    ensure_acquired_locks,
                ]
            )

        rm_dags >> state_specific_task_group >> end_sftp


dag = sftp_dag()
