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
import os
from typing import Any, Dict, List, Optional, Union

from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.tasks import (
    CloudTasksQueueGetOperator,
    CloudTasksQueuePauseOperator,
    CloudTasksQueueResumeOperator,
)
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.retry import Retry
from google.cloud.tasks_v2.types.queue import Queue

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
from recidiviz.airflow.dags.utils.cloud_sql import cloud_sql_conn_id_for_schema_type
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.gcsfs_utils import read_yaml_config
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

project_id = os.environ.get("GCP_PROJECT")

retry: Retry = Retry(predicate=lambda _: False)

GCS_LOCK_BUCKET = f"{project_id}-gcslock"
GCS_CONFIG_BUCKET = f"{project_id}-configs"
GCS_ENABLED_STATES_CONFIG_PATH = GcsfsFilePath(
    bucket_name=GCS_CONFIG_BUCKET, blob_name="sftp_enabled_in_airflow_config.yaml"
)
GCS_EXCLUDED_REMOTE_FILES_CONFIG_PATH = GcsfsFilePath(
    bucket_name=GCS_CONFIG_BUCKET, blob_name="sftp_excluded_remote_file_paths.yaml"
)


QUEUE_LOCATION = "us-east1"

# This is the maximum number of tasks to run in parallel when they are dynamically
# generated. This prevents the scheduler and all workers from being overloaded.
MAX_TASKS_TO_RUN_IN_PARALLEL = 15


def sftp_enabled_states() -> List[str]:
    enabled_states = []
    for state_code in StateCode:
        try:
            delegate = SftpDownloadDelegateFactory.build(region_code=state_code.value)
            if project_id in delegate.supported_environments():
                enabled_states.append(state_code.value)
        except ValueError:
            logging.info(
                "%s does not have a configured SFTP delegate.", state_code.value
            )
            continue
    return enabled_states


# TODO(#17283): Remove usage of config once all states are enabled in Airflow.
def is_enabled_in_config(state_code: str) -> bool:
    config = read_yaml_config(GCS_ENABLED_STATES_CONFIG_PATH)
    return state_code in config.pop_list("states", str)


# TODO(#17277): Convert to the Airflow-supported version of GCSPseudoLockManager
def check_if_lock_does_not_exist(lock_id: str) -> bool:
    return not GCSHook().exists(bucket_name=GCS_LOCK_BUCKET, object_name=lock_id)


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
    task_id_if_equal: Union[str, List[str]],
    task_id_if_not_equal_or_empty: Union[str, List[str]],
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
    return task_id_if_equal


def get_running_queue_instances(*queues: Optional[Dict[str, Any]]) -> List[str]:
    """Returns the queues that are currently running and will need to be unpaused later."""
    ingest_instances_to_unpause: List[str] = []
    for queue in queues:
        if queue is None:
            raise ValueError("Found null queue in queues list")
        if queue["state"] == Queue.State.RUNNING:
            if "secondary" in queue["name"]:
                ingest_instances_to_unpause.append(
                    DirectIngestInstance.SECONDARY.value.lower()
                )
            else:
                ingest_instances_to_unpause.append(
                    DirectIngestInstance.PRIMARY.value.lower()
                )
    return ingest_instances_to_unpause


def queues_were_unpaused(
    queues_to_resume: Optional[List[str]],
    task_ids_for_queues_to_resume: List[str],
    task_id_if_no_queues_to_resume: str,
) -> Union[str, List[str]]:
    if queues_to_resume is None:
        raise ValueError("Found unexpectedly null queues_paused list")
    tasks_to_return_if_unpaused: List[str] = []
    for task_id in task_ids_for_queues_to_resume:
        for queue in queues_to_resume:
            if queue in task_id:
                tasks_to_return_if_unpaused.append(task_id)
    if tasks_to_return_if_unpaused:
        # If any queues were paused and need to be unpaused, return those
        return tasks_to_return_if_unpaused
    # Otherwise, if no queues were paused and need to be unpaused, return this task
    return task_id_if_no_queues_to_resume


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
    dag_id=f"{project_id}_sftp_dag",
    # TODO(apache/airflow#29903) Remove this override and only override mapped task-level retries.
    default_args={
        **DEFAULT_ARGS,  # type: ignore
        "retries": 3,
    },
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def sftp_dag() -> None:
    """This executes operations to handle files downloaded from SFTP servers."""

    # We want to make sure that the export for the operations DB is not running when
    # we start SFTP operations. Otherwise, we skip everything.
    start_sftp = ShortCircuitOperator(
        task_id="start_sftp",
        python_callable=check_if_lock_does_not_exist,
        op_kwargs={"lock_id": "EXPORT_PROCESS_RUNNING_OPERATIONS"},
        ignore_downstream_trigger_rules=True,
    )
    rm_dags = remove_queued_up_dags()
    start_sftp >> rm_dags
    end_sftp = EmptyOperator(task_id="end_sftp", trigger_rule=TriggerRule.ALL_DONE)
    for state_code in sftp_enabled_states():
        with TaskGroup(group_id=state_code) as state_specific_task_group:
            # We want to make sure that SFTP is enabled for the state, otherwise we skip
            # everything for the state.
            check_config = ShortCircuitOperator(
                task_id="check_config",
                python_callable=is_enabled_in_config,
                op_kwargs={"state_code": state_code},
                ignore_downstream_trigger_rules=True,
            )

            operations_cloud_sql_conn_id = cloud_sql_conn_id_for_schema_type(
                SchemaType.OPERATIONS
            )

            # Remote File Discovery
            with TaskGroup("remote_file_discovery") as remote_file_discovery:
                find_sftp_files_from_server = FindSftpFilesOperator(
                    task_id="find_sftp_files_to_download",
                    state_code=state_code,
                    excluded_remote_files_config_path=GCS_EXCLUDED_REMOTE_FILES_CONFIG_PATH,
                )
                filter_downloaded_files = CloudSqlQueryOperator(
                    task_id="filter_downloaded_files",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=FilterDownloadedFilesSqlQueryGenerator(
                        region_code=state_code,
                        find_sftp_files_task_id=find_sftp_files_from_server.task_id,
                    ),
                )
                mark_remote_files_discovered = CloudSqlQueryOperator(
                    task_id="mark_remote_files_discovered",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=MarkRemoteFilesDiscoveredSqlQueryGenerator(
                        region_code=state_code,
                        filter_downloaded_files_task_id=filter_downloaded_files.task_id,
                    ),
                )
                gather_discovered_remote_files = CloudSqlQueryOperator(
                    task_id="gather_discovered_remote_files",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=GatherDiscoveredRemoteFilesSqlQueryGenerator(
                        region_code=state_code
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
                    region_code=state_code,
                    max_active_tis_per_dag=MAX_TASKS_TO_RUN_IN_PARALLEL,
                ).expand_kwargs(gather_discovered_remote_files.output)
                post_process_downloaded_files = RecidivizGcsFileTransformOperator.partial(
                    task_id="post_process_downloaded_files",
                    project_id=project_id,
                    region_code=state_code,
                    max_active_tis_per_dag=MAX_TASKS_TO_RUN_IN_PARALLEL,
                    # These tasks will always trigger no matter the status of the prior
                    # tasks. We need to wait until downloads are finished in order
                    # to decide what to post process.
                    trigger_rule=TriggerRule.ALL_DONE,
                ).expand_kwargs(
                    download_sftp_files.output
                )
                mark_remote_files_downloaded = CloudSqlQueryOperator(
                    task_id="mark_remote_files_downloaded",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=MarkRemoteFilesDownloadedSqlQueryGenerator(
                        region_code=state_code,
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
                    region_code=state_code
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
                        region_code=state_code,
                        filter_invalid_gcs_files_task_id=filter_invalid_files_downloaded.task_id,
                    ),
                )
                gather_discovered_ingest_ready_files = CloudSqlQueryOperator(
                    task_id="gather_discovered_ingest_ready_files",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=GatherDiscoveredIngestReadyFilesSqlQueryGenerator(
                        region_code=state_code
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

            scheduler_queue_name = (
                f"direct-ingest-state-{state_code.lower().replace('_', '-')}-scheduler"
            )
            scheduler_queues: Dict[DirectIngestInstance, str] = {
                DirectIngestInstance.PRIMARY: scheduler_queue_name,
                DirectIngestInstance.SECONDARY: f"{scheduler_queue_name}-secondary",
            }

            # Ingest Ready File Upload
            with TaskGroup("ingest_ready_file_upload") as ingest_ready_file_upload:
                prior_queue_statuses = [
                    CloudTasksQueueGetOperator(
                        task_id=f"get_scheduler_queue_{ingest_instance.value.lower()}_status",
                        location=QUEUE_LOCATION,
                        queue_name=queue_name,
                        project_id=project_id,
                        retry=retry,
                    )
                    for ingest_instance, queue_name in scheduler_queues.items()
                ]
                # Keep track of which queues are running at the beginning so we can
                # re-start them later.
                gather_previously_running_queue_instances = PythonOperator(
                    task_id="gather_previously_running_queue_instances",
                    python_callable=get_running_queue_instances,
                    op_args=[
                        XComArg(queue_status) for queue_status in prior_queue_statuses
                    ],
                )
                pause_scheduler_queues = [
                    CloudTasksQueuePauseOperator(
                        task_id=f"pause_scheduler_queue_{ingest_instance.value.lower()}",
                        location=QUEUE_LOCATION,
                        queue_name=queue_name,
                        project_id=project_id,
                        retry=retry,
                    )
                    for ingest_instance, queue_name in scheduler_queues.items()
                ]
                upload_files_to_ingest_bucket = SFTPGcsToGcsOperator.partial(
                    task_id="upload_files_to_ingest_bucket",
                    project_id=project_id,
                    region_code=state_code,
                    max_active_tis_per_dag=MAX_TASKS_TO_RUN_IN_PARALLEL,
                ).expand_kwargs(gather_discovered_ingest_ready_files.output)
                mark_ingest_ready_files_uploaded = CloudSqlQueryOperator(
                    task_id="mark_ingest_ready_files_uploaded",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=MarkIngestReadyFilesUploadedSqlQueryGenerator(
                        region_code=state_code,
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

                resume_scheduler_queues = [
                    CloudTasksQueueResumeOperator(
                        task_id=f"resume_scheduler_queue_{ingest_instance.value.lower()}",
                        location=QUEUE_LOCATION,
                        queue_name=queue_name,
                        project_id=project_id,
                        retry=retry,
                    )
                    for ingest_instance, queue_name in scheduler_queues.items()
                ]

                do_not_resume_scheduler_queues = EmptyOperator(
                    task_id="do_not_resume_scheduler_queues"
                )

                start_resume_queues_branch = BranchPythonOperator(
                    task_id="start_resume_queues_branch",
                    python_callable=queues_were_unpaused,
                    op_kwargs={
                        "queues_to_resume": XComArg(
                            gather_previously_running_queue_instances
                        ),
                        "task_ids_for_queues_to_resume": [
                            resume_scheduler_queue.task_id
                            for resume_scheduler_queue in resume_scheduler_queues
                        ],
                        "task_id_if_no_queues_to_resume": do_not_resume_scheduler_queues.task_id,
                    },
                )

                end_resume_queues_branch = EmptyOperator(
                    task_id="end_resume_queues_branch",
                    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                )

                (
                    prior_queue_statuses
                    >> gather_previously_running_queue_instances
                    >> pause_scheduler_queues
                    >> upload_files_to_ingest_bucket
                    >> check_ingest_ready_files_uploaded
                    >> [
                        mark_ingest_ready_files_uploaded,
                        do_not_mark_ingest_ready_files_uploaded,
                    ]
                    >> end_ingest_ready_files_uploaded
                    >> start_resume_queues_branch
                    >> [*resume_scheduler_queues, do_not_resume_scheduler_queues]
                    >> end_resume_queues_branch
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
                    "task_id_if_equal": [
                        prior_queue_status.task_id
                        for prior_queue_status in prior_queue_statuses
                    ],
                    "task_id_if_not_equal_or_empty": [
                        do_not_upload_ingest_ready_files.task_id
                    ],
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
                >> [ingest_ready_file_upload, do_not_upload_ingest_ready_files]
            )

        rm_dags >> state_specific_task_group >> end_sftp


dag = sftp_dag()
