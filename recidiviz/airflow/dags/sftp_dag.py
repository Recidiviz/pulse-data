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
import uuid
from typing import List, Optional

from airflow.decorators import dag
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.tasks import (
    CloudTasksQueuePauseOperator,
    CloudTasksQueueResumeOperator,
)
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.retry import Retry

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils.yaml_dict import YAMLDict

# Custom Airflow operators in the recidiviz.airflow.dags.operators package are imported into the
# Cloud Composer environment at the top-level. However, for unit tests, we still need to
# import the recidiviz-top-level.
# pylint: disable=ungrouped-imports
try:
    from operators.cloud_sql_query_operator import CloudSqlQueryOperator  # type: ignore
    from operators.sftp.filter_invalid_gcs_files import (  # type: ignore
        FilterInvalidGcsFilesOperator,
    )
    from operators.sftp.find_sftp_files_operator import (  # type: ignore
        FindSftpFilesOperator,
    )
    from operators.sftp.gcs_to_gcs_operator import SFTPGcsToGcsOperator  # type: ignore
    from operators.sftp.gcs_transform_file_operator import (  # type:ignore
        RecidivizGcsFileTransformOperator,
    )
    from operators.sftp.sftp_to_gcs_operator import (  # type: ignore
        RecidivizSftpToGcsOperator,
    )
    from sftp.filter_downloaded_files_sql_query_generator import (  # type: ignore
        FilterDownloadedFilesSqlQueryGenerator,
    )
    from sftp.gather_discovered_ingest_ready_files_sql_query_generator import (  # type: ignore
        GatherDiscoveredIngestReadyFilesSqlQueryGenerator,
    )
    from sftp.gather_discovered_remote_files_sql_query_generator import (  # type: ignore
        GatherDiscoveredRemoteFilesSqlQueryGenerator,
    )
    from sftp.mark_ingest_ready_files_discovered_sql_query_generator import (  # type: ignore
        MarkIngestReadyFilesDiscoveredSqlQueryGenerator,
    )
    from sftp.mark_ingest_ready_files_uploaded_sql_query_generator import (  # type: ignore
        MarkIngestReadyFilesUploadedSqlQueryGenerator,
    )
    from sftp.mark_remote_files_discovered_sql_query_generator import (  # type: ignore
        MarkRemoteFilesDiscoveredSqlQueryGenerator,
    )
    from sftp.mark_remote_files_downloaded_sql_query_generator import (  # type: ignore
        MarkRemoteFilesDownloadedSqlQueryGenerator,
    )
    from utils.cloud_sql import cloud_sql_conn_id_for_schema_type  # type: ignore
    from utils.default_args import DEFAULT_ARGS  # type: ignore
except ImportError:
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

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

project_id = os.environ.get("GCP_PROJECT")

retry: Retry = Retry(predicate=lambda _: False)

GCS_LOCK_BUCKET = f"{project_id}-gcslock"
GCS_CONFIG_BUCKET = f"{project_id}-configs"

QUEUE_LOCATION = "us-east1"

# This is the maximum number of tasks to run in parallel when they are dynamically
# generated. This prevents the scheduler and all workers from being overloaded.
MAX_TASKS_TO_RUN_IN_PARALLEL = 8


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
    with GCSHook().provide_file(
        bucket_name=GCS_CONFIG_BUCKET,
        object_name="sftp_enabled_in_airflow_config.yaml",
    ) as f:  # type: ignore
        config = YAMLDict.from_io(f)  # type: ignore
        return state_code in config.pop_list("states", str)


# TODO(#17277): Convert to the Airflow-supported version of GCSPseudoLockManager
def check_if_lock_does_not_exist(lock_id: str) -> bool:
    return not GCSHook().exists(bucket_name=GCS_LOCK_BUCKET, object_name=lock_id)


def write_lock(lock_id: str) -> None:
    GCSHook().upload(
        bucket_name=GCS_LOCK_BUCKET, object_name=lock_id, data=str(uuid.uuid4())
    )


def delete_lock(lock_id: str) -> None:
    GCSHook().delete(bucket_name=GCS_LOCK_BUCKET, object_name=lock_id)


def xcom_output_is_non_empty_list(xcom_output: Optional[List]) -> bool:
    return xcom_output is not None and len(xcom_output) > 0


@dag(
    dag_id=f"{project_id}_sftp_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
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
    end_sftp = EmptyOperator(task_id="end_sftp", trigger_rule=TriggerRule.ALL_DONE)
    for state_code in sftp_enabled_states():
        with TaskGroup(group_id=state_code) as state_specific_task_group:
            # TODO(#17283): Remove usage of config once all states are enabled in Airflow.
            check_config = ShortCircuitOperator(
                task_id="check_config",
                python_callable=is_enabled_in_config,
                op_kwargs={"state_code": state_code},
                ignore_downstream_trigger_rules=True,
            )
            # TODO(#17277): Convert to the Airflow-supported version of DirectIngestRegionLockManager
            gcs_sftp_bucket_lock = f"GCS_SFTP_BUCKET_LOCK_{state_code}"
            gcs_ingest_bucket_lock = f"GCS_INGEST_BUCKET_LOCK_{state_code}"
            lock_names_with_lock_ids = {
                "gcs_sftp": gcs_sftp_bucket_lock,
                "gcs_ingest": gcs_ingest_bucket_lock,
            }
            set_locks = [
                PythonOperator(
                    task_id=f"set_{lock_name}_lock",
                    python_callable=write_lock,
                    op_kwargs={"lock_id": lock_id},
                )
                for lock_name, lock_id in lock_names_with_lock_ids.items()
            ]

            operations_cloud_sql_conn_id = cloud_sql_conn_id_for_schema_type(
                SchemaType.OPERATIONS
            )

            # Discovery flow
            with TaskGroup("remote_file_discovery") as remote_file_discovery:
                find_sftp_files_from_server = FindSftpFilesOperator(
                    task_id="find_sftp_files_to_download",
                    state_code=state_code,
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

            # Download flow
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
                check_remote_files_downloaded_and_post_processed = ShortCircuitOperator(
                    task_id="check_remote_files_downloaded_and_post_processed",
                    python_callable=xcom_output_is_non_empty_list,
                    op_args=[XComArg(post_process_downloaded_files)],
                    # If the condition is False, that there were no files downloaded or
                    # post processed, then the next direct task is skipped only.
                    ignore_downstream_trigger_rules=False,
                    # This task will always trigger no matter the status of the prior tasks.
                    trigger_rule=TriggerRule.ALL_DONE,
                )
                mark_remote_files_downloaded = CloudSqlQueryOperator(
                    task_id="mark_remote_files_downloaded",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=MarkRemoteFilesDownloadedSqlQueryGenerator(
                        region_code=state_code,
                        post_process_sftp_files_task_id=post_process_downloaded_files.task_id,
                    ),
                )
                (
                    download_sftp_files
                    >> post_process_downloaded_files
                    >> check_remote_files_downloaded_and_post_processed
                    >> mark_remote_files_downloaded
                )

            scheduler_queue = (
                f"direct-ingest-state-{state_code.lower().replace('_', '-')}-scheduler"
            )
            pause_scheduler_queue = CloudTasksQueuePauseOperator(
                task_id="pause_scheduler_queue",
                location=QUEUE_LOCATION,
                queue_name=scheduler_queue,
                project_id=project_id,
                retry=retry,
            )

            # Discovery flow
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

            # Upload flow
            with TaskGroup("ingest_ready_file_upload") as ingest_ready_file_upload:
                upload_files_to_ingest_bucket = SFTPGcsToGcsOperator.partial(
                    task_id="upload_files_to_ingest_bucket",
                    project_id=project_id,
                    region_code=state_code,
                    max_active_tis_per_dag=MAX_TASKS_TO_RUN_IN_PARALLEL,
                ).expand_kwargs(gather_discovered_ingest_ready_files.output)
                check_ingest_ready_files_uploaded = ShortCircuitOperator(
                    task_id="check_ingest_ready_files_uploaded",
                    python_callable=xcom_output_is_non_empty_list,
                    op_args=[XComArg(upload_files_to_ingest_bucket)],
                    # If the condition is False, that there were no files uploaded,
                    # then the next direct task is skipped only.
                    ignore_downstream_trigger_rules=False,
                    # This task will always trigger no matter the status of the prior tasks.
                    trigger_rule=TriggerRule.ALL_DONE,
                )
                mark_ingest_ready_files_uploaded = CloudSqlQueryOperator(
                    task_id="mark_ingest_ready_files_uploaded",
                    cloud_sql_conn_id=operations_cloud_sql_conn_id,
                    query_generator=MarkIngestReadyFilesUploadedSqlQueryGenerator(
                        region_code=state_code,
                        upload_files_to_ingest_bucket_task_id=upload_files_to_ingest_bucket.task_id,
                    ),
                )
                (
                    upload_files_to_ingest_bucket
                    >> check_ingest_ready_files_uploaded
                    >> mark_ingest_ready_files_uploaded
                )

            resume_scheduler_queue = CloudTasksQueueResumeOperator(
                task_id="resume_scheduler_queue",
                location=QUEUE_LOCATION,
                queue_name=scheduler_queue,
                project_id=project_id,
                # This will trigger the task regardless of the failure or success of the
                # upstream uploads/downloads.
                trigger_rule=TriggerRule.ALL_DONE,
                retry=retry,
            )

            release_locks = [
                PythonOperator(
                    task_id=f"release_{lock_name}_lock",
                    python_callable=delete_lock,
                    op_kwargs={"lock_id": lock_id},
                    # This will trigger the task regardless of the failure or success of the
                    # upstream uploads/downloads.
                    trigger_rule=TriggerRule.ALL_DONE,
                )
                for lock_name, lock_id in lock_names_with_lock_ids.items()
            ]

            (
                check_config
                >> set_locks
                >> remote_file_discovery
                >> remote_file_download
                >> pause_scheduler_queue
                >> ingest_ready_file_discovery
                >> ingest_ready_file_upload
                >> resume_scheduler_queue
                >> release_locks
            )

        start_sftp >> state_specific_task_group >> end_sftp


dag = sftp_dag()
