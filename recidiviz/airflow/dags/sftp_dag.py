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
from typing import Dict, List

from airflow.decorators import dag
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)
from recidiviz.utils.yaml_dict import YAMLDict

# Custom Airflow operators in the recidiviz.airflow.dags.operators package are imported into the
# Cloud Composer environment at the top-level. However, for unit tests, we still need to
# import the recidiviz-top-level.
# pylint: disable=ungrouped-imports
try:
    from utils.default_args import DEFAULT_ARGS  # type: ignore
except ImportError:
    from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

project_id = os.environ.get("GCP_PROJECT")

# TODO(#17283): Remove test buckets once SFTP is switched over
GCS_LOCK_BUCKET = f"{project_id}-test-gcslock"
GCS_CONFIG_BUCKET = f"{project_id}-test-configs"


def _files_to_download(state_code: str) -> List[Dict[str, str]]:
    if state_code == "US_PA":
        return [{"state_code": state_code, "file": "file.zip"}]
    return [
        {"state_code": state_code, "file": file}
        for file in ["file_1.csv", "file_2.csv", "file_3.csv"]
    ]


def _transform_files(state_code: str, file: str) -> List[Dict[str, str]]:
    if ".zip" in file:
        return [
            {"state_code": state_code, "file": file}
            for file in ["file_1.csv", "file_2.csv", "file_3.csv"]
        ]
    return [{"state_code": state_code, "file": file}]


def _collect_all_files(
    files_to_transform: List[List[Dict[str, str]]]
) -> List[Dict[str, str]]:
    return [item for inner_list in files_to_transform for item in inner_list]


def _upload_files(state_code: str, file: str) -> str:
    return f"{state_code}_{file}"


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
    end_sftp = EmptyOperator(task_id="end_sftp")
    for state_code in sftp_enabled_states():
        with TaskGroup(group_id=state_code) as task_group:
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

            find_files_to_download = PythonOperator(
                task_id="find_sftp_files_to_download",
                python_callable=_files_to_download,
                op_kwargs={"state_code": state_code},
            )
            transform_files = PythonOperator.partial(
                task_id="transform_files", python_callable=_transform_files
            ).expand(op_kwargs=find_files_to_download.output)
            bridge = PythonOperator(
                task_id="collect_all_files",
                python_callable=_collect_all_files,
                op_args=[XComArg(transform_files)],
            )
            upload_files = PythonOperator.partial(
                task_id="upload_files", python_callable=_upload_files
            ).expand(op_kwargs=bridge.output)

            # TODO(#17387): Add ability to pause scheduler queue when upload operation starts

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
                >> find_files_to_download
                >> transform_files
                >> bridge
                >> upload_files
                >> release_locks
            )

        start_sftp >> task_group >> end_sftp


dag = sftp_dag()
