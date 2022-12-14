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
import os
from typing import Dict, List

from airflow.decorators import dag
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.task_group import TaskGroup

# Custom Airflow operators in the recidiviz.airflow.dags.operators package are imported into the
# Cloud Composer environment at the top-level. However, for unit tests, we still need to
# import the recidiviz-top-level.
try:
    from utils.default_args import DEFAULT_ARGS  # type: ignore
except ImportError:
    from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

project_id = os.environ.get("GCP_PROJECT")


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


def check_if_lock_does_not_exist(lock_id: str) -> bool:
    return not GCSHook().exists(
        bucket_name=f"{project_id}-gcslock", object_name=lock_id
    )


@dag(
    dag_id=f"{project_id}_sftp_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)
def sftp_dag() -> None:
    """This executes operations to handle files downloaded from SFTP servers."""

    start_sftp = ShortCircuitOperator(
        task_id="start_sftp",
        python_callable=check_if_lock_does_not_exist,
        op_kwargs={"lock_id": "EXPORT_PROCESS_RUNNING_OPERATIONS"},
        ignore_downstream_trigger_rules=True,
    )
    end_sftp = EmptyOperator(task_id="end_sftp")
    for state_code in ["US_ID", "US_MI", "US_ME", "US_PA"]:
        with TaskGroup(group_id=state_code) as task_group:
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
            find_files_to_download >> transform_files >> bridge >> upload_files

        start_sftp >> task_group >> end_sftp


dag = sftp_dag()
