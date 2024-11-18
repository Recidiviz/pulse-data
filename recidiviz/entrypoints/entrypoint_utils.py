# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Contains the utils for entrypoints"""
import json
import os
from typing import Any, Dict, List, Union

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.utils.kubernetes_pod_operator_task_output_handler import (
    KubernetesPodOperatorTaskOutputHandler,
)

XCOM_RETURN_FILE = "/airflow/xcom/return.json"


# TODO(#31148) Remove this function once all entrypoints are using save_to_gcs_xcom
def save_to_xcom(json_data: Union[Dict, List[Any], str, int, bool]) -> None:
    """Saves the given json data to xcom return file in directory. To be used by kubernetes pod entrypoints."""

    os.makedirs(os.path.dirname(XCOM_RETURN_FILE), exist_ok=True)
    with open(XCOM_RETURN_FILE, "w", encoding="utf-8") as f:
        json.dump(json_data, f)


def save_to_gcs_xcom(fs: GCSFileSystem, output_str: str) -> None:
    """Writes serialized task output to GCS. To be used by kubernetes pod entrypoints."""
    output_writer = KubernetesPodOperatorTaskOutputHandler.create_kubernetes_pod_operator_task_output_handler_from_pod_env(
        fs
    )
    output_writer.write_serialized_task_output(output_str)
