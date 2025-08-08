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
"""Classes for reading/writing KubernetesPodOperator task outputs from/to GCS."""
import logging
from typing import Any, List, Optional

import attr

from recidiviz.cloud_storage.gcs_file_system import (
    GCSBlobDoesNotExistError,
    GCSFileSystem,
)
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common import attr_validators
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    AirflowKubernetesPodEnvironment,
    in_airflow_kubernetes_pod,
)

OUTPUT_BASE_FILE_NAME: str = "output"


@attr.define
class KubernetesPodOperatorTaskOutputFilePathBuilder:
    """Builds paths for storing KubernetesPodOperator task outputs."""

    project_id: str = attr.ib(validator=attr_validators.is_str)
    dag_id: str = attr.ib(validator=attr_validators.is_str)
    run_id: str = attr.ib(validator=attr_validators.is_str)

    @property
    def bucket_name(self) -> str:
        return f"{self.project_id}-airflow-kubernetes-pod-operator-outputs"

    @property
    def bucket(self) -> GcsfsBucketPath:
        return GcsfsBucketPath(self.bucket_name)

    def _build_subdirectory_path(self, task_id: str) -> str:
        """Builds the subdirectory path for a task output.
        Converts task_id to a subdirectory path by replacing '.' with '/'
        ex: task_id: raw_data_branching.us_oz_secondary_import_branch.pre_import_normalization.raw_data_file_chunking ->
                     raw_data_branching/us_oz_secondary_import_branch/pre_import_normalization/raw_data_file_chunking
        """
        return f"{self.dag_id}/{task_id.replace('.', '/')}/{self.run_id}/"

    def _build_directory_path(
        self, bucket: GcsfsBucketPath, subdirectory_str: str
    ) -> GcsfsDirectoryPath:
        return GcsfsDirectoryPath.from_dir_and_subdir(bucket, subdirectory_str)

    def _build_file_name(self, task_map_index: Optional[str] = None) -> str:
        suffix = f"_{task_map_index}" if task_map_index is not None else ""
        return f"{OUTPUT_BASE_FILE_NAME}{suffix}.json"

    def output_file_path(
        self, task_id: str, task_map_index: Optional[str] = None
    ) -> GcsfsFilePath:
        subdirectory_str = self._build_subdirectory_path(task_id)
        directory_path = self._build_directory_path(self.bucket, subdirectory_str)
        file_name = self._build_file_name(task_map_index)
        return GcsfsFilePath.from_directory_and_file_name(directory_path, file_name)

    def output_file_subdirectory_path_prefix(self, task_id: str) -> str:
        return self._build_subdirectory_path(task_id) + OUTPUT_BASE_FILE_NAME


class KubernetesPodOperatorTaskOutputHandler:
    """Writes/reads KubernetesPodOperator task outputs to/from GCS."""

    def __init__(
        self,
        fs: GCSFileSystem,
        file_path_handler: KubernetesPodOperatorTaskOutputFilePathBuilder,
    ):
        self.fs = fs
        self.file_path_handler = file_path_handler

    @classmethod
    def create_kubernetes_pod_operator_task_output_handler_from_pod_env(
        cls, fs: GCSFileSystem
    ) -> "KubernetesPodOperatorTaskOutputHandler":
        """Creates a KubernetesPodOperatorTaskOutputHandler from the current Airflow KubernetesPodOperator task environment."""
        if not in_airflow_kubernetes_pod():
            raise ValueError(
                "This method should only be used from within an Airflow KubernetesPodOperator task."
            )

        file_path_handler = KubernetesPodOperatorTaskOutputFilePathBuilder(
            project_id=metadata.project_id(),
            dag_id=AirflowKubernetesPodEnvironment.get_dag_id(),
            run_id=AirflowKubernetesPodEnvironment.get_run_id(),
        )
        return cls(fs=fs, file_path_handler=file_path_handler)

    @classmethod
    def create_kubernetes_pod_operator_task_output_handler_from_context(
        cls, context: Any, fs: GCSFileSystem
    ) -> "KubernetesPodOperatorTaskOutputHandler":
        """Creates a KubernetesPodOperatorTaskOutputHandler from an Airflow task context."""
        ti = context["ti"]
        file_path_handler = KubernetesPodOperatorTaskOutputFilePathBuilder(
            project_id=metadata.project_id(), dag_id=ti.dag_id, run_id=ti.run_id
        )
        return cls(fs=fs, file_path_handler=file_path_handler)

    def write_serialized_task_output(
        self,
        output_str: str,
    ) -> None:
        """Writes serialized task output to GCS."""
        if not in_airflow_kubernetes_pod():
            raise ValueError(
                "This method should only be used from within an Airflow KubernetesPodOperator task."
            )
        task_id = AirflowKubernetesPodEnvironment.get_task_id()
        task_map_index = AirflowKubernetesPodEnvironment.get_map_index()

        output_path = self.file_path_handler.output_file_path(task_id, task_map_index)
        self.fs.upload_from_string(output_path, output_str, "text/plain")

    def read_serialized_task_output(self, task_id: str) -> str:
        """Reads serialized task output from GCS."""
        file_path = self.file_path_handler.output_file_path(task_id)
        with self.fs.open(file_path, "r") as f:
            return f.read()

    def read_serialized_mapped_task_output(self, task_id: str) -> List[str]:
        """Reads serialized mapped task output from GCS."""
        file_paths = [
            path
            for path in self.fs.ls(
                self.file_path_handler.bucket_name,
                blob_prefix=self.file_path_handler.output_file_subdirectory_path_prefix(
                    task_id
                ),
            )
            if isinstance(path, GcsfsFilePath)
        ]
        if not file_paths:
            raise ValueError(
                f"No mapped task output found for task_id [{task_id}] in GCS bucket [{self.file_path_handler.bucket_name}] "
                f"with path prefix [{self.file_path_handler.output_file_subdirectory_path_prefix(task_id)}]."
            )

        return_data: List[str] = []
        for file_path in file_paths:
            with self.fs.open(file_path, "r") as f:
                return_data.append(f.read())
        return return_data

    def delete_task_output(self, task_id: str) -> None:
        """Deletes task output from GCS."""
        file_path = self.file_path_handler.output_file_path(task_id)
        try:
            self.fs.delete(file_path)
        except GCSBlobDoesNotExistError:
            logging.info(
                "Attempted to delete file [%s] but it does not exist.", file_path
            )

    def delete_mapped_task_output(self, task_id: str) -> None:
        """Deletes mapped task output from GCS."""
        file_paths = [
            path
            for path in self.fs.ls(
                self.file_path_handler.bucket_name,
                blob_prefix=self.file_path_handler.output_file_subdirectory_path_prefix(
                    task_id
                ),
            )
            if isinstance(path, GcsfsFilePath)
        ]
        for file_path in file_paths:
            try:
                self.fs.delete(file_path)
            except GCSBlobDoesNotExistError:
                logging.info(
                    "Attempted to delete file [%s] but it does not exist.", file_path
                )
