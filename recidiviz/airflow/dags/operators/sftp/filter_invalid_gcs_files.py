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
"""A custom operator that reads a GCS bucket and filters out any invalid files downloaded
from SFTP that should not be uploaded."""
import os
from typing import Any, Dict, List, Optional, Union

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from recidiviz.airflow.dags.sftp.metadata import (
    INGEST_READY_FILE_PATH,
    POST_PROCESSED_FILE_PATH,
    POST_PROCESSED_NORMALIZED_FILE_PATH,
)
from recidiviz.airflow.dags.utils.gcsfs_utils import get_gcsfs_from_hook
from recidiviz.cloud_storage.gcs_file_system_impl import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils.types import assert_type


class FilterInvalidGcsFilesOperator(BaseOperator):
    """A custom operator that reads a GCS bucket and filters out any invalid files
    downloaded from SFTP that should not be uploaded. By the time files reach this
    operator, all zip files should be unzipped."""

    SUPPORTED_EXTENSIONS: List[str] = [".csv", ".txt"]

    def __init__(
        self, collect_all_post_processed_files_task_id: str, **kwargs: Any
    ) -> None:
        self.collect_all_post_processed_files_task_id = (
            collect_all_post_processed_files_task_id
        )

        super().__init__(**kwargs)

    def execute(self, context: Context) -> List[Dict[str, Union[str, int]]]:
        # The prior step to this may have been skipped. If that is the case, the XCOMS
        # is None.
        file_metadatas: Optional[List[Dict[str, Union[str, int]]]] = self.xcom_pull(
            context,
            key="return_value",
            task_ids=self.collect_all_post_processed_files_task_id,
        )
        if file_metadatas:
            gcsfs = get_gcsfs_from_hook()
            return self.filter_invalid_files(gcsfs, file_metadatas)

        return []

    def filter_invalid_files(
        self, gcsfs: GCSFileSystem, file_metadatas: List[Dict[str, Union[str, int]]]
    ) -> List[Dict[str, Union[str, int]]]:
        """Given a set of post processed file paths, if the file path is a directory,
        eliminates any files that do not have the correct supported extensions."""
        final_metadatas: List[Dict[str, Union[str, int]]] = []
        for metadata in file_metadatas:
            post_processed_file_path = assert_type(
                metadata[POST_PROCESSED_FILE_PATH], str
            )
            if gcsfs.is_dir(post_processed_file_path):
                raise ValueError(
                    "We should not be expecting a directory at this point"
                    "in the SFTP DAG"
                )
            if gcsfs.is_file(post_processed_file_path):
                file = GcsfsFilePath.from_absolute_path(post_processed_file_path)
                if self._is_supported_extension(file.abs_path()):
                    # TODO(#53587) Define custom types for operator XCom outputs
                    final_metadatas.append(
                        {
                            **metadata,
                            INGEST_READY_FILE_PATH: file.abs_path(),
                            POST_PROCESSED_NORMALIZED_FILE_PATH: file.blob_name,
                        }
                    )

        return final_metadatas

    def _is_supported_extension(self, path: str) -> bool:
        _, ext = os.path.splitext(path)
        if not ext or ext not in self.SUPPORTED_EXTENSIONS:
            return False
        return True
