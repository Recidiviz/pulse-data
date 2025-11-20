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
"""A subclass of GCSToGCSOperator that returns output to the DAG."""
import datetime
import os
from typing import Any, Dict, Union

from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.context import Context

from recidiviz.airflow.dags.sftp.metadata import (
    POST_PROCESSED_NORMALIZED_FILE_PATH,
    REMOTE_FILE_PATH,
    UPLOADED_FILE_PATH,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_sftp_download_bucket_path_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class SFTPGcsToGcsOperator(GCSToGCSOperator):
    """A subclass of GCSToGCSOperator that returns successfully uploaded files to the DAG."""

    def __init__(
        self,
        project_id: str,
        region_code: str,
        remote_file_path: str,
        post_processed_normalized_file_path: str,
        **kwargs: Any,
    ):
        self.project_id = project_id
        self.region_code = region_code
        self.remote_file_path = remote_file_path
        self.post_processed_normalized_file_path = post_processed_normalized_file_path

        self.sftp_bucket = gcsfs_sftp_download_bucket_path_for_state(
            region_code, project_id
        )
        self.ingest_bucket = gcsfs_direct_ingest_bucket_for_state(
            region_code=region_code,
            ingest_instance=DirectIngestInstance.PRIMARY,
            project_id=project_id,
        )

        self.uploaded_file_path = self.build_upload_path()

        super().__init__(
            source_bucket=self.sftp_bucket.bucket_name,
            source_object=self.post_processed_normalized_file_path,
            destination_bucket=self.ingest_bucket.bucket_name,
            destination_object=self.uploaded_file_path.file_name,
            move_object=False,
            **kwargs,
        )

    def build_upload_path(self) -> GcsfsFilePath:
        date_str = self.post_processed_normalized_file_path.split("/")[0]
        normalized_file_name = os.path.basename(
            to_normalized_unprocessed_raw_file_path(
                self.post_processed_normalized_file_path,
                datetime.datetime.fromisoformat(date_str),
            )
        )
        return GcsfsFilePath.from_directory_and_file_name(
            self.ingest_bucket, normalized_file_name
        )

    def execute(self, context: Context) -> Dict[str, Union[str, int]]:
        super().execute(context)
        # TODO(#53587) Define custom types for operator XCom outputs
        return {
            REMOTE_FILE_PATH: self.remote_file_path,
            POST_PROCESSED_NORMALIZED_FILE_PATH: self.post_processed_normalized_file_path,
            UPLOADED_FILE_PATH: self.uploaded_file_path.abs_path(),
        }
