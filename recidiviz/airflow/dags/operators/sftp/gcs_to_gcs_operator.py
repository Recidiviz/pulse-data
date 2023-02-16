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

import pytz
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.context import Context

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.ingest.direct.direct_ingest_bucket_name_utils import (
    INGEST_SFTP_BUCKET_SUFFIX,
    build_ingest_bucket_name,
)
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_path,
)

# Custom Airflow operators in the recidiviz.airflow.dags.operators package are imported into the
# Cloud Composer environment at the top-level. However, for unit tests, we still need to
# import the recidiviz-top-level.
# pylint: disable=ungrouped-imports
try:
    from sftp.metadata import (  # type: ignore
        DOWNLOADED_FILE_PATH,
        INGEST_READY_FILE_PATH,
        POST_PROCESSED_FILE_PATH,
        POST_PROCESSED_NORMALIZED_FILE_PATH,
        REMOTE_FILE_PATH,
        SFTP_TIMESTAMP,
        UPLOADED_FILE_PATH,
    )
except ImportError:
    from recidiviz.airflow.dags.sftp.metadata import (
        DOWNLOADED_FILE_PATH,
        INGEST_READY_FILE_PATH,
        POST_PROCESSED_FILE_PATH,
        POST_PROCESSED_NORMALIZED_FILE_PATH,
        REMOTE_FILE_PATH,
        SFTP_TIMESTAMP,
        UPLOADED_FILE_PATH,
    )


class SFTPGcsToGcsOperator(GCSToGCSOperator):
    """A subclass of GCSToGCSOperator that returns successfully uploaded files to the DAG."""

    def __init__(
        self,
        project_id: str,
        region_code: str,
        remote_file_path: str,
        sftp_timestamp: int,
        downloaded_file_path: str,
        post_processed_file_path: str,
        post_processed_normalized_file_path: str,
        ingest_ready_file_path: str,
        **kwargs: Any,
    ):
        self.project_id = project_id
        self.region_code = region_code
        self.remote_file_path = remote_file_path
        self.sftp_timestamp = sftp_timestamp
        self.downloaded_file_path = downloaded_file_path
        self.post_processed_file_path = post_processed_file_path
        self.post_processed_normalized_file_path = post_processed_normalized_file_path
        self.ingest_ready_file_path = ingest_ready_file_path

        # TODO(#17283): Change to original buckets when SFTP is switched over
        self.sftp_bucket = GcsfsBucketPath(
            f"{build_ingest_bucket_name(project_id=self.project_id, region_code=self.region_code, suffix=INGEST_SFTP_BUCKET_SUFFIX)}-test"
        )
        self.ingest_bucket = GcsfsBucketPath(
            f"{build_ingest_bucket_name(project_id=self.project_id, region_code=self.region_code, suffix='')}-test"
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
        normalized_file_name = os.path.basename(
            to_normalized_unprocessed_raw_file_path(
                self.ingest_ready_file_path,
                datetime.datetime.fromtimestamp(self.sftp_timestamp).astimezone(
                    pytz.UTC
                ),
            )
        )
        return GcsfsFilePath.from_directory_and_file_name(
            self.ingest_bucket, normalized_file_name
        )

    def execute(self, context: Context) -> Dict[str, Union[str, int]]:
        super().execute(context)
        return {
            REMOTE_FILE_PATH: self.remote_file_path,
            SFTP_TIMESTAMP: self.sftp_timestamp,
            DOWNLOADED_FILE_PATH: self.downloaded_file_path,
            POST_PROCESSED_FILE_PATH: self.post_processed_file_path,
            POST_PROCESSED_NORMALIZED_FILE_PATH: self.post_processed_normalized_file_path,
            INGEST_READY_FILE_PATH: self.ingest_ready_file_path,
            UPLOADED_FILE_PATH: self.uploaded_file_path.abs_path(),
        }
