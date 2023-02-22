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
"""A custom GCSTransformOperator that uses Recidiviz utils, returns output to the DAG and uses
the SFTP delegate to stream the output back to GCS."""
from typing import Any, Dict, List, Union

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.context import Context

from recidiviz.cloud_storage.gcs_file_system_impl import GCSFileSystemImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_sftp_download_bucket_path_for_state,
)
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)

# Custom Airflow operators in the recidiviz.airflow.dags.operators package are imported into the
# Cloud Composer environment at the top-level. However, for unit tests, we still need to
# import the recidiviz-top-level.
# pylint: disable=ungrouped-imports
try:
    from sftp.metadata import (  # type: ignore
        DOWNLOADED_FILE_PATH,
        POST_PROCESSED_FILE_PATH,
        REMOTE_FILE_PATH,
        SFTP_TIMESTAMP,
    )
except ImportError:
    from recidiviz.airflow.dags.sftp.metadata import (
        DOWNLOADED_FILE_PATH,
        POST_PROCESSED_FILE_PATH,
        REMOTE_FILE_PATH,
        SFTP_TIMESTAMP,
    )


class RecidivizGcsFileTransformOperator(BaseOperator):
    """An operator that post processes downloaded files from SFTP into the current GCS
    folder and returns the paths of all successfully processed files."""

    def __init__(
        self,
        project_id: str,
        region_code: str,
        remote_file_path: str,
        sftp_timestamp: int,
        downloaded_file_path: str,
        **kwargs: Any,
    ) -> None:
        self.project_id = project_id
        self.region_code = region_code
        self.delegate = SftpDownloadDelegateFactory.build(region_code=region_code)
        self.remote_file_path = remote_file_path
        self.sftp_timestamp = sftp_timestamp
        self.downloaded_file_path = downloaded_file_path

        self.bucket = gcsfs_sftp_download_bucket_path_for_state(region_code, project_id)

        super().__init__(**kwargs)

    # pylint:disable=unused-argument
    def execute(self, context: Context) -> List[Dict[str, Union[str, float]]]:  # type: ignore
        gcs_hook = GCSHook()
        gcs_file_system = GCSFileSystemImpl(client=gcs_hook.get_conn())

        post_processed_paths = self.delegate.post_process_downloads(
            downloaded_path=GcsfsFilePath.from_absolute_path(self.downloaded_file_path),
            gcsfs=gcs_file_system,
        )

        return [
            {
                REMOTE_FILE_PATH: self.remote_file_path,
                SFTP_TIMESTAMP: self.sftp_timestamp,
                DOWNLOADED_FILE_PATH: self.downloaded_file_path,
                POST_PROCESSED_FILE_PATH: post_processed_path,
            }
            for post_processed_path in post_processed_paths
        ]
