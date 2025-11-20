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
import logging
from typing import Any, Dict, List, Union

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from recidiviz.airflow.dags.sftp.metadata import (
    DOWNLOADED_FILE_PATH,
    POST_PROCESSED_FILE_PATH,
    REMOTE_FILE_PATH,
    SFTP_TIMESTAMP,
)
from recidiviz.airflow.dags.utils.gcsfs_utils import get_gcsfs_from_hook
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_sftp_download_bucket_path_for_state,
)
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
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
        gcs_file_system = get_gcsfs_from_hook()

        logging.info("Starting post-processing of [%s]", self.downloaded_file_path)
        post_processed_paths = self.delegate.post_process_downloads(
            downloaded_path=GcsfsFilePath.from_absolute_path(self.downloaded_file_path),
            gcsfs=gcs_file_system,
        )
        logging.info(
            "Post-processed [%s] to [%s]",
            self.downloaded_file_path,
            post_processed_paths,
        )
        if post_processed_paths:
            # TODO(#53587) Define custom types for operator XCom outputs
            return [
                {
                    REMOTE_FILE_PATH: self.remote_file_path,
                    SFTP_TIMESTAMP: self.sftp_timestamp,
                    DOWNLOADED_FILE_PATH: self.downloaded_file_path,
                    POST_PROCESSED_FILE_PATH: post_processed_path,
                }
                for post_processed_path in post_processed_paths
            ]
        raise ValueError(f"{self.downloaded_file_path} failed to post-process.")
