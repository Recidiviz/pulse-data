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
"""A customized SFTPToGCSOperator that uses Recidiviz utils and returns output to the DAG."""
import datetime
import logging
import os
from typing import Any, Dict, Union

import pytz
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from recidiviz.airflow.dags.hooks.sftp_hook import RecidivizSFTPHook
from recidiviz.airflow.dags.sftp.metadata import (
    DOWNLOADED_FILE_PATH,
    REMOTE_FILE_PATH,
    SFTP_TIMESTAMP,
)
from recidiviz.airflow.dags.utils.gcsfs_utils import get_gcsfs_from_hook
from recidiviz.cloud_storage.gcs_file_system import BYTES_CONTENT_TYPE
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.sftp_file_contents_handle import SftpFileContentsHandle
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_sftp_download_bucket_path_for_state,
)
from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)


class RecidivizSftpToGcsOperator(BaseOperator):
    """An operator that downloads files from SFTP server to upload to GCS and returns
    the paths of all successfully downloaded files."""

    def __init__(
        self,
        project_id: str,
        region_code: str,
        remote_file_path: str,
        sftp_timestamp: int,
        **kwargs: Any,
    ) -> None:
        self.project_id = project_id
        self.region_code = region_code
        self.remote_file_path = remote_file_path
        self.sftp_timestamp = sftp_timestamp

        self.delegate = SftpDownloadDelegateFactory.build(region_code=self.region_code)
        self.bucket = gcsfs_sftp_download_bucket_path_for_state(region_code, project_id)
        self.download_path = self.build_download_path()

        super().__init__(**kwargs)

    def build_download_path(self) -> GcsfsFilePath:
        """Builds an SFTP path to download to GCS by first replacing dashes with
        underscores within the path, then setting up the path to be only the base
        directory and the base file name. Afterwards, we need the SFTP timestamp
        subdirectory.

        The base directory is needed because sometimes files can be
        uploaded twice within one drop (due to historical vs. updates)."""
        sftp_timestamp_subdir = (
            datetime.datetime.fromtimestamp(self.sftp_timestamp)
            .astimezone(pytz.UTC)
            .strftime("%Y-%m-%dT%H:%M:%S:%f")
        )
        normalized_file_path = os.path.normpath(self.remote_file_path)
        base_directory = os.path.basename(os.path.dirname(normalized_file_path))
        base_file_name = os.path.basename(normalized_file_path)
        return GcsfsFilePath.from_bucket_and_blob_name(
            bucket_name=self.bucket.bucket_name,
            blob_name=os.path.join(
                sftp_timestamp_subdir, base_directory, base_file_name
            ),
        )

    # pylint: disable=unused-argument
    def execute(self, context: Context) -> Dict[str, Union[str, int]]:
        """Downloads a file from SFTP to GCS and returns metadata about the download."""
        sftp_hook = RecidivizSFTPHook(
            ssh_conn_id=f"{self.region_code.lower()}_sftp_conn_id",
            transport_kwargs=self.delegate.get_transport_kwargs(),
        )

        gcsfs = get_gcsfs_from_hook()

        logging.info("Starting to download [%s]", self.remote_file_path)
        gcsfs.upload_from_contents_handle_stream(
            path=self.download_path,
            contents_handle=SftpFileContentsHandle(
                sftp_file_path=self.remote_file_path,
                sftp_client=sftp_hook.get_conn(),
                read_kwargs=self.delegate.get_read_kwargs(),
            ),
            content_type=BYTES_CONTENT_TYPE,
        )
        logging.info(
            "Downloaded [%s] to [%s]",
            self.remote_file_path,
            self.download_path.abs_path(),
        )

        self.delegate.post_download_actions(
            sftp_client=sftp_hook.get_conn(),
            remote_path=self.remote_file_path,
        )

        # TODO(#53587) Define custom types for operator XCom outputs
        return {
            REMOTE_FILE_PATH: self.remote_file_path,
            SFTP_TIMESTAMP: self.sftp_timestamp,
            DOWNLOADED_FILE_PATH: self.download_path.abs_path(),
        }
