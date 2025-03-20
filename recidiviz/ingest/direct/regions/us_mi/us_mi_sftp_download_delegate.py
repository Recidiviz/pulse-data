# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Class containing logic for how US_MI SFTP downloads are handled."""
import io
import re
from datetime import datetime
from typing import Any, Dict, List
from zipfile import ZipFile, is_zipfile

import paramiko
from more_itertools import one

from recidiviz.cloud_storage.gcs_file_system import BYTES_CONTENT_TYPE, GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.io.zip_file_contents_handle import ZipFileContentsHandle
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)
from recidiviz.ingest.direct.sftp.metadata import (
    DISABLED_ALGORITHMS_KWARG,
    SFTP_DISABLED_ALGORITHMS_PUB_KEYS,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION


class UsMiSftpDownloadDelegate(BaseSftpDownloadDelegate):
    """Class containing logic for how US_MI SFTP downloads are handled."""

    # Michigan moves files to a different place once a file has been downloaded prior,
    # so it's very likely to have no files once a complete SFTP download process has run
    # prior.
    allow_empty_sftp_directory: bool = True

    CURRENT_ROOT = "/CORrecidiviz"

    def _matches(self, path: str) -> bool:
        """File names must match ADH_{TABLE_NAME}.zip or ATG_{TABLE}_YYYYMMDD.zip"""
        # Expected format for files from MI's old systems (OMNI and COMPAS)
        OMNI_match_string = r"ADH\_[A-Z_]+(104A)?.zip"
        # Expected format for files from MI's new system (COMS)
        COMS_match_string = r"ATG\_[A-Z_]+(\d{8})?.zip"

        return (
            re.match(OMNI_match_string, path) is not None
            or re.match(COMS_match_string, path) is not None
        )

    def root_directory(self, _: List[str]) -> str:
        """The US_MI server is set to use the root directory for any files that have
        never been downloaded before."""
        return self.CURRENT_ROOT

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        """For US_MI, find all file uploads that match ADH_*_MMDDYYYY.zip format."""
        return [
            candidate_path
            for candidate_path in candidate_paths
            if self._matches(candidate_path)
        ]

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        """For US_MI, all of the files downloaded are zip files that contain one raw file
        each. Therefore, the zip file has to be downloaded into memory and unzipped
        in order to be uploaded to the GCS bucket."""
        zipbytes = io.BytesIO(gcsfs.download_as_bytes(downloaded_path))
        directory = GcsfsDirectoryPath.from_file_path(downloaded_path)
        if is_zipfile(zipbytes):
            with ZipFile(zipbytes, "r") as z:
                content_filename = one(z.namelist())
                # Expected format for files from MI's old systems (OMNI and COMPAS)
                matches_OMNI_content_path = re.match(
                    r"ADH\_[A-Z_]+\_(104A\_)?(\d{8}).csv", content_filename
                )
                # Expected format for files from MI's new system (COMS)
                matches_COMS_content_path = re.match(
                    r"COMS\_[A-Za-z_]+.txt", content_filename
                )
                if matches_OMNI_content_path is not None:
                    date_portion = matches_OMNI_content_path.group(2)
                    _ = datetime.strptime(date_portion, "%m%d%Y")
                    # For files from OMNI, the date is attached to the end of the zip file name, so
                    # we will strip out the date when uploading the file to GCS
                    new_file_name = re.sub(r"\_\d{8}", "", content_filename)
                elif matches_COMS_content_path is not None:
                    # For files from COMS, the date will not be attached to the end of the file name,
                    # so no need to strip anything off and we can use the file name directly
                    new_file_name = content_filename
                else:
                    raise ValueError(
                        f"Unexpected content file name in zip file: {content_filename}"
                    )

                new_path = GcsfsFilePath.from_directory_and_file_name(
                    directory, new_file_name
                )

                gcsfs.upload_from_contents_handle_stream(
                    path=new_path,
                    contents_handle=ZipFileContentsHandle(content_filename, z),
                    content_type=BYTES_CONTENT_TYPE,
                )
                return [new_path.abs_path()]
        return [downloaded_path.abs_path()]

    def supported_environments(self) -> List[str]:
        return [GCP_PROJECT_PRODUCTION]

    def get_transport_kwargs(self) -> Dict[str, Any]:
        return {DISABLED_ALGORITHMS_KWARG: SFTP_DISABLED_ALGORITHMS_PUB_KEYS}

    def get_read_kwargs(self) -> Dict[str, Any]:
        return {}

    def post_download_actions(
        self, *, sftp_client: paramiko.SFTPClient, remote_path: str
    ) -> None:
        pass
