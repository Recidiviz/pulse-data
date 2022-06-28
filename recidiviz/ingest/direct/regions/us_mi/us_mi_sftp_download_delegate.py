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
from typing import List
from zipfile import ZipFile, is_zipfile

from more_itertools import one

from recidiviz.cloud_storage.gcs_file_system import BYTES_CONTENT_TYPE, GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.io.zip_file_contents_handle import ZipFileContentsHandle
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)


class UsMiSftpDownloadDelegate(BaseSftpDownloadDelegate):
    """Class containing logic for how US_MI SFTP downloads are handled."""

    CURRENT_ROOT = "."

    def _matches(self, path: str) -> bool:
        """File names must match ADH_{TABLE_NAME}.zip"""
        matches_overall_path = re.match(r"ADH\_[A-Z_]+.zip", path)
        return matches_overall_path is not None

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
                matches_content_path = re.match(
                    r"ADH\_[A-Z_]+\_(\d{8}).csv", content_filename
                )
                if not matches_content_path:
                    raise ValueError(
                        f"Unexpected content file name in zip file: {content_filename}"
                    )
                date_portion = matches_content_path.group(1)
                try:
                    _ = datetime.strptime(date_portion, "%m%d%Y")
                except ValueError as e:
                    raise ValueError(
                        f"Unexpected content file name in zip file: {content_filename}"
                    ) from e
                # For MI, the date is attached to the end of the zip file name, so
                # we will strip out the date when uploading the file to GCS
                new_file_name = re.sub(r"\_\d{8}", "", content_filename)
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
