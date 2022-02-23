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
"""Class containing logic for how US_PA SFTP downloads are handled."""
import io
import re
from typing import List
from zipfile import ZipFile, is_zipfile

from recidiviz.cloud_storage.gcs_file_system import BYTES_CONTENT_TYPE, GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.io.zip_file_contents_handle import ZipFileContentsHandle
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)


class UsPaSftpDownloadDelegate(BaseSftpDownloadDelegate):
    """Class containing logic for how US_PA SFTP downloads are handled."""

    CURRENT_ROOT = "."
    DELIMITER = "-"
    PREFIX = "Recidiviz"

    def _matches(self, path: str) -> bool:
        """File names must match Recidiviz-YYYYMMDD_to_YYYYMMDD_-_generated_YYYYMMDD_HHmmss.zip"""
        if path.startswith(self.PREFIX):
            try:
                _, date_range_str, generated_timestamp = path.split(self.DELIMITER)
                matches_date = re.match(r"\d{8}\_to\_\d{8}\_", date_range_str)
                matches_timestamp = re.match(
                    r"\_generated\_\d{8}\_\d{6}.zip", generated_timestamp
                )
                return matches_date is not None and matches_timestamp is not None
            except Exception:
                return False
        return False

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        """For US_PA, find all file uploads that match
        Recidiviz-YYYYMMDD_to_YYYYMMDD_-_generated_YYYYMMDD_HHmmss.zip format."""
        return [
            candidate_path
            for candidate_path in candidate_paths
            if self._matches(candidate_path)
        ]

    def root_directory(self, _: List[str]) -> str:
        """The US_PA server is set to use the root directory, so candidate_paths is
        effectively ignored."""
        return self.CURRENT_ROOT

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        """For US_PA, the initial file downloaded is a zip file which contains the
        individual raw files. Therefore the zip file has be downloaded into memory
        and unzipped in order to be uploaded to the GCS bucket."""
        post_processed_files = []
        zipbytes = io.BytesIO(gcsfs.download_as_bytes(downloaded_path))
        directory = GcsfsDirectoryPath.from_file_path(downloaded_path)
        if is_zipfile(zipbytes):
            with ZipFile(zipbytes, "r") as z:
                for content_filename in z.namelist():
                    new_path = GcsfsFilePath.from_directory_and_file_name(
                        directory, content_filename
                    )
                    gcsfs.upload_from_contents_handle_stream(
                        path=new_path,
                        contents_handle=ZipFileContentsHandle(content_filename, z),
                        content_type=BYTES_CONTENT_TYPE,
                    )
                    post_processed_files.append(new_path.abs_path())
            return post_processed_files

        return [downloaded_path.abs_path()]
