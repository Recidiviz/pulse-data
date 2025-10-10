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
"""Class containing logic for how US_TX SFTP downloads are handled."""
import re
from typing import Any, Dict, List

import paramiko

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)
from recidiviz.ingest.direct.sftp.metadata import (
    DISABLED_ALGORITHMS_KWARG,
    SFTP_DISABLED_ALGORITHMS_PUB_KEYS,
)
from recidiviz.ingest.direct.sftp.remote_file_cleanup_mixin import (
    RemoteFileCleanupMixin,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION


class UsTxSftpDownloadDelegate(BaseSftpDownloadDelegate, RemoteFileCleanupMixin):
    """Class containing logic for how US_TX SFTP downloads are handled."""

    # Texas asks us to delete files once we've downloaded them, so it's very likely to
    # have no files once a complete SFTP download process has run prior.
    allow_empty_sftp_directory: bool = True

    CURRENT_ROOT = "/"
    # Define the constant prefix and suffix
    file_prefix: str = "Recidiviz_"
    file_extension: str = "csv"

    def _matches(self, path: str) -> bool:
        """Only accept files that match the pattern: Recidiviz_<name>_<date>_<timestamp>.csv
        eg. /Recidiviz_File_2024_12_27_01_01_01.csv"""

        # Regex pattern to match the desired filename format with a date
        pattern = r"^Recidiviz_(.+?)_(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})\.csv$"

        # Check if the file matches the pattern
        if path and re.match(pattern, path):
            return True
        return False

    def root_directory(self, candidate_paths: List[str]) -> str:
        """Returns the path of the root directory new files should be searched in."""
        return self.CURRENT_ROOT

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        """For US_TX, find all file uploads that match a path like "Recidiviz_{File_Name}_{timestamp}.csv"""
        return [
            candidate_path
            for candidate_path in candidate_paths
            if self._matches(candidate_path)
        ]

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        """Rename downloaded files in GCS by removing "Recidiviz" prefix and date timestamp suffix."""

        # Regex pattern to match filenames starting with "Recidiviz_", ending with ".csv",
        # and potentially containing a timestamp before the extension.
        filename_pattern = (
            r"^Recidiviz_([^_]+)(_\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})?\.csv$"
        )

        # Match the file name with the pattern
        match = re.match(filename_pattern, downloaded_path.file_name)

        if match:
            # Extract the base filename without the "Recidiviz_" prefix and timestamp
            base_filename = match.group(
                1
            )  # This is the part of the filename after "Recidiviz_"

            # Create the new filename by appending the file extension
            new_filename = f"{base_filename}.{self.file_extension}"

            # Create a new GcsfsFilePath with the new file name
            new_filepath = GcsfsFilePath.with_new_file_name(
                downloaded_path, new_filename
            )

            # Rename the file in GCS
            gcsfs.mv(downloaded_path, new_filepath)

            # Return the new path of the renamed file
            return [new_filepath.abs_path()]

        # If the filename does not match the expected pattern, return the original path (no renaming)
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
        """Texas has requested that we remove files from their SFTP server after
        downloading them as they don't want transfers piling up as time goes on.
        """
        self.remove_remote_file(
            sftp_client=sftp_client,
            remote_path=remote_path,
            supported_environments=self.supported_environments(),
        )
