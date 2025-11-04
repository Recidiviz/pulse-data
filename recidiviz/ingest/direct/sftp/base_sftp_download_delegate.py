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
"""Base delegate class for handling SFTP downloads"""
import abc
from typing import Any, Dict, List, Set, Tuple

import paramiko

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class BaseSftpDownloadDelegate(abc.ABC):
    """Contains the base class to handle region-specific SFTP downloading logic."""

    @abc.abstractmethod
    def root_directory(self, candidate_paths: List[str]) -> str:
        """Returns the path of the root directory new files should be searched in."""

    @abc.abstractmethod
    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        """Given a list of paths discovered in the SFTP, returns a filtered list including only the values
        that are to be downloaded."""

    @abc.abstractmethod
    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        """Should be implemented if any of the downloaded values need post-processing prior to sending to ingest
        (e.g. unzipping a zip file). Returns the absolute path of the post-processed download(s).
        """

    @abc.abstractmethod
    def supported_environments(self) -> List[str]:
        """Should be implemented to indicate which environments the SFTP download should
        occur."""

    @abc.abstractmethod
    def get_transport_kwargs(self) -> Dict[str, Any]:
        """State-specific kwargs to be passed to paramiko's Transport client to help
        configure connection settings during initial SFTP connection
        """

    @abc.abstractmethod
    def get_read_kwargs(self) -> Dict[str, Any]:
        """State-specific kwargs to be passed to paramiko's SFTPClient.getfo to help
        configure file reading settings.
        """

    @abc.abstractmethod
    def post_download_actions(
        self, *, sftp_client: paramiko.SFTPClient, remote_path: str
    ) -> None:
        """Should be implemented to execute any actions needed after a remote file has
        been successfully downloaded to a GCS.
        """

    @property
    @abc.abstractmethod
    def allow_empty_sftp_directory(self) -> bool:
        """Whether or not we are concerned if we find an empty SFTP directory. In
        states where files are moved or deleted after we download them, we would expect
        to find an empty directory. However, in most states, an empty directory likely
        indicates that no files have every been uploaded, we have a connection issue,
        or we have the incorrect root directory.
        """

    def validate_file_discovery(
        self,
        discovered_file_to_timestamp_set: Set[Tuple[str, int]],
        already_downloaded_file_to_timestamp_set: Set[Tuple[str, int]],
    ) -> None:
        """Validates that the discovered files are appropriate for download.

        Args:
            discovered_file_to_timestamp_set: Set of (remote_file_path, sftp_timestamp)
                tuples found on SFTP
            already_downloaded_file_to_timestamp_set: Set of
                (remote_file_path, sftp_timestamp) tuples that have already been
                downloaded according to the metadata table

        Raises:
            ValueError: If the discovered files are invalid for state-specific reasons
        """
        # Default implementation does nothing - states can override for custom validation
