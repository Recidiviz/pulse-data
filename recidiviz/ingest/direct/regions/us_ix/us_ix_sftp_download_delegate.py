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
"""Class containing logic for how US_IX SFTP downloads are handled."""
import datetime
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
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS


class UsIxSftpDownloadDelegate(BaseSftpDownloadDelegate):
    """Class containing logic for how US_IX SFTP downloads are handled."""

    allow_empty_sftp_directory: bool = False

    CURRENT_ROOT = "/"
    FILE_PREFIX = "recidiviz-atlas-cmd-"

    def _matches(self, path: str) -> bool:
        """Folder names should match recidiviz-atlas-cmd-YYYY-MM-DD-HH-MM.

        Sometimes the IDOC script drops the "HH-MM" suffix, so we accept anything that
        starts with recidiviz-atlas-cmd-YYYY-MM-DD-.
        """
        if path.startswith(self.FILE_PREFIX):
            date_str = path.split(self.FILE_PREFIX, 1)[1]
            date_str = date_str[: len("YYYY-MM-DD-")]
            try:
                datetime.datetime.strptime(date_str, "%Y-%m-%d-")
                return True
            except ValueError:
                return False
        return False

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        """Find all folders that match recidiviz-atlas-cmd-YYYY-MM-DD-HH-MM."""
        return list(filter(self._matches, candidate_paths))

    def root_directory(self, _: List[str]) -> str:
        """The US_IX server is set to use the root directory, so candidate_paths is effectively ignored."""
        return self.CURRENT_ROOT

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        """The US_IX server doesn't require any post-processing."""
        return [downloaded_path.abs_path()]

    def supported_environments(self) -> List[str]:
        return DATA_PLATFORM_GCP_PROJECTS

    def get_transport_kwargs(self) -> Dict[str, Any]:
        return {DISABLED_ALGORITHMS_KWARG: SFTP_DISABLED_ALGORITHMS_PUB_KEYS}

    def get_read_kwargs(self) -> Dict[str, Any]:
        return {}

    def post_download_actions(
        self, *, sftp_client: paramiko.SFTPClient, remote_path: str
    ) -> None:
        pass

    def ingest_ready_files_have_stabilized(
        self, _ingest_ready_normalized_file_paths: List[str]
    ) -> bool:
        """Idaho-specific stabilization check for ingest-ready files."""
        return True
