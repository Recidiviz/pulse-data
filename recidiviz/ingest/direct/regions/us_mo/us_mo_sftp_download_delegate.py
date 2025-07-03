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
"""Class containing logic for how US_MO SFTP downloads are handled."""
from typing import Any, Dict, List

import paramiko

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)
from recidiviz.ingest.direct.sftp.metadata import (
    DISABLED_ALGORITHMS_KWARG,
    MAX_CONCURRENT_READ_THREADS,
    SFTP_DISABLED_ALGORITHMS_PUB_KEYS,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS


class UsMoSftpDownloadDelegate(BaseSftpDownloadDelegate):
    """Class containing logic for how US_MO SFTP downloads are handled."""

    allow_empty_sftp_directory: bool = False

    CURRENT_ROOT = "/Distribution/doc/co.prod.recidiviz/process"

    def _matches(self, path: str) -> bool:
        """File names have no prefix in MO, so we grab all files in folder."""
        if path is not None:
            return True
        return False

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        """For US_MO, find all file uploads in folder to download."""
        return [
            candidate_path
            for candidate_path in candidate_paths
            if self._matches(candidate_path)
        ]

    def root_directory(self, _: List[str]) -> str:
        """The US_MO server uploads files to the same path every time, so we use the same root each time."""
        return self.CURRENT_ROOT

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        """The US_MO server doesn't require any post-processing."""
        return [downloaded_path.abs_path()]

    def supported_environments(self) -> List[str]:
        return DATA_PLATFORM_GCP_PROJECTS

    def get_transport_kwargs(self) -> Dict[str, Any]:
        return {DISABLED_ALGORITHMS_KWARG: SFTP_DISABLED_ALGORITHMS_PUB_KEYS}

    def get_read_kwargs(self) -> Dict[str, Any]:
        return {MAX_CONCURRENT_READ_THREADS: 1}

    def post_download_actions(
        self, *, sftp_client: paramiko.SFTPClient, remote_path: str
    ) -> None:
        pass
