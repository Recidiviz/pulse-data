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
"""Class containing logic for how US_CA SFTP downloads are handled."""
from datetime import datetime
from pathlib import Path
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


class UsCaSftpDownloadDelegate(BaseSftpDownloadDelegate):
    """Class containing logic for how US_CA SFTP downloads are handled."""

    allow_empty_sftp_directory: bool = False

    CURRENT_ROOT = "/"
    TIMESTAMP_FORMAT = "%B %d, %Y"
    POST_FIX = " data"

    def _matches(self, path: str) -> bool:
        """File names must match "/<Month day, Year> Data/{PAROLE,INCUSTODY,Delta}/"""
        path_obj = Path(path)

        # Directory names must match "/<Month day, Year> Data/"
        folder = path_obj.name.lower()

        # Checks the "<Month day, Year> Data" part
        if folder.endswith(self.POST_FIX):
            datetime_str = folder.removesuffix(self.POST_FIX)
            try:
                datetime.strptime(datetime_str, self.TIMESTAMP_FORMAT)
            except ValueError:
                return False
        else:
            return False

        return True

    def root_directory(self, candidate_paths: List[str]) -> str:
        """Returns the path of the root directory new files should be searched in."""
        return self.CURRENT_ROOT

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        """For US_CA, find all file uploads that match a path like "/<Month day, Year> Data/{PAROLE,INCUSTODY,Delta}/*.txt"""
        return [
            candidate_path
            for candidate_path in candidate_paths
            if self._matches(candidate_path)
        ]

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        """For US_CA, the downloaded path is the same that will be streamed directly to GCS."""
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
        """California-specific stabilization check for ingest-ready files."""
        return True
