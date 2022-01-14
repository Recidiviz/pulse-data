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
"""Class containing logic for how US_ME SFTP downloads are handled."""
from datetime import datetime
from typing import List

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.base_sftp_download_delegate import BaseSftpDownloadDelegate


class UsMeSftpDownloadDelegate(BaseSftpDownloadDelegate):
    """Class containing logic for how US_ME SFTP downloads are handled."""

    CURRENT_ROOT = "/Users/recidiviz"
    DIR_PREFIX = "Recidiviz_"
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H%M%S"

    def _matches(self, path: str) -> bool:
        """Directory names must match Recidiviz_YYYY-MM-DDTHHMMSS"""
        if path.startswith(self.DIR_PREFIX):
            try:
                date_str = path.split(self.DIR_PREFIX, 1)[1]
                datetime.strptime(date_str, self.TIMESTAMP_FORMAT)
                return True
            except ValueError:
                return False
        return False

    def root_directory(self, candidate_paths: List[str]) -> str:
        """Returns the path of the root directory new files should be searched in."""
        return self.CURRENT_ROOT

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        """For US_ME, find all file uploads that match Recidiviz_YYYY-MM-DDTHHMMSS format."""
        return [
            candidate_path
            for candidate_path in candidate_paths
            if self._matches(candidate_path)
        ]

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        """For US_ME, the downloaded path is the same that will be streamed directly to GCS."""
        return [downloaded_path.abs_path()]
