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
"""Class containing logic for how US_ID SFTP downloads are handled."""
import datetime
from typing import List

from recidiviz.ingest.direct.controllers.download_files_from_sftp import BaseSftpDownloadDelegate


class UsIdSftpDownloadDelegate(BaseSftpDownloadDelegate):
    """Class containing logic for how US_ID SFTP downloads are handled."""

    CURRENT_ROOT = "."
    FILE_PREFIX = "Recidiviz"

    def _matches(self, path: str) -> bool:
        """File names should match the RecidivizYYYYMMDD format."""
        if path.startswith(self.FILE_PREFIX):
            try:
                date_str = path.split(self.FILE_PREFIX, 1)[1]
                datetime.datetime.strptime(date_str, "%Y%m%d")
                return True
            except ValueError:
                return False
        return False

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        """For US_ID, find all file uploads that match RecidivizYYYYMMDD format."""
        return [candidate_path for candidate_path in candidate_paths if self._matches(candidate_path)]

    def root_directory(self, _: List[str]) -> str:
        """The US_ID server is set to use the root directory, so candidate_paths is effectively ignored."""
        return self.CURRENT_ROOT

    def post_process_downloads(self, _: str) -> None:
        """The US_ID server doesn't require any post-processing."""
        return
