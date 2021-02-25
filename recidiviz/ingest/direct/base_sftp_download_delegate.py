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

from typing import List

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
    def post_process_downloads(self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem) -> str:
        """Should be implemented if any of the downloaded values need post-processing prior to sending to ingest
        (e.g. unzipping a zip file). Returns the absolute path of the post-processed download."""
