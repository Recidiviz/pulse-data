# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Shared testing utilities for sftp operators"""
from typing import Any, Dict, List

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.sftp.base_sftp_download_delegate import (
    BaseSftpDownloadDelegate,
)

TEST_PROJECT_ID = "recidiviz-testing"


class FakeSftpDownloadDelegate(BaseSftpDownloadDelegate):
    def root_directory(self, candidate_paths: List[str]) -> str:
        return "/"

    def filter_paths(self, candidate_paths: List[str]) -> List[str]:
        return candidate_paths

    def supported_environments(self) -> List[str]:
        return [TEST_PROJECT_ID]

    def post_process_downloads(
        self, downloaded_path: GcsfsFilePath, gcsfs: GCSFileSystem
    ) -> List[str]:
        if "fail" in downloaded_path.abs_path():
            return []
        return [downloaded_path.abs_path()]

    def get_transport_kwargs(self) -> Dict[str, Any]:
        return {}
