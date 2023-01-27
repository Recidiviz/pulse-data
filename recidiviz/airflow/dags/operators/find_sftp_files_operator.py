# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Operator that reads from SFTP and finds the files to be downloaded."""
import os
import stat
from collections import deque
from typing import Any, Dict, List, Union

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from recidiviz.ingest.direct.sftp.sftp_download_delegate_factory import (
    SftpDownloadDelegateFactory,
)

# Custom Airflow operators in the recidiviz.airflow.dags.operators package are imported into the
# Cloud Composer environment at the top-level. However, for unit tests, we still need to
# import the recidiviz-top-level.
# pylint: disable=ungrouped-imports
try:
    from hooks.sftp_hook import RecidivizSFTPHook  # type: ignore
except ImportError:
    from recidiviz.airflow.dags.hooks.sftp_hook import RecidivizSFTPHook


class FindSftpFilesOperator(BaseOperator):
    """Operator that reads from SFTP and finds files to be downloaded based on criteria."""

    def __init__(self, state_code: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.state_code = state_code
        self.delegate = SftpDownloadDelegateFactory.build(region_code=self.state_code)

    # pylint: disable=unused-argument
    def execute(self, context: Context) -> List[Dict[str, Union[str, int]]]:
        sftp_hook = RecidivizSFTPHook(
            ssh_conn_id=f"{self.state_code.lower()}_sftp_conn_id"
        )

        return self._get_paths_to_download_from_sftp(sftp_hook)

    def _get_paths_to_download_from_sftp(
        self, sftp_hook: RecidivizSFTPHook
    ) -> List[Dict[str, Union[str, int]]]:
        """Obtains paths to download based on configured root directories and a depth-first
        search through the SFTP server.

        We return a list of metadata that contains two fields:
            - file - the remote file path on SFTP
            - timestamp - a float indicating the SFTP mtime"""
        remote_dirs = sftp_hook.list_directory(".")
        root = self.delegate.root_directory(remote_dirs)
        dirs_with_attributes = sftp_hook.get_conn().listdir_attr(root)
        paths = {}
        file_modes_of_paths = {}
        for sftp_attr in dirs_with_attributes:
            paths[sftp_attr.filename] = sftp_attr.st_mtime
            file_modes_of_paths[sftp_attr.filename] = sftp_attr.st_mode

        paths_to_download = self.delegate.filter_paths(list(paths.keys()))
        files_to_download_with_timestamps: List[Dict[str, Any]] = []

        for path in paths_to_download:
            file_timestamp = paths[path]
            file_mode = file_modes_of_paths[path]
            if file_mode and stat.S_ISREG(file_mode):
                files_to_download_with_timestamps.append(
                    {"file": os.path.join(root, path), "timestamp": file_timestamp}
                )
            else:
                inner_paths = deque(
                    [
                        os.path.join(root, path, inner_path)
                        for inner_path in sftp_hook.list_directory(path)
                    ]
                )
                while len(inner_paths) > 0:
                    current_path = inner_paths.popleft()
                    sftp_attr_of_current_path = sftp_hook.get_conn().stat(current_path)
                    if sftp_attr_of_current_path.st_mode and stat.S_ISDIR(
                        sftp_attr_of_current_path.st_mode
                    ):
                        for entry in sftp_hook.list_directory(current_path):
                            inner_paths.append(os.path.join(current_path, entry))
                    else:
                        files_to_download_with_timestamps.append(
                            {"file": current_path, "timestamp": file_timestamp}
                        )

        return files_to_download_with_timestamps
