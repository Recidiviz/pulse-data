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
"""Defines a class that can be used to access contents of a file on an SFTP server."""

from contextlib import contextmanager
from typing import Iterator

from paramiko import SFTPFile

from recidiviz.common.io.file_contents_handle import FileContentsHandle
from recidiviz.common.sftp_connection import RecidivizSftpConnection


class SftpFileContentsHandle(FileContentsHandle[bytes, SFTPFile]):
    """A class that can be used to access contents of a file on an SFTP server."""

    def __init__(self, sftp_file_path: str, sftp_connection: RecidivizSftpConnection):
        self.sftp_file_path = sftp_file_path
        self.sftp_connection = sftp_connection

    def get_contents_iterator(self) -> Iterator[bytes]:
        with self.open() as f:
            while line := f.readline():
                yield line

    @contextmanager
    def open(self, mode: str = "r") -> Iterator[SFTPFile]:  # type: ignore
        with self.sftp_connection.open(remote_file=self.sftp_file_path, mode=mode) as f:
            yield f
