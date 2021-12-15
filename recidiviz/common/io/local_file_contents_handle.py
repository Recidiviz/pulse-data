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
"""Defines a class that can be used to access local file contents."""
import os
from contextlib import contextmanager
from typing import IO, Iterator

from recidiviz.common.io.file_contents_handle import FileContentsHandle


class LocalFileContentsHandle(FileContentsHandle[str, IO]):
    """A class that can be used to access local file contents."""

    def __init__(self, local_file_path: str, cleanup_file: bool = True):
        self.local_file_path = local_file_path
        self.cleanup_file = cleanup_file

    def get_contents_iterator(self) -> Iterator[str]:
        """Lazy function (generator) to read a file line by line."""
        with self.open() as f:
            while line := f.readline():
                yield line

    @contextmanager
    def open(self, mode: str = "r") -> Iterator[IO]:
        encoding = None if "b" in mode else "utf-8"
        with open(self.local_file_path, mode=mode, encoding=encoding) as f:
            yield f

    def __del__(self) -> None:
        """This ensures that the file contents on local disk are deleted when
        this handle is garbage collected.
        """
        if self.cleanup_file and os.path.exists(self.local_file_path):
            os.remove(self.local_file_path)
