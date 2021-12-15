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
"""Defines class that can be used to access contents of a local zip file."""
from contextlib import contextmanager
from typing import Iterator
from zipfile import ZipExtFile, ZipFile

from recidiviz.common.io.file_contents_handle import FileContentsHandle


class ZipFileContentsHandle(FileContentsHandle[bytes, ZipExtFile]):
    """A class that can be used to access contents of file within a local zip file."""

    def __init__(self, zip_internal_file_path: str, zip_file: ZipFile):
        self.zip_internal_file_path = zip_internal_file_path
        self.zip_file = zip_file

    def get_contents_iterator(self) -> Iterator[bytes]:
        with self.open() as f:
            while line := f.readline():
                yield line

    @contextmanager
    def open(self, _: str = "r") -> Iterator[ZipExtFile]:  # type: ignore
        with self.zip_file.open(self.zip_internal_file_path, mode="r") as f:
            yield f  # type: ignore
