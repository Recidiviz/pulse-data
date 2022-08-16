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
"""Defines a class that can be used to access temporary file contents."""
from contextlib import contextmanager
from typing import IO, Iterator

from werkzeug.datastructures import FileStorage

from recidiviz.common.io.file_contents_handle import FileContentsHandle


class FlaskFileStorageContentsHandle(FileContentsHandle[str, IO]):
    """A class that can be used to access flask FileStorage contents."""

    def __init__(self, file_storage: FileStorage):
        self.file_storage = file_storage

    def get_contents_iterator(self) -> Iterator[str]:
        """Lazy function (generator) to read a file line by line."""
        with self.open() as f:
            while line := f.readline():
                yield line

    @contextmanager
    def open(self, mode: str = "r") -> Iterator[IO]:
        self.file_storage.stream.seek(
            0  # Make sure that the stream is starting at the beginning of the file.
        )
        yield self.file_storage.stream
