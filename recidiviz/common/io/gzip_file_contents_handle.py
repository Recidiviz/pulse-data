# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Defines a class that can be used to access the decompressed contents of a gzip
stream."""
from contextlib import contextmanager
from gzip import GzipFile
from typing import IO, Iterator

from recidiviz.common.io.file_contents_handle import FileContentsHandle


class GzipFileContentsHandle(FileContentsHandle[bytes, GzipFile]):
    """A class that can be used to access the decompressed contents of a gzip stream.

    The compressed stream must be seekable: each open() rewinds it so that a retried
    upload re-reads the contents from the start.
    """

    def __init__(self, compressed_stream: IO[bytes]):
        if not compressed_stream.seekable():
            raise ValueError(
                f"Expected a seekable compressed stream, found [{type(compressed_stream)}]"
            )
        self.compressed_stream = compressed_stream

    @contextmanager
    def open(self, mode: str = "rb") -> Iterator[GzipFile]:
        if mode != "rb":
            raise ValueError(f"Unsupported mode [{mode}], expected [rb]")
        self.compressed_stream.seek(0)
        with GzipFile(fileobj=self.compressed_stream, mode="rb") as gz_file:
            yield gz_file
