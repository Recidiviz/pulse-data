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
"""Code for finding byte offsets for row-safe chunk boundaries for Google Cloud Storage CSV files. """
import io
from typing import Annotated, List, Optional

import attr
from annotated_types import Gt

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.csv import (
    DEFAULT_CSV_ENCODING,
    DEFAULT_CSV_LINE_TERMINATOR,
)

DEFAULT_CHUNK_SIZE = 100 * 1024 * 1024  # TODO(#28653) configure a sensible default
DEFAULT_PEEK_SIZE = 300


@attr.define
class CsvChunkBoundary:
    """Information about a given csv chunk boundary, where:
    - start_inclusive is the first byte of the row-safe csv chunk (0 indexed)
    - end_exclusive is the last byte (exclusive) of the row-safe csv chunk
    - chunk_num is the 0-indexed chunk sequence number
    """

    start_inclusive: int
    end_exclusive: int
    chunk_num: int


class GcsfsCsvChunkBoundaryFinder:
    """Class for finding the byte offsets for row-safe chunk boundaries for Google Cloud
    Storage CSV files.
    """

    def __init__(
        self,
        fs: GCSFileSystem,
        chunk_size: Optional[Annotated[int, Gt(0)]] = None,
        peek_size: Optional[Annotated[int, Gt(0)]] = None,
        line_terminator: Optional[str] = None,
        encoding: Optional[str] = None,
    ) -> None:
        self._fs = fs
        # approximately how large each individual chunk will be
        self._chunk_size = chunk_size if chunk_size is not None else DEFAULT_CHUNK_SIZE

        if self._chunk_size <= 0:
            raise ValueError("chunk_size must be at least 1 byte")

        # how much we will "peek" past the chunk size boundary to look for the next
        # newline char
        self._peek_size = peek_size if peek_size is not None else DEFAULT_PEEK_SIZE

        if self._peek_size <= 0:
            raise ValueError("peek_size must be at least 1 byte")

        # byte-encoded newline terminator
        self._line_terminator = bytes(
            line_terminator or DEFAULT_CSV_LINE_TERMINATOR,
            encoding or DEFAULT_CSV_ENCODING,
        )

    def get_chunks_for_gcs_path(self, path: GcsfsFilePath) -> List[CsvChunkBoundary]:
        """Given a GcsfsFilePath, this function will return a list of CsvChunkBoundary
        objects.

        In a file with n chunks, the size of the first n-1 chunks will always be:

                    chunk_size <= actual chunk size <= chunk_size + N

        where N is the size of the longest row in the file.

        Because we don't know the exact size of each chunk ahead of time, we cannot know
        with certainty the size of the n-th chunk; however, we expect the n-th chunk
        to be approximately:

                            file_size % (chunk_size + M)

        where M is the average row length in the file.
        """
        _size = self._fs.get_file_size(path)
        if _size is None:
            raise ValueError(
                "Cannot find chunk boundaries for path without a file size"
            )

        with self._fs.open(path, mode="rb", verifiable=False) as f:

            at_eof = False
            chunks: List[CsvChunkBoundary] = []
            cursor = 0  # file pointer, our current place in the file (more or less)

            while not (at_eof := cursor == _size):
                chunk_start = cursor
                cursor += self._chunk_size

                peeked_bytes = b""
                lineterm_index = -1

                while (
                    not at_eof
                    and (lineterm_index := peeked_bytes.find(self._line_terminator))
                    == -1
                ):
                    f.seek(cursor + len(peeked_bytes), io.SEEK_SET)
                    end_of_chunk_peek = f.read(self._peek_size)

                    # protects against there not being a line term at the end of the file
                    at_eof = not end_of_chunk_peek

                    # we += here in case the newline character is represented by more than
                    # byte and gets split between two reads
                    peeked_bytes += end_of_chunk_peek

                cursor = (
                    _size
                    if at_eof
                    else cursor + lineterm_index + len(self._line_terminator)
                )
                chunks.append(
                    CsvChunkBoundary(
                        start_inclusive=chunk_start,
                        end_exclusive=cursor,
                        chunk_num=len(chunks),
                    )
                )

            return chunks
