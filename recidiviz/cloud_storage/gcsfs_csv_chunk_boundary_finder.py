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
import csv
import io
import logging
from typing import Annotated, List, Optional

from annotated_types import Gt

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.types import CsvChunkBoundary
from recidiviz.common.constants.csv import DEFAULT_CSV_QUOTE_CHAR, VALID_QUOTE_CHARS
from recidiviz.utils.quoted_csv_line_terminator_finder import (
    find_line_terminator_for_quoted_csv,
)

DEFAULT_CHUNK_SIZE = 100 * 1024 * 1024  # TODO(#28653) configure a sensible default
DEFAULT_PEEK_SIZE_NO_QUOTES = 600
DEFAULT_PEEK_SIZE_QUOTES = 1024 * 512
DEFAULT_MAX_CELL_SIZE = 1024 * 1024


class GcsfsCsvChunkBoundaryFinder:
    """Class for finding the byte offsets for row-safe chunk boundaries for Google Cloud
    Storage CSV files.
    """

    def __init__(
        self,
        *,
        fs: GCSFileSystem,
        encoding: str,
        line_terminator: str,
        separator: str,
        quoting_mode: int,
        quote_char: str = DEFAULT_CSV_QUOTE_CHAR,
        chunk_size: Optional[Annotated[int, Gt(0)]] = None,
        peek_size: Optional[Annotated[int, Gt(0)]] = None,
        max_cell_size: Optional[int] = None,
    ) -> None:
        self._fs = fs

        # approximately how large each individual chunk will be
        self._chunk_size = chunk_size if chunk_size is not None else DEFAULT_CHUNK_SIZE

        if self._chunk_size <= 0:
            raise ValueError("chunk_size must be at least 1 byte")

        # how much we will "peek" past the chunk size boundary to look for the next
        # newline char
        self._peek_size = self._get_peek_size(peek_size, quoting_mode)

        if self._peek_size <= 0:
            raise ValueError("peek_size must be at least 1 byte")

        if self._chunk_size < self._peek_size:
            raise ValueError("chunk_size should never be smaller than peek_size")

        max_cell_size = (
            max_cell_size if max_cell_size is not None else DEFAULT_MAX_CELL_SIZE
        )

        if max_cell_size <= 0:
            raise ValueError("max_cell_size must be at least 1 byte")

        # we will, at most, read twice the max cell size to ensure that we will always
        # capture the entirety of a quoted field
        self._max_read_size = max_cell_size * 2

        self._quoting_mode = quoting_mode

        if quote_char is not None and quote_char not in VALID_QUOTE_CHARS:
            raise ValueError(
                f"Found invalid quote character [{quote_char}], expected one of: [{VALID_QUOTE_CHARS}]"
            )

        self._encoding = encoding
        self._line_terminator = bytes(line_terminator, self._encoding)
        self._quote_char = bytes(quote_char, self._encoding)
        self._separator = bytes(separator, self._encoding)

    @staticmethod
    def _get_peek_size(peek_size: Optional[int], quoting: int) -> int:
        """If |peek_size| is provided, use the provided value. If it is not and we are
        using quoting, we will want to read large chunks at a time as we are looking
        for quoted fields, not just line terminators.
        """
        if peek_size is not None:
            return peek_size

        return (
            DEFAULT_PEEK_SIZE_NO_QUOTES
            if quoting == csv.QUOTE_NONE
            else DEFAULT_PEEK_SIZE_QUOTES
        )

    def _find_line_terminator_for_quoted_csv(
        self,
        buffer: bytes,
        buffer_byte_start: int,
    ) -> int | None:
        """Searches |buffer| in an attempt to find an non-quoted newline. We will keep
        on reading until |buffer| is bigger than self._max_read_size. At this point,
        we think that: we are in a minimally quoted file that has no usable quotes,
        so we will warn, declare bankruptcy and return the first newline we can find.
        """
        line_term_index = find_line_terminator_for_quoted_csv(
            buffer=buffer,
            buffer_byte_start=buffer_byte_start,
            quote_char=self._quote_char,
            separator=self._separator,
            line_terminator=self._line_terminator,
        )

        if line_term_index is not None:
            return line_term_index

        # if we haven't been able to determine our place in the file, let's declare
        # bankruptcy and guess
        if len(buffer) >= self._max_read_size:
            logging.warning(
                "[Warning] encountered chunk between byte offsets [%s] and [%s] without any "
                "usable quotes so we think we (1) are in a quote minimal file that has "
                "no quotes and (2) each quoted cell is no larger than [%s] bytes so we "
                "are returning the first newline we find",
                buffer_byte_start,
                buffer_byte_start + len(buffer),
                self._max_read_size,
            )
            first_line_term = buffer.find(self._line_terminator)
            return first_line_term if first_line_term != -1 else None

        # if we have't read to our max read size, let's keep looking
        return None

    def _find_line_term_quote_safe(
        self,
        buffer: bytes,
        buffer_byte_start: int,
    ) -> int | None:
        """Searches |buffer| for the first quote-safe line terminator. If the file is
        not quoted, returns the first line terminator we find.
        """
        if self._quoting_mode == csv.QUOTE_NONE:
            first_line_term = buffer.find(self._line_terminator)
            return first_line_term if first_line_term != -1 else None

        return self._find_line_terminator_for_quoted_csv(
            buffer=buffer, buffer_byte_start=buffer_byte_start
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
        path_size = self._fs.get_file_size(path)
        if path_size is None:
            raise ValueError(
                "Cannot find chunk boundaries for path without a file size"
            )

        logging.info(
            "reading [%s] w quoting [%s]",
            path.file_name,
            "Off" if self._quoting_mode == csv.QUOTE_NONE else "On",
        )

        with self._fs.open(path, mode="rb", verifiable=False) as f:

            at_eof = False
            chunks: List[CsvChunkBoundary] = []
            cursor = 0  # file pointer, our current place in the file (more or less)

            while not (at_eof := cursor == path_size):

                chunk_start = cursor
                cursor += self._chunk_size

                peeked_bytes = b""
                line_term_index: int | None = None

                # until we find the end of the file or a line terminator, keep reading
                while (
                    not at_eof
                    and (
                        line_term_index := self._find_line_term_quote_safe(
                            peeked_bytes, cursor
                        )
                    )
                    is None
                ):
                    if len(peeked_bytes) >= self._max_read_size:
                        raise ValueError(
                            f"Could not find line terminator between bytes offset [{cursor}] "
                            f"and [{cursor + len(peeked_bytes)}] without exceeding our "
                            f"max_read_size of [{self._max_read_size}]for [{path.uri()}]. "
                            f"Please ensure that the encoding [{self._encoding}], "
                            f"separator [{self._separator!r}], line terminator [{self._line_terminator!r}] "
                            f"quote char [{self._quote_char!r}] and quoting mode [{self._quoting_mode}] "
                            f"are accurate for this file. If they are, consider "
                            f"increasing the [max_cell_size] for this file."
                        )

                    logging.info(
                        "\t reading from [%s]: from byte offset [%s] -> [%s]",
                        path.file_name,
                        cursor + len(peeked_bytes),
                        cursor + len(peeked_bytes) + self._peek_size,
                    )

                    f.seek(cursor + len(peeked_bytes), io.SEEK_SET)
                    end_of_chunk_peek = f.read(self._peek_size)

                    # protects against there not being a line term at the end of the file
                    at_eof = not end_of_chunk_peek

                    # we += here in case the newline character is represented by more than
                    # byte and gets split between two reads
                    peeked_bytes += end_of_chunk_peek

                if at_eof:
                    cursor = path_size
                elif line_term_index is None:
                    raise ValueError(
                        "Should always have a non-None line_term_index if we have not"
                        "reached the end of the file."
                    )
                else:
                    cursor = cursor + line_term_index + len(self._line_terminator)

                logging.info(
                    "\t\t[%s]: found CSV line boundary for [%s] @ byte offset [%s]",
                    len(chunks),
                    path.file_name,
                    cursor,
                )
                chunks.append(
                    CsvChunkBoundary(
                        start_inclusive=chunk_start,
                        end_exclusive=cursor,
                        chunk_num=len(chunks),
                    )
                )

            return chunks
