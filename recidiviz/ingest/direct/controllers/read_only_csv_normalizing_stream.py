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
"""Wrapper around a text stream that will convert the format of a CSV file from a
format with arbitrary delimiters and newline terminators to a standardized format.
"""
import csv
import logging
from typing import Optional, TextIO

DOUBLE_QUOTE = '"'
ESCAPED_DOUBLE_QUOTE = '""'
COMMA_SURROUNDED_BY_DOUBLE_QUOTES = '","'
NEWLINE = "\n"
NEWLINE_SURROUNDED_BY_DOUBLE_QUOTES = '"\n"'
NULL_BYTE = "\x00"

READ_LINE_INCREMENT_SIZE = 300


class ReadOnlyCsvNormalizingStream:
    """Wrapper around a text stream that will convert the format of a CSV file from a
    format with arbitrary delimiters and newline terminators to a standardized format
    with fully-quoted fields, escaped quotes in free-text columns, commas for delimiters
    and newlines for terminators.
    """

    def __init__(
        self, fp: TextIO, delimiter: str, line_terminator: str, quoting: int
    ) -> None:
        if quoting != csv.QUOTE_NONE:
            raise ValueError("No support for files with quoted values.")

        self.fp = fp
        self.delimiter = delimiter
        self.line_terminator = line_terminator

        # Holds the next part of the text stream that we may have read but have not yet
        # done any preprocessing to. Between calls to read(), this will never hold more
        # than part of a single CSV line.
        self._unnormalized_buffer = ""

        # Holds the next portion of the stream that has been normalized and is ready
        # to read. It is only added to in one-line increments, but may be read from in
        # any size increment.
        self._normalized_buffer = ""

        # When True, indicates that we have reached the end of the file stream and
        # should not attempt to read more. This may be set before all of the normalized
        # stream has been fully read.
        self._eof = False

        # Tracks the number of lines that we have processed and added to the normalized
        # buffer.
        self._num_lines_processed = 0

    def _add_to_buffer(self, block_size: Optional[int]) -> None:
        """Reads the next portion of the file and normalizes any full lines, adding them
        to the end of the normalized buffer.
        """
        if self._eof:
            return

        while not self._eof and self.line_terminator not in self._unnormalized_buffer:
            block = self.fp.read(block_size or -1)
            if not block or block_size is None:
                self._eof = True
            self._unnormalized_buffer += block

        lines = self._unnormalized_buffer.split(self.line_terminator)
        if self._eof:
            full_lines = lines
        else:
            if len(lines) < 2:
                raise ValueError("Expected more than one line, found only one.")
            self._unnormalized_buffer = lines[-1]
            full_lines = lines[: len(lines) - 1]

        if not full_lines and not self._eof:
            raise ValueError("Expect to have lines if not EOF, found None.")

        for full_line in full_lines:
            if self._num_lines_processed > 0:
                self._normalized_buffer += NEWLINE
            if full_line:
                self._normalized_buffer += (
                    DOUBLE_QUOTE
                    + full_line.replace(DOUBLE_QUOTE, ESCAPED_DOUBLE_QUOTE)
                    .replace(self.delimiter, COMMA_SURROUNDED_BY_DOUBLE_QUOTES)
                    .replace(NULL_BYTE, "")
                    + DOUBLE_QUOTE
                )
            self._num_lines_processed += 1

    def read(self, __size: Optional[int] = None) -> str:
        if __size is not None and __size < 1:
            raise ValueError(f"Bad size [{__size}]")

        if __size is None:
            self._add_to_buffer(None)
            if not self._eof:
                raise ValueError("Must have reached EOF if reading full file.")
        else:
            while not self._eof and len(self._normalized_buffer) < __size:
                self._add_to_buffer(__size - len(self._normalized_buffer))

        read_length = __size or len(self._normalized_buffer)
        ret = self._normalized_buffer[:read_length]
        self._normalized_buffer = self._normalized_buffer[read_length:]
        return ret

    def readline(self, __size: Optional[int] = None) -> str:
        """Read a single line from the stream, or up to __size bytes, whichever is
        shorter.
        """
        if __size == 0:
            raise ValueError(f"Invalid size [{__size}]")

        bytes_read = 0
        while True:
            index = self._normalized_buffer.find("\n")
            if index != -1 or self._eof or (__size and bytes_read >= __size):
                break

            bytes_to_read = (
                min(__size - bytes_read, READ_LINE_INCREMENT_SIZE)
                if __size
                else READ_LINE_INCREMENT_SIZE
            )
            try:
                self._add_to_buffer(bytes_to_read)
            except Exception as e:
                logging.exception(e)
                raise e

            bytes_read += bytes_to_read

        if index != -1:
            # Read the whole line, including the newline
            read_length = index + 1
        elif not __size:
            read_length = len(self._normalized_buffer)
        else:
            read_length = __size

        # Read length can never be more than __size
        read_length = min(__size, read_length) if __size else read_length

        ret = self._normalized_buffer[:read_length]

        self._normalized_buffer = self._normalized_buffer[read_length:]

        if not ret and not self._eof:
            raise ValueError("Should not have empty return if not EOF")
        return ret

    def write(self, __s: str) -> int:
        # This function is required to pass the pandas "is file like" check
        raise NotImplementedError("The write method is not yet implemented")

    def __iter__(self) -> None:
        # This function is required to pass the pandas "is file like" check
        raise NotImplementedError("The __iter__ method is not yet implemented")
