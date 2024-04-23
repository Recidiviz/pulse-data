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
"""Utility class for reading just a chunk from a seekable IO stream"""
import io
from typing import IO, Annotated, Any, BinaryIO, Iterable, Optional

from annotated_types import Ge


class BytesChunkReader(io.BufferedIOBase, BinaryIO):
    """Utility class for reading just a chunk from a seekable IO stream"""

    def __init__(
        self,
        seekable_io: IO,
        read_start_inclusive: Annotated[int, Ge(0)],
        read_end_exclusive: Annotated[int, Ge(0)],
        name: Optional[str] = None,
    ) -> None:
        if not seekable_io.seekable():
            raise ValueError("Cannot have an offset reader for io that is not seekable")

        if read_end_exclusive < read_start_inclusive:
            raise ValueError(
                f"read_end_exclusive [{read_end_exclusive}] must be greater than or "
                f"equal to read_start_inclusive [{read_start_inclusive}]"
            )

        if read_start_inclusive < 0 or read_end_exclusive < 0:
            raise ValueError(
                f"Both read_end_exclusive [{read_end_exclusive}] and "
                f"read_start_inclusive [{read_start_inclusive}] must be greater than "
                f"or equal to 0"
            )

        self._seekable_io = seekable_io
        self._read_start_inclusive = read_start_inclusive
        self._read_end_exclusive = read_end_exclusive
        self._pos = self._seekable_io.seek(self._read_start_inclusive)
        self._name = name or self.__class__.__name__
        self._end_of_chunk = False

    def read(self, n: Optional[int] = None) -> bytes:
        """Wrapper around a read() call to make sure that we don't ever read before the
        read_start_inclusive or after the read_end_exclusive. Assumes that the this
        class is the only one manipulating the underlying seekable stream (i.e.
        _seekable_io.tell() remains stable bewteen calls to this function).
        """
        # we have read to the limit; let's return an empty byte (python sign that we at eof)
        if self._end_of_chunk:
            return b""

        # n is None or -1 (read all) OR n bytes would read over the limit,
        # make the read size so we would just hit in the end of the chunk
        if n is None or n == -1 or n >= self._read_end_exclusive - self._pos:
            n = self._read_end_exclusive - self._pos

        # trusts that our underlying io read here will be no larger than n bytes which
        # is a generally (?) defined invariant of buffers... if this is faulty, add a
        # buffer here
        chunk = self._seekable_io.read(n)
        self._pos = self._seekable_io.tell()
        if not chunk or self._pos == self._read_end_exclusive:
            self._end_of_chunk = True
        return chunk

    def read1(self, size: Optional[int] = -1) -> bytes:
        return self.read(size)

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    # Functions below this line are here to appease pylint & mypy ---------------------
    @property
    def closed(self) -> bool:
        return self._end_of_chunk

    @property
    def mode(self) -> str:
        return "rb"

    @property
    def name(self) -> str:
        return self._name

    def __enter__(self) -> "BytesChunkReader":
        return self

    def write(self, s: Any) -> int:
        raise NotImplementedError

    def writelines(self, s: Iterable[Any]) -> None:
        raise NotImplementedError
