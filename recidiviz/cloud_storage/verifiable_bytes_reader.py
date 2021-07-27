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
"""Defines VerifiableBytesReader to verify checksums over binary streams."""


import base64
import io
import logging
from typing import BinaryIO, Optional

import google_crc32c


class CorruptDownloadError(Exception):
    pass


class VerifiableBytesReader(io.BufferedIOBase, BinaryIO):
    """Calculates a checksum as it is reading the file that can be verified when the
    whole file has been read.

    This is useful when the entire file is not stored as is locally, but is transformed
    as it is streamed from a server.
    """

    def __init__(self, wrapped_io: BinaryIO, name: str) -> None:
        super().__init__()

        self._wrapped_io = wrapped_io
        self._name = name

        self._crc32c = google_crc32c.Checksum()
        self._eof = False

    def read(self, n: Optional[int] = -1) -> bytes:
        chunk = self._wrapped_io.read(-1 if n is None else n)
        if n is None or n == -1 or len(chunk) < n:
            self._eof = True
        self._crc32c.update(chunk)
        return chunk

    def verify_crc32c(self, expected_checksum: str) -> None:
        if not self._eof:
            logging.info(
                "The checksum for %s cannot be verified as the file was not fully downloaded.",
                self._name,
            )
            return
        # GCS provides Base64 encoded string representation of the big-endian ordered
        # checksum. We match that here:
        # - Checksum.digest() ensures it is big-endian ordered
        # - b64encode returns base64 encoded bytes
        # - str.decode() on those bytes gives us the string
        actual_checksum = base64.b64encode(self._crc32c.digest()).decode("utf-8")
        if actual_checksum != expected_checksum:
            raise CorruptDownloadError(
                f"Downloaded file {self._name} had checksum {actual_checksum}, expected {expected_checksum}"
            )

    def read1(self, size: Optional[int] = -1) -> bytes:
        return self.read(size)

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    # These are here to appease pylint.
    @property
    def closed(self) -> bool:
        return False

    @property
    def mode(self) -> str:
        return "rb"

    @property
    def name(self) -> str:
        return self._name

    # This is overridden purely to appease mypy which otherwise will complain about
    # incompatible definitions in super classes.
    def __enter__(self) -> "VerifiableBytesReader":
        pass
