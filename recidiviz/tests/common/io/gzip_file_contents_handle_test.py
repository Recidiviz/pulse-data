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
"""Tests for GzipFileContentsHandle."""
import gzip
import io
from unittest import TestCase

from recidiviz.common.io.gzip_file_contents_handle import GzipFileContentsHandle


class _NonSeekableBytesIO(io.BytesIO):
    def seekable(self) -> bool:
        return False


class TestGzipFileContentsHandle(TestCase):
    """Tests for GzipFileContentsHandle."""

    def test_open_decompresses(self) -> None:
        handle = GzipFileContentsHandle(io.BytesIO(gzip.compress(b"a,b\n1,2\n")))

        with handle.open("rb") as f:
            self.assertEqual(b"a,b\n1,2\n", f.read())

    def test_reopen_rewinds_stream(self) -> None:
        """A retried upload re-opens the handle after a partial read and must see the
        contents from the start."""
        handle = GzipFileContentsHandle(io.BytesIO(gzip.compress(b"a,b\n1,2\n")))

        with handle.open("rb") as f:
            f.read(3)
        with handle.open("rb") as f:
            self.assertEqual(b"a,b\n1,2\n", f.read())

    def test_non_seekable_stream_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, r"Expected a seekable compressed"):
            GzipFileContentsHandle(_NonSeekableBytesIO(gzip.compress(b"a")))
