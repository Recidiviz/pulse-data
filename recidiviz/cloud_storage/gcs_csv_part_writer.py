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
"""Writer that accumulates CSV rows and flushes them to successive GCS part
files, bounding the in-memory buffer regardless of total row count."""

import csv
import io
from collections.abc import Callable, Sequence
from types import TracebackType

import attr

from recidiviz.cloud_storage.gcs_file_system import CSV_CONTENT_TYPE, GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common import attr_validators


@attr.define(kw_only=True)
class GcsCsvPartWriter:
    """Writer that accumulates CSV rows and flushes them to successive GCS part
    files every |rows_per_part| rows, bounding the in-memory buffer regardless
    of total row count. GCS objects are immutable (no append), so each flush
    necessarily produces a distinct object; |path_for_part| maps each
    successive part index to its path.

    Part files are preferred over streaming one object via blob.open("w")
    because a resumable upload only commits its object on close — a crash
    mid-stream (e.g. an OOMKill) would lose every row, whereas already-flushed
    part files survive.

    Usable as a context manager, in which case any buffered rows are flushed on
    clean exit only — rows buffered when the block raises are discarded, so a
    consumer that fails mid-stream does not record a misleading final part:

        with GcsCsvPartWriter(fs=fs, path_for_part=..., rows_per_part=100_000) as writer:
            for row in rows:
                writer.write_row(row)
    """

    fs: GCSFileSystem = attr.ib(
        # GCSFileSystem is abstract; mypy flags it where a concrete type is
        # expected, but instance_of accepts an ABC fine at runtime.
        validator=attr.validators.instance_of(GCSFileSystem)  # type: ignore[type-abstract]
    )
    """Filesystem the part files are uploaded to."""

    path_for_part: Callable[[int], GcsfsFilePath] = attr.ib(
        validator=attr.validators.is_callable()
    )
    """Maps a 0-indexed part index to the GCS path for that part file."""

    rows_per_part: int = attr.ib(validator=attr_validators.is_positive_int)
    """Number of rows buffered in memory before being flushed as a part file."""

    _part_index: int = attr.ib(default=0, init=False)
    _rows_in_buffer: int = attr.ib(default=0, init=False)
    _buffer: io.StringIO = attr.ib(factory=io.StringIO, init=False)

    def write_row(self, row: Sequence[object]) -> None:
        """Appends one CSV row, flushing a part file to GCS when the buffer
        reaches |rows_per_part| rows."""
        csv.writer(self._buffer).writerow(row)
        self._rows_in_buffer += 1
        if self._rows_in_buffer >= self.rows_per_part:
            self.flush()

    def flush(self) -> None:
        """Uploads any buffered rows as the next CSV part file. A no-op when
        the buffer is empty."""
        if self._rows_in_buffer == 0:
            return
        self.fs.upload_from_string(
            path=self.path_for_part(self._part_index),
            contents=self._buffer.getvalue(),
            content_type=CSV_CONTENT_TYPE,
        )
        self._part_index += 1
        self._buffer = io.StringIO()
        self._rows_in_buffer = 0

    def __enter__(self) -> "GcsCsvPartWriter":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.flush()
