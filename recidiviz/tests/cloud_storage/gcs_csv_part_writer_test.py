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
"""Tests for GcsCsvPartWriter."""
import csv
import io
import unittest
from unittest.mock import MagicMock

from recidiviz.cloud_storage.gcs_csv_part_writer import GcsCsvPartWriter
from recidiviz.cloud_storage.gcs_file_system import CSV_CONTENT_TYPE, GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


def _path_for_part(part_index: int) -> GcsfsFilePath:
    return GcsfsFilePath.from_absolute_path(f"gs://test-bucket/part_{part_index}.csv")


class TestGcsCsvPartWriter(unittest.TestCase):
    """Tests for GcsCsvPartWriter."""

    def setUp(self) -> None:
        self.fs = MagicMock(spec=GCSFileSystem)

    def _writer(self, rows_per_part: int) -> GcsCsvPartWriter:
        return GcsCsvPartWriter(
            fs=self.fs, path_for_part=_path_for_part, rows_per_part=rows_per_part
        )

    def _uploaded_parts(self) -> list[tuple[str, list[list[str]]]]:
        """Returns (blob name, parsed CSV rows) for each uploaded part, in
        upload order."""
        return [
            (
                call.kwargs["path"].blob_name,
                list(csv.reader(io.StringIO(call.kwargs["contents"]))),
            )
            for call in self.fs.upload_from_string.call_args_list
        ]

    def test_rows_flushed_to_successive_parts(self) -> None:
        with self._writer(rows_per_part=2) as writer:
            for i in range(5):
                writer.write_row([f"doc_{i}", "SUCCESS"])

        self.assertEqual(
            self._uploaded_parts(),
            [
                ("part_0.csv", [["doc_0", "SUCCESS"], ["doc_1", "SUCCESS"]]),
                ("part_1.csv", [["doc_2", "SUCCESS"], ["doc_3", "SUCCESS"]]),
                ("part_2.csv", [["doc_4", "SUCCESS"]]),
            ],
        )
        for call in self.fs.upload_from_string.call_args_list:
            self.assertEqual(call.kwargs["content_type"], CSV_CONTENT_TYPE)

    def test_no_rows_writes_no_parts(self) -> None:
        with self._writer(rows_per_part=2):
            pass

        self.fs.upload_from_string.assert_not_called()

    def test_flush_is_idempotent(self) -> None:
        writer = self._writer(rows_per_part=10)
        writer.write_row(["doc_0", "SUCCESS"])
        writer.flush()
        writer.flush()

        self.assertEqual(
            self._uploaded_parts(), [("part_0.csv", [["doc_0", "SUCCESS"]])]
        )

    def test_exception_discards_buffered_rows_but_keeps_flushed_parts(self) -> None:
        # The first two rows complete a part and are flushed; the third is
        # still buffered when the block raises and must not be uploaded.
        with self.assertRaisesRegex(RuntimeError, r"^consumer failed$"):
            with self._writer(rows_per_part=2) as writer:
                for i in range(3):
                    writer.write_row([f"doc_{i}", "SUCCESS"])
                raise RuntimeError("consumer failed")

        self.assertEqual(
            self._uploaded_parts(),
            [("part_0.csv", [["doc_0", "SUCCESS"], ["doc_1", "SUCCESS"]])],
        )

    def test_invalid_rows_per_part(self) -> None:
        with self.assertRaises(ValueError):
            self._writer(rows_per_part=0)
