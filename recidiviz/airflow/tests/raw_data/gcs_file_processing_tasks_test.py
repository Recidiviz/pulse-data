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
"""Unit tests for gcs file processing tasks"""
import unittest
from typing import ClassVar, List
from unittest.mock import MagicMock

from recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks import (
    batch_files_by_size,
    create_chunk_batches,
)
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RequiresPreImportNormalizationFile,
)


class TestCreateFileBatches(unittest.TestCase):
    """Tests for file batching"""

    file_paths: ClassVar[List[str]]
    file_sizes: ClassVar[List[int]]
    fs: ClassVar[MagicMock]

    @classmethod
    def setUpClass(cls) -> None:
        cls.file_paths = [
            "test_bucket/file1",
            "test_bucket/file2",
            "test_bucket/file3",
            "test_bucket/file4",
            "test_bucket/file5",
        ]
        cls.file_sizes = [100, 200, 300, 400, 500]

        cls.fs = MagicMock()
        cls.fs.get_file_size.side_effect = lambda x: cls.file_sizes[
            cls.file_paths.index(x.abs_path())
        ]

    def test_three_batches(self) -> None:
        num_batches = 3
        expected_batches = [
            ["test_bucket/file5"],
            ["test_bucket/file4", "test_bucket/file1"],
            ["test_bucket/file3", "test_bucket/file2"],
        ]
        batches = batch_files_by_size(self.fs, self.file_paths, num_batches)
        self.assertEqual(batches, expected_batches)

    def test_two_batches(self) -> None:
        num_batches = 2
        expected_batches = [
            ["test_bucket/file5", "test_bucket/file2", "test_bucket/file1"],
            ["test_bucket/file4", "test_bucket/file3"],
        ]
        batches = batch_files_by_size(self.fs, self.file_paths, num_batches)
        self.assertEqual(batches, expected_batches)

    def test_fewer_files_than_batches(self) -> None:
        num_batches = 6
        expected_batches = [
            ["test_bucket/file5"],
            ["test_bucket/file4"],
            ["test_bucket/file3"],
            ["test_bucket/file2"],
            ["test_bucket/file1"],
        ]
        batches = batch_files_by_size(self.fs, self.file_paths, num_batches)
        self.assertEqual(batches, expected_batches)

    def test_no_files(self) -> None:
        batches = batch_files_by_size(self.fs, [], 3)
        self.assertEqual(batches, [])

    def test_file_size_not_found(self) -> None:
        num_batches = 2
        # file3 returns None for size so it's size is treated as 0
        file_sizes = [100, 200, None, 400, 500]
        fs = MagicMock()
        fs.get_file_size.side_effect = lambda x: file_sizes[
            self.file_paths.index(x.abs_path())
        ]
        expected_batches = [
            ["test_bucket/file5", "test_bucket/file1", "test_bucket/file3"],
            ["test_bucket/file4", "test_bucket/file2"],
        ]
        batches = batch_files_by_size(fs, self.file_paths, num_batches)
        self.assertEqual(batches, expected_batches)


class TestCreateChunkBatches(unittest.TestCase):
    """Tests for create_chunk_batches function"""

    def test_even_distribution(self) -> None:
        chunks = [
            RequiresPreImportNormalizationFile(
                path=f"test/path_{i}.csv",
                chunk_boundaries=self._generate_chunk_boundaries(count=4),
                normalization_type=None,
                headers="ID,Name,Age",
            )
            for i in range(5)
        ]
        batches = create_chunk_batches(chunks, 5)

        self.assertEqual(len(batches), 5)
        for batch in batches:
            self.assertEqual(len(batch), 4)

    def test_split_chunk_between_batches(self) -> None:
        chunks = [
            RequiresPreImportNormalizationFile(
                path=f"test/path_{i}.csv",
                chunk_boundaries=self._generate_chunk_boundaries(count=3),
                normalization_type=None,
                headers="ID,Name,Age",
            )
            for i in range(5)
        ]
        batches = create_chunk_batches(chunks, 2)

        self.assertEqual(len(batches), 2)
        # The first batch should have one more chunk than the second batch
        # since there is an odd number of total chunks
        self.assertEqual(len(batches[0]), len(batches[1]) + 1)

    def test_more_batches_than_chunks(self) -> None:
        chunks = [
            RequiresPreImportNormalizationFile(
                path=f"test/path_{i}.csv",
                chunk_boundaries=self._generate_chunk_boundaries(count=3),
                normalization_type=None,
                headers="ID,Name,Age",
            )
            for i in range(2)
        ]
        batches = create_chunk_batches(chunks, 10)

        # Should reduce batch size to the number of chunks
        self.assertEqual(len(batches), 6)

    @staticmethod
    def _generate_chunk_boundaries(count: int) -> List[CsvChunkBoundary]:
        """Generates a list of arbitrary CsvChunkBoundary objects based on the specified count."""
        return [
            CsvChunkBoundary(start_inclusive=i, end_exclusive=i + 1, chunk_num=i)
            for i in range(count)
        ]
