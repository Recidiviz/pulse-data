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
import base64
import unittest
from typing import ClassVar, List
from unittest.mock import MagicMock, patch

import google_crc32c

from recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks import (
    batch_files_by_size,
    create_chunk_batches,
    regroup_normalized_file_chunks,
    verify_file_checksums_and_build_import_ready_file,
)
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.ingest.direct.types.raw_data_import_types import (
    NormalizedCsvChunkResult,
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
                headers=["ID", "Name", "Age"],
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
                headers=["ID", "Name", "Age"],
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
                headers=["ID", "Name", "Age"],
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


class TestRegroupAndVerifyFileChunks(unittest.TestCase):
    """Tests for regrouping and verifying checksums of normalized file chunks"""

    def setUp(self) -> None:
        file_bytes = b"these are file bytes"

        file_checksum = google_crc32c.Checksum()
        file_checksum.update(file_bytes)
        file_checksum_str = base64.b64encode(file_checksum.digest()).decode("utf-8")
        self.fs = MagicMock()
        self.fs.get_crc32c.return_value = file_checksum_str

        fs_patcher = patch(
            "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.GcsfsFactory.build",
            return_value=self.fs,
        ).start()
        self.addCleanup(fs_patcher.stop)

        self.normalized_chunks = [
            NormalizedCsvChunkResult(
                input_file_path="test_bucket/file1",
                output_file_path="test_bucket/file1_0",
                chunk_boundary=CsvChunkBoundary(0, 10, 0),
                crc32c=self._get_checksum_int(b"these are "),
            ),
            NormalizedCsvChunkResult(
                input_file_path="test_bucket/file1",
                output_file_path="test_bucket/file1_1",
                chunk_boundary=CsvChunkBoundary(10, 20, 1),
                crc32c=self._get_checksum_int(b"file bytes"),
            ),
        ]

        self.file_to_normalized_chunks = {"test_bucket/file1": self.normalized_chunks}

    def test_regroup_normalized_file_chunks(self) -> None:
        file_to_normalized_chunks = regroup_normalized_file_chunks(
            self.normalized_chunks, normalized_chunks_errors=[]
        )

        self.assertEqual(len(file_to_normalized_chunks), 1)
        self.assertIn("test_bucket/file1", file_to_normalized_chunks)
        self.assertEqual(
            file_to_normalized_chunks["test_bucket/file1"],
            self.normalized_chunks,
        )

    def test_regroup_normalized_file_chunks_unsorted(self) -> None:
        chunk0 = NormalizedCsvChunkResult(
            input_file_path="test_bucket/file1",
            output_file_path="test_bucket/file1_1",
            chunk_boundary=CsvChunkBoundary(0, 10, 0),
            crc32c=self._get_checksum_int(b"these are "),
        )
        chunk1 = NormalizedCsvChunkResult(
            input_file_path="test_bucket/file1",
            output_file_path="test_bucket/file1_0",
            chunk_boundary=CsvChunkBoundary(10, 20, 1),
            crc32c=self._get_checksum_int(b"file bytes"),
        )

        file_to_normalized_chunks = regroup_normalized_file_chunks(
            normalized_chunks_result=[chunk1, chunk0], normalized_chunks_errors=[]
        )

        self.assertEqual(len(file_to_normalized_chunks), 1)
        self.assertIn("test_bucket/file1", file_to_normalized_chunks)
        self.assertEqual(
            file_to_normalized_chunks["test_bucket/file1"],
            [
                chunk0,
                chunk1,
            ],
        )

    def test_verify_checksum_success(self) -> None:
        result = verify_file_checksums_and_build_import_ready_file(
            self.file_to_normalized_chunks
        )

        self.assertEqual(len(result.results), 1)
        self.assertEqual(result.results[0].input_file_path, "test_bucket/file1")
        self.assertEqual(
            result.results[0].output_file_paths,
            ["test_bucket/file1_0", "test_bucket/file1_1"],
        )
        self.assertEqual(result.errors, [])

    def test_verify_checksum_mismatch(self) -> None:
        self.fs.get_crc32c.return_value = "different_checksum"
        result = verify_file_checksums_and_build_import_ready_file(
            self.file_to_normalized_chunks
        )

        self.assertEqual(result.results, [])
        self.assertEqual(len(result.errors), 1)
        self.assertIn(
            "Checksum mismatch for test_bucket/file1", result.errors[0].error_msg
        )

    @staticmethod
    def _get_checksum_int(bytes_to_checksum: bytes) -> int:
        checksum = google_crc32c.Checksum()
        checksum.update(bytes_to_checksum)
        return int.from_bytes(checksum.digest(), byteorder="big")
