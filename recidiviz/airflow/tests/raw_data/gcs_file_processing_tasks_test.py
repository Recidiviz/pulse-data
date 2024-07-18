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
import datetime
import unittest
from typing import ClassVar, List, Optional
from unittest.mock import MagicMock, patch

import google_crc32c

from recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks import (
    build_import_ready_files,
    create_chunk_batches,
    create_file_batches,
    regroup_normalized_file_chunks,
    verify_file_checksums,
)
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.types.raw_data_import_types import (
    ImportReadyFile,
    PreImportNormalizationType,
    PreImportNormalizedCsvChunkResult,
    RawBigQueryFileMetadataSummary,
    RawGCSFileMetadataSummary,
    RequiresPreImportNormalizationFile,
)


class TestCreateFileBatches(unittest.TestCase):
    """Tests for file batching"""

    requires_normalization_files: ClassVar[List[str]]
    file_sizes: ClassVar[List[int]]
    fs: ClassVar[MagicMock]

    @staticmethod
    def _get_serialized_file(file_num: int) -> str:
        return GcsfsFilePath.from_absolute_path(f"test/file{file_num}").abs_path()

    @classmethod
    def setUpClass(cls) -> None:
        cls.requires_normalization_files = [
            cls._get_serialized_file(file_num=1),
            cls._get_serialized_file(file_num=2),
            cls._get_serialized_file(file_num=3),
            cls._get_serialized_file(file_num=4),
            cls._get_serialized_file(file_num=5),
        ]

        cls.fs = MagicMock()
        # Hackily assign size by using the last character (containing file_num) in the file path
        cls.fs.get_file_size.side_effect = lambda x: int(x.abs_path()[-1])
        fs_patcher = patch(
            "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.GcsfsFactory.build",
            return_value=cls.fs,
        ).start()
        cls.addClassCleanup(fs_patcher.stop)

    def test_three_batches(self) -> None:
        num_batches = 3
        expected_batches = [
            [self._get_serialized_file(file_num=5)],
            [
                self._get_serialized_file(file_num=4),
                self._get_serialized_file(file_num=1),
            ],
            [
                self._get_serialized_file(file_num=3),
                self._get_serialized_file(file_num=2),
            ],
        ]
        batches = create_file_batches(self.requires_normalization_files, num_batches)
        self.assertEqual(batches, expected_batches)

    def test_two_batches(self) -> None:
        num_batches = 2
        expected_batches = [
            [
                self._get_serialized_file(file_num=5),
                self._get_serialized_file(file_num=2),
                self._get_serialized_file(file_num=1),
            ],
            [
                self._get_serialized_file(file_num=4),
                self._get_serialized_file(file_num=3),
            ],
        ]
        batches = create_file_batches(self.requires_normalization_files, num_batches)
        self.assertEqual(batches, expected_batches)

    def test_fewer_files_than_batches(self) -> None:
        num_batches = 6
        expected_batches = [
            [self._get_serialized_file(file_num=5)],
            [self._get_serialized_file(file_num=4)],
            [self._get_serialized_file(file_num=3)],
            [self._get_serialized_file(file_num=2)],
            [self._get_serialized_file(file_num=1)],
        ]
        batches = create_file_batches(self.requires_normalization_files, num_batches)
        self.assertEqual(batches, expected_batches)

    def test_no_files(self) -> None:
        batches = create_file_batches([], 3)
        self.assertEqual(batches, [])

    def test_file_size_not_found(self) -> None:
        def side_effect(file_path: GcsfsFilePath) -> Optional[int]:
            # file3 returns None for size so it's size is treated as 0
            if file_path.abs_path() == "test/file3":
                return None
            return int(file_path.abs_path()[-1])

        fs = MagicMock()
        fs.get_file_size.side_effect = side_effect

        num_batches = 2
        expected_batches = [
            [
                self._get_serialized_file(file_num=5),
                self._get_serialized_file(file_num=1),
                self._get_serialized_file(file_num=3),
            ],
            [
                self._get_serialized_file(file_num=4),
                self._get_serialized_file(file_num=2),
            ],
        ]
        with patch(
            "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.GcsfsFactory.build",
            return_value=fs,
        ):
            batches = create_file_batches(
                self.requires_normalization_files, num_batches
            )
        self.assertEqual(batches, expected_batches)


class TestCreateChunkBatches(unittest.TestCase):
    """Tests for create_chunk_batches function"""

    def test_even_distribution(self) -> None:
        chunks = [
            RequiresPreImportNormalizationFile(
                path=GcsfsFilePath.from_absolute_path(f"test/path_{i}.csv"),
                chunk_boundaries=self._generate_chunk_boundaries(count=4),
                pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
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
                path=GcsfsFilePath.from_absolute_path(f"test/path_{i}.csv"),
                chunk_boundaries=self._generate_chunk_boundaries(count=3),
                pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
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
                path=GcsfsFilePath.from_absolute_path(f"test/path_{i}.csv"),
                chunk_boundaries=self._generate_chunk_boundaries(count=3),
                pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
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
            PreImportNormalizedCsvChunkResult(
                input_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file1"),
                output_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/file1_0"
                ),
                chunk_boundary=CsvChunkBoundary(0, 10, 0),
                crc32c=self._get_checksum_int(b"these are "),
            ),
            PreImportNormalizedCsvChunkResult(
                input_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file1"),
                output_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/file1_1"
                ),
                chunk_boundary=CsvChunkBoundary(10, 20, 1),
                crc32c=self._get_checksum_int(b"file bytes"),
            ),
        ]

        self.file_to_normalized_chunks = {
            GcsfsFilePath.from_absolute_path(
                "test_bucket/file1"
            ): self.normalized_chunks
        }

    def test_regroup_normalized_file_chunks(self) -> None:
        file_to_normalized_chunks = regroup_normalized_file_chunks(
            self.normalized_chunks, normalized_chunks_errors=[]
        )

        self.assertEqual(len(file_to_normalized_chunks), 1)

        file1 = GcsfsFilePath.from_absolute_path("test_bucket/file1")
        self.assertIn(file1, file_to_normalized_chunks)
        self.assertEqual(
            file_to_normalized_chunks[file1],
            self.normalized_chunks,
        )

    def test_regroup_normalized_file_chunks_unsorted(self) -> None:
        chunk0 = PreImportNormalizedCsvChunkResult(
            input_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file1"),
            output_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file1_1"),
            chunk_boundary=CsvChunkBoundary(0, 10, 0),
            crc32c=self._get_checksum_int(b"these are "),
        )
        chunk1 = PreImportNormalizedCsvChunkResult(
            input_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file1"),
            output_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file1_0"),
            chunk_boundary=CsvChunkBoundary(10, 20, 1),
            crc32c=self._get_checksum_int(b"file bytes"),
        )

        file_to_normalized_chunks = regroup_normalized_file_chunks(
            normalized_chunks_result=[chunk1, chunk0], normalized_chunks_errors=[]
        )

        file1 = GcsfsFilePath.from_absolute_path("test_bucket/file1")
        self.assertEqual(len(file_to_normalized_chunks), 1)
        self.assertIn(
            file1,
            file_to_normalized_chunks,
        )
        self.assertEqual(
            file_to_normalized_chunks[file1],
            [
                chunk0,
                chunk1,
            ],
        )

    def test_verify_checksum_success(self) -> None:
        errors, results = verify_file_checksums(self.file_to_normalized_chunks)

        self.assertEqual(len(results), 1)
        path = GcsfsFilePath.from_absolute_path("test_bucket/file1")
        assert path in results
        self.assertEqual(
            [result.output_file_path for result in results[path]],
            [
                GcsfsFilePath.from_absolute_path("test_bucket/file1_0"),
                GcsfsFilePath.from_absolute_path("test_bucket/file1_1"),
            ],
        )
        self.assertEqual(errors, [])

    def test_verify_checksum_mismatch(self) -> None:
        self.fs.get_crc32c.return_value = "different_checksum"
        errors, results = verify_file_checksums(self.file_to_normalized_chunks)

        self.assertEqual(results, {})
        self.assertEqual(len(errors), 1)
        self.assertIn("Checksum mismatch for test_bucket/file1", errors[0].error_msg)

    @staticmethod
    def _get_checksum_int(bytes_to_checksum: bytes) -> int:
        checksum = google_crc32c.Checksum()
        checksum.update(bytes_to_checksum)
        return int.from_bytes(checksum.digest(), byteorder="big")

    def test_build_import_ready_files_none(self) -> None:
        assert not build_import_ready_files({}, [])

    def test_build_import_ready_files_failures_no_skips(self) -> None:
        results = {
            GcsfsFilePath.from_absolute_path("path/a.csv"): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/a.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 1, 0),
                    crc32c=1,
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/a.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a1.csv"),
                    chunk_boundary=CsvChunkBoundary(1, 2, 1),
                    crc32c=2,
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/a.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a2.csv"),
                    chunk_boundary=CsvChunkBoundary(2, 3, 2),
                    crc32c=3,
                ),
            ],
            GcsfsFilePath.from_absolute_path("path/aa.csv"): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/aa.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/aa.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 1, 0),
                    crc32c=1,
                ),
            ],
            GcsfsFilePath.from_absolute_path("path/b.csv"): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/b.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/b.csv"),
                    chunk_boundary=CsvChunkBoundary(2, 3, 2),
                    crc32c=3,
                ),
            ],
            GcsfsFilePath.from_absolute_path("path/c.csv"): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/c.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/c.csv"),
                    chunk_boundary=CsvChunkBoundary(2, 3, 2),
                    crc32c=3,
                ),
            ],
        }
        bq_metadata = [
            RawBigQueryFileMetadataSummary(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadataSummary(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="path", blob_name="a.csv"),
                    ),
                    RawGCSFileMetadataSummary(
                        gcs_file_id=2,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="path", blob_name="aa.csv"),
                    ),
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadataSummary(
                file_id=2,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadataSummary(
                        gcs_file_id=4,
                        file_id=2,
                        path=GcsfsFilePath(bucket_name="path", blob_name="b.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadataSummary(
                file_id=3,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadataSummary(
                        gcs_file_id=5,
                        file_id=3,
                        path=GcsfsFilePath(bucket_name="path", blob_name="c.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]
        output = build_import_ready_files(results, bq_metadata)

        created_files = [
            ImportReadyFile.from_bq_metadata_and_normalized_chunk_result(
                metadata,
                {file.path: results[file.path] for file in metadata.gcs_files},
            )
            for metadata in bq_metadata
        ]

        for i, output_file in enumerate(output):
            assert output_file.file_id == created_files[i].file_id
            assert output_file.file_tag == created_files[i].file_tag
            assert output_file.update_datetime == created_files[i].update_datetime
            assert set(output_file.file_paths) == set(created_files[i].file_paths)
            assert set(output_file.original_file_paths or []) == set(
                created_files[i].original_file_paths or []
            )

    def test_build_import_ready_files_failures_with_skips(self) -> None:
        results = {
            GcsfsFilePath.from_absolute_path("path/a.csv"): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/a.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 1, 0),
                    crc32c=1,
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/a.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a1.csv"),
                    chunk_boundary=CsvChunkBoundary(1, 2, 1),
                    crc32c=2,
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/a.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a2.csv"),
                    chunk_boundary=CsvChunkBoundary(2, 3, 2),
                    crc32c=3,
                ),
            ],
            # since this file is missing, a and aa will fail
            # GcsfsFilePath.from_absolute_path("path/aa.csv"): [
            #     PreImportNormalizedCsvChunkResult(
            #         input_file_path=GcsfsFilePath.from_absolute_path("path/aa.csv"),
            #         output_file_path=GcsfsFilePath.from_absolute_path("outpath/aa.csv"),
            #         chunk_boundary=CsvChunkBoundary(0, 1, 0),
            #         crc32c=1,
            #     ),
            # ],
            GcsfsFilePath.from_absolute_path("path/b.csv"): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/b.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/b.csv"),
                    chunk_boundary=CsvChunkBoundary(2, 3, 2),
                    crc32c=3,
                ),
            ],
            GcsfsFilePath.from_absolute_path("path/c.csv"): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path("path/c.csv"),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/c.csv"),
                    chunk_boundary=CsvChunkBoundary(2, 3, 2),
                    crc32c=3,
                ),
            ],
        }
        bq_metadata = [
            RawBigQueryFileMetadataSummary(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadataSummary(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="path", blob_name="a.csv"),
                    ),
                    RawGCSFileMetadataSummary(
                        gcs_file_id=2,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="path", blob_name="aa.csv"),
                    ),
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadataSummary(
                file_id=2,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadataSummary(
                        gcs_file_id=4,
                        file_id=2,
                        path=GcsfsFilePath(bucket_name="path", blob_name="b.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadataSummary(
                file_id=3,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadataSummary(
                        gcs_file_id=5,
                        file_id=3,
                        path=GcsfsFilePath(bucket_name="path", blob_name="c.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]
        output = build_import_ready_files(results, bq_metadata)
        created_files = [
            ImportReadyFile.from_bq_metadata_and_normalized_chunk_result(
                metadata,
                {file.path: results[file.path] for file in metadata.gcs_files},
            )
            for i, metadata in enumerate(bq_metadata)
            if i != 0
        ]

        for i, output_file in enumerate(output):
            assert output_file.file_id == created_files[i].file_id
            assert output_file.file_tag == created_files[i].file_tag
            assert output_file.update_datetime == created_files[i].update_datetime
            assert set(output_file.file_paths) == set(created_files[i].file_paths)
            assert set(output_file.original_file_paths or []) == set(
                created_files[i].original_file_paths or []
            )
