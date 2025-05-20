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
from itertools import chain
from typing import ClassVar, List, Optional
from unittest.mock import MagicMock, patch

import google_crc32c

from recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks import (
    build_import_ready_files,
    create_file_batches,
    divide_file_chunks_into_batches,
    filter_normalization_results_based_on_errors,
    read_and_verify_column_headers_concurrently,
    regroup_and_verify_file_chunks,
    regroup_normalized_file_chunks,
    verify_file_checksums,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.types import CsvChunkBoundary
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    ImportReadyFile,
    PreImportNormalizationType,
    PreImportNormalizedCsvChunkResult,
    RawBigQueryFileMetadata,
    RawFileBigQueryLoadConfig,
    RawFileProcessingError,
    RawGCSFileMetadata,
    RequiresPreImportNormalizationFile,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.utils.airflow_types import BatchedTaskInstanceOutput
from recidiviz.utils.types import assert_type


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
        cls.fs.get_file_size.side_effect = (
            lambda x: int(x.abs_path()[-1]) * 1024 * 1024 * 100
        )
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
        batches = create_file_batches(
            serialized_requires_pre_import_normalization_file_paths=self.requires_normalization_files,
            target_num_chunking_airflow_tasks=num_batches,
            max_chunks_per_airflow_task=100,
        )
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
        batches = create_file_batches(
            serialized_requires_pre_import_normalization_file_paths=self.requires_normalization_files,
            target_num_chunking_airflow_tasks=num_batches,
            max_chunks_per_airflow_task=100,
        )
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
        batches = create_file_batches(
            serialized_requires_pre_import_normalization_file_paths=self.requires_normalization_files,
            target_num_chunking_airflow_tasks=num_batches,
            max_chunks_per_airflow_task=100,
        )
        self.assertEqual(batches, expected_batches)

    def test_no_files(self) -> None:
        batches = create_file_batches(
            serialized_requires_pre_import_normalization_file_paths=[],
            target_num_chunking_airflow_tasks=3,
            max_chunks_per_airflow_task=100,
        )
        self.assertEqual(batches, [])

    def test_file_size_not_found(self) -> None:
        def side_effect(file_path: GcsfsFilePath) -> Optional[int]:
            # file1 returns None for size so it's size is treated as 1 chunk
            if file_path.abs_path() == "test/file1":
                return None
            return int(file_path.abs_path()[-1]) * 1024 * 1024 * 100

        fs = MagicMock()
        fs.get_file_size.side_effect = side_effect

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
        with patch(
            "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.GcsfsFactory.build",
            return_value=fs,
        ):
            batches = create_file_batches(
                serialized_requires_pre_import_normalization_file_paths=self.requires_normalization_files,
                target_num_chunking_airflow_tasks=num_batches,
                max_chunks_per_airflow_task=100,
            )
        self.assertEqual(batches, expected_batches)


class TestCreateChunkBatches(unittest.TestCase):
    """Tests for divide_file_chunks_into_batches function"""

    def test_even_distribution(self) -> None:
        files = [
            RequiresPreImportNormalizationFile(
                path=GcsfsFilePath.from_absolute_path(f"test/path_{i}.csv"),
                chunk_boundaries=self._generate_chunk_boundaries(count=4),
                pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            )
            for i in range(5)
        ]
        batches = divide_file_chunks_into_batches(
            all_pre_import_files=files,
            target_num_normalization_airflow_tasks=5,
            max_file_chunks_per_airflow_task=500,
        )

        self.assertEqual(len(batches), 5)
        for batch in batches:
            self.assertEqual(len(batch), 4)

    def test_split_chunk_between_batches(self) -> None:
        files = [
            RequiresPreImportNormalizationFile(
                path=GcsfsFilePath.from_absolute_path(f"test/path_{i}.csv"),
                chunk_boundaries=self._generate_chunk_boundaries(count=3),
                pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            )
            for i in range(5)
        ]
        batches = divide_file_chunks_into_batches(
            all_pre_import_files=files,
            target_num_normalization_airflow_tasks=2,
            max_file_chunks_per_airflow_task=500,
        )

        self.assertEqual(len(batches), 2)
        # The first batch should have one more chunk than the second batch
        # since there is an odd number of total chunks
        self.assertEqual(len(batches[0]), len(batches[1]) + 1)

    def test_more_batches_than_chunks(self) -> None:
        files = [
            RequiresPreImportNormalizationFile(
                path=GcsfsFilePath.from_absolute_path(f"test/path_{i}.csv"),
                chunk_boundaries=self._generate_chunk_boundaries(count=3),
                pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            )
            for i in range(2)
        ]
        batches = divide_file_chunks_into_batches(
            all_pre_import_files=files,
            target_num_normalization_airflow_tasks=10,
            max_file_chunks_per_airflow_task=500,
        )

        # Should reduce batch size to the number of chunks
        self.assertEqual(len(batches), 6)

    @staticmethod
    def _generate_chunk_boundaries(count: int) -> List[CsvChunkBoundary]:
        """Generates a list of arbitrary CsvChunkBoundary objects based on the specified count."""
        return [
            CsvChunkBoundary(start_inclusive=i, end_exclusive=i + 1, chunk_num=i)
            for i in range(count)
        ]


class TestRegroupAndVerifyFileChunkUnitTests(unittest.TestCase):
    """Unit tests for individual parts of regrouping and verifying checksums of
    normalized file chunks
    """

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
        )
        fs_patcher.start()
        self.addCleanup(fs_patcher.stop)
        region_module_patch = patch(
            "recidiviz.airflow.dags.raw_data.utils.direct_ingest_regions_module",
            fake_regions,
        )
        region_module_patch.start()
        self.addCleanup(region_module_patch.stop)

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
        (
            files_with_previous_errors,
            file_to_normalized_chunks,
        ) = regroup_normalized_file_chunks(
            self.normalized_chunks, normalized_chunks_errors=[]
        )

        self.assertEqual(len(file_to_normalized_chunks), 1)
        self.assertFalse(files_with_previous_errors)

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

        (
            files_with_previous_errors,
            file_to_normalized_chunks,
        ) = regroup_normalized_file_chunks(
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
        self.assertFalse(files_with_previous_errors)

    def test_regroup_normalized_file_chunks_with_errors(self) -> None:
        file1_chunk0 = PreImportNormalizedCsvChunkResult(
            input_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file1"),
            output_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file1_1"),
            chunk_boundary=CsvChunkBoundary(0, 10, 0),
            crc32c=self._get_checksum_int(b"these are "),
        )
        file1_chunk1 = PreImportNormalizedCsvChunkResult(
            input_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file1"),
            output_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file1_0"),
            chunk_boundary=CsvChunkBoundary(10, 20, 1),
            crc32c=self._get_checksum_int(b"file bytes"),
        )
        file2_chunk0 = PreImportNormalizedCsvChunkResult(
            input_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file2"),
            output_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file2_0"),
            chunk_boundary=CsvChunkBoundary(10, 20, 1),
            crc32c=self._get_checksum_int(b"file bytes"),
        )
        file2_chunk1 = PreImportNormalizedCsvChunkResult(
            input_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file2"),
            output_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file2_0"),
            chunk_boundary=CsvChunkBoundary(10, 20, 1),
            crc32c=self._get_checksum_int(b"file bytes"),
        )
        error = RawFileProcessingError(
            original_file_path=GcsfsFilePath.from_absolute_path("test_bucket/file2"),
            temporary_file_paths=[],
            error_msg="this is an error",
        )

        (
            files_with_previous_errors,
            file_to_normalized_chunks,
        ) = regroup_normalized_file_chunks(
            normalized_chunks_result=[
                file1_chunk0,
                file1_chunk1,
                file2_chunk0,
                file2_chunk1,
            ],
            normalized_chunks_errors=[error],
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
                file1_chunk0,
                file1_chunk1,
            ],
        )
        self.assertEqual(len(files_with_previous_errors), 2)
        self.assertEqual(
            files_with_previous_errors[0].temporary_file_paths,
            [file2_chunk0.output_file_path],
        )
        self.assertEqual(
            files_with_previous_errors[1].temporary_file_paths,
            [file2_chunk1.output_file_path],
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
        assert build_import_ready_files({}, [], {}) == ([], [])

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
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="path", blob_name="a.csv"),
                    ),
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="path", blob_name="aa.csv"),
                    ),
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=4,
                        file_id=2,
                        path=GcsfsFilePath(bucket_name="path", blob_name="b.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=3,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
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
        bq_schemas = {
            1: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
            2: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
            3: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
        }
        conceptual_file_incomplete_errors, output = build_import_ready_files(
            results,
            bq_metadata,
            bq_schemas,
        )

        assert not conceptual_file_incomplete_errors

        created_files = [
            ImportReadyFile.from_bq_metadata_load_config_and_normalized_chunk_result(
                metadata,
                bq_schemas[assert_type(metadata.file_id, int)],
                {file.path: results[file.path] for file in metadata.gcs_files},
            )
            for metadata in bq_metadata
        ]

        for i, output_file in enumerate(output):
            assert output_file.file_id == created_files[i].file_id
            assert output_file.file_tag == created_files[i].file_tag
            assert output_file.update_datetime == created_files[i].update_datetime
            assert set(output_file.original_file_paths) == set(
                created_files[i].original_file_paths
            )
            assert set(output_file.pre_import_normalized_file_paths or []) == set(
                created_files[i].pre_import_normalized_file_paths or []
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
            #
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
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="path", blob_name="a.csv"),
                    ),
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=1,
                        path=GcsfsFilePath(bucket_name="path", blob_name="aa.csv"),
                    ),
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=4,
                        file_id=2,
                        path=GcsfsFilePath(bucket_name="path", blob_name="b.csv"),
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=3,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
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
        bq_schemas = {
            1: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
            2: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
            3: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
        }
        conceptual_file_incomplete_errors, output = build_import_ready_files(
            results,
            bq_metadata,
            bq_schemas,
        )
        created_files = [
            ImportReadyFile.from_bq_metadata_load_config_and_normalized_chunk_result(
                metadata,
                bq_schemas[assert_type(metadata.file_id, int)],
                {file.path: results[file.path] for file in metadata.gcs_files},
            )
            for i, metadata in enumerate(bq_metadata)
            if i != 0
        ]

        for i, output_file in enumerate(output):
            assert output_file.file_id == created_files[i].file_id
            assert output_file.file_tag == created_files[i].file_tag
            assert output_file.update_datetime == created_files[i].update_datetime
            assert set(output_file.original_file_paths) == set(
                created_files[i].original_file_paths
            )
            assert set(output_file.pre_import_normalized_file_paths or []) == set(
                created_files[i].pre_import_normalized_file_paths or []
            )

        assert len(conceptual_file_incomplete_errors) == 1
        assert conceptual_file_incomplete_errors[
            0
        ].original_file_path == GcsfsFilePath.from_absolute_path("path/a.csv")
        assert conceptual_file_incomplete_errors[0].temporary_file_paths == [
            GcsfsFilePath.from_absolute_path("outpath/a.csv"),
            GcsfsFilePath.from_absolute_path("outpath/a1.csv"),
            GcsfsFilePath.from_absolute_path("outpath/a2.csv"),
        ]

    def test_filter_normalization_results_based_on_errors_empty(self) -> None:
        assert filter_normalization_results_based_on_errors([], []) == ([], [])

    def test_filter_normalization_results_based_on_errors_no_blocking(self) -> None:
        files = [
            ImportReadyFile(
                file_id=1,
                file_tag="tag_a",
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
                original_file_paths=[GcsfsFilePath.from_absolute_path("path/a.csv")],
                pre_import_normalized_file_paths=None,
                bq_load_config=RawFileBigQueryLoadConfig(
                    schema_fields=[], skip_leading_rows=0
                ),
            ),
            ImportReadyFile(
                file_id=2,
                file_tag="tag_b",
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
                original_file_paths=[GcsfsFilePath.from_absolute_path("path/b.csv")],
                pre_import_normalized_file_paths=None,
                bq_load_config=RawFileBigQueryLoadConfig(
                    schema_fields=[], skip_leading_rows=0
                ),
            ),
        ]

        failures = [
            # tag b failure is newer than successful tag b
            RawFileProcessingError(
                error_msg="test error",
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_b.csv"
                ),
                temporary_file_paths=None,
            ),
            # no successful tag c
            RawFileProcessingError(
                error_msg="test error",
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_c.csv"
                ),
                temporary_file_paths=None,
            ),
        ]

        assert filter_normalization_results_based_on_errors(files, failures) == (
            files,
            [],
        )

    def test_filter_normalization_results_based_on_errors_blocking(self) -> None:
        files = [
            ImportReadyFile(
                file_id=1,
                file_tag="tag_a",
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
                original_file_paths=[GcsfsFilePath.from_absolute_path("path/a.csv")],
                pre_import_normalized_file_paths=None,
                bq_load_config=RawFileBigQueryLoadConfig(
                    schema_fields=[], skip_leading_rows=0
                ),
            ),
            ImportReadyFile(
                file_id=2,
                file_tag="tag_b",
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
                original_file_paths=[GcsfsFilePath.from_absolute_path("path/b.csv")],
                pre_import_normalized_file_paths=None,
                bq_load_config=RawFileBigQueryLoadConfig(
                    schema_fields=[], skip_leading_rows=0
                ),
            ),
            ImportReadyFile(
                file_id=3,
                file_tag="tag_b",
                update_datetime=datetime.datetime(
                    2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC
                ),
                original_file_paths=[GcsfsFilePath.from_absolute_path("path/b.csv")],
                pre_import_normalized_file_paths=None,
                bq_load_config=RawFileBigQueryLoadConfig(
                    schema_fields=[], skip_leading_rows=0
                ),
            ),
        ]

        failures = [
            # tag b failure is newer than one successful tag b but older than another,
            # so it should block import
            RawFileProcessingError(
                error_msg="test error",
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_b.csv"
                ),
                temporary_file_paths=None,
            ),
            RawFileProcessingError(
                error_msg="test error",
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-04T00:00:00:000000_raw_tag_b.csv"
                ),
                temporary_file_paths=None,
            ),
            # no successful tag c
            RawFileProcessingError(
                error_msg="test error",
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_c.csv"
                ),
                temporary_file_paths=None,
            ),
        ]

        (
            non_blocked_files,
            skipped_errors,
        ) = filter_normalization_results_based_on_errors(files, failures)

        assert non_blocked_files == files[:2]
        assert len(skipped_errors) == 1
        assert skipped_errors[0].original_file_path == files[2].original_file_paths[0]


class TestRegroupAndVerifyFileChunkIntegrationTests(unittest.TestCase):
    """Integration tests for individual parts of regrouping and verifying checksums of
    normalized file chunks
    """

    @staticmethod
    def _get_checksum_int(bytes_to_checksum: bytes) -> int:
        checksum = google_crc32c.Checksum()
        checksum.update(bytes_to_checksum)
        return int.from_bytes(checksum.digest(), byteorder="big")

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
        )
        fs_patcher.start()
        self.addCleanup(fs_patcher.stop)

    def test_empty(self) -> None:
        output = regroup_and_verify_file_chunks.function([], [], {})

        assert BatchedTaskInstanceOutput.deserialize(
            output, ImportReadyFile, RawFileProcessingError
        ) == BatchedTaskInstanceOutput[ImportReadyFile, RawFileProcessingError](
            results=[], errors=[]
        )

    def test_success(self) -> None:
        results = {
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 6, 0),
                    crc32c=self._get_checksum_int(b"these "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a1.csv"),
                    chunk_boundary=CsvChunkBoundary(6, 10, 1),
                    crc32c=self._get_checksum_int(b"are "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a2.csv"),
                    chunk_boundary=CsvChunkBoundary(10, 20, 2),
                    crc32c=self._get_checksum_int(b"file bytes"),
                ),
            ],
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 6, 0),
                    crc32c=self._get_checksum_int(b"these "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a1.csv"),
                    chunk_boundary=CsvChunkBoundary(6, 10, 1),
                    crc32c=self._get_checksum_int(b"are "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a2.csv"),
                    chunk_boundary=CsvChunkBoundary(10, 20, 2),
                    crc32c=self._get_checksum_int(b"file bytes"),
                ),
            ],
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/aa.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 10, 0),
                    crc32c=self._get_checksum_int(b"these are "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/aa.csv"),
                    chunk_boundary=CsvChunkBoundary(10, 20, 1),
                    crc32c=self._get_checksum_int(b"file bytes"),
                ),
            ],
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagMoreBasicData.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagMoreBasicData.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/c.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 20, 0),
                    crc32c=self._get_checksum_int(b"these are file bytes"),
                ),
            ],
        }
        chunks = chain.from_iterable(results.values())
        errors: list[RawFileProcessingError] = []
        bq_metadata = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagChunkedFile",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                        ),
                    ),
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                        ),
                    ),
                ],
                update_datetime=datetime.datetime(2021, 1, 1, tzinfo=datetime.UTC),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=4,
                        file_id=2,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                        ),
                    )
                ],
                update_datetime=datetime.datetime(2021, 1, 2, tzinfo=datetime.UTC),
            ),
            RawBigQueryFileMetadata(
                file_id=3,
                file_tag="tagMoreBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=5,
                        file_id=3,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagMoreBasicData.csv"
                        ),
                    )
                ],
                update_datetime=datetime.datetime(2021, 1, 1, tzinfo=datetime.UTC),
            ),
        ]
        bq_schemas = {
            1: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
            2: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
            3: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
        }

        output_str = regroup_and_verify_file_chunks.function(
            [
                BatchedTaskInstanceOutput(
                    results=list(chunks), errors=errors
                ).serialize()
            ],
            [meta_.serialize() for meta_ in bq_metadata],
            {str(id_): schema_.serialize() for id_, schema_ in bq_schemas.items()},
        )
        output = BatchedTaskInstanceOutput.deserialize(
            output_str, ImportReadyFile, RawFileProcessingError
        )
        assert not output.errors
        assert sorted(output.results, key=lambda x: x.file_id) == sorted(
            [
                ImportReadyFile.from_bq_metadata_load_config_and_normalized_chunk_result(
                    metadata,
                    bq_schemas[assert_type(metadata.file_id, int)],
                    {file.path: results[file.path] for file in metadata.gcs_files},
                )
                for metadata in bq_metadata
            ],
            key=lambda x: x.file_id,
        )

    def test_some_errors(self) -> None:
        results = {
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 6, 0),
                    crc32c=self._get_checksum_int(b"these "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a1.csv"),
                    chunk_boundary=CsvChunkBoundary(6, 10, 1),
                    crc32c=self._get_checksum_int(b"are "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a2.csv"),
                    chunk_boundary=CsvChunkBoundary(10, 20, 2),
                    crc32c=self._get_checksum_int(b"file bytes"),
                ),
            ],
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 6, 0),
                    crc32c=self._get_checksum_int(b"these "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a1.csv"),
                    chunk_boundary=CsvChunkBoundary(6, 10, 1),
                    crc32c=self._get_checksum_int(b"are "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a2.csv"),
                    chunk_boundary=CsvChunkBoundary(10, 20, 2),
                    crc32c=self._get_checksum_int(b"file bytes"),
                ),
            ],
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/aa.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 10, 0),
                    crc32c=self._get_checksum_int(b"these are "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/aa.csv"),
                    chunk_boundary=CsvChunkBoundary(10, 20, 1),
                    crc32c=self._get_checksum_int(b"file bytes"),
                ),
            ],
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagMoreBasicData.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagMoreBasicData.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/c.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 20, 0),
                    crc32c=self._get_checksum_int(b"these are file bytes"),
                ),
            ],
        }
        chunks = chain.from_iterable(results.values())
        errors: list[RawFileProcessingError] = []
        bq_metadata = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagChunkedFile",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                        ),
                    ),
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                        ),
                    ),
                ],
                update_datetime=datetime.datetime(2021, 1, 1, tzinfo=datetime.UTC),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=4,
                        file_id=2,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                        ),
                    )
                ],
                update_datetime=datetime.datetime(2021, 1, 2, tzinfo=datetime.UTC),
            ),
            RawBigQueryFileMetadata(
                file_id=3,
                file_tag="tagMoreBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=5,
                        file_id=3,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagMoreBasicData.csv"
                        ),
                    )
                ],
                update_datetime=datetime.datetime(2021, 1, 1, tzinfo=datetime.UTC),
            ),
        ]
        bq_schemas = {
            1: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
            2: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
            3: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
        }

        output_str = regroup_and_verify_file_chunks.function(
            [
                BatchedTaskInstanceOutput(
                    results=list(chunks), errors=errors
                ).serialize()
            ],
            [meta_.serialize() for meta_ in bq_metadata],
            {str(id_): schema_.serialize() for id_, schema_ in bq_schemas.items()},
        )
        output = BatchedTaskInstanceOutput.deserialize(
            output_str, ImportReadyFile, RawFileProcessingError
        )
        assert not output.errors
        assert sorted(output.results, key=lambda x: x.file_id) == sorted(
            [
                ImportReadyFile.from_bq_metadata_load_config_and_normalized_chunk_result(
                    metadata,
                    bq_schemas[assert_type(metadata.file_id, int)],
                    {file.path: results[file.path] for file in metadata.gcs_files},
                )
                for metadata in bq_metadata
            ],
            key=lambda x: x.file_id,
        )

    def test_checksum_error_block(self) -> None:
        results = {
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 6, 0),
                    crc32c=self._get_checksum_int(b"these "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a1.csv"),
                    chunk_boundary=CsvChunkBoundary(6, 10, 1),
                    crc32c=self._get_checksum_int(b"are "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a2.csv"),
                    chunk_boundary=CsvChunkBoundary(10, 20, 2),
                    crc32c=self._get_checksum_int(b"file bytes"),
                ),
            ],
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 6, 0),
                    crc32c=self._get_checksum_int(b"these "),
                ),
                # this chunk had an issue!
                # PreImportNormalizedCsvChunkResult(
                #     input_file_path=GcsfsFilePath.from_absolute_path(
                #         "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                #     ),
                #     output_file_path=GcsfsFilePath.from_absolute_path("outpath/a1.csv"),
                #     chunk_boundary=CsvChunkBoundary(6, 10, 1),
                #     crc32c=self._get_checksum_int(b"are "),
                #
                # ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a2.csv"),
                    chunk_boundary=CsvChunkBoundary(10, 20, 2),
                    crc32c=self._get_checksum_int(b"file bytes"),
                ),
            ],
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagChunkedFile-2.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 6, 0),
                    crc32c=self._get_checksum_int(b"these "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a1.csv"),
                    chunk_boundary=CsvChunkBoundary(6, 10, 1),
                    crc32c=self._get_checksum_int(b"are "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagChunkedFile-2.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/a2.csv"),
                    chunk_boundary=CsvChunkBoundary(10, 20, 2),
                    crc32c=self._get_checksum_int(b"file bytes"),
                ),
            ],
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
            ): [
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/aa.csv"),
                    chunk_boundary=CsvChunkBoundary(0, 10, 0),
                    crc32c=self._get_checksum_int(b"these are "),
                ),
                PreImportNormalizedCsvChunkResult(
                    input_file_path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                    ),
                    output_file_path=GcsfsFilePath.from_absolute_path("outpath/aa.csv"),
                    chunk_boundary=CsvChunkBoundary(10, 20, 1),
                    crc32c=self._get_checksum_int(b"file bytes"),
                ),
            ],
        }
        chunks = chain.from_iterable(results.values())
        errors = [
            RawFileProcessingError(
                original_file_path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                ),
                error_msg="yike!",
                temporary_file_paths=[
                    GcsfsFilePath.from_absolute_path("outpath/a1.csv")
                ],
            )
        ]
        bq_metadata = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagChunkedFile",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv"
                        ),
                    ),
                    RawGCSFileMetadata(
                        gcs_file_id=2,
                        file_id=1,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv"
                        ),
                    ),
                ],
                update_datetime=datetime.datetime(2021, 1, 1, tzinfo=datetime.UTC),
            ),
            RawBigQueryFileMetadata(
                file_id=4,
                file_tag="tagChunkedFile",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=6,
                        file_id=4,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagChunkedFile-2.csv"
                        ),
                    ),
                ],
                update_datetime=datetime.datetime(2021, 1, 2, tzinfo=datetime.UTC),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=4,
                        file_id=2,
                        path=GcsfsFilePath.from_absolute_path(
                            "test_bucket/unprocessed_2021-01-02T00:00:00:000000_raw_tagBasicData.csv"
                        ),
                    )
                ],
                update_datetime=datetime.datetime(2021, 1, 2, tzinfo=datetime.UTC),
            ),
        ]
        bq_schemas = {
            1: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
            2: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
            4: RawFileBigQueryLoadConfig(schema_fields=[], skip_leading_rows=0),
        }

        output_str = regroup_and_verify_file_chunks.function(
            [
                BatchedTaskInstanceOutput(
                    results=list(chunks), errors=errors
                ).serialize()
            ],
            [meta_.serialize() for meta_ in bq_metadata],
            {str(id_): schema_.serialize() for id_, schema_ in bq_schemas.items()},
        )
        output = BatchedTaskInstanceOutput.deserialize(
            output_str, ImportReadyFile, RawFileProcessingError
        )
        assert sorted(output.errors, key=lambda x: x.error_msg) == sorted(
            [
                *errors,
                RawFileProcessingError(
                    error_msg="Missing [test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv] paths so could not build full conceptual file",
                    original_file_path=GcsfsFilePath(
                        bucket_name="test_bucket",
                        blob_name="unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-1.csv",
                    ),
                    temporary_file_paths=[
                        GcsfsFilePath(bucket_name="outpath", blob_name="a.csv"),
                        GcsfsFilePath(bucket_name="outpath", blob_name="a1.csv"),
                        GcsfsFilePath(bucket_name="outpath", blob_name="a2.csv"),
                    ],
                ),
                RawFileProcessingError(
                    error_msg="Chunk [0] of [test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv] skipped due to error encountered with a different chunk with the same input path",
                    original_file_path=GcsfsFilePath(
                        bucket_name="test_bucket",
                        blob_name="unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv",
                    ),
                    temporary_file_paths=[
                        GcsfsFilePath(bucket_name="outpath", blob_name="a.csv")
                    ],
                    error_type=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                ),
                RawFileProcessingError(
                    error_msg="Chunk [2] of [test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv] skipped due to error encountered with a different chunk with the same input path",
                    original_file_path=GcsfsFilePath(
                        bucket_name="test_bucket",
                        blob_name="unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv",
                    ),
                    temporary_file_paths=[
                        GcsfsFilePath(bucket_name="outpath", blob_name="a2.csv")
                    ],
                    error_type=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                ),
                RawFileProcessingError(
                    error_msg="Blocked Import: failed due to import-blocking failure from GcsfsFilePath(bucket_name='test_bucket', blob_name='unprocessed_2021-01-01T00:00:00:000000_raw_tagChunkedFile-2.csv')",
                    original_file_path=GcsfsFilePath(
                        bucket_name="test_bucket",
                        blob_name="unprocessed_2021-01-02T00:00:00:000000_raw_tagChunkedFile-2.csv",
                    ),
                    temporary_file_paths=[
                        GcsfsFilePath(bucket_name="outpath", blob_name="a.csv"),
                        GcsfsFilePath(bucket_name="outpath", blob_name="a1.csv"),
                        GcsfsFilePath(bucket_name="outpath", blob_name="a2.csv"),
                    ],
                    error_type=DirectIngestRawFileImportStatus.FAILED_IMPORT_BLOCKED,
                ),
            ],
            key=lambda x: x.error_msg,
        )
        assert sorted(output.results, key=lambda x: x.file_id) == sorted(
            [
                ImportReadyFile.from_bq_metadata_load_config_and_normalized_chunk_result(
                    metadata,
                    bq_schemas[assert_type(metadata.file_id, int)],
                    {file.path: results[file.path] for file in metadata.gcs_files},
                )
                for metadata in bq_metadata
                if metadata.file_id
                not in {
                    1,  # failed checksum
                    4,  # blocked due to above
                }
            ],
            key=lambda x: x.file_id,
        )


class TestReadAndVerifyColumnHeaders(unittest.TestCase):
    """Tests for read_and_verify_column_headers_concurrently"""

    def setUp(self) -> None:
        self.file_paths = [
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-01-01T00:00:00:000000_raw_tagBasicData_1.csv"
            ),
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-02-01T00:00:00:000000_raw_tagBasicData_2.csv"
            ),
            GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2021-03-01T00:00:00:000000_raw_tagBasicData_3.csv"
            ),
        ]
        self.headers = ["ID", "Name", "Age"]
        self.file_ids = [1, 2, 3]
        self.bq_metadata = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1, file_id=1, path=self.file_paths[0]
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=2, file_id=2, path=self.file_paths[1]
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=3,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=3, file_id=3, path=self.file_paths[2]
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 3, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]

        self.fs = MagicMock()
        self.region_raw_file_config = MagicMock()

        self.file_reader = MagicMock()
        self.file_reader_patcher = patch(
            "recidiviz.airflow.dags.raw_data.gcs_file_processing_tasks.DirectIngestRawFileHeaderReader",
            return_value=self.file_reader,
        )
        self.file_reader_patcher.start()
        self.addCleanup(self.file_reader_patcher.stop)

    def test_read_and_verify_column_headers_concurrently(self) -> None:
        self.file_reader.read_and_validate_column_headers.return_value = self.headers

        result, errors = read_and_verify_column_headers_concurrently(
            self.fs, self.region_raw_file_config, self.bq_metadata
        )
        self.assertEqual(
            result,
            {i: self.headers for i in self.file_ids},
        )
        self.assertEqual(errors, [])

    def test_read_and_verify_column_headers_concurrently_error(self) -> None:
        def side_effect(gcs_file_path: GcsfsFilePath) -> List[str]:
            if gcs_file_path == self.file_paths[1]:
                raise RuntimeError("Error reading file")

            return self.headers

        self.file_reader.read_and_validate_column_headers.side_effect = side_effect

        result, errors = read_and_verify_column_headers_concurrently(
            self.fs, self.region_raw_file_config, self.bq_metadata
        )
        self.assertEqual(
            result,
            {
                1: self.headers,
                3: self.headers,
            },
        )
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].original_file_path, self.file_paths[1])

    def test_conceptual_file_header_mismatch(self) -> None:
        def side_effect(gcs_file_path: GcsfsFilePath) -> List[str]:
            if gcs_file_path == self.file_paths[1]:
                return ["ID", "Name", "Age", "ExtraColumn"]

            return self.headers

        self.file_reader.read_and_validate_column_headers.side_effect = side_effect
        bq_metadata = [
            RawBigQueryFileMetadata(
                file_id=1,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=1, file_id=1, path=self.file_paths[0]
                    ),
                    RawGCSFileMetadata(
                        gcs_file_id=2, file_id=1, path=self.file_paths[1]
                    ),
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
            RawBigQueryFileMetadata(
                file_id=2,
                file_tag="tagBasicData",
                gcs_files=[
                    RawGCSFileMetadata(
                        gcs_file_id=3, file_id=2, path=self.file_paths[2]
                    )
                ],
                update_datetime=datetime.datetime(
                    2024, 1, 2, 1, 1, 1, tzinfo=datetime.UTC
                ),
            ),
        ]

        result, errors = read_and_verify_column_headers_concurrently(
            self.fs, self.region_raw_file_config, bq_metadata
        )

        self.assertEqual(
            result,
            {
                2: self.headers,
            },
        )
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].original_file_path, self.file_paths[1])
