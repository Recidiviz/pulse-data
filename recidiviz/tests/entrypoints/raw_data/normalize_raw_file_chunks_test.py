# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for normalize_raw_file_chunks."""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.types import CsvChunkBoundary
from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.raw_data.normalize_raw_file_chunks import (
    FILE_CHUNK_LIST_DELIMITER,
    RawDataChunkNormalizationEntrypoint,
    normalize_raw_file_chunks,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    PreImportNormalizationType,
    PreImportNormalizedCsvChunkResult,
    RawFileProcessingError,
    RequiresPreImportNormalizationFile,
    RequiresPreImportNormalizationFileChunk,
)
from recidiviz.utils.airflow_types import BatchedTaskInstanceOutput


class TestRawDataChunkNormalization(unittest.TestCase):
    """Tests for raw data chunk normalization entrypoint."""

    def setUp(self) -> None:
        self.mock_fs = MagicMock()
        self.mock_normalizer_instance = MagicMock()

        self.file_path = GcsfsFilePath.from_absolute_path("test_bucket/test_file.csv")
        self.chunk_boundary = CsvChunkBoundary(0, 100, 0)
        self.requires_normalization_chunk = RequiresPreImportNormalizationFileChunk(
            path=self.file_path,
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            chunk_boundary=self.chunk_boundary,
        )
        self.serialized_requires_normalization_chunks = [
            self.requires_normalization_chunk.serialize()
        ]

        self.output_file_path = GcsfsFilePath.from_absolute_path(
            "temp_bucket/temp_test_file_0.csv"
        )
        self.normalized_chunk = PreImportNormalizedCsvChunkResult(
            input_file_path=self.file_path,
            output_file_path=self.output_file_path,
            chunk_boundary=self.chunk_boundary,
            crc32c=0xFFFFF,
        )

        self.state_code = StateCode("US_XX")

        self.save_to_gcs_patcher = patch(
            "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.save_to_gcs_xcom",
        )
        self.mock_save_to_gcs = self.save_to_gcs_patcher.start()

    def tearDown(self) -> None:
        self.save_to_gcs_patcher.stop()

    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.DirectIngestRawFilePreImportNormalizer"
    )
    def test_normalize_raw_file_chunks_success(
        self,
        mock_normalizer: MagicMock,
    ) -> None:
        mock_normalizer.return_value = self.mock_normalizer_instance

        self.mock_normalizer_instance.normalize_chunk_for_import.return_value = (
            self.normalized_chunk
        )

        result = BatchedTaskInstanceOutput.deserialize(
            json_str=normalize_raw_file_chunks(
                self.mock_fs,
                self.serialized_requires_normalization_chunks,
                self.state_code,
            ),
            result_cls=PreImportNormalizedCsvChunkResult,
            error_cls=RawFileProcessingError,
        )

        self.assertEqual(result.results, [self.normalized_chunk])
        self.assertEqual(result.errors, [])
        self.mock_normalizer_instance.normalize_chunk_for_import.assert_called_once_with(
            self.requires_normalization_chunk
        )

    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.DirectIngestRawFilePreImportNormalizer"
    )
    def test_normalize_raw_file_chunks_error(
        self,
        mock_normalizer: MagicMock,
    ) -> None:
        mock_normalizer.return_value = self.mock_normalizer_instance

        self.mock_normalizer_instance.normalize_chunk_for_import.side_effect = (
            Exception("Normalization error")
        )
        self.mock_normalizer_instance.output_path_for_chunk.return_value = (
            GcsfsFilePath(bucket_name="temp", blob_name="file.path")
        )

        result = BatchedTaskInstanceOutput.deserialize(
            json_str=normalize_raw_file_chunks(
                self.mock_fs,
                self.serialized_requires_normalization_chunks,
                self.state_code,
            ),
            result_cls=RequiresPreImportNormalizationFile,
            error_cls=RawFileProcessingError,
        )

        self.assertEqual(result.results, [])
        self.assertEqual(len(result.errors), 1)
        self.assertEqual(self.file_path, result.errors[0].original_file_path)
        self.assertIn("Normalization error", result.errors[0].error_msg)
        self.mock_normalizer_instance.normalize_chunk_for_import.assert_called_once_with(
            self.requires_normalization_chunk
        )

    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.normalize_raw_file_chunks"
    )
    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.GcsfsFactory.build"
    )
    def test_run_entrypoint(
        self,
        mock_fs_factory: MagicMock,
        mock_normalize_raw_file_chunks: MagicMock,
    ) -> None:
        mock_fs_factory.return_value = self.mock_fs
        parser = RawDataChunkNormalizationEntrypoint.get_parser()
        args = parser.parse_args(
            [
                "--file_chunks",
                f"serialized_chunk1{FILE_CHUNK_LIST_DELIMITER}serialized_chunk2",
                "--state_code",
                self.state_code.value,
            ]
        )

        mock_normalize_raw_file_chunks.return_value = {
            "results": ["normalized_chunk1", "normalized_chunk2"],
            "errors": [],
        }

        RawDataChunkNormalizationEntrypoint.run_entrypoint(args=args)

        mock_normalize_raw_file_chunks.assert_called_once_with(
            self.mock_fs, ["serialized_chunk1", "serialized_chunk2"], self.state_code
        )
        self.mock_save_to_gcs.assert_called_once_with(
            fs=self.mock_fs,
            output_str={
                "results": ["normalized_chunk1", "normalized_chunk2"],
                "errors": [],
            },
        )
