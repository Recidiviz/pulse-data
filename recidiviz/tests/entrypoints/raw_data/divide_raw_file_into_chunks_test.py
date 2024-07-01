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
"""Tests for divide_raw_file_into_chunks."""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks import (
    FILE_LIST_DELIMITER,
    RawDataFileChunkingEntrypoint,
    extract_file_chunks_concurrently,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    BatchedTaskInstanceOutput,
    PreImportNormalizationType,
    RequiresNormalizationFile,
    RequiresPreImportNormalizationFile,
)


class TestExtractFileChunksConcurrently(unittest.TestCase):
    """Tests for processing files into chunks"""

    TEST_BUCKET = "test_bucket"
    INGEST_PREFIX = "unprocessed_2023-11-13T08:02:58:832284_raw_"
    INGEST_SUFFIX = ".csv"

    @classmethod
    def _formatted_raw_file_path(cls, file_tag: str) -> str:
        return f"{cls.TEST_BUCKET}/{cls.INGEST_PREFIX}{file_tag}{cls.INGEST_SUFFIX}"

    def setUp(self) -> None:
        self.state_code = StateCode("US_XX")
        self.requires_normalization_files = [
            RequiresNormalizationFile(
                path=self._formatted_raw_file_path("test1"),
                normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            ).serialize(),
            RequiresNormalizationFile(
                path=self._formatted_raw_file_path("test2"),
                normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            ).serialize(),
        ]
        self.region_raw_file_config = MagicMock()
        self.region_raw_file_config.raw_file_configs = {
            "test1": "example_config",
            "test2": "example_config",
        }
        patch_region_config = patch(
            "recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks.DirectIngestRegionRawFileConfig",
            return_value=self.region_raw_file_config,
        ).start()
        self.addCleanup(patch_region_config.stop)

        self.fs = MagicMock()
        patch_gcsfs_build = patch(
            "recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks.GcsfsFactory.build",
            return_value=self.fs,
        ).start()
        self.addCleanup(patch_gcsfs_build.stop)

        mock_chunker = MagicMock()
        mock_chunker.get_chunks_for_gcs_path.return_value = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=100, chunk_num=0)
        ]
        patch_chunker = patch(
            "recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks.GcsfsCsvChunkBoundaryFinder",
            return_value=mock_chunker,
        ).start()
        self.addCleanup(patch_chunker.stop)

        patch_headers = patch(
            "recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks._get_file_headers",
            return_value=["ID", "Name", "Age"],
        ).start()
        self.addCleanup(patch_headers.stop)

    def test_successful_processing(self) -> None:
        serialized_result = extract_file_chunks_concurrently(
            self.requires_normalization_files, self.state_code
        )
        result = BatchedTaskInstanceOutput.deserialize(
            serialized_result, result_cls=RequiresPreImportNormalizationFile
        )
        self.assertEqual(len(result.results), 2)
        self.assertEqual(len(result.errors), 0)

    def test_partial_failures(self) -> None:
        def side_effect(
            _fs: GCSFileSystem,
            requires_normalization_file: RequiresNormalizationFile,
            _region_raw_file_config: DirectIngestRegionRawFileConfig,
        ) -> RequiresPreImportNormalizationFile:
            if requires_normalization_file.path == self._formatted_raw_file_path(
                "test2"
            ):
                raise RuntimeError("Error processing file")

            return RequiresPreImportNormalizationFile(
                path=requires_normalization_file.path,
                chunk_boundaries=[],
                normalization_type=requires_normalization_file.normalization_type,
                headers=["ID", "Name", "Age"],
            )

        with patch(
            "recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks._extract_file_chunks",
            side_effect=side_effect,
        ):
            serialized_result = extract_file_chunks_concurrently(
                self.requires_normalization_files, self.state_code
            )
        result = BatchedTaskInstanceOutput.deserialize(
            serialized_result, result_cls=RequiresPreImportNormalizationFile
        )
        self.assertEqual(len(result.results), 1)
        self.assertEqual(len(result.errors), 1)

    @patch("recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks.save_to_xcom")
    @patch(
        "recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks.extract_file_chunks_concurrently"
    )
    def test_run_entrypoint(
        self,
        mock_extract_file_chunks_concurrently: MagicMock,
        mock_save_to_xcom: MagicMock,
    ) -> None:
        parser = RawDataFileChunkingEntrypoint.get_parser()
        args = parser.parse_args(
            [
                "--requires_normalization_files",
                f"serialized_file1{FILE_LIST_DELIMITER}serialized_file2",
                "--state_code",
                self.state_code.value,
            ]
        )

        mock_extract_file_chunks_concurrently.return_value = {
            "results": ["chunked_file1", "chunked_file2"],
            "errors": [],
        }

        RawDataFileChunkingEntrypoint.run_entrypoint(args)

        mock_extract_file_chunks_concurrently.assert_called_once_with(
            ["serialized_file1", "serialized_file2"], self.state_code
        )
        mock_save_to_xcom.assert_called_once_with(
            {
                "results": ["chunked_file1", "chunked_file2"],
                "errors": [],
            }
        )
