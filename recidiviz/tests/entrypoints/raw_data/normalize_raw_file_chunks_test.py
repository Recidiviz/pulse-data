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

from recidiviz.common.constants.states import StateCode
from recidiviz.entrypoints.raw_data.normalize_raw_file_chunks import (
    FILE_CHUNK_LIST_DELIMITER,
    RawDataChunkNormalizationEntrypoint,
    normalize_raw_file_chunks,
)


class TestRawDataChunkNormalization(unittest.TestCase):
    """Tests for raw data chunk normalization entrypoint."""

    def setUp(self) -> None:
        self.mock_fs = MagicMock()
        self.mock_normalizer_instance = MagicMock()
        self.mock_chunk = MagicMock()
        self.serialized_chunk = "serialized_chunk"
        self.state_code = StateCode("US_XX")
        self.chunks = [self.serialized_chunk]
        self.normalized_chunk = "normalized_chunk"

    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.GcsfsFactory.build"
    )
    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.DirectIngestRawFilePreImportNormalizer"
    )
    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.RequiresPreImportNormalizationFileChunk.deserialize"
    )
    def test_normalize_raw_file_chunks_success(
        self,
        mock_deserialize: MagicMock,
        mock_normalizer: MagicMock,
        mock_gcsfs_factory: MagicMock,
    ) -> None:
        mock_gcsfs_factory.return_value = self.mock_fs
        mock_normalizer.return_value = self.mock_normalizer_instance
        mock_deserialize.return_value = self.mock_chunk

        self.mock_normalizer_instance.normalize_chunk_for_import.return_value.serialize.return_value = (
            self.normalized_chunk
        )

        result = normalize_raw_file_chunks(self.chunks, self.state_code)

        self.assertEqual(result["normalized_chunks"], [self.normalized_chunk])
        self.assertEqual(result["errors"], [])
        mock_deserialize.assert_called_once_with(self.serialized_chunk)
        self.mock_normalizer_instance.normalize_chunk_for_import.assert_called_once_with(
            self.mock_chunk
        )

    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.GcsfsFactory.build"
    )
    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.DirectIngestRawFilePreImportNormalizer"
    )
    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.RequiresPreImportNormalizationFileChunk.deserialize"
    )
    def test_normalize_raw_file_chunks_error(
        self,
        mock_deserialize: MagicMock,
        mock_normalizer: MagicMock,
        mock_gcsfs_factory: MagicMock,
    ) -> None:
        mock_gcsfs_factory.return_value = self.mock_fs
        mock_normalizer.return_value = self.mock_normalizer_instance
        mock_deserialize.return_value = self.mock_chunk

        self.mock_normalizer_instance.normalize_chunk_for_import.side_effect = (
            Exception("Normalization error")
        )

        result = normalize_raw_file_chunks(self.chunks, self.state_code)

        self.assertEqual(result["normalized_chunks"], [])
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("Normalization error", result["errors"][0])
        mock_deserialize.assert_called_once_with(self.serialized_chunk)
        self.mock_normalizer_instance.normalize_chunk_for_import.assert_called_once_with(
            self.mock_chunk
        )

    @patch("recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.save_to_xcom")
    @patch(
        "recidiviz.entrypoints.raw_data.normalize_raw_file_chunks.normalize_raw_file_chunks"
    )
    def test_run_entrypoint(
        self, mock_normalize_raw_file_chunks: MagicMock, mock_save_to_xcom: MagicMock
    ) -> None:
        parser = RawDataChunkNormalizationEntrypoint.get_parser()
        args = parser.parse_args(
            [
                "--file_chunks",
                f"serialized_chunk1{FILE_CHUNK_LIST_DELIMITER}serialized_chunk2",
                "--state_code",
                "US_XX",
            ]
        )

        mock_normalize_raw_file_chunks.return_value = {
            "normalized_chunks": ["normalized_chunk1", "normalized_chunk2"],
            "errors": [],
        }

        RawDataChunkNormalizationEntrypoint.run_entrypoint(args)

        mock_normalize_raw_file_chunks.assert_called_once_with(
            ["serialized_chunk1", "serialized_chunk2"], StateCode("US_XX")
        )
        mock_save_to_xcom.assert_called_once_with(
            {
                "normalized_chunks": ["normalized_chunk1", "normalized_chunk2"],
                "errors": [],
            }
        )
