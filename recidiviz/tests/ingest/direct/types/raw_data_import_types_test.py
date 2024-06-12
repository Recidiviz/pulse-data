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
"""Tests for raw_data_import_types.py"""
import unittest

import attr

from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.common.constants.csv import (
    DEFAULT_CSV_ENCODING,
    DEFAULT_CSV_LINE_TERMINATOR,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    NormalizedCsvChunkResult,
    PreImportNormalizationType,
    RequiresPreImportNormalizationFileChunk,
    RequiresPreImportNormalizationFileChunks,
)


class PreImportNormalizationTypeTest(unittest.TestCase):
    """Tests for PreImportNormalizationType.required_pre_import_normalization_type"""

    def setUp(self) -> None:
        self.sparse_config = DirectIngestRawFileConfig(
            file_tag="myFile",
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[],
            custom_line_terminator=DEFAULT_CSV_LINE_TERMINATOR,
            primary_key_cols=[],
            supplemental_order_by_clause="",
            encoding=DEFAULT_CSV_ENCODING,
            separator=",",
            ignore_quotes=False,
            always_historical_export=True,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )

    def test_no_line_term(self) -> None:
        test_config = attr.evolve(self.sparse_config, custom_line_terminator=None)
        assert (
            PreImportNormalizationType.required_pre_import_normalization_type(
                test_config
            )
            is None
        )

    def test_all_these_encodings_work(self) -> None:
        for encoding in ["uTf-8", "latin-1", "cp819", "UTF_16-BE", "utF-32be"]:
            test_config = attr.evolve(self.sparse_config, encoding=encoding)
            assert (
                PreImportNormalizationType.required_pre_import_normalization_type(
                    test_config
                )
                is None
            )

    def test_all_these_encodings_require_translation(self) -> None:
        for encoding in ["WINDOWS-1252", "cp1252", "US-ASCII"]:
            test_config = attr.evolve(self.sparse_config, encoding=encoding)
            assert (
                PreImportNormalizationType.required_pre_import_normalization_type(
                    test_config
                )
                == PreImportNormalizationType.ENCODING_UPDATE_ONLY
            )

    def test_all_of_these_line_term_require_normalization(self) -> None:
        for line_term in ["†", "†\n", "‡", "‡\n", "n", ","]:
            test_config = attr.evolve(
                self.sparse_config, custom_line_terminator=line_term
            )
            assert (
                PreImportNormalizationType.required_pre_import_normalization_type(
                    test_config
                )
                == PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE
            )

    def test_none_of_these_sep_require_normalization(self) -> None:
        for line_term in [",", "|", "\t", "?"]:
            test_config = attr.evolve(self.sparse_config, separator=line_term)
            assert (
                PreImportNormalizationType.required_pre_import_normalization_type(
                    test_config
                )
                is None
            )

    def test_two_byte_sep(self) -> None:
        # ‡ is 2 bytes in utf-8
        test_config = attr.evolve(self.sparse_config, separator="‡", encoding="UTF-8")
        assert (
            PreImportNormalizationType.required_pre_import_normalization_type(
                test_config
            )
            == PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE
        )

    def test_two_byte_sep_ii(self) -> None:
        # ‡ is only 1 bytes in windows-1252 but since we have to convert it to utf-8
        # we want to make sure we measure it's size in utf-8 (which is 2 bytes)
        test_config = attr.evolve(
            self.sparse_config, separator="‡", encoding="WINDOWS-1252"
        )
        assert (
            PreImportNormalizationType.required_pre_import_normalization_type(
                test_config
            )
            == PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE
        )


class TestSerialization(unittest.TestCase):
    """Test serialization and deserialization of RequiresPreImportNormalizationFileChunk and NormalizedCsvChunkResult"""

    def test_requires_pre_import_normalization_file_chunk(self) -> None:
        chunk_boundary = CsvChunkBoundary(
            start_inclusive=0, end_exclusive=100, chunk_num=0
        )
        original = RequiresPreImportNormalizationFileChunk(
            path="path/to/file.csv",
            normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=chunk_boundary,
            headers="id,name,age",
        )

        serialized = original.serialize()
        deserialized = RequiresPreImportNormalizationFileChunk.deserialize(serialized)

        self.assertEqual(original.path, deserialized.path)
        self.assertEqual(original.headers, deserialized.headers)
        self.assertEqual(
            original.chunk_boundary.start_inclusive,
            deserialized.chunk_boundary.start_inclusive,
        )
        self.assertEqual(
            original.chunk_boundary.end_exclusive,
            deserialized.chunk_boundary.end_exclusive,
        )
        self.assertEqual(
            original.chunk_boundary.chunk_num, deserialized.chunk_boundary.chunk_num
        )
        self.assertEqual(original.normalization_type, deserialized.normalization_type)

    def test_requires_pre_import_normalization_file_chunks(self) -> None:
        chunk_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=100, chunk_num=0),
            CsvChunkBoundary(start_inclusive=100, end_exclusive=200, chunk_num=1),
        ]
        original = RequiresPreImportNormalizationFileChunks(
            path="path/to/file.csv",
            normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundaries=chunk_boundaries,
            headers="id,name,age",
        )

        serialized = original.serialize()
        deserialized = RequiresPreImportNormalizationFileChunks.deserialize(serialized)

        self.assertEqual(original.path, deserialized.path)
        self.assertEqual(original.headers, deserialized.headers)
        self.assertEqual(original.chunk_boundaries, deserialized.chunk_boundaries)
        self.assertEqual(original.normalization_type, deserialized.normalization_type)

    def test_normalized_csv_chunk_result(self) -> None:
        chunk_boundary = CsvChunkBoundary(
            start_inclusive=0, end_exclusive=100, chunk_num=0
        )
        original = NormalizedCsvChunkResult(
            path="path/to/result.csv", chunk_boundary=chunk_boundary, crc32c="abcd1234"
        )

        serialized = original.serialize()
        deserialized = NormalizedCsvChunkResult.deserialize(serialized)

        self.assertEqual(original.path, deserialized.path)
        self.assertEqual(original.crc32c, deserialized.crc32c)
        self.assertEqual(
            original.chunk_boundary.start_inclusive,
            deserialized.chunk_boundary.start_inclusive,
        )
        self.assertEqual(
            original.chunk_boundary.end_exclusive,
            deserialized.chunk_boundary.end_exclusive,
        )
        self.assertEqual(
            original.chunk_boundary.chunk_num, deserialized.chunk_boundary.chunk_num
        )
