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
from typing import Any, Type

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
    BatchedTaskInstanceOutput,
    ImportReadyNormalizedFile,
    NormalizedCsvChunkResult,
    PreImportNormalizationType,
    RequiresNormalizationFile,
    RequiresPreImportNormalizationFile,
    RequiresPreImportNormalizationFileChunk,
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
            headers=["id", "name", "age"],
        )

        self._validate_serialization(original, RequiresPreImportNormalizationFileChunk)

    def test_requires_pre_import_normalization_file(self) -> None:
        chunk_boundaries = [
            CsvChunkBoundary(start_inclusive=0, end_exclusive=100, chunk_num=0),
            CsvChunkBoundary(start_inclusive=100, end_exclusive=200, chunk_num=1),
        ]
        original = RequiresPreImportNormalizationFile(
            path="path/to/file.csv",
            normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundaries=chunk_boundaries,
            headers=["id", "name", "age"],
        )

        self._validate_serialization(original, RequiresPreImportNormalizationFile)

    def test_normalized_csv_chunk_result(self) -> None:
        chunk_boundary = CsvChunkBoundary(
            start_inclusive=0, end_exclusive=100, chunk_num=0
        )
        original = NormalizedCsvChunkResult(
            input_file_path="path/to/file.csv",
            output_file_path="path/to/result.csv",
            chunk_boundary=chunk_boundary,
            crc32c=0xFFFFFFFF,
        )

        self._validate_serialization(original, NormalizedCsvChunkResult)

    def test_import_ready_normalized_file(self) -> None:
        original = ImportReadyNormalizedFile(
            input_file_path="test_bucket/file", output_file_paths=["temp_bucket/file_0"]
        )

        self._validate_serialization(original, ImportReadyNormalizedFile)

    def test_requires_normalization_file(self) -> None:
        original = RequiresNormalizationFile(
            path="test/file.csv",
            normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
        )

        self._validate_serialization(original, RequiresNormalizationFile)

    def test_task_result(self) -> None:
        result = ImportReadyNormalizedFile(
            input_file_path="test_bucket/file", output_file_paths=["temp_bucket/file_0"]
        )
        original = BatchedTaskInstanceOutput(results=[result], errors=[])

        serialized = original.serialize()
        deserialized = BatchedTaskInstanceOutput.deserialize(
            json_str=serialized, result_cls=ImportReadyNormalizedFile
        )

        self.assertEqual(original, deserialized)

    def _validate_serialization(self, obj: Any, obj_type: Type) -> None:
        serialized = obj.serialize()
        deserialized = obj_type.deserialize(serialized)

        self.assertEqual(obj, deserialized)
