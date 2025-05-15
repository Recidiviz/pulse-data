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
"""Tests for direct_ingest_raw_file_normalization_pass.py"""
import os
import unittest
from unittest.mock import Mock, patch

import attr

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.cloud_storage.types import CsvChunkBoundary
from recidiviz.common.constants.states import StateCode
from recidiviz.common.io.codec_error_handler import ExceededDecodingErrorThreshold
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_pre_import_normalizer import (
    DirectIngestRawFilePreImportNormalizer,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    PreImportNormalizationType,
    PreImportNormalizedCsvChunkResult,
    RequiresPreImportNormalizationFileChunk,
)
from recidiviz.tests.cloud_storage import fixtures
from recidiviz.tests.ingest.direct import fake_regions

WINDOWS_FILE = "windows_file.csv"

WINDOWS_FILE_CUSTOM_NEWLINES = "windows_file_with_custom_newlines.csv"

WINDOWS_FILE_CUSTOM_NEWLINES_CUSTOM_DELIM = (
    "windows_file_with_custom_newlines_custom_delim.csv"
)

WINDOWS_FILE_MULIBYTE_NEWLINES = "windows_file_with_multibyte_newlines.csv"

WINDOWS_FILE_CUSTOM_TERMINATOR_TRAILING_NEWLINE = (
    "windows_file_custom_terminator_trailing_newline.csv"
)

WINDOWS_FILE_CUSTOM_DELIM_TERMINATOR_UNPARSEABLE = (
    "windows_file_with_custom_newlines_custom_delim_unparseable.csv"
)


class DirectIngestRawFileNormalizationPassTest(unittest.TestCase):
    """Tests for DirectIngestRawFileNormalizationPass"""

    def setUp(self) -> None:
        self.fs = FakeGCSFileSystem()
        self.us_xx_region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions,
        )
        self.sparse_config = DirectIngestRawFileConfig(
            state_code=StateCode.US_XX,
            file_tag="myFile",
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[],
            custom_line_terminator=None,
            primary_key_cols=[],
            supplemental_order_by_clause="",
            encoding="UTF-8",
            separator=",",
            ignore_quotes=True,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
            no_valid_primary_keys=False,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": self.sparse_config}
        self.region_patcher = patch(
            "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_pre_import_normalizer.get_region_raw_file_config",
            Mock(return_value=self.us_xx_region_config),
        )

        self.region_patcher.start()

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.region_patcher.stop()
        self.project_id_patcher.stop()

    def run_local_test(
        self,
        fixture_name: str,
        chunk: RequiresPreImportNormalizationFileChunk,
        expected_output: str,
    ) -> PreImportNormalizedCsvChunkResult:
        fixture_path = os.path.abspath(
            os.path.join(os.path.dirname(fixtures.__file__), fixture_name)
        )
        self.fs.test_add_path(path=chunk.path, local_path=fixture_path)

        normalizer = DirectIngestRawFilePreImportNormalizer(self.fs, StateCode.US_XX)

        pass_result = normalizer.normalize_chunk_for_import(chunk)

        with self.fs.open(pass_result.output_file_path, "r", encoding="UTF-8") as f:
            assert f.read() == expected_output

        return pass_result

    def test_encoding_pass_simple_strip_headers(self) -> None:
        windows_config = attr.evolve(self.sparse_config, encoding="WINDOWS-1252")
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=28, chunk_num=0
            ),
        )
        expected_output = "hello,its,me\n"

        self.run_local_test(WINDOWS_FILE, chunk, expected_output)

    def test_encoding_pass_simple(self) -> None:
        windows_config = attr.evolve(self.sparse_config, encoding="WINDOWS-1252")
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=28, end_exclusive=81, chunk_num=1
            ),
        )
        expected_output = "i was,wondering,if\nyou could, decode, the following:\n"

        self.run_local_test(WINDOWS_FILE, chunk, expected_output)

    def test_encoding_pass_simple_decode(self) -> None:
        windows_config = attr.evolve(self.sparse_config, encoding="WINDOWS-1252")
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=81, end_exclusive=93, chunk_num=2
            ),
        )
        expected_output = "á,æ,Ö\nì,ÿ,÷\n"

        self.run_local_test(WINDOWS_FILE, chunk, expected_output)

    def test_normalization_pass_simple_strip_headers(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=28, chunk_num=0
            ),
        )
        expected_output = '"hello","its","me"\n'
        self.run_local_test(WINDOWS_FILE_CUSTOM_NEWLINES, chunk, expected_output)

    def test_normalization_pass_simple_decode(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=81, end_exclusive=93, chunk_num=1
            ),
        )
        expected_output = '"á","æ","Ö"\n"ì","ÿ","÷"\n'

        self.run_local_test(WINDOWS_FILE_CUSTOM_NEWLINES, chunk, expected_output)

    def test_normalization_pass_simple_custom_terminator_trailing_newline(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=100, chunk_num=0
            ),
        )
        expected_output = '"hello","its","me"\n"i was","wondering","if"\n"you could"," decode"," the following:"'
        self.run_local_test(
            WINDOWS_FILE_CUSTOM_TERMINATOR_TRAILING_NEWLINE, chunk, expected_output
        )

    def test_normalization_pass_more_complex_strip_headers(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
            separator="†",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=40, chunk_num=0
            ),
        )
        expected_output = '"hello,,,","its,,,,","me,,,,,"\n'
        self.run_local_test(
            WINDOWS_FILE_CUSTOM_NEWLINES_CUSTOM_DELIM, chunk, expected_output
        )

    def test_normalization_pass_more_complex(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
            separator="†",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=96, end_exclusive=108, chunk_num=1
            ),
        )
        expected_output = '"á","æ","Ö"\n"ì","ÿ","÷"\n'

        self.run_local_test(
            WINDOWS_FILE_CUSTOM_NEWLINES_CUSTOM_DELIM, chunk, expected_output
        )

    def test_normalization_pass_more_complex_ii_strip_headers(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡\n",
            separator="†",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=42, chunk_num=0
            ),
        )
        expected_output = '"hello,,,","its,,,,","me,,,,,"\n'
        self.run_local_test(WINDOWS_FILE_MULIBYTE_NEWLINES, chunk, expected_output)

    def test_normalization_pass_more_complex_ii(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡\n",
            separator="†",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=100, end_exclusive=107, chunk_num=3
            ),
        )
        expected_output = '"á","æ","Ö"\n'

        self.run_local_test(WINDOWS_FILE_MULIBYTE_NEWLINES, chunk, expected_output)

    def test_normalization_replace_bytes_none_replaced(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡\n",
            separator="†",
            max_num_unparseable_bytes=0,
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=100, end_exclusive=107, chunk_num=3
            ),
        )
        expected_output = '"á","æ","Ö"\n'

        self.run_local_test(WINDOWS_FILE_MULIBYTE_NEWLINES, chunk, expected_output)

    def test_normalization_replace_bytes_some_replaced(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
            separator="†",
            max_num_unparseable_bytes=3,
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=40, end_exclusive=111, chunk_num=1
            ),
        )
        # the three ? marks are replacement chars for un-parse-able bytes
        expected_output = '"i was","wondering","if"\n"you could",",,decode",",,,the following:"\n"á?","æ","Ö"\n"ì??","ÿ","÷"\n'

        result = self.run_local_test(
            WINDOWS_FILE_CUSTOM_DELIM_TERMINATOR_UNPARSEABLE, chunk, expected_output
        )

        assert len(result.byte_decoding_errors) == 3
        assert result.byte_decoding_errors == [
            "'charmap' codec can't decode byte 0x9d in position 57: character maps to <undefined>",
            "'charmap' codec can't decode byte 0x9d in position 64: character maps to <undefined>",
            "'charmap' codec can't decode byte 0x9d in position 65: character maps to <undefined>",
        ]

    def test_normalization_replace_bytes_too_many(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
            separator="†",
            max_num_unparseable_bytes=2,
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path=GcsfsFilePath.from_absolute_path(
                "gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv"
            ),
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=40, end_exclusive=111, chunk_num=1
            ),
        )
        expected_output = '"i was","wondering","if"\n"you could",",,decode",",,,the following:"\n"á?","æ","Ö"\n"ì??","ÿ","÷"\n'

        with self.assertRaisesRegex(
            ExceededDecodingErrorThreshold,
            r"Exceeded max number of decoding errors \[2\]:\n\t- 'charmap' codec can't decode byte 0x9d in position 57: character maps to <undefined>\n\t- 'charmap' codec can't decode byte 0x9d in position 64: character maps to <undefined>\n\t- 'charmap' codec can't decode byte 0x9d in position 65: character maps to <undefined>",
        ):
            self.run_local_test(
                WINDOWS_FILE_CUSTOM_DELIM_TERMINATOR_UNPARSEABLE, chunk, expected_output
            )

    def test_temp_file_naming_multiple_update_datetimes(self) -> None:
        normalizer = DirectIngestRawFilePreImportNormalizer(self.fs, StateCode.US_XX)

        file_tag = "myFile"
        older_path = GcsfsFilePath.from_absolute_path(
            f"gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_{file_tag}.csv"
        )
        older_chunk = RequiresPreImportNormalizationFileChunk(
            path=older_path,
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=100, chunk_num=0
            ),
        )
        newer_path = GcsfsFilePath.from_absolute_path(
            f"gs://my-bucket/unprocessed_2024-01-21T16:35:33:617135_raw_{file_tag}.csv"
        )
        newer_chunk = RequiresPreImportNormalizationFileChunk(
            path=newer_path,
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=100, chunk_num=0
            ),
        )

        self.assertNotEqual(
            normalizer.output_path_for_chunk(older_chunk),
            normalizer.output_path_for_chunk(newer_chunk),
        )

    def test_temp_file_naming_chunked_file(self) -> None:
        normalizer = DirectIngestRawFilePreImportNormalizer(self.fs, StateCode.US_XX)

        file_tag = "myFile"
        update_datetime = "2024-01-20T16:35:33:617135"
        chunked_file_1_path = GcsfsFilePath.from_absolute_path(
            f"gs://my-bucket/unprocessed_{update_datetime}_raw_{file_tag}-1.csv"
        )
        chunk1 = RequiresPreImportNormalizationFileChunk(
            path=chunked_file_1_path,
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=100, chunk_num=0
            ),
        )
        chunked_file_2_path = GcsfsFilePath.from_absolute_path(
            f"gs://my-bucket/unprocessed_{update_datetime}_raw_{file_tag}-2.csv"
        )
        chunk2 = RequiresPreImportNormalizationFileChunk(
            path=chunked_file_2_path,
            pre_import_normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=100, chunk_num=0
            ),
        )

        self.assertNotEqual(
            normalizer.output_path_for_chunk(chunk1),
            normalizer.output_path_for_chunk(chunk2),
        )
