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
import unittest
from unittest.mock import Mock, patch

import attr

from recidiviz.cloud_storage.gcsfs_csv_chunk_boundary_finder import CsvChunkBoundary
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_pre_import_normalizer import (
    DirectIngestRawFilePreImportNormalizer,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    PreImportNormalizationType,
    RequiresPreImportNormalizationFileChunk,
)
from recidiviz.tests.cloud_storage.gcsfs_csv_chunk_boundary_finder_test import (
    WINDOWS_FILE,
    WINDOWS_FILE_CUSTOM_NEWLINES,
    WINDOWS_FILE_CUSTOM_NEWLINES_CUSTOM_DELIM,
    WINDOWS_FILE_MULIBYTE_NEWLINES,
)
from recidiviz.tests.ingest.direct import fake_regions


class DirectIngestRawFileNormalizationPassTest(unittest.TestCase):
    """Tests for DirectIngestRawFileNormalizationPass"""

    def setUp(self) -> None:
        self.fs = FakeGCSFileSystem()
        self.us_xx_region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions,
        )
        self.sparse_config = DirectIngestRawFileConfig(
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
            always_historical_export=True,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
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
        local_path: str,
        chunk: RequiresPreImportNormalizationFileChunk,
        expected_output: str,
    ) -> None:
        input_gcs_path = GcsfsFilePath.from_absolute_path(chunk.path)
        self.fs.test_add_path(path=input_gcs_path, local_path=local_path)

        normalizer = DirectIngestRawFilePreImportNormalizer(self.fs, StateCode.US_XX)

        pass_result = normalizer.normalize_chunk_for_import(chunk)
        output_gcs_path = GcsfsFilePath.from_absolute_path(pass_result.output_file_path)

        with self.fs.open(output_gcs_path, "r", encoding="UTF-8") as f:
            assert f.read() == expected_output

    def test_encoding_pass_simple(self) -> None:
        windows_config = attr.evolve(self.sparse_config, encoding="WINDOWS-1252")
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path="gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv",
            normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=28, chunk_num=0
            ),
            headers="we-dont-want-headers-here",
        )
        expected_output = "col1,col2,col3\nhello,its,me\n"

        self.run_local_test(WINDOWS_FILE, chunk, expected_output)

    def test_encoding_pass_simple_add_headers(self) -> None:
        windows_config = attr.evolve(self.sparse_config, encoding="WINDOWS-1252")
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path="gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv",
            normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=28, end_exclusive=81, chunk_num=1
            ),
            headers="col1,col2,col3\n",
        )
        expected_output = (
            "col1,col2,col3\ni was,wondering,if\nyou could, decode, the following:\n"
        )

        self.run_local_test(WINDOWS_FILE, chunk, expected_output)

    def test_encoding_pass_simple_add_headers_decode(self) -> None:
        windows_config = attr.evolve(self.sparse_config, encoding="WINDOWS-1252")
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path="gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv",
            normalization_type=PreImportNormalizationType.ENCODING_UPDATE_ONLY,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=81, end_exclusive=93, chunk_num=2
            ),
            headers="col1,col2,col3\n",
        )
        expected_output = "col1,col2,col3\ná,æ,Ö\nì,ÿ,÷\n"

        self.run_local_test(WINDOWS_FILE, chunk, expected_output)

    def test_normalization_pass_simple(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path="gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv",
            normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=28, chunk_num=0
            ),
            headers="we-dont-want-to-add-headers",
        )
        expected_output = '"col1","col2","col3"\n"hello","its","me"\n'
        self.run_local_test(WINDOWS_FILE_CUSTOM_NEWLINES, chunk, expected_output)

    def test_normalization_pass_simple_add_headers_decode(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path="gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv",
            normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=81, end_exclusive=93, chunk_num=1
            ),
            headers='"col1","col2","col3"\n',
        )
        expected_output = '"col1","col2","col3"\n"á","æ","Ö"\n"ì","ÿ","÷"\n'

        self.run_local_test(WINDOWS_FILE_CUSTOM_NEWLINES, chunk, expected_output)

    def test_normalization_pass_more_complex(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
            separator="†",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path="gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv",
            normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=40, chunk_num=0
            ),
            headers="we-dont-want-to-add-headers",
        )
        expected_output = '"col1","col2","col3"\n"hello,,,","its,,,,","me,,,,,"\n'
        self.run_local_test(
            WINDOWS_FILE_CUSTOM_NEWLINES_CUSTOM_DELIM, chunk, expected_output
        )

    def test_normalization_pass_more_complex_add_headers_decode(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡",
            separator="†",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path="gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv",
            normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=96, end_exclusive=108, chunk_num=1
            ),
            headers='"col1","col2","col3"\n',
        )
        expected_output = '"col1","col2","col3"\n"á","æ","Ö"\n"ì","ÿ","÷"\n'

        self.run_local_test(
            WINDOWS_FILE_CUSTOM_NEWLINES_CUSTOM_DELIM, chunk, expected_output
        )

    def test_normalization_pass_more_complex_ii(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡\n",
            separator="†",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path="gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv",
            normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=0, end_exclusive=42, chunk_num=0
            ),
            headers="we-dont-want-to-add-headers",
        )
        expected_output = '"col1","col2","col3"\n"hello,,,","its,,,,","me,,,,,"\n'
        self.run_local_test(WINDOWS_FILE_MULIBYTE_NEWLINES, chunk, expected_output)

    def test_normalization_pass_more_complex_ii_add_headers_decode(self) -> None:
        windows_config = attr.evolve(
            self.sparse_config,
            encoding="WINDOWS-1252",
            custom_line_terminator="‡\n",
            separator="†",
        )
        self.us_xx_region_config.raw_file_configs = {"myFile": windows_config}

        chunk = RequiresPreImportNormalizationFileChunk(
            path="gs://my-bucket/unprocessed_2024-01-20T16:35:33:617135_raw_myFile.csv",
            normalization_type=PreImportNormalizationType.ENCODING_DELIMITER_AND_TERMINATOR_UPDATE,
            chunk_boundary=CsvChunkBoundary(
                start_inclusive=100, end_exclusive=107, chunk_num=3
            ),
            headers='"col1","col2","col3"\n',
        )
        expected_output = '"col1","col2","col3"\n"á","æ","Ö"\n'

        self.run_local_test(WINDOWS_FILE_MULIBYTE_NEWLINES, chunk, expected_output)
