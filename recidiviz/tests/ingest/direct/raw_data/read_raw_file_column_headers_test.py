# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Test for read_raw_file_column_headers.py"""
import os
import unittest

import attr

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.raw_data.read_raw_file_column_headers import (
    DirectIngestRawFileHeaderReader,
)
from recidiviz.tests.ingest.direct import direct_ingest_fixtures
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module


class ValidateRawFileColumnHeadersTest(unittest.TestCase):
    """Tests for normalize_and_validate_column_headers"""

    def setUp(self) -> None:
        self.fs = FakeGCSFileSystem()

        self.state_code = "us_xx"
        file_tag = "tagCustomLineTerminatorNonUTF8"
        self.file_path = self._get_and_register_csv_gcs_path(file_tag, ".txt")

        self.region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx", region_module=fake_regions_module
        )
        self.default_file_config = self.region_raw_file_config.raw_file_configs[
            file_tag
        ]

    def test_no_valid_encoding(self) -> None:
        updated_file_config = attr.evolve(
            self.default_file_config, encoding="utf-32-le"
        )
        header_reader = DirectIngestRawFileHeaderReader(self.fs, updated_file_config)

        with self.assertRaisesRegex(ValueError, r"Unable to read path"):
            header_reader.read_and_validate_column_headers(self.file_path)

    def test_custom_line_terminator(self) -> None:
        header_reader = DirectIngestRawFileHeaderReader(
            self.fs, self.default_file_config
        )

        result = header_reader.read_and_validate_column_headers(self.file_path)

        self.assertEqual(result, ["PRIMARY_COL1", "COL2", "COL3", "COL4"])

    def test_wrong_line_terminator(self) -> None:
        updated_file_config = attr.evolve(
            self.default_file_config, custom_line_terminator="^"
        )

        header_reader = DirectIngestRawFileHeaderReader(self.fs, updated_file_config)

        with self.assertRaisesRegex(
            ValueError, r"^Column name \[COL4_1000\] not found in config"
        ):
            header_reader.read_and_validate_column_headers(self.file_path)

    def test_wrong_column_separator(self) -> None:
        updated_file_config = attr.evolve(self.default_file_config, separator="#")

        header_reader = DirectIngestRawFileHeaderReader(self.fs, updated_file_config)

        with self.assertRaisesRegex(
            ValueError,
            r"^Column name \[PRIMARY_COL1_COL2_COL3_COL4\] not found in config",
        ):
            header_reader.read_and_validate_column_headers(self.file_path)

    def test_empty_file(self) -> None:
        file_tag = "tagFullyEmptyFile"
        file_path = self._get_and_register_csv_gcs_path(file_tag)

        header_reader = DirectIngestRawFileHeaderReader(
            self.fs, self.default_file_config
        )

        with self.assertRaisesRegex(
            ValueError, r"empty or does not contain valid rows"
        ):
            header_reader.read_and_validate_column_headers(file_path)

    def test_multibyte_raw_file_alternate_separator_and_encoding(self) -> None:
        file_tag = "tagDoubleDaggerWINDOWS1252"
        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        file_path = self._get_and_register_csv_gcs_path(file_tag)

        header_reader = DirectIngestRawFileHeaderReader(self.fs, file_config)
        result = header_reader.read_and_validate_column_headers(file_path)

        self.assertEqual(result, ["PRIMARY_COL1", "COL2", "COL3", "COL4"])

    def test_column_capitalization_doesnt_match_config(self) -> None:
        file_tag = "tagColCapsDoNotMatchConfig"
        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        file_path = self._get_and_register_csv_gcs_path(file_tag)

        header_reader = DirectIngestRawFileHeaderReader(self.fs, file_config)
        result = header_reader.read_and_validate_column_headers(file_path)

        self.assertEqual(result, ["COL1", "COL_2", "Col3"])

    def test_infer_columns_unexpected_header_row(self) -> None:
        file_tag = "tagFileConfigHeadersUnexpectedHeader"
        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        file_path = self._get_and_register_csv_gcs_path(file_tag)

        header_reader = DirectIngestRawFileHeaderReader(self.fs, file_config)

        with self.assertRaisesRegex(
            ValueError,
            "Found unexpected header in the CSV. Please remove the header row from the CSV",
        ):
            header_reader.read_and_validate_column_headers(file_path)

    def test_headers_normalized_for_bq(self) -> None:
        file_tag = "tagInvalidCharacters"
        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        file_path = self._get_and_register_csv_gcs_path(file_tag)

        header_reader = DirectIngestRawFileHeaderReader(self.fs, file_config)
        result = header_reader.read_and_validate_column_headers(file_path)

        self.assertEqual(result, ["COL_1", "_COL2", "_3COL", "_4_COL"])

    def test_headers_not_found_in_config(self) -> None:
        file_tag = "tagNormalizationConflict"
        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        file_path = self._get_and_register_csv_gcs_path(file_tag)

        header_reader = DirectIngestRawFileHeaderReader(self.fs, file_config)

        with self.assertRaisesRegex(
            ValueError, r"^Column name \[_COL2\] not found in config"
        ):
            header_reader.read_and_validate_column_headers(file_path)

    def test_headers_not_found_allow_incomplete_config(self) -> None:
        file_tag = "tagBasicData"
        file_path = self._get_and_register_csv_gcs_path(file_tag)

        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        updated_file_config = attr.evolve(
            file_config,
            columns=[
                RawTableColumnInfo(
                    name="COL1",
                    description="is primary key",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                ),
            ],
        )

        header_reader = DirectIngestRawFileHeaderReader(
            self.fs, updated_file_config, allow_incomplete_configs=True
        )

        result = header_reader.read_and_validate_column_headers(file_path)
        self.assertEqual(result, ["COL1", "COL2", "COL3"])

    def test_infer_headers_mismatched_column_count(self) -> None:
        file_tag = "tagInvalidFileConfigHeaders"
        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        file_path = self._get_and_register_csv_gcs_path(file_tag)

        header_reader = DirectIngestRawFileHeaderReader(self.fs, file_config)

        with self.assertRaisesRegex(
            ValueError,
            r".*Make sure all expected columns are defined in the raw data configuration.$",
        ):
            header_reader.read_and_validate_column_headers(file_path)

    def test_custom_separator(self) -> None:
        file_tag = "tagPipeSeparatedNonUTF8"
        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        file_path = self._get_and_register_csv_gcs_path(file_tag, suffix=".txt")

        header_reader = DirectIngestRawFileHeaderReader(self.fs, file_config)

        result = header_reader.read_and_validate_column_headers(file_path)
        self.assertEqual(result, ["PRIMARY_COL1", "COL2", "COL3", "COL4"])

    def test_infer_columns_from_config(self) -> None:
        file_tag = "tagFileConfigHeaders"
        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        file_path = self._get_and_register_csv_gcs_path(file_tag)

        header_reader = DirectIngestRawFileHeaderReader(self.fs, file_config)

        result = header_reader.read_and_validate_column_headers(file_path)
        self.assertEqual(result, ["COL1", "COL2", "COL3"])

    def _get_and_register_csv_gcs_path(
        self, file_tag: str, suffix: str = ".csv"
    ) -> GcsfsFilePath:
        local_path = os.path.join(
            os.path.relpath(
                os.path.dirname(direct_ingest_fixtures.__file__),
            ),
            self.state_code + "/" + file_tag + suffix,
        )
        gcs_path = GcsfsFilePath.from_absolute_path(local_path)

        self.fs.test_add_path(gcs_path, local_path)

        return gcs_path
