# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for BigQueryViewExportValidator implementations."""

import csv
import io
from unittest import TestCase

from recidiviz.big_query.export.big_query_view_export_validator import (
    ExistsBigQueryViewExportValidator,
    NonEmptyColumnsBigQueryViewExportValidator,
)
from recidiviz.big_query.export.export_query_config import ExportOutputFormatType
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class BigQueryViewExportValidatorTestCase(TestCase):
    """Base test case for BigQueryViewExportValidator implementations."""

    def setUp(self) -> None:
        self.fake_gcs = FakeGCSFileSystem()
        self.fake_path = GcsfsFilePath("fake-bucket", "fake-file.csv")


class ExistsBigQueryViewExportValidatorTest(BigQueryViewExportValidatorTestCase):
    """Implements tests for ExistsBigQueryViewExportValidator."""

    def setUp(self) -> None:
        super().setUp()
        self.validator = ExistsBigQueryViewExportValidator(self.fake_gcs)

    def test_valid(self) -> None:
        self.fake_gcs.upload_from_string(self.fake_path, "test contents", "text/csv")

        self.assertTrue(self.validator.validate(self.fake_path, allow_empty=True))
        self.assertTrue(self.validator.validate(self.fake_path, allow_empty=False))

    def test_empty_file(self) -> None:
        self.fake_gcs.upload_from_string(self.fake_path, "", "text/csv")

        self.assertTrue(self.validator.validate(self.fake_path, allow_empty=True))
        self.assertFalse(self.validator.validate(self.fake_path, allow_empty=False))

    def test_does_not_exist(self) -> None:
        self.assertFalse(self.validator.validate(self.fake_path, allow_empty=True))
        self.assertFalse(self.validator.validate(self.fake_path, allow_empty=False))


class NonEmptyColumnsBigQueryViewExportValidatorTest(
    BigQueryViewExportValidatorTestCase
):
    """Implements tests for NonEmptyColumnsBigQueryViewExportValidator."""

    def setUp(self) -> None:
        super().setUp()
        self.validator_with_header = NonEmptyColumnsBigQueryViewExportValidator(
            self.fake_gcs, contains_headers=True
        )
        self.validator_without_header = NonEmptyColumnsBigQueryViewExportValidator(
            self.fake_gcs, contains_headers=False
        )

    def test_supports_output_type(self) -> None:
        self.assertTrue(
            self.validator_with_header.supports_output_type(ExportOutputFormatType.CSV)
        )
        self.assertFalse(
            self.validator_with_header.supports_output_type(
                ExportOutputFormatType.HEADERLESS_CSV
            )
        )
        self.assertTrue(
            self.validator_without_header.supports_output_type(
                ExportOutputFormatType.HEADERLESS_CSV
            )
        )
        self.assertFalse(
            self.validator_without_header.supports_output_type(
                ExportOutputFormatType.CSV
            )
        )

    def test_valid_with_headers(self) -> None:
        csv_string = io.StringIO()
        writer = csv.writer(csv_string)
        writer.writerows(
            [
                ["col1", "col2", "col3"],
                ["val1", None, 0],
                ["val2", "val3", None],
            ]
        )

        self.fake_gcs.upload_from_string(
            self.fake_path, csv_string.getvalue(), "text/csv"
        )
        self.assertTrue(
            self.validator_with_header.validate(self.fake_path, allow_empty=True)
        )
        self.assertTrue(
            self.validator_with_header.validate(self.fake_path, allow_empty=False)
        )

    def test_valid_without_headers(self) -> None:
        csv_string = io.StringIO()
        writer = csv.writer(csv_string)

        writer.writerows(
            [
                ["val1", None, 0],
                ["val2", "val3", None],
            ]
        )
        self.fake_gcs.upload_from_string(
            self.fake_path, csv_string.getvalue(), "text/csv"
        )

        self.assertTrue(
            self.validator_without_header.validate(self.fake_path, allow_empty=True)
        )
        self.assertTrue(
            self.validator_without_header.validate(self.fake_path, allow_empty=False)
        )

    def test_valid_big_file_with_headers(self) -> None:
        csv_string = io.StringIO()
        writer = csv.writer(csv_string)

        writer.writerow(["col1", "col2"])
        writer.writerows([["val", None]] * 1000 + [["val", "val2"]])
        print(f"csv_string: {csv_string.getvalue()}")
        self.fake_gcs.upload_from_string(
            self.fake_path, csv_string.getvalue(), "text/csv"
        )

        self.assertTrue(
            self.validator_with_header.validate(self.fake_path, allow_empty=True)
        )
        self.assertTrue(
            self.validator_with_header.validate(self.fake_path, allow_empty=False)
        )

    def test_valid_big_file_without_headers(self) -> None:
        csv_string = io.StringIO()
        writer = csv.writer(csv_string)

        writer.writerows([["val", None]] * 1000 + [["val", "val2"]])
        self.fake_gcs.upload_from_string(
            self.fake_path, csv_string.getvalue(), "text/csv"
        )

        self.assertTrue(
            self.validator_with_header.validate(self.fake_path, allow_empty=True)
        )
        self.assertTrue(
            self.validator_with_header.validate(self.fake_path, allow_empty=False)
        )

    def test_empty_file(self) -> None:
        self.fake_gcs.upload_from_string(self.fake_path, "", "text/csv")

        self.assertTrue(
            self.validator_with_header.validate(self.fake_path, allow_empty=True)
        )
        self.assertFalse(
            self.validator_with_header.validate(self.fake_path, allow_empty=False)
        )
        self.assertTrue(
            self.validator_without_header.validate(self.fake_path, allow_empty=True)
        )
        self.assertFalse(
            self.validator_without_header.validate(self.fake_path, allow_empty=False)
        )

    def test_empty_column_with_headers(self) -> None:
        csv_string = io.StringIO()
        writer = csv.writer(csv_string)

        writer.writerow(["col1", "col2"])
        writer.writerows([["val", None]] * 2000)
        self.fake_gcs.upload_from_string(
            self.fake_path, csv_string.getvalue(), "text/csv"
        )

        self.assertFalse(
            self.validator_with_header.validate(self.fake_path, allow_empty=True)
        )
        self.assertFalse(
            self.validator_with_header.validate(self.fake_path, allow_empty=False)
        )

    def test_empty_column_without_headers(self) -> None:
        csv_string = io.StringIO()
        writer = csv.writer(csv_string)

        writer.writerows([["val", None]] * 2000)
        self.fake_gcs.upload_from_string(
            self.fake_path, csv_string.getvalue(), "text/csv"
        )

        self.assertFalse(
            self.validator_with_header.validate(self.fake_path, allow_empty=True)
        )
        self.assertFalse(
            self.validator_with_header.validate(self.fake_path, allow_empty=False)
        )

    def test_empty_string_column(self) -> None:
        csv_string = io.StringIO()
        writer = csv.writer(csv_string)

        writer.writerows(
            [
                ["val1", None],
                ["val2", ""],
            ]
        )
        self.fake_gcs.upload_from_string(
            self.fake_path, csv_string.getvalue(), "text/csv"
        )

        self.assertFalse(
            self.validator_with_header.validate(self.fake_path, allow_empty=True)
        )
        self.assertFalse(
            self.validator_with_header.validate(self.fake_path, allow_empty=False)
        )
