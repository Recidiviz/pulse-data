# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for raw_data_pre_import_validation.py"""

import unittest

from recidiviz.ingest.direct.types.raw_data_pre_import_validation import (
    RawDataBlockingValidationFailure,
    RawDataNonBlockingValidationFailure,
    RawDataPreImportValidationError,
)
from recidiviz.ingest.direct.types.raw_data_pre_import_validation_type import (
    RawDataPreImportValidationType,
)


class TestRawDataPreImportValidationError(unittest.TestCase):
    """Unit tests for RawDataPreImportValidationError."""

    def test_str_returns_error_message(self) -> None:
        error = RawDataPreImportValidationError(
            file_tag="my_file",
            failures=[
                RawDataBlockingValidationFailure(
                    validation_type=RawDataPreImportValidationType.NONNULL_VALUES,
                    validation_query="SELECT 1",
                    error_msg="all values null in col foo",
                )
            ],
        )
        expected = (
            "1 pre-import validation(s) failed for file [my_file]."
            " If you wish [my_file] to be permanently excluded from any validation, "
            " please add the validation_type and exemption_reason to pre_import_validation_exemptions"
            " for a table-wide exemption or to pre_import_column_validation_exemptions"
            " for a column-specific exemption in the raw file config."
            "\n**Blocking failure**"
            "\nFile import blocked until issue is addressed."
            "\nError: all values null in col foo"
            "\nValidation type: NONNULL_VALUES"
            "\nValidation query: SELECT 1"
        )
        self.assertEqual(str(error), expected)
        self.assertIsNone(error.non_blocking_failure_message)

    def test_non_blocking_failure_message_with_warnings(self) -> None:
        error = RawDataPreImportValidationError(
            file_tag="my_file",
            failures=[
                RawDataBlockingValidationFailure(
                    validation_type=RawDataPreImportValidationType.NONNULL_VALUES,
                    validation_query="SELECT 1",
                    error_msg="all values null in col foo",
                )
            ],
            warnings=[
                RawDataNonBlockingValidationFailure(
                    validation_type=RawDataPreImportValidationType.KNOWN_VALUES,
                    validation_query="SELECT 2",
                    error_msg="unknown value 'Z' in column bar",
                )
            ],
        )
        expected = (
            "1 pre-import validation(s) failed for file [my_file]."
            " If you wish [my_file] to be permanently excluded from any validation, "
            " please add the validation_type and exemption_reason to pre_import_validation_exemptions"
            " for a table-wide exemption or to pre_import_column_validation_exemptions"
            " for a column-specific exemption in the raw file config."
            "\n**Non-blocking failure**"
            "\nThis failure does not block the file import but should be reviewed and addressed."
            "\nError: unknown value 'Z' in column bar"
            "\nValidation type: KNOWN_VALUES"
            "\nValidation query: SELECT 2"
        )
        self.assertEqual(error.non_blocking_failure_message, expected)
