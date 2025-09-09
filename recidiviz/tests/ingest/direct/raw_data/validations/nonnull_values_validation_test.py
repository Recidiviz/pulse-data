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
"""Unit tests for nonnull_values_column_validation.py."""


import attr

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    RawDataExportLookbackWindow,
)
from recidiviz.ingest.direct.raw_data.validations.nonnull_values_validation import (
    NonNullValuesValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct.raw_data.validations.import_blocking_validation_test_case import (
    RawDataImportBlockingValidationTestCase,
)


class TestNonNullValuesValidation(RawDataImportBlockingValidationTestCase):
    """Unit tests for NonNullValuesValidation"""

    def setUp(self) -> None:
        super().setUp()
        self.validation = NonNullValuesValidation.create_validation(self.context)

    def test_validation_success(self) -> None:
        valid_data: list[dict[str, str | None]] = [
            {self.happy_col_name: "1", self.sad_col_name: "3"},
            {self.happy_col_name: "2", self.sad_col_name: None},
        ]
        self.validation_success_test(validation=self.validation, test_data=valid_data)

    def test_validation_failure(self) -> None:
        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.NONNULL_VALUES,
            validation_query=self.validation.build_query(),
            error_msg=f"Found column(s) on raw file [{self.file_tag}] with only null values.\nColumn name: [{self.sad_col_name}]",
        )
        invalid_data: list[dict[str, str | None]] = [
            {self.happy_col_name: "1", self.sad_col_name: None},
            {self.happy_col_name: "2", self.sad_col_name: None},
        ]

        self.validation_failure_test(
            validation=self.validation,
            test_data=invalid_data,
            expected_error=expected_error,
        )

    def test_validation_applies_to_file(self) -> None:
        incremental_raw_file_config = attr.evolve(
            self.raw_file_config,
            export_lookback_window=RawDataExportLookbackWindow.TWO_WEEK_INCREMENTAL_LOOKBACK,
        )
        incremental_context = attr.evolve(
            self.context, raw_file_config=incremental_raw_file_config
        )

        self.assertTrue(
            NonNullValuesValidation.validation_applies_to_file(self.context)
        )
        self.assertFalse(
            NonNullValuesValidation.validation_applies_to_file(incremental_context)
        )
