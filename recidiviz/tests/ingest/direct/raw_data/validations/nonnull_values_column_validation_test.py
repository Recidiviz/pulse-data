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

from typing import Dict, List, Optional, Type

from recidiviz.ingest.direct.raw_data.validations.nonnull_values_column_validation import (
    NonNullValuesColumnValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct.raw_data.validations.column_validation_test_case import (
    ColumnValidationTestCase,
)


class TestNonNullValuesColumnValidation(ColumnValidationTestCase):
    """Unit tests for NonNullValuesColumnValidation"""

    def get_validation_class(self) -> Type[RawDataColumnImportBlockingValidation]:
        return NonNullValuesColumnValidation

    def get_test_data(self) -> List[Dict[str, Optional[str]]]:
        return [
            {self.happy_col_name: "1", self.sad_col_name: None},
            {self.happy_col_name: "2", self.sad_col_name: None},
        ]

    def test_validation_success(self) -> None:
        self.validation_success_test()

    def test_validation_failure(self) -> None:
        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.NONNULL_VALUES,
            validation_query=self.create_validation(self.sad_col).query,
            error_msg=f"Found column [{self.sad_col_name}] on raw file [{self.file_tag}] with only null values.",
        )

        self.validation_failure_test(expected_error)

    def test_validation_applies_to_column(self) -> None:
        # validation applies to all columns
        self.assertTrue(
            NonNullValuesColumnValidation.validation_applies_to_column(self.happy_col)
        )
