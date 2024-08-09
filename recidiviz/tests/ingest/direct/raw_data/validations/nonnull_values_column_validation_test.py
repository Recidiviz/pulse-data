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

from typing import Dict, List, Optional

from recidiviz.ingest.direct.raw_data.validations.nonnull_values_column_validation import (
    NonNullValuesColumnValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_types import (
    RawDataTableImportBlockingValidation,
    RawDataTableImportBlockingValidationFailure,
    RawDataTableImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct.raw_data.validations.column_validation_test_case import (
    ColumnValidationTestCase,
)


class TestNonNullValuesColumnValidation(ColumnValidationTestCase):
    """Unit tests for NonNullValuesColumnValidation"""

    def create_validation(
        self, column_name: str
    ) -> RawDataTableImportBlockingValidation:
        return NonNullValuesColumnValidation(
            file_tag=self.file_tag,
            project_id=self.project_id,
            temp_table_address=self.temp_table_address,
            column_name=column_name,
        )

    def get_test_data(self) -> List[Dict[str, Optional[str]]]:
        return [
            {self.happy_col: "1", self.sad_col: None},
            {self.happy_col: "2", self.sad_col: None},
        ]

    def test_validation_success(self) -> None:
        self.validation_success_test()

    def test_validation_failure(self) -> None:
        expected_error = RawDataTableImportBlockingValidationFailure(
            validation_type=RawDataTableImportBlockingValidationType.NONNULL_VALUES,
            error_msg=f"Found column [{self.sad_col}] on raw file [{self.file_tag}] with only null values."
            f"\nValidation query: {self.create_validation(self.sad_col).query}",
        )

        self.validation_failure_test(expected_error)
