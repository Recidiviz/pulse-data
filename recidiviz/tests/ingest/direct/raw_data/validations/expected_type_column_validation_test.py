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
"""Unit tests for expected_type_column_validation.py."""

from typing import Dict, List, Optional

from recidiviz.ingest.direct.raw_data.raw_file_configs import RawTableColumnFieldType
from recidiviz.ingest.direct.raw_data.validations.expected_type_column_validation import (
    ExpectedTypeColumnValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_types import (
    RawDataTableImportBlockingValidationFailure,
    RawDataTableImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct.raw_data.validations.column_validation_test_case import (
    ColumnValidationTestCase,
)


class TestExpectedTypeColumnValidation(ColumnValidationTestCase):
    """Unit tests for ExpectedTypeColumnValidation"""

    def setUp(self) -> None:
        super().setUp()
        self.column_type = RawTableColumnFieldType.INTEGER

    def create_validation(self, column_name: str) -> ExpectedTypeColumnValidation:
        return ExpectedTypeColumnValidation(
            file_tag=self.file_tag,
            project_id=self.project_id,
            temp_table_address=self.temp_table_address,
            column_name=column_name,
            column_type=self.column_type,
        )

    def get_test_data(self) -> List[Dict[str, Optional[str]]]:
        return [
            {self.happy_col: "1", self.sad_col: "5"},
            {self.happy_col: "2", self.sad_col: "5D"},
        ]

    def test_dont_validate_string_type(self) -> None:
        for column_type in [
            RawTableColumnFieldType.STRING,
            RawTableColumnFieldType.PERSON_EXTERNAL_ID,
            RawTableColumnFieldType.STAFF_EXTERNAL_ID,
        ]:
            with self.assertRaises(
                ValueError,
            ) as context_manager:
                ExpectedTypeColumnValidation(
                    file_tag=self.file_tag,
                    project_id=self.project_id,
                    temp_table_address=self.temp_table_address,
                    column_name=self.sad_col,
                    column_type=column_type,
                )
            self.assertEqual(
                str(context_manager.exception),
                f"field_type [{column_type.value}] has BigQuery type STRING, "
                "expected type validation should not be run on string columns, "
                f"as all values can be cast to string."
                f"\nFile tag: [{self.file_tag}], Column: [{self.sad_col}]",
            )

    def test_validation_success(self) -> None:
        self.validation_success_test()

    def test_validation_failure(self) -> None:
        expected_error = RawDataTableImportBlockingValidationFailure(
            validation_type=RawDataTableImportBlockingValidationType.EXPECTED_TYPE,
            error_msg=f"Found column [{self.sad_col}] on raw file [{self.file_tag}] "
            f"not matching the field_type defined in its configuration YAML."
            f"Defined type: [{self.column_type.value}]."
            f"\nFirst value that does not parse: [5D]."
            f"\nValidation query: {self.create_validation(self.sad_col).query}",
        )

        self.validation_failure_test(expected_error)