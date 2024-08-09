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
"""Unit tests for datetime_parsers_column_validation.py."""

from typing import Dict, List, Optional

from recidiviz.ingest.direct.raw_data.validations.datetime_parsers_column_validation import (
    DatetimeParsersColumnValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_types import (
    RawDataTableImportBlockingValidationFailure,
    RawDataTableImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct.raw_data.validations.column_validation_test_case import (
    ColumnValidationTestCase,
)


class TestDatetimeParsersColumnValidation(ColumnValidationTestCase):
    """Unit tests for DatetimeParsersColumnValidation"""

    def setUp(self) -> None:
        super().setUp()
        self.datetime_sql_parsers = [
            "SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', {col_name})",
            "SAFE.PARSE_TIMESTAMP('%m/%d/%Y', {col_name})",
        ]

    def create_validation(self, column_name: str) -> DatetimeParsersColumnValidation:
        return DatetimeParsersColumnValidation(
            file_tag=self.file_tag,
            project_id=self.project_id,
            temp_table_address=self.temp_table_address,
            column_name=column_name,
            datetime_sql_parsers=self.datetime_sql_parsers,
        )

    def get_test_data(self) -> List[Dict[str, Optional[str]]]:
        return [
            {self.happy_col: "01/01/2022 12:00:00 AM", self.sad_col: "01/01/2022"},
            {self.happy_col: "01/01/2022", self.sad_col: "5D"},
        ]

    def test_build_query_empty_datetime_sql_parsers(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            f"datetime_sql_parsers for {self.sad_col} must not be empty",
        ):
            DatetimeParsersColumnValidation(
                file_tag=self.file_tag,
                project_id=self.project_id,
                temp_table_address=self.temp_table_address,
                column_name=self.sad_col,
                datetime_sql_parsers=[],
            )

    def test_validation_success(self) -> None:
        self.validation_success_test()

    def test_validation_failure(self) -> None:
        expected_error = RawDataTableImportBlockingValidationFailure(
            validation_type=RawDataTableImportBlockingValidationType.DATETIME_PARSERS,
            error_msg=f"Found column [{self.sad_col}] on raw file [{self.file_tag}] "
            f"not matching any of the datetime_sql_parsers defined in its configuration YAML."
            f"\nDefined parsers: [{', '.join(self.datetime_sql_parsers)}]."
            f"\nFirst value that does not parse: [5D]."
            f"\nValidation query: {self.create_validation(self.sad_col).query}",
        )

        self.validation_failure_test(expected_error)
