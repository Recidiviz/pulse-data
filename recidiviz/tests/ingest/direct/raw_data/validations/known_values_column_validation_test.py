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
"""Unit tests for known_values_column_validation.py."""

from typing import Dict, List, Optional, Type

import attr

from recidiviz.ingest.direct.raw_data.raw_file_configs import ColumnEnumValueInfo
from recidiviz.ingest.direct.raw_data.validations.known_values_column_validation import (
    KnownValuesColumnValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct.raw_data.validations.column_validation_test_case import (
    ColumnValidationTestCase,
)


class TestKnownValuesColumnValidation(ColumnValidationTestCase):
    """Unit tests for KnownValuesColumnValidation"""

    def setUp(self) -> None:
        super().setUp()
        self.known_values = ["a", "b", "c"]
        self.known_values_enums = [
            ColumnEnumValueInfo(value=x, description="descript")
            for x in self.known_values
        ]
        self.happy_col = attr.evolve(
            self.happy_col,
            known_values=self.known_values_enums,
        )
        self.sad_col = attr.evolve(
            self.sad_col,
            known_values=self.known_values_enums,
        )

    def get_validation_class(self) -> Type[RawDataColumnImportBlockingValidation]:
        return KnownValuesColumnValidation

    def get_test_data(self) -> List[Dict[str, Optional[str]]]:
        return [
            {self.happy_col_name: "a", self.sad_col_name: "b"},
            {self.happy_col_name: "b", self.sad_col_name: "z"},
        ]

    def test_build_query_empty_known_values(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            f"known_values for {self.sad_col_name} must not be empty",
        ):
            KnownValuesColumnValidation(
                file_tag=self.file_tag,
                project_id=self.project_id,
                temp_table_address=self.temp_table_address,
                column_name=self.sad_col_name,
                known_values=[],
            )

    def test_validation_success(self) -> None:
        self.validation_success_test()

    def test_validation_failure(self) -> None:
        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.KNOWN_VALUES,
            validation_query=self.create_validation(self.sad_col).query,
            error_msg=f"Found column [{self.sad_col_name}] on raw file [{self.file_tag}] "
            f"not matching any of the known_values defined in its configuration YAML."
            f"\nDefined known values: [{', '.join(self.known_values)}]."
            f"\nFirst value that does not parse: [z].",
        )

        self.validation_failure_test(expected_error)

    def test_known_values_column_validation_applies_to_column(self) -> None:
        known_values_column = self.happy_col
        non_known_values_column = attr.evolve(self.happy_col, known_values=None)

        self.assertTrue(
            KnownValuesColumnValidation.validation_applies_to_column(
                known_values_column
            )
        )
        self.assertFalse(
            KnownValuesColumnValidation.validation_applies_to_column(
                non_known_values_column
            )
        )
