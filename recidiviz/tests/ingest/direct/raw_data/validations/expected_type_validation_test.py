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
"""Unit tests for expected_type_validation.py."""

from unittest.mock import patch

import attr

from recidiviz.ingest.direct.raw_data.raw_file_configs import RawTableColumnFieldType
from recidiviz.ingest.direct.raw_data.validations.expected_type_validation import (
    COLUMN_TYPE_TO_BIG_QUERY_TYPE,
    ExpectedTypeValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct.raw_data.validations.import_blocking_validation_test_case import (
    RawDataImportBlockingValidationTestCase,
)


class TestExpectedTypeValidation(RawDataImportBlockingValidationTestCase):
    """Unit tests for ExpectedTypeValidation"""

    def setUp(self) -> None:
        super().setUp()
        self.column_type = RawTableColumnFieldType.INTEGER
        self.happy_col = attr.evolve(
            self.happy_col, field_type=self.column_type, null_values=["N/A"]
        )
        self.sad_col = attr.evolve(self.sad_col, field_type=self.column_type)
        self.raw_file_config = attr.evolve(
            self.raw_file_config,
            columns=[self.happy_col, self.sad_col],
        )
        self.context.raw_file_config = self.raw_file_config
        self.validation = ExpectedTypeValidation.create_validation(context=self.context)
        self.limit_values_patcher = patch(
            "recidiviz.ingest.direct.raw_data.validations.expected_type_validation.ERROR_MESSAGE_ROW_LIMIT",
            3,
        )
        self.limit_values_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.limit_values_patcher.stop()

    def test_dont_validate_string_type(self) -> None:
        col_1 = attr.evolve(self.happy_col, field_type=RawTableColumnFieldType.STRING)
        col_2 = attr.evolve(
            self.sad_col, field_type=RawTableColumnFieldType.PERSON_EXTERNAL_ID
        )
        col_3 = attr.evolve(
            self.sad_col,
            field_type=RawTableColumnFieldType.STAFF_EXTERNAL_ID,
            name="col_3",
        )
        file_config = attr.evolve(self.raw_file_config, columns=[col_1, col_2, col_3])
        context = attr.evolve(self.context, raw_file_config=file_config)
        with self.assertRaisesRegex(
            ValueError,
            rf"No columns requiring expected type validation in \[{self.file_tag}\]",
        ):
            ExpectedTypeValidation.create_validation(context=context)

    def test_validation_success(self) -> None:
        valid_data: list[dict[str, str | None]] = [
            {self.happy_col_name: "1", self.sad_col_name: "5"},
            {self.happy_col_name: "N/A", self.sad_col_name: "50"},
        ]
        self.validation_success_test(validation=self.validation, test_data=valid_data)

    def test_validation_failure(self) -> None:
        invalid_data: list[dict[str, str | None]] = [
            {self.happy_col_name: "1", self.sad_col_name: "5"},
            {self.happy_col_name: "2", self.sad_col_name: "5D"},
            {self.happy_col_name: "3", self.sad_col_name: "5D"},
            {self.happy_col_name: "4", self.sad_col_name: "5E"},
            {self.happy_col_name: "N/A", self.sad_col_name: "50"},
        ]
        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.EXPECTED_TYPE,
            validation_query=self.validation.build_query(),
            error_msg=f"Found column(s) on raw file [{self.file_tag}]"
            f" not matching the field_type defined in its configuration YAML."
            f"\nColumn name: [{self.sad_col_name}]"
            f"\nDefined type: [{self.column_type.value}]."
            f"\nAll [2] values that do not parse: [5D, 5E].\n",
        )

        self.validation_failure_test(
            validation=self.validation,
            test_data=invalid_data,
            expected_error=expected_error,
        )

    def test_validation_failure_first_n(self) -> None:
        invalid_data: list[dict[str, str | None]] = [
            {self.happy_col_name: "1", self.sad_col_name: "5"},
            {self.happy_col_name: "2", self.sad_col_name: "5D"},
            {self.happy_col_name: "3", self.sad_col_name: "5D"},
            {self.happy_col_name: "4", self.sad_col_name: "5E"},
            {self.happy_col_name: "N/A", self.sad_col_name: "50"},
            {self.happy_col_name: "5", self.sad_col_name: "5F"},
        ]

        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.EXPECTED_TYPE,
            validation_query=self.validation.build_query(),
            error_msg=f"Found column(s) on raw file [{self.file_tag}]"
            f" not matching the field_type defined in its configuration YAML."
            f"\nColumn name: [{self.sad_col_name}]"
            f"\nDefined type: [{self.column_type.value}]."
            f"\nFirst [3] of many values that do not parse: [5D, 5E, 5F].\n",
        )

        self.validation_failure_test(
            validation=self.validation,
            test_data=invalid_data,
            expected_error=expected_error,
        )

    def test_all_field_types_have_corresponding_bq_type(self) -> None:
        for column_type in RawTableColumnFieldType:
            self.assertIsNotNone(COLUMN_TYPE_TO_BIG_QUERY_TYPE.get(column_type))

    def test_validation_applies_to_file(self) -> None:
        string_column = attr.evolve(
            self.happy_col, field_type=RawTableColumnFieldType.STRING
        )
        string_file_config = attr.evolve(self.raw_file_config, columns=[string_column])
        int_file_config = attr.evolve(self.raw_file_config, columns=[self.happy_col])

        context_string = attr.evolve(self.context, raw_file_config=string_file_config)
        context_int = attr.evolve(self.context, raw_file_config=int_file_config)

        self.assertTrue(ExpectedTypeValidation.validation_applies_to_file(context_int))
        self.assertFalse(
            ExpectedTypeValidation.validation_applies_to_file(context_string)
        )
