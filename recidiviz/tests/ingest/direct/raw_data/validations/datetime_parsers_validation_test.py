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


import attr

from recidiviz.ingest.direct.raw_data.raw_file_configs import RawTableColumnFieldType
from recidiviz.ingest.direct.raw_data.validations.datetime_parsers_validation import (
    DatetimeParsersValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct.raw_data.validations.import_blocking_validation_test_case import (
    RawDataImportBlockingValidationTestCase,
)


class TestDatetimeParsersValidation(RawDataImportBlockingValidationTestCase):
    """Unit tests for DatetimeParsersValidation"""

    def setUp(self) -> None:
        super().setUp()
        self.datetime_sql_parsers = [
            "SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', {col_name})",
            "SAFE.PARSE_DATETIME('%m/%d/%Y', {col_name})",
        ]
        self.happy_col = attr.evolve(
            self.happy_col,
            field_type=RawTableColumnFieldType.DATETIME,
            datetime_sql_parsers=self.datetime_sql_parsers,
        )
        self.sad_col = attr.evolve(
            self.sad_col,
            field_type=RawTableColumnFieldType.DATETIME,
            datetime_sql_parsers=self.datetime_sql_parsers,
        )
        self.raw_file_config = attr.evolve(
            self.raw_file_config, columns=[self.happy_col, self.sad_col]
        )
        self.context = attr.evolve(self.context, raw_file_config=self.raw_file_config)
        self.validation = DatetimeParsersValidation.create_validation(
            context=self.context
        )

        self.non_datetime_column = attr.evolve(
            self.happy_col, datetime_sql_parsers=None
        )
        self.non_datetime_config = attr.evolve(
            self.raw_file_config, columns=[self.non_datetime_column]
        )
        self.non_datetime_context = attr.evolve(
            self.context, raw_file_config=self.non_datetime_config
        )

    def test_build_query_empty_datetime_sql_parsers(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            rf"No columns with configured datetime_sql_parsers in \[{self.file_tag}\]",
        ):
            DatetimeParsersValidation.create_validation(
                context=self.non_datetime_context
            )

    def test_validation_success(self) -> None:
        valid_data: list[dict[str, str | None]] = [
            {
                self.happy_col_name: "01/01/2022 12:00:00 AM",
                self.sad_col_name: "01/01/2022",
            },
            {self.happy_col_name: "0000", self.sad_col_name: "02/01/2022"},
        ]
        self.validation_success_test(validation=self.validation, test_data=valid_data)

    def test_validation_failure(self) -> None:
        invalid_data: list[dict[str, str | None]] = [
            {
                self.happy_col_name: "01/01/2022 12:00:00 AM",
                self.sad_col_name: "01/01/2022",
            },
            {self.happy_col_name: "01/01/2022", self.sad_col_name: "5D"},
            {self.happy_col_name: "01/02/2022", self.sad_col_name: "5D"},
            {self.happy_col_name: "01/03/2022", self.sad_col_name: "5E"},
            {self.happy_col_name: "0000", self.sad_col_name: "02/01/2022"},
        ]
        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.DATETIME_PARSERS,
            validation_query=self.validation.build_query(),
            error_msg="""Found column(s) on raw file [test_file_tag] not matching any of the datetime_sql_parsers defined in its configuration YAML.
Column name: [sad_col]
Defined parsers: [SAFE.PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', {col_name}), SAFE.PARSE_DATETIME('%m/%d/%Y', {col_name})].
All [2] values that do not parse: [5D, 5E].
""",
        )

        self.validation_failure_test(
            validation=self.validation,
            test_data=invalid_data,
            expected_error=expected_error,
        )

    def test_validation_applies_to_file(self) -> None:
        self.assertTrue(
            DatetimeParsersValidation.validation_applies_to_file(self.context)
        )
        self.assertFalse(
            DatetimeParsersValidation.validation_applies_to_file(
                self.non_datetime_context
            )
        )
