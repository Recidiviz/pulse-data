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

from typing import Dict, List, Optional, Type

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
)
from recidiviz.ingest.direct.raw_data.validations.datetime_parsers_column_validation import (
    DatetimeParsersColumnValidation,
)
from recidiviz.ingest.direct.raw_data.validations.import_blocking_validations_query_runner import (
    RawDataImportBlockingValidationQueryRunner,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct.raw_data.validations.column_validation_test_case import (
    ColumnValidationTestCase,
)


class TestDatetimeParsersColumnValidation(ColumnValidationTestCase):
    """Unit tests for DatetimeParsersColumnValidation"""

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

    def get_validation_class(self) -> Type[RawDataColumnImportBlockingValidation]:
        return DatetimeParsersColumnValidation

    def get_test_data(self) -> List[Dict[str, Optional[str]]]:
        return [
            {
                self.happy_col_name: "01/01/2022 12:00:00 AM",
                self.sad_col_name: "01/01/2022",
            },
            {self.happy_col_name: "01/01/2022", self.sad_col_name: "5D"},
            {self.happy_col_name: "01/02/2022", self.sad_col_name: "5D"},
            {self.happy_col_name: "01/03/2022", self.sad_col_name: "5E"},
            {self.happy_col_name: "0000", self.sad_col_name: "02/01/2022"},
        ]

    def test_build_query_empty_datetime_sql_parsers(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            f"datetime_sql_parsers for {self.sad_col_name} must not be empty",
        ):
            DatetimeParsersColumnValidation(
                file_tag=self.file_tag,
                project_id=self.project_id,
                temp_table_address=self.temp_table_address,
                column_name=self.sad_col_name,
                datetime_sql_parsers=[],
                null_values=[],
                state_code=StateCode.US_XX,
                query_runner=RawDataImportBlockingValidationQueryRunner(
                    bq_client=self.bq_client
                ),
            )

    def test_validation_success(self) -> None:
        self.validation_success_test()

    def test_validation_failure(self) -> None:
        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.DATETIME_PARSERS,
            validation_query=self.create_validation(self.sad_col).query,
            error_msg=f"Found column [{self.sad_col_name}] on raw file [{self.file_tag}] "
            f"not matching any of the datetime_sql_parsers defined in its configuration YAML."
            f"\nDefined parsers: [{', '.join(self.datetime_sql_parsers)}]."
            f"\nAll [2] values that do not parse: [5D, 5E].",
        )

        self.validation_failure_test(expected_error)

    def test_validation_applies_to_column(self) -> None:
        datetime_column = self.happy_col
        non_datetime_column = attr.evolve(self.happy_col, datetime_sql_parsers=None)
        raw_file_config = DirectIngestRawFileConfig(
            state_code=StateCode.US_XX,
            file_tag="myFile",
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[],
            custom_line_terminator=None,
            primary_key_cols=[],
            supplemental_order_by_clause="",
            encoding="UTF-8",
            separator=",",
            ignore_quotes=True,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
            no_valid_primary_keys=False,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )

        self.assertTrue(
            DatetimeParsersColumnValidation.validation_applies_to_column(
                datetime_column, raw_file_config
            )
        )
        self.assertFalse(
            DatetimeParsersColumnValidation.validation_applies_to_column(
                non_datetime_column, raw_file_config
            )
        )
