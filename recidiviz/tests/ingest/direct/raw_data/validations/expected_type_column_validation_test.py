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

from typing import Dict, List, Optional, Type
from unittest.mock import patch

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
)
from recidiviz.ingest.direct.raw_data.validations.expected_type_column_validation import (
    COLUMN_TYPE_TO_BIG_QUERY_TYPE,
    ExpectedTypeColumnValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct.raw_data.validations.column_validation_test_case import (
    ColumnValidationTestCase,
)
from recidiviz.tests.ingest.direct.raw_data.validations.datetime_parsers_column_validation_test import (
    RawDataImportBlockingValidationQueryRunner,
)


class TestExpectedTypeColumnValidation(ColumnValidationTestCase):
    """Unit tests for ExpectedTypeColumnValidation"""

    def setUp(self) -> None:
        super().setUp()
        self.column_type = RawTableColumnFieldType.INTEGER
        self.happy_col = attr.evolve(
            self.happy_col, field_type=self.column_type, null_values=["N/A"]
        )
        self.sad_col = attr.evolve(self.sad_col, field_type=self.column_type)
        self.limit_values_patcher = patch(
            "recidiviz.ingest.direct.raw_data.validations.expected_type_column_validation.ERROR_MESSAGE_ROW_LIMIT",
            3,
        )
        self.limit_values_patcher.start()
        self.test_data: List[Dict[str, Optional[str]]] = [
            {self.happy_col_name: "1", self.sad_col_name: "5"},
            {self.happy_col_name: "2", self.sad_col_name: "5D"},
            {self.happy_col_name: "3", self.sad_col_name: "5D"},
            {self.happy_col_name: "4", self.sad_col_name: "5E"},
            {self.happy_col_name: "N/A", self.sad_col_name: "50"},
        ]

    def tearDown(self) -> None:
        self.limit_values_patcher.stop()
        return super().tearDown()

    def get_validation_class(self) -> Type[RawDataColumnImportBlockingValidation]:
        return ExpectedTypeColumnValidation

    def get_test_data(self) -> List[Dict[str, Optional[str]]]:
        return self.test_data

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
                    state_code=StateCode.US_XX,
                    temp_table_address=self.temp_table_address,
                    column_name=self.sad_col_name,
                    column_type=column_type,
                    null_values=None,
                    query_runner=RawDataImportBlockingValidationQueryRunner(
                        bq_client=self.bq_client
                    ),
                )
            self.assertEqual(
                str(context_manager.exception),
                f"field_type [{column_type.value}] has BigQuery type STRING, "
                "expected type validation should not be run on string columns, "
                f"as all values can be cast to string."
                f"\nFile tag: [{self.file_tag}], Column: [{self.sad_col_name}]",
            )

    def test_validation_success(self) -> None:
        self.validation_success_test()

    def test_validation_failure(self) -> None:
        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.EXPECTED_TYPE,
            validation_query=self.create_validation(self.sad_col).query,
            error_msg=f"Found column [{self.sad_col_name}] on raw file [{self.file_tag}]"
            f" not matching the field_type defined in its configuration YAML."
            f"\nDefined type: [{self.column_type.value}]."
            f"\nAll [2] values that do not parse: [5D, 5E].",
        )

        self.validation_failure_test(expected_error)

    def test_validation_failure_first_n(self) -> None:
        self.test_data = [
            *self.test_data,
            {self.happy_col_name: "5", self.sad_col_name: "5F"},
        ]

        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.EXPECTED_TYPE,
            validation_query=self.create_validation(self.sad_col).query,
            error_msg=f"Found column [{self.sad_col_name}] on raw file [{self.file_tag}]"
            f" not matching the field_type defined in its configuration YAML."
            f"\nDefined type: [{self.column_type.value}]."
            f"\nFirst [3] of many values that do not parse: [5D, 5E, 5F].",
        )

        self.validation_failure_test(expected_error)

    def test_all_field_types_have_corresponding_bq_type(self) -> None:
        for column_type in RawTableColumnFieldType:
            self.assertIsNotNone(COLUMN_TYPE_TO_BIG_QUERY_TYPE.get(column_type))

    def test_validation_applies_to_column(self) -> None:
        int_column = self.happy_col
        string_column = attr.evolve(
            self.happy_col, field_type=RawTableColumnFieldType.STRING
        )
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
            ExpectedTypeColumnValidation.validation_applies_to_column(
                int_column, raw_file_config
            )
        )
        self.assertFalse(
            ExpectedTypeColumnValidation.validation_applies_to_column(
                string_column, raw_file_config
            )
        )
