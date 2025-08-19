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
"""Common test cases for column validations."""
import abc
from datetime import datetime, timezone
from typing import Dict, List, Optional, Type

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class ColumnValidationTestCase(BigQueryEmulatorTestCase):
    """Common test cases for column validations."""

    def setUp(self) -> None:
        super().setUp()
        self.temp_table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id="test_table"
        )
        self.file_tag = "test_file_tag"
        self.happy_col_name = "happy_col"
        self.sad_col_name = "sad_col"
        self.happy_col = RawTableColumnInfo(
            name=self.happy_col_name,
            state_code=StateCode.US_XX,
            file_tag=self.file_tag,
            description="description",
            is_pii=True,
            field_type=RawTableColumnFieldType.STRING,
            null_values=["0000"],
        )
        self.sad_col = RawTableColumnInfo(
            name=self.sad_col_name,
            state_code=StateCode.US_XX,
            file_tag=self.file_tag,
            description="description",
            is_pii=True,
            field_type=RawTableColumnFieldType.STRING,
        )

    def create_validation(
        self, column: RawTableColumnInfo
    ) -> RawDataColumnImportBlockingValidation:
        """Create the validation to test."""
        return self.get_validation_class().create_column_validation(
            project_id=self.project_id,
            file_tag=self.file_tag,
            state_code=StateCode.US_XX,
            temp_table_address=self.temp_table_address,
            column=column,
            file_upload_datetime=datetime.now(tz=timezone.utc),
            bq_client=self.bq_client,
        )

    @abc.abstractmethod
    def get_validation_class(self) -> Type[RawDataColumnImportBlockingValidation]:
        """Get the validation class to test."""

    @abc.abstractmethod
    def get_test_data(self) -> List[Dict[str, Optional[str]]]:
        """Get the test data to load into the temp table."""

    def load_data(self) -> None:
        self.data = self.get_test_data()

        self.create_mock_table(
            address=self.temp_table_address,
            schema=[
                schema_field_for_type(self.happy_col_name, str),
                schema_field_for_type(self.sad_col_name, str),
            ],
        )
        self.load_rows_into_table(self.temp_table_address, self.data)

    def validation_failure_test(
        self,
        expected_error: RawDataImportBlockingValidationFailure,
    ) -> None:
        validation = self.create_validation(self.sad_col)
        self.load_data()

        error = validation.run_validation()

        if not error:
            self.fail("Expected error not found")

        self.assertEqual(expected_error.validation_type, error.validation_type)
        self.assertEqual(expected_error.validation_query, error.validation_query)
        self.assertEqual(expected_error.error_msg, error.error_msg)

    def validation_success_test(
        self,
    ) -> None:
        validation = self.create_validation(self.happy_col)
        self.load_data()

        error = validation.run_validation()

        self.assertIsNone(error)
