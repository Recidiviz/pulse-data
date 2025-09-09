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
"""Common test cases for discrete column-based validations."""
import datetime
from typing import Optional

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    BaseRawDataImportBlockingValidation,
    RawDataImportBlockingValidationContext,
    RawDataImportBlockingValidationFailure,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class RawDataImportBlockingValidationTestCase(BigQueryEmulatorTestCase):
    """Common test cases for import blocking validations that test column-by-column
    invariants rather than whole-file invariants."""

    def setUp(self) -> None:
        super().setUp()
        self.temp_table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id="test_table"
        )
        self.file_tag = "test_file_tag"
        self.happy_col_name = "happy_col"
        self.sad_col_name = "sad_col"
        self.state_code = StateCode.US_XX
        self.file_update_datetime = datetime.datetime.now()
        self.direct_ingest_instance = DirectIngestInstance.PRIMARY
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
        self.raw_file_config = DirectIngestRawFileConfig(
            state_code=self.state_code,
            file_tag=self.file_tag,
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[self.happy_col, self.sad_col],
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
        self.context = RawDataImportBlockingValidationContext(
            state_code=self.state_code,
            file_tag=self.file_tag,
            project_id=self.project_id,
            temp_table_address=self.temp_table_address,
            raw_file_config=self.raw_file_config,
            file_update_datetime=self.file_update_datetime,
            raw_data_instance=self.direct_ingest_instance,
        )

    def _load_data(self, test_data: list[dict[str, Optional[str]]]) -> None:
        self.create_mock_table(
            address=self.temp_table_address,
            schema=[
                schema_field_for_type(self.happy_col_name, str),
                schema_field_for_type(self.sad_col_name, str),
            ],
        )
        self.load_rows_into_table(self.temp_table_address, test_data)

    def validation_failure_test(
        self,
        validation: BaseRawDataImportBlockingValidation,
        test_data: list[dict[str, Optional[str]]],
        expected_error: RawDataImportBlockingValidationFailure,
    ) -> None:
        self._load_data(test_data)

        results = self.query(validation.build_query())
        error = validation.get_error_from_results(results.to_dict("records"))

        if not error:
            self.fail("Expected error not found")

        self.assertEqual(expected_error.validation_type, error.validation_type)
        self.assertEqual(expected_error.validation_query, error.validation_query)
        self.assertEqual(expected_error.error_msg, error.error_msg)

    def validation_success_test(
        self,
        validation: BaseRawDataImportBlockingValidation,
        test_data: list[dict[str, Optional[str]]],
    ) -> None:
        self._load_data(test_data)

        results = self.query(validation.build_query())
        error = validation.get_error_from_results(results.to_dict("records"))

        self.assertIsNone(error)
