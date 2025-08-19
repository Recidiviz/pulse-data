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
"""Unit tests for direct_ingest_raw_table_pre_import_validator.py."""
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_pre_import_validator import (
    DirectIngestRawTablePreImportValidator,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnUpdateInfo,
    ColumnUpdateOperation,
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.raw_data.validations.nonnull_values_column_validation import (
    NonNullValuesColumnValidation,
)
from recidiviz.ingest.direct.raw_data.validations.stable_historical_raw_data_counts_table_validation import (
    RAW_ROWS_MEDIAN_KEY,
    TEMP_TABLE_ROW_COUNT_KEY,
    StableHistoricalRawDataCountsTableValidation,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationError,
    RawDataImportBlockingValidationFailure,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestDirectIngestRawTablePreImportValidator(unittest.TestCase):
    """Unit tests for DirectIngestRawTablePreImportValidator"""

    def setUp(self) -> None:
        self.project_id = "test-project"
        self.temp_table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id="test_table"
        )
        self.region_code = "us_xx"
        self.raw_data_instance = DirectIngestInstance.PRIMARY
        self.file_tag = "myFile"
        self.file_update_datetime = datetime(2022, 1, 1, tzinfo=timezone.utc)
        self.column_name = "Col1"

        self.raw_file_config = DirectIngestRawFileConfig(
            state_code=StateCode(self.region_code.upper()),
            file_tag=self.file_tag,
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[
                RawTableColumnInfo(
                    name=self.column_name,
                    state_code=StateCode.US_XX,
                    file_tag=self.file_tag,
                    description="description",
                    is_pii=True,
                    field_type=RawTableColumnFieldType.STRING,
                ),
                # We should not run validations on columns that have since been deleted
                # because we don't want to block import for issues with data that is no longer being used
                RawTableColumnInfo(
                    name="Col2",
                    state_code=StateCode.US_XX,
                    file_tag=self.file_tag,
                    description="description",
                    is_pii=True,
                    field_type=RawTableColumnFieldType.STRING,
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.DELETION,
                            update_datetime=datetime(2023, 1, 1, tzinfo=timezone.utc),
                        )
                    ],
                ),
                # Should not run validations on columns that have been added after the file upload datetime
                # because it won't exist in the temp table
                RawTableColumnInfo(
                    name="Col3",
                    state_code=StateCode.US_XX,
                    file_tag=self.file_tag,
                    description="description",
                    is_pii=True,
                    field_type=RawTableColumnFieldType.STRING,
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.ADDITION,
                            update_datetime=datetime(2023, 1, 1, tzinfo=timezone.utc),
                        )
                    ],
                ),
            ],
            primary_key_cols=[],
            supplemental_order_by_clause="",
            separator=",",
            ignore_quotes=False,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
            no_valid_primary_keys=False,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
            encoding="utf-8",
            custom_line_terminator="\n",
        )
        self.region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code=self.region_code, region_module=fake_regions
        )
        self.region_raw_file_config.raw_file_configs[
            self.file_tag
        ] = self.raw_file_config
        self.big_query_client = MagicMock()
        self.stable_counts_job = MagicMock()
        self.stable_counts_job.result.return_value = [
            {TEMP_TABLE_ROW_COUNT_KEY: 101, RAW_ROWS_MEDIAN_KEY: 100},
        ]

    def test_run_raw_table_validations_success(self) -> None:
        validator = DirectIngestRawTablePreImportValidator(
            project_id=self.project_id,
            region_raw_file_config=self.region_raw_file_config,
            region_code=self.region_code,
            raw_data_instance=self.raw_data_instance,
            big_query_client=self.big_query_client,
        )

        with patch.object(
            StableHistoricalRawDataCountsTableValidation,
            "run_validation",
            return_value=None,
        ), patch.object(
            NonNullValuesColumnValidation,
            "run_validation",
            return_value=None,
        ):
            # should not raise any exceptions
            validator.run_raw_data_temp_table_validations(
                self.file_tag, self.file_update_datetime, self.temp_table_address
            )

    def test_run_raw_table_validations_renamed_col(self) -> None:
        raw_file_config = attr.evolve(
            self.raw_file_config,
            columns=[
                # Since column was renamed after the file upload datetime, we should query for the old column name
                RawTableColumnInfo(
                    name=self.column_name,
                    state_code=StateCode.US_XX,
                    file_tag=self.raw_file_config.file_tag,
                    description="description",
                    is_pii=True,
                    field_type=RawTableColumnFieldType.STRING,
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.RENAME,
                            update_datetime=datetime(
                                2023,
                                1,
                                1,
                                tzinfo=timezone.utc,
                            ),
                            previous_value="OldCol1",
                        )
                    ],
                ),
            ],
        )
        self.region_raw_file_config.raw_file_configs[self.file_tag] = raw_file_config
        validator = DirectIngestRawTablePreImportValidator(
            project_id=self.project_id,
            region_raw_file_config=self.region_raw_file_config,
            region_code=self.region_code,
            raw_data_instance=self.raw_data_instance,
            big_query_client=self.big_query_client,
        )

        # Create a mock validation instance that will be returned by create_column_validation
        mock_validation_instance = MagicMock()
        mock_validation_instance.run_validation.return_value = None

        with patch.object(
            StableHistoricalRawDataCountsTableValidation,
            "run_validation",
            return_value=None,
        ), patch.object(
            NonNullValuesColumnValidation,
            "create_column_validation",
            return_value=mock_validation_instance,
        ):
            # should not raise any exceptions
            validator.run_raw_data_temp_table_validations(
                self.file_tag, self.file_update_datetime, self.temp_table_address
            )

        # Check that the validation was created with the old column name
        call_args_list = (
            mock_validation_instance.create_column_validation.call_args_list
        )
        for call_args in call_args_list:
            expected_column_name = "OldCol1"
            actual_column = call_args[1]["column"]
            self.assertEqual(actual_column.name, expected_column_name)

    def test_run_raw_table_validations_failure(self) -> None:
        validator = DirectIngestRawTablePreImportValidator(
            project_id=self.project_id,
            region_raw_file_config=self.region_raw_file_config,
            region_code=self.region_code,
            raw_data_instance=self.raw_data_instance,
            big_query_client=self.big_query_client,
        )
        expected_error_message = "1 pre-import validation(s) failed for file [myFile]. If you wish [myFile] to be permanently excluded from any validation,  please add the validation_type and exemption_reason to import_blocking_validation_exemptions for a table-wide exemption or to import_blocking_column_validation_exemptions for a column-specific exemption in the raw file config.\nError: test_query\nValidation type: NONNULL_VALUES\nValidation query: test_query"

        with self.assertRaises(
            RawDataImportBlockingValidationError,
        ) as context, patch.object(
            StableHistoricalRawDataCountsTableValidation,
            "run_validation",
            return_value=None,
        ), patch.object(
            NonNullValuesColumnValidation,
            "run_validation",
            return_value=RawDataImportBlockingValidationFailure(
                validation_type=NonNullValuesColumnValidation.validation_type(),
                validation_query="test_query",
                error_msg="test_query",
            ),
        ):
            validator.run_raw_data_temp_table_validations(
                self.file_tag, self.file_update_datetime, self.temp_table_address
            )

        self.assertIn(expected_error_message, str(context.exception))
