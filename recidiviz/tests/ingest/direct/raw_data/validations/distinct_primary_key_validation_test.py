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
"""Unit tests for distinct_primary_key_validation.py."""
import datetime
from typing import Any, Dict, List, Optional

import attr
from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_config_enums import (
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.raw_data.validations.distinct_primary_key_validation import (
    DistinctPrimaryKeyValidation,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationContext,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_type import (
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class TestDistinctPrimaryKeyValidation(BigQueryEmulatorTestCase):
    """Unit tests for DistinctPrimaryKeyValidation"""

    def setUp(self) -> None:
        super().setUp()

        self.file_tag = "test_file_tag"
        self.temp_table = "test_table"
        self.state_code = StateCode.US_XX
        self.ingest_instance = DirectIngestInstance.PRIMARY
        self.temp_table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id=self.temp_table
        )
        self.columns = [
            RawTableColumnInfo(
                name="id",
                state_code=self.state_code,
                file_tag=self.file_tag,
                field_type=RawTableColumnFieldType.STRING,
                description="description",
            ),
            RawTableColumnInfo(
                name="name",
                state_code=self.state_code,
                file_tag=self.file_tag,
                field_type=RawTableColumnFieldType.STRING,
                description="description",
            ),
            RawTableColumnInfo(
                name="value",
                state_code=self.state_code,
                file_tag=self.file_tag,
                field_type=RawTableColumnFieldType.STRING,
                description="description",
            ),
        ]
        self.primary_key_cols = ["id", "name"]

        self.raw_file_config = DirectIngestRawFileConfig(
            state_code=self.state_code,
            file_tag=self.file_tag,
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=self.columns,
            custom_line_terminator=None,
            primary_key_cols=self.primary_key_cols,
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
        self.file_update_datetime = datetime.datetime.now()
        self.context = RawDataImportBlockingValidationContext(
            state_code=self.state_code,
            file_tag=self.file_tag,
            project_id=self.project_id,
            temp_table_address=self.temp_table_address,
            raw_file_config=self.raw_file_config,
            file_update_datetime=self.file_update_datetime,
            raw_data_instance=self.ingest_instance,
        )

        self.validation = DistinctPrimaryKeyValidation.create_validation(
            context=self.context
        )

    def _load_data(
        self,
        temp_table_data: Optional[List[Dict[str, Any]]] = None,
        schema: Optional[List[SchemaField]] = None,
    ) -> None:
        """Load data for the test.

        This function loads mock data into the temporary BigQuery table.
        If no data is provided, it will use an empty list.
        """
        if schema is None:
            schema = [
                schema_field_for_type("id", str),
                schema_field_for_type("name", str),
                schema_field_for_type("value", int),
            ]

        self._create_and_load_table(
            table_address=self.temp_table_address,
            schema=schema,
            data=temp_table_data if temp_table_data is not None else [],
        )

    def _create_and_load_table(
        self,
        table_address: BigQueryAddress,
        schema: List[SchemaField],
        data: List[Dict[str, Any]],
    ) -> None:
        """Helper function to create a mock table and load data into it."""
        self.create_mock_table(
            address=table_address,
            schema=schema,
        )
        self.load_rows_into_table(table_address, data)

    def test_validation_success_distinct_keys(self) -> None:
        temp_table_data = [
            {"id": "1", "name": "Alice", "value": 10},
            {"id": "2", "name": "Bob", "value": 20},
            {"id": "3", "name": "Charlie", "value": 30},
        ]
        self._load_data(temp_table_data=temp_table_data)

        results = self.query(self.validation.build_query())
        error = self.validation.get_error_from_results(results.to_dict("records"))

        self.assertIsNone(error)

    def test_case_insensitivity(self) -> None:
        expected_error_msg = (
            f"Found duplicate primary keys for raw file [{self.file_tag}]"
            "\nPrimary key columns: ['id', 'name']"
        )
        temp_table_data = [
            {"id": "1", "name": "Alice", "value": 10},
            {"id": "2", "name": "Bob", "value": 20},
            {
                "id": "1",
                "name": "alice",
                "value": 40,
            },  # Duplicate by case-insensitivity
            {"id": "3", "name": "Charlie", "value": 30},
        ]
        self._load_data(temp_table_data=temp_table_data)

        results = self.query(self.validation.build_query())
        error = self.validation.get_error_from_results(results.to_dict("records"))

        if error is None:
            self.fail("Expected an error to be returned.")
        self.assertEqual(
            error.validation_type,
            RawDataImportBlockingValidationType.DISTINCT_PRIMARY_KEYS,
        )
        self.assertEqual(
            expected_error_msg,
            error.error_msg,
        )

    def test_duplicate_keys(self) -> None:
        expected_error_message = (
            f"Found duplicate primary keys for raw file [{self.file_tag}]"
            "\nPrimary key columns: ['id', 'name']"
        )
        temp_table_data = [
            {"id": "1", "name": "Alice", "value": 10},
            {"id": "2", "name": "Bob", "value": 20},
            {"id": "1", "name": "Alice", "value": 40},  # Duplicate 1
            {"id": "3", "name": "Charlie", "value": 30},
            {"id": "2", "name": "Bob", "value": 50},  # Duplicate 2
        ]
        self._load_data(temp_table_data=temp_table_data)

        results = self.query(self.validation.build_query())
        error = self.validation.get_error_from_results(results.to_dict("records"))

        if error is None:
            self.fail("Expected an error to be returned.")
        self.maxDiff = None
        self.assertEqual(
            error.validation_type,
            RawDataImportBlockingValidationType.DISTINCT_PRIMARY_KEYS,
        )
        self.assertEqual(expected_error_message, error.error_msg)

    def test_validation_applies_to_table(self) -> None:
        no_pk_file_config = DirectIngestRawFileConfig(
            state_code=self.state_code,
            file_tag=self.file_tag,
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[],
            custom_line_terminator=None,
            primary_key_cols=[],
            supplemental_order_by_clause="",
            encoding="UTF-8",
            separator=",",
            ignore_quotes=False,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
            no_valid_primary_keys=True,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )
        pk_file_config = attr.evolve(
            no_pk_file_config,
            no_valid_primary_keys=False,
            columns=[
                RawTableColumnInfo(
                    name="id",
                    state_code=self.state_code,
                    file_tag=self.file_tag,
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description=None,
                )
            ],
            primary_key_cols=["id"],
        )

        context_no_pk = attr.evolve(self.context, raw_file_config=no_pk_file_config)
        context_with_pk = attr.evolve(self.context, raw_file_config=pk_file_config)

        self.assertFalse(
            DistinctPrimaryKeyValidation.validation_applies_to_file(context_no_pk)
        )
        self.assertTrue(
            DistinctPrimaryKeyValidation.validation_applies_to_file(context_with_pk)
        )
