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
"""Unit tests for stable_historical_raw_data_counts_table_validation.py."""
import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.ingest.direct.raw_data.validations.stable_historical_raw_data_counts_table_validation import (
    RAW_ROWS_MEDIAN_KEY,
    ROW_COUNT_PERCENT_CHANGE_TOLERANCE,
    StableHistoricalRawDataCountsTableValidation,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_types import (
    RawDataTableImportBlockingValidationFailure,
    RawDataTableImportBlockingValidationType,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class TestStableHistoricalRawDataCountsTableValidation(BigQueryEmulatorTestCase):
    """Unit tests for StableHistoricalRawDataCountsTableValidation"""

    def setUp(self) -> None:
        super().setUp()

        self.file_tag = "test_file_tag"
        self.temp_table = "test_table"
        self.temp_table_address = BigQueryAddress(
            dataset_id="test_dataset", table_id=self.temp_table
        )
        self.validation = StableHistoricalRawDataCountsTableValidation(
            file_tag=self.file_tag,
            project_id=self.project_id,
            temp_table_address=self.temp_table_address,
            region_code="us_xx",
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        self.expected_median = 10
        self.default_sessions_data = [
            {"file_id": 1, "raw_rows": 8, "import_status": "SUCCEEDED"},
            {
                "file_id": 2,
                "raw_rows": self.expected_median,
                "import_status": "SUCCEEDED",
            },
            {"file_id": 3, "raw_rows": 13, "import_status": "SUCCEEDED"},
        ]
        self.update_datetime = datetime.datetime.now(datetime.timezone.utc)
        self.default_bq_metadata_data = [
            {
                "file_id": 1,
                "file_tag": "test_file_tag",
                "is_invalidated": False,
                "update_datetime": self.update_datetime,
                "region_code": "us_xx",
                "raw_data_instance": "PRIMARY",
            },
            {
                "file_id": 2,
                "file_tag": "test_file_tag",
                "is_invalidated": False,
                "update_datetime": self.update_datetime,
                "region_code": "us_xx",
                "raw_data_instance": "PRIMARY",
            },
            {
                "file_id": 3,
                "file_tag": "test_file_tag",
                "is_invalidated": False,
                "update_datetime": self.update_datetime,
                "region_code": "us_xx",
                "raw_data_instance": "PRIMARY",
            },
        ]
        self.default_temp_table_data = [{"col1": "test"} for _ in range(11)]

    def _load_data(
        self,
        sessions_data: Optional[List[Dict[str, Any]]] = None,
        bq_metadata_data: Optional[List[Dict[str, Any]]] = None,
        temp_table_data: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Load operations data for the test.

        This function loads mock data into three BigQuery tables:
        sessions data, BigQuery metadata, and table metadata.
        If no data is provided, it will use the default data attributes of the class.
        """
        operations_dataset = "operations"
        import_sessions_table = "direct_ingest_raw_data_import_session"
        bq_metadata_table = "direct_ingest_raw_big_query_file_metadata"
        temp_table = "test_table"

        table_data = {
            import_sessions_table: (
                sessions_data
                if sessions_data is not None
                else self.default_sessions_data
            ),
            bq_metadata_table: (
                bq_metadata_data
                if bq_metadata_data is not None
                else self.default_bq_metadata_data
            ),
            temp_table: (
                temp_table_data
                if temp_table_data is not None
                else self.default_temp_table_data
            ),
        }

        table_schemas = {
            import_sessions_table: [
                schema_field_for_type("file_id", int),
                schema_field_for_type("raw_rows", int),
                schema_field_for_type("import_status", str),
            ],
            bq_metadata_table: [
                schema_field_for_type("file_id", int),
                schema_field_for_type("file_tag", str),
                schema_field_for_type("is_invalidated", bool),
                # there's not a great way to distinguish between TIMESTAMP and DATETIME using python types
                # so manually create SchemaField instead of using schema_field_for_type
                SchemaField("update_datetime", "TIMESTAMP"),
                schema_field_for_type("region_code", str),
                schema_field_for_type("raw_data_instance", str),
            ],
            temp_table: [schema_field_for_type("col1", str)],
        }

        table_addresses = {
            import_sessions_table: BigQueryAddress(
                dataset_id=operations_dataset, table_id=import_sessions_table
            ),
            bq_metadata_table: BigQueryAddress(
                dataset_id=operations_dataset, table_id=bq_metadata_table
            ),
            temp_table: self.temp_table_address,
        }

        for table in [import_sessions_table, bq_metadata_table, temp_table]:
            self._create_and_load_table(
                table_address=table_addresses[table],
                schema=table_schemas[table],
                data=table_data[table],
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

    def test_ignore_failed_imports(self) -> None:
        # Failed imports should be ignored
        sessions_data = self.default_sessions_data + [
            {"file_id": 4, "raw_rows": 2000, "import_status": "FAILED"}
        ]
        bq_metadata_data = self.default_bq_metadata_data + [
            {
                "file_id": 4,
                "file_tag": "test_file_tag",
                "is_invalidated": False,
                "update_datetime": self.update_datetime,
                "region_code": "us_xx",
                "raw_data_instance": "PRIMARY",
            }
        ]
        self._load_data(sessions_data=sessions_data, bq_metadata_data=bq_metadata_data)

        results = self.query(self.validation.query)
        stats = results.to_dict("records")[0]

        median = stats[RAW_ROWS_MEDIAN_KEY]

        self.assertEqual(median, self.expected_median)

    def ignore_different_file_tag(self) -> None:
        # Imports with a different file tag should be ignored
        sessions_data = self.default_sessions_data + [
            {"file_id": 4, "raw_rows": 2000, "import_status": "SUCCEEDED"}
        ]
        bq_metadata_data = self.default_bq_metadata_data + [
            {
                "file_id": 4,
                "file_tag": "test_file_tag_other",
                "is_invalidated": False,
                "update_datetime": self.update_datetime,
                "region_code": "us_xx",
                "raw_data_instance": "PRIMARY",
            }
        ]
        self._load_data(sessions_data=sessions_data, bq_metadata_data=bq_metadata_data)

        results = self.query(self.validation.query)
        stats = results.to_dict("records")[0]

        median = stats[RAW_ROWS_MEDIAN_KEY]

        self.assertEqual(median, self.expected_median)

    def test_ignore_invalidated_files(self) -> None:
        # Invalidated files should be ignored
        sessions_data = self.default_sessions_data + [
            {"file_id": 4, "raw_rows": 2000, "import_status": "SUCCEEDED"}
        ]
        bq_metadata_data = self.default_bq_metadata_data + [
            {
                "file_id": 4,
                "file_tag": "test_file_tag",
                "is_invalidated": True,
                "update_datetime": self.update_datetime,
                "region_code": "us_xx",
                "raw_data_instance": "PRIMARY",
            }
        ]
        self._load_data(sessions_data=sessions_data, bq_metadata_data=bq_metadata_data)

        results = self.query(self.validation.query)
        stats = results.to_dict("records")[0]

        median = stats[RAW_ROWS_MEDIAN_KEY]

        self.assertEqual(median, self.expected_median)

    def test_ignore_different_region_code(self) -> None:
        # Imports with a different region code should be ignored
        sessions_data = self.default_sessions_data + [
            {"file_id": 4, "raw_rows": 2000, "import_status": "SUCCEEDED"}
        ]
        bq_metadata_data = self.default_bq_metadata_data + [
            {
                "file_id": 4,
                "file_tag": "test_file_tag",
                "is_invalidated": False,
                "update_datetime": self.update_datetime,
                "region_code": "us_yy",
                "raw_data_instance": "PRIMARY",
            }
        ]
        self._load_data(sessions_data=sessions_data, bq_metadata_data=bq_metadata_data)

        results = self.query(self.validation.query)
        stats = results.to_dict("records")[0]

        median = stats[RAW_ROWS_MEDIAN_KEY]

        self.assertEqual(median, self.expected_median)

    def test_ignore_different_raw_data_instance(self) -> None:
        # Imports with a different raw data instance should be ignored
        sessions_data = self.default_sessions_data + [
            {"file_id": 4, "raw_rows": 2000, "import_status": "SUCCEEDED"}
        ]
        bq_metadata_data = self.default_bq_metadata_data + [
            {
                "file_id": 4,
                "file_tag": "test_file_tag",
                "is_invalidated": False,
                "update_datetime": self.update_datetime,
                "region_code": "us_xx",
                "raw_data_instance": "SECONDARY",
            }
        ]
        self._load_data(sessions_data=sessions_data, bq_metadata_data=bq_metadata_data)

        results = self.query(self.validation.query)
        stats = results.to_dict("records")[0]

        median = stats[RAW_ROWS_MEDIAN_KEY]

        self.assertEqual(median, self.expected_median)

    def test_ignore_old_entries(self) -> None:
        sessions_data = self.default_sessions_data + [
            {"file_id": 4, "raw_rows": 2000, "import_status": "SUCCEEDED"}
        ]
        bq_metadata_data = self.default_bq_metadata_data + [
            {
                "file_id": 4,
                "file_tag": "test_file_tag",
                "is_invalidated": False,
                "update_datetime": "2023-01-01T00:00:00Z",
                "region_code": "us_xx",
                "raw_data_instance": "PRIMARY",
            }
        ]
        self._load_data(sessions_data=sessions_data, bq_metadata_data=bq_metadata_data)

        results = self.query(self.validation.query)
        stats = results.to_dict("records")[0]

        median = stats[RAW_ROWS_MEDIAN_KEY]

        self.assertEqual(median, self.expected_median)

    def test_insufficient_data(self) -> None:
        # If this is the first time the file tag is imported, there will be no historical data
        self._load_data(sessions_data=[], bq_metadata_data=[])

        results = self.query(self.validation.query)
        # pandas NA type isn't truthy so map to None
        results = results.applymap(lambda x: None if pd.isna(x) else x)

        error = self.validation.get_error_from_results(results.to_dict("records"))

        self.assertIsNone(error)

    def test_no_results(self) -> None:
        # The query should always return a row, even when there is no historical data
        # If the query returns no rows, it means something unexpected went wrong
        with self.assertRaises(RuntimeError) as context_manager:
            self.validation.get_error_from_results(results=[])
        self.assertEqual(
            str(context_manager.exception),
            "No results found for stable historical counts validation."
            f"\nFile tag: [{self.file_tag}]."
            f"\nValidation query: {self.validation.query}",
        )

    def test_validation_success(self) -> None:
        # defaults to loading 11 rows into the temp table
        self._load_data()

        results = self.query(self.validation.query)
        error = self.validation.get_error_from_results(results.to_dict("records"))

        self.assertIsNone(error)

    def test_validation_failure(self) -> None:
        row_count = 1
        expected_error = RawDataTableImportBlockingValidationFailure(
            validation_type=RawDataTableImportBlockingValidationType.STABLE_HISTORICAL_RAW_DATA_COUNTS,
            error_msg=f"Median historical raw rows count [{self.expected_median}] is more than [{ROW_COUNT_PERCENT_CHANGE_TOLERANCE}] different than the current count [{row_count}]."
            f"\nFile tag: [{self.file_tag}]."
            f"\nValidation query: {self.validation.query}",
        )
        data = [{"col1": "test"}]
        self._load_data(temp_table_data=data)

        results = self.query(self.validation.query)
        error = self.validation.get_error_from_results(results.to_dict("records"))

        if error is None:
            self.fail("Expected an error to be returned.")

        self.assertEqual(expected_error.validation_type, error.validation_type)
        self.assertEqual(expected_error.error_msg, error.error_msg)