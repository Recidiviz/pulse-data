# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for raw_data_pruning_utils"""
import pandas as pd
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.direct.raw_data.raw_data_pruning_utils import (
    get_pruned_table_row_counts,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class TestRawDataPruningUtils(BigQueryEmulatorTestCase):
    """Tests for raw data pruning utilities"""

    def setUp(self) -> None:
        super().setUp()
        self.test_dataset = "test_dataset"
        self.test_table = "test_pruning_table"
        self.test_address = BigQueryAddress(
            dataset_id=self.test_dataset, table_id=self.test_table
        )

        self.bq_client.create_dataset_if_necessary(self.test_dataset)

    def _create_test_table_with_data(self, data: pd.DataFrame) -> None:
        """Helper to create and populate a test table with the given data."""
        schema = [
            bigquery.SchemaField("is_deleted", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("file_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("update_datetime", "TIMESTAMP", mode="REQUIRED"),
        ]

        self.bq_client.create_table_with_schema(
            address=self.test_address,
            schema_fields=schema,
        )

        if not data.empty:
            rows = data.to_dict("records")
            self.load_rows_into_table(self.test_address, rows)

    def test_get_pruned_table_row_counts(self) -> None:
        num_deleted = 150
        num_new_or_updated = 250

        deleted_rows = pd.DataFrame(
            {
                "is_deleted": [True] * num_deleted,
                "file_id": [1] * num_deleted,
                "update_datetime": ["2023-01-01 12:00:00"] * num_deleted,
            }
        )

        new_or_updated_rows = pd.DataFrame(
            {
                "is_deleted": [False] * num_new_or_updated,
                "file_id": [1] * num_new_or_updated,
                "update_datetime": ["2023-01-01 12:00:00"] * num_new_or_updated,
            }
        )

        test_data = pd.concat([deleted_rows, new_or_updated_rows], ignore_index=True)

        self._create_test_table_with_data(test_data)

        net_new_or_updated, deleted = get_pruned_table_row_counts(
            big_query_client=self.bq_client,
            project_id=self.project_id,
            temp_raw_data_diff_table_address=self.test_address,
        )

        self.assertEqual(net_new_or_updated, num_new_or_updated)
        self.assertEqual(deleted, num_deleted)
