# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Unit tests for direct_ingest_raw_data_table_manager.py"""

import unittest
from typing import List
from unittest.mock import patch

from google.cloud import bigquery

from recidiviz.ingest.direct.raw_data import direct_ingest_raw_data_table_manager
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawTableColumnInfo,
)


class DirectIngestRawDataTableManagerTest(unittest.TestCase):
    """Unit tests for update_raw_data_tables_schemas"""

    def setUp(self) -> None:
        self.project_id = "fake-recidiviz-project"
        self.fake_state = "us_xx"
        self.region_config = DirectIngestRegionRawFileConfig(
            region_code=self.fake_state,
            raw_file_configs={
                "raw_data_table": DirectIngestRawFileConfig(
                    file_tag="raw_data_table",
                    file_path="some-path",
                    file_description="some-description",
                    primary_key_cols=["primary_key_col"],
                    columns=[
                        RawTableColumnInfo(
                            name="primary_key_col",
                            description="is primary key",
                            is_datetime=False,
                            is_pii=False,
                        ),
                        RawTableColumnInfo(
                            name="column 1",
                            description="desc",
                            is_datetime=False,
                            is_pii=False,
                        ),
                        RawTableColumnInfo(
                            name="column 2",
                            description=None,
                            is_datetime=False,
                            is_pii=False,
                        ),
                    ],
                    data_classification=RawDataClassification.SOURCE,
                    encoding="UTF-8",
                    separator=",",
                    supplemental_order_by_clause="",
                    custom_line_terminator="|",
                    always_historical_export=False,
                    infer_columns_from_config=False,
                    ignore_quotes=True,
                    import_chunk_size_rows=200,
                )
            },
        )
        self.schema: List[bigquery.SchemaField] = [
            bigquery.SchemaField(
                name="primary_key_col",
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                name="column 1",
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                name="column 2",
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                name="file_id",
                field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name="update_datetime",
                field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
                mode="REQUIRED",
            ),
        ]

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id
        self.project_number_patcher = patch("recidiviz.utils.metadata.project_number")
        self.project_number_patcher.start().return_value = "123456789"

        self.bq_client_patcher = patch(
            "recidiviz.ingest.direct.raw_data.direct_ingest_raw_data_table_manager.BigQueryClientImpl"
        )
        self.mock_client = self.bq_client_patcher.start().return_value

        self.region_config_patcher = patch(
            "recidiviz.ingest.direct.raw_data.direct_ingest_raw_data_table_manager.get_region_raw_file_config"
        )
        self.region_config_patcher.start().return_value = self.region_config

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()
        self.region_config_patcher.stop()

    def test_update_raw_data_tables_schemas_create_table(self) -> None:
        self.mock_client.table_exists.return_value = False

        direct_ingest_raw_data_table_manager.update_raw_data_tables_schemas_in_dataset(
            self.fake_state
        )

        self.mock_client.create_table_with_schema.assert_called()
        self.mock_client.update_schema.assert_not_called()
        self.mock_client.create_table_with_schema.assert_called_with(
            "us_xx_raw_data", "raw_data_table", self.schema
        )

    def test_update_raw_data_tables_schemas_update_table(self) -> None:
        self.mock_client.table_exists.return_value = True

        direct_ingest_raw_data_table_manager.update_raw_data_tables_schemas_in_dataset(
            self.fake_state
        )

        self.mock_client.update_schema.assert_called()
        self.mock_client.create_table_with_schema.assert_not_called()
        self.mock_client.update_schema.assert_called_with(
            "us_xx_raw_data", "raw_data_table", self.schema, allow_field_deletions=False
        )
