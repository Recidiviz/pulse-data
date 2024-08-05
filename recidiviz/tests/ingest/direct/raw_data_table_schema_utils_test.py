#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Unit tests the functionality of raw_data_table_schema_utils.py"""

import unittest
from typing import List
from unittest.mock import create_autospec, patch

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.raw_data_table_schema_utils import (
    update_raw_data_table_schema,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    FILE_ID_COL_DESCRIPTION,
    FILE_ID_COL_NAME,
    IS_DELETED_COL_DESCRIPTION,
    IS_DELETED_COL_NAME,
    UPDATE_DATETIME_COL_DESCRIPTION,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class RawTableSchemaUtilsTest(unittest.TestCase):
    """Unit tests for raw_data_table_schema_utils.py"""

    def setUp(self) -> None:
        self.project_id = "fake-recidiviz-project"
        self.fake_state = StateCode.US_XX
        self.region_config = DirectIngestRegionRawFileConfig(
            region_code=self.fake_state.value.lower(),
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
                            field_type=RawTableColumnFieldType.STRING,
                            is_pii=False,
                        ),
                        RawTableColumnInfo(
                            name="column 1",
                            description="desc",
                            field_type=RawTableColumnFieldType.STRING,
                            is_pii=False,
                        ),
                        RawTableColumnInfo(
                            name="column 2",
                            description=None,
                            field_type=RawTableColumnFieldType.STRING,
                            is_pii=False,
                        ),
                    ],
                    data_classification=RawDataClassification.SOURCE,
                    encoding="UTF-8",
                    separator=",",
                    supplemental_order_by_clause="",
                    custom_line_terminator="|",
                    always_historical_export=False,
                    no_valid_primary_keys=False,
                    infer_columns_from_config=False,
                    ignore_quotes=True,
                    import_chunk_size_rows=200,
                    table_relationships=[],
                    update_cadence=RawDataFileUpdateCadence.WEEKLY,
                    is_code_file=False,
                )
            },
        )
        self.schema: List[bigquery.SchemaField] = [
            bigquery.SchemaField(
                name="primary_key_col",
                description="is primary key",
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                name="column 1",
                description="desc",
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                name="column 2",
                description="",
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="NULLABLE",
            ),
            bigquery.SchemaField(
                name=FILE_ID_COL_NAME,
                description=FILE_ID_COL_DESCRIPTION,
                field_type=bigquery.enums.SqlTypeNames.INTEGER.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=UPDATE_DATETIME_COL_NAME,
                description=UPDATE_DATETIME_COL_DESCRIPTION,
                field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=IS_DELETED_COL_NAME,
                description=IS_DELETED_COL_DESCRIPTION,
                field_type=bigquery.enums.SqlTypeNames.BOOLEAN.value,
                mode="REQUIRED",
            ),
        ]

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id
        self.project_number_patcher = patch("recidiviz.utils.metadata.project_number")
        self.project_number_patcher.start().return_value = "123456789"

        self.mock_client = create_autospec(BigQueryClient)

        self.region_config_patcher = patch(
            "recidiviz.ingest.direct.raw_data_table_schema_utils.get_region_raw_file_config"
        )
        self.region_config_patcher.start().return_value = self.region_config

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()
        self.region_config_patcher.stop()

    def test_update_raw_data_tables_schemas_create_table(self) -> None:
        self.mock_client.table_exists.return_value = False

        update_raw_data_table_schema(
            state_code=self.fake_state,
            raw_file_tag="raw_data_table",
            instance=DirectIngestInstance.PRIMARY,
            big_query_client=self.mock_client,
        )

        self.mock_client.create_table_with_schema.assert_called()
        self.mock_client.update_schema.assert_not_called()
        self.mock_client.create_table_with_schema.assert_called_with(
            BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="raw_data_table",
            ),
            self.schema,
            clustering_fields=["file_id"],
        )

    def test_update_raw_data_tables_schemas_update_table(self) -> None:
        self.mock_client.table_exists.return_value = True

        update_raw_data_table_schema(
            state_code=self.fake_state,
            raw_file_tag="raw_data_table",
            instance=DirectIngestInstance.PRIMARY,
            big_query_client=self.mock_client,
        )

        self.mock_client.update_schema.assert_called()
        self.mock_client.create_table_with_schema.assert_not_called()
        self.mock_client.update_schema.assert_called_with(
            BigQueryAddress(dataset_id="us_xx_raw_data", table_id="raw_data_table"),
            self.schema,
            allow_field_deletions=False,
        )
