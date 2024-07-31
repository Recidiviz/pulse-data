# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for Flash Database tools"""

import datetime
import unittest
from unittest.mock import call, create_autospec, patch

from freezegun import freeze_time
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.flash_database_tools import (
    copy_raw_data_between_instances,
    copy_raw_data_to_backup,
    delete_contents_of_raw_data_tables,
)


class FlashDatabaseToolsTest(unittest.TestCase):
    """tests for flash_database_tools.py"""

    def setUp(self) -> None:
        self.region_code = StateCode.US_XX
        self.mock_project_id = "recidiviz-456"
        self.mock_project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_patcher.start().return_value = self.mock_project_id

        self.mock_bq_client = create_autospec(BigQueryClient)

    def tearDown(self) -> None:
        self.mock_project_id_patcher.stop()

    def test_copy_raw_data_between_instances_secondary_to_primary(
        self,
    ) -> None:
        move_to_date = datetime.datetime(2022, 2, 1, 0, 0, 0)

        raw_source_id = "us_xx_raw_data_secondary"

        raw_destination_id = "us_xx_raw_data"

        self.mock_bq_client.add_timestamp_suffix_to_dataset_id.return_value = (
            raw_destination_id
        )

        with freeze_time(move_to_date):
            copy_raw_data_between_instances(
                state_code=self.region_code,
                ingest_instance_source=DirectIngestInstance.SECONDARY,
                ingest_instance_destination=DirectIngestInstance.PRIMARY,
                big_query_client=self.mock_bq_client,
            )

        self.mock_bq_client.assert_has_calls(
            [
                call.create_dataset_if_necessary(
                    dataset_id=raw_destination_id,
                ),
                call.copy_dataset_tables(
                    source_dataset_id=raw_source_id,
                    destination_dataset_id=raw_destination_id,
                    overwrite_destination_tables=True,
                ),
            ]
        )

    def test_delete_contents_of_raw_data_tables(self) -> None:
        deletion_date = datetime.datetime(2022, 2, 1, 0, 0, 0)

        raw_source_id = "us_xx_raw_data_secondary"

        mock_table_info = {
            "tableReference": {
                "projectId": "recidiviz-staging",
                "datasetId": raw_source_id,
                "tableId": "my_table",
            },
        }

        self.mock_bq_client.list_tables.return_value = [
            bigquery.table.TableListItem(mock_table_info)
        ]

        with freeze_time(deletion_date):
            delete_contents_of_raw_data_tables(
                state_code=self.region_code,
                ingest_instance=DirectIngestInstance.SECONDARY,
                big_query_client=self.mock_bq_client,
            )

        self.mock_bq_client.assert_has_calls(
            [
                call.list_tables(
                    dataset_id=raw_source_id,
                ),
                call.delete_from_table_async(
                    dataset_id=raw_source_id, table_id="my_table"
                ),
            ]
        )

    def test_copy_raw_data_to_backup_secondary(self) -> None:
        move_to_date = datetime.datetime(2022, 2, 1, 0, 0, 0)
        with freeze_time(move_to_date):
            raw_source_id = "us_xx_raw_data_secondary"

            copy_raw_data_to_backup(
                state_code=self.region_code,
                ingest_instance=DirectIngestInstance.SECONDARY,
                big_query_client=self.mock_bq_client,
            )

        self.mock_bq_client.assert_has_calls(
            [
                call.backup_dataset_tables_if_dataset_exists(
                    dataset_id=raw_source_id,
                )
            ]
        )
