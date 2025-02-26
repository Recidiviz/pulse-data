# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for big_query_address.py"""
import unittest

import attrs
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)


class TestBigQueryAddress(unittest.TestCase):
    """Tests for big_query_address.py"""

    def test_big_query_address(self) -> None:
        address_1 = BigQueryAddress(dataset_id="my_dataset", table_id="my_table")
        address_2 = BigQueryAddress(dataset_id="my_dataset", table_id="my_table_2")

        self.assertEqual("my_dataset.my_table", address_1.to_str())
        self.assertEqual("my_dataset.my_table_2", address_2.to_str())

        # Ensure they are hashable
        self.assertEqual(
            {address_1, address_2}, {address_1, address_2, attrs.evolve(address_1)}
        )

        list_item = bigquery.table.TableListItem(
            {
                "tableReference": {
                    "projectId": "recidiviz-456",
                    "datasetId": "my_dataset",
                    "tableId": "my_table",
                }
            }
        )

        self.assertEqual(address_1, BigQueryAddress.from_list_item(list_item))

    def test_project_specific_big_query_address(self) -> None:
        address_1 = ProjectSpecificBigQueryAddress(
            project_id="recidiviz-456", dataset_id="my_dataset", table_id="my_table"
        )
        address_2 = ProjectSpecificBigQueryAddress(
            project_id="recidiviz-789", dataset_id="my_dataset", table_id="my_table_2"
        )

        self.assertEqual("recidiviz-456.my_dataset.my_table", address_1.to_str())
        self.assertEqual("recidiviz-789.my_dataset.my_table_2", address_2.to_str())

        self.assertEqual(
            "`recidiviz-456.my_dataset.my_table`", address_1.format_address_for_query()
        )
        self.assertEqual(
            "`recidiviz-789.my_dataset.my_table_2`",
            address_2.format_address_for_query(),
        )

        self.assertEqual(
            "SELECT * FROM `recidiviz-456.my_dataset.my_table`",
            address_1.select_query(),
        )
        self.assertEqual(
            "SELECT * FROM `recidiviz-789.my_dataset.my_table_2`",
            address_2.select_query(),
        )

        # Ensure they are hashable
        self.assertEqual(
            {address_1, address_2}, {address_1, address_2, attrs.evolve(address_1)}
        )

        list_item = bigquery.table.TableListItem(
            {
                "tableReference": {
                    "projectId": "recidiviz-456",
                    "datasetId": "my_dataset",
                    "tableId": "my_table",
                }
            }
        )

        self.assertEqual(
            address_1, ProjectSpecificBigQueryAddress.from_list_item(list_item)
        )
