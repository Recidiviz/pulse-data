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
from recidiviz.common.constants.states import StateCode


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

        self.assertEqual(
            "SELECT * FROM `{project_id}.my_dataset.my_table`",
            address_1.select_query_template(),
        )
        self.assertEqual(
            "SELECT * FROM `{project_id}.my_dataset.my_table_2`",
            address_2.select_query_template(),
        )
        self.assertEqual(
            "SELECT foo, bar FROM `{project_id}.my_dataset.my_table_2`",
            address_2.select_query_template(select_statement="SELECT foo, bar"),
        )

        with self.assertRaisesRegex(
            ValueError, r"Any custom select_statement must start with SELECT"
        ):
            address_2.select_query_template(select_statement="foo, bar")

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

        self.assertEqual(
            "SELECT foo, bar FROM `recidiviz-789.my_dataset.my_table_2`",
            address_2.select_query(select_statement="SELECT foo, bar"),
        )

        with self.assertRaisesRegex(
            ValueError, r"Any custom select_statement must start with SELECT"
        ):
            address_2.select_query(select_statement="foo, bar")

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

    def test_big_query_address_from_str(self) -> None:
        self.assertEqual(
            BigQueryAddress(dataset_id="my_dataset", table_id="my_table"),
            BigQueryAddress.from_str("my_dataset.my_table"),
        )

        with self.assertRaisesRegex(
            ValueError, "Input must be in the format 'dataset.table'."
        ):
            BigQueryAddress.from_str("my_dataset")

        with self.assertRaisesRegex(
            ValueError, "Input must be in the format 'dataset.table'."
        ):
            BigQueryAddress.from_str("my_dataset.my_table.my_table_2")

        with self.assertRaisesRegex(
            ValueError, "Input must be in the format 'dataset.table'."
        ):
            BigQueryAddress.from_str(".my_table")

    def test_is_state_specific_address(self) -> None:
        state_specific_addresses = [
            BigQueryAddress.from_str("us_xx_raw_data.my_table"),
            BigQueryAddress.from_str("my_dataset.us_xx_table"),
            BigQueryAddress.from_str("my_dataset_us_xx.my_table"),
            BigQueryAddress.from_str("my_dataset.my_table_us_xx"),
            BigQueryAddress.from_str("US_XX_raw_data.my_table"),
            BigQueryAddress.from_str("my_dataset.Us_Xx_table"),
            BigQueryAddress.from_str("my_dataset_Us_Xx.my_table"),
            BigQueryAddress.from_str("my_dataset.my_table_US_XX"),
        ]

        for address in state_specific_addresses:
            self.assertTrue(
                address.is_state_specific_address(),
                f"Expected address [{address.to_str()}] to be identified as a "
                f"state-specific address",
            )

        not_state_specific_addresses = [
            BigQueryAddress.from_str("my_dataset.my_table"),
            BigQueryAddress.from_str("us_states.my_table"),
            BigQueryAddress.from_str("my_dataset.us_states"),
            BigQueryAddress.from_str("my_dataset.US_STATES"),
        ]

        for address in not_state_specific_addresses:
            self.assertFalse(
                address.is_state_specific_address(),
                f"Did not expect address [{address.to_str()}] to be identified as a "
                f"state-specific address",
            )

    def test_state_code_for_address(self) -> None:
        us_xx_state_specific_addresses = [
            BigQueryAddress.from_str("us_xx_raw_data.my_table"),
            BigQueryAddress.from_str("us_xx_raw_data.us_xx_my_table"),
            BigQueryAddress.from_str("my_dataset.us_xx_table"),
            BigQueryAddress.from_str("my_dataset_us_xx.my_table"),
            BigQueryAddress.from_str("my_dataset.my_table_us_xx"),
            BigQueryAddress.from_str("US_XX_raw_data.my_table"),
            BigQueryAddress.from_str("my_dataset.Us_Xx_table"),
            BigQueryAddress.from_str("my_dataset_Us_Xx.my_table"),
            BigQueryAddress.from_str("my_dataset.my_table_US_XX"),
        ]

        for address in us_xx_state_specific_addresses:
            self.assertEqual(
                StateCode.US_XX,
                address.state_code_for_address(),
                f"Expected address [{address.to_str()}] to be identified as a "
                f"state-specific address for state US_XX",
            )

        us_yy_state_specific_addresses = [
            BigQueryAddress.from_str("us_yy_raw_data.my_table"),
            BigQueryAddress.from_str("my_dataset.my_table_us_yy"),
        ]

        for address in us_yy_state_specific_addresses:
            self.assertEqual(
                StateCode.US_YY,
                address.state_code_for_address(),
                f"Expected address [{address.to_str()}] to be identified as a "
                f"state-specific address for state US_YY",
            )

        not_state_specific_addresses = [
            BigQueryAddress.from_str("my_dataset.my_table"),
            BigQueryAddress.from_str("us_states.my_table"),
            BigQueryAddress.from_str("my_dataset.us_states"),
            BigQueryAddress.from_str("my_dataset.US_STATES"),
        ]

        for address in not_state_specific_addresses:
            self.assertIsNone(
                address.state_code_for_address(),
                f"Did not expect address [{address.to_str()}] to be identified as a "
                f"state-specific address",
            )

    def test_state_code_for_address_multiple_states_referenced(self) -> None:
        address = BigQueryAddress.from_str("us_yy_raw_data.us_xx_my_table")
        with self.assertRaisesRegex(
            ValueError,
            r"Found more than one state code referenced by address "
            r"us_yy_raw_data.us_xx_my_table: \['US_XX', 'US_YY'\]",
        ):
            _ = address.state_code_for_address()

        address = BigQueryAddress.from_str("us_yy_raw_data_us_xx.my_table")
        with self.assertRaisesRegex(
            ValueError,
            r"Found more than one state code referenced by address "
            r"us_yy_raw_data_us_xx.my_table: \['US_XX', 'US_YY'\]",
        ):
            _ = address.state_code_for_address()

    def test_addresses_to_str(self) -> None:
        self.assertEqual("", BigQueryAddress.addresses_to_str([]))

        expected = """
* dataset.table
* dataset.table_2
* dataset_1.table
* dataset_2.table
"""
        self.assertEqual(
            expected,
            BigQueryAddress.addresses_to_str(
                {
                    BigQueryAddress.from_str("dataset.table"),
                    BigQueryAddress.from_str("dataset_1.table"),
                    BigQueryAddress.from_str("dataset_2.table"),
                    BigQueryAddress.from_str("dataset.table_2"),
                }
            ),
        )

        expected = """
    * dataset.table_2
    * dataset_2.table
"""
        self.assertEqual(
            expected,
            BigQueryAddress.addresses_to_str(
                {
                    BigQueryAddress.from_str("dataset_2.table"),
                    BigQueryAddress.from_str("dataset.table_2"),
                },
                indent_level=4,
            ),
        )
