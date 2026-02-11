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
"""Tests for big_query_address_formatter.py"""
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_address_formatter import (
    BigQueryAddressFormatterFilterByStateCode,
    BigQueryAddressFormatterLimitZero,
    BigQueryAddressFormatterSimple,
    LimitZeroBigQueryAddressFormatterProvider,
    StateFilteringBigQueryAddressFormatterProvider,
)
from recidiviz.common.constants.states import StateCode


class TestBigQueryAddressFormatter(unittest.TestCase):
    """Tests for BigQueryAddressFormatter subclasses"""

    def test_simple_formatter(self) -> None:
        address = BigQueryAddress.from_str(
            "my_dataset.my_table"
        ).to_project_specific_address(project_id="recidiviz-456")
        formatter = BigQueryAddressFormatterSimple()
        formatted_address = formatter.format_address(address)

        self.assertEqual(
            "`recidiviz-456.my_dataset.my_table`",
            formatted_address,
        )

    def test_limit_zero_formatter(self) -> None:
        address = BigQueryAddress.from_str(
            "my_dataset.my_table"
        ).to_project_specific_address(project_id="recidiviz-456")
        formatter = BigQueryAddressFormatterLimitZero()
        formatted_address = formatter.format_address(address)

        self.assertEqual(
            "(SELECT * FROM `recidiviz-456.my_dataset.my_table` LIMIT 0)",
            formatted_address,
        )

    def test_filter_by_state_code_formatter(self) -> None:
        address = BigQueryAddress.from_str(
            "my_dataset.my_table"
        ).to_project_specific_address(project_id="recidiviz-456")
        formatter = BigQueryAddressFormatterFilterByStateCode(
            state_code_filter=StateCode.US_XX
        )
        formatted_address = formatter.format_address(address)

        self.assertEqual(
            '(SELECT * FROM `recidiviz-456.my_dataset.my_table` WHERE state_code = "US_XX")',
            formatted_address,
        )


class TestBigQueryAddressFormatterProvider(unittest.TestCase):
    """Tests for BigQueryAddressFormatterProvider subclasses"""

    def test_state_filtering_provider(self) -> None:
        formatter_provider = StateFilteringBigQueryAddressFormatterProvider(
            state_code_filter=StateCode.US_XX,
            missing_state_code_addresses={
                BigQueryAddress.from_str("us_xx_dataset.missing_state_code"),
                BigQueryAddress.from_str("us_yy_dataset.missing_state_code"),
                BigQueryAddress.from_str("state_agnostic.missing_state_code"),
                BigQueryAddress.from_str("external_data_table.missing_state_code"),
            },
            pseudocolumns_by_address={
                BigQueryAddress.from_str("external_data_table.missing_state_code"): [
                    "_FILE_NAME"
                ],
                BigQueryAddress.from_str("external_data_table.has_state_code"): [
                    "_FILE_NAME"
                ],
            },
        )

        # Do not modify US_XX-specific tables
        self.assertIsInstance(
            formatter_provider.get_formatter(
                BigQueryAddress.from_str("us_xx_dataset.my_table"),
            ),
            BigQueryAddressFormatterSimple,
        )
        self.assertIsInstance(
            formatter_provider.get_formatter(
                BigQueryAddress.from_str("us_xx_dataset.missing_state_code"),
            ),
            BigQueryAddressFormatterSimple,
        )

        # Always LIMIT 0 non-US_XX state-specific tables
        self.assertIsInstance(
            formatter_provider.get_formatter(
                BigQueryAddress.from_str("us_yy_dataset.my_table"),
            ),
            BigQueryAddressFormatterLimitZero,
        )
        self.assertIsInstance(
            formatter_provider.get_formatter(
                BigQueryAddress.from_str("us_yy_dataset.missing_state_code"),
            ),
            BigQueryAddressFormatterLimitZero,
        )

        # Do not modify state agnostic tables with no state_code column
        self.assertIsInstance(
            formatter_provider.get_formatter(
                BigQueryAddress.from_str("state_agnostic.missing_state_code"),
            ),
            BigQueryAddressFormatterSimple,
        )

        # Do not modify state agnostic tables with pseudocolumns, whether or not they
        # have a state_code column
        self.assertIsInstance(
            formatter_provider.get_formatter(
                BigQueryAddress.from_str("external_data_table.missing_state_code"),
            ),
            BigQueryAddressFormatterSimple,
        )
        self.assertIsInstance(
            formatter_provider.get_formatter(
                BigQueryAddress.from_str("external_data_table.has_state_code"),
            ),
            BigQueryAddressFormatterSimple,
        )

        # Filter other state-agnostic tables by state_code
        self.assertIsInstance(
            formatter_provider.get_formatter(
                BigQueryAddress.from_str("my_dataset.my_table"),
            ),
            BigQueryAddressFormatterFilterByStateCode,
        )

    def test_limit_zero_provider(self) -> None:
        formatter_provider = LimitZeroBigQueryAddressFormatterProvider()

        # All addresses should get a LimitZero formatter
        self.assertIsInstance(
            formatter_provider.get_formatter(
                BigQueryAddress.from_str("my_dataset.my_table"),
            ),
            BigQueryAddressFormatterLimitZero,
        )
        self.assertIsInstance(
            formatter_provider.get_formatter(
                BigQueryAddress.from_str("us_xx_dataset.my_table"),
            ),
            BigQueryAddressFormatterLimitZero,
        )
