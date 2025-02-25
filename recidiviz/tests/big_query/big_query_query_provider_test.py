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
"""Tests for big_query_query_provider.py"""
import unittest

from recidiviz.big_query.big_query_query_provider import (
    BigQueryQueryProvider,
    SimpleBigQueryQueryProvider,
    StateFilteredQueryProvider,
)
from recidiviz.common.constants.states import StateCode


class TestBigQueryQueryProvider(unittest.TestCase):
    def test_resolve(self) -> None:
        query = "SELECT * FROM `recidiviz-test.dataset.table`;"
        query_provider = SimpleBigQueryQueryProvider(query=query)

        self.assertEqual(query, BigQueryQueryProvider.resolve(query_provider))
        self.assertEqual(query, BigQueryQueryProvider.resolve(query))
        self.assertEqual(query, query_provider.resolve(query_provider))

    def test_strip_semicolon(self) -> None:
        query = "SELECT * FROM `recidiviz-test.dataset.table`;\n  \t "
        query_provider = SimpleBigQueryQueryProvider(query=query)
        self.assertEqual(
            "SELECT * FROM `recidiviz-test.dataset.table`",
            BigQueryQueryProvider.strip_semicolon(query_provider),
        )
        self.assertEqual(
            "SELECT * FROM `recidiviz-test.dataset.table`",
            BigQueryQueryProvider.strip_semicolon(query),
        )

        query_no_semicolon = "SELECT * FROM `recidiviz-test.dataset.table`\n  \t "
        query_provider_no_semicolon = SimpleBigQueryQueryProvider(query=query)
        self.assertEqual(
            "SELECT * FROM `recidiviz-test.dataset.table`",
            BigQueryQueryProvider.strip_semicolon(query_provider_no_semicolon),
        )
        self.assertEqual(
            "SELECT * FROM `recidiviz-test.dataset.table`",
            BigQueryQueryProvider.strip_semicolon(query_no_semicolon),
        )


class TestStateFilteredQueryProvider(unittest.TestCase):
    def test_state_filter(self) -> None:
        original_query = "SELECT * FROM `recidiviz-test.dataset.table`;"
        query_provider = StateFilteredQueryProvider(
            original_query=original_query, state_code_filter=StateCode.US_XX
        )

        expected_query = """SELECT * FROM (
SELECT * FROM `recidiviz-test.dataset.table`
) WHERE state_code = 'US_XX';
"""
        self.assertEqual(expected_query, query_provider.get_query())

    def test_state_filter_composed(self) -> None:
        original_query = SimpleBigQueryQueryProvider(
            "SELECT * FROM `recidiviz-test.dataset.table`;"
        )
        query_provider = StateFilteredQueryProvider(
            original_query=original_query, state_code_filter=StateCode.US_YY
        )
        expected_query = """SELECT * FROM (
SELECT * FROM `recidiviz-test.dataset.table`
) WHERE state_code = 'US_YY';
"""
        self.assertEqual(expected_query, query_provider.get_query())
