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
"""Tests for big_query_sqlglot_helpers.py"""
import unittest

import sqlglot
import sqlglot.expressions as expr

from recidiviz.big_query.big_query_sqlglot_helpers import (
    get_state_code_literal_references,
    get_undocumented_ctes,
)
from recidiviz.common.constants.states import StateCode


class TestBigQuerySqlglotHelpers(unittest.TestCase):
    """Tests for big_query_sqlglot_helpers.py"""

    def _parse_query(self, query: str) -> expr.Query:
        tree = sqlglot.parse_one(query, dialect="bigquery")
        if not isinstance(tree, expr.Query):
            raise ValueError(f"Non-Query SQL expression built from query: {query}")
        return tree

    def test_get_state_code_literal_references(self) -> None:
        no_state_logic_query = self._parse_query(
            "SELECT a, b FROM `{project_id}.dataset.table`"
        )
        self.assertEqual(set(), get_state_code_literal_references(no_state_logic_query))

        state_specific_dataset_query = self._parse_query(
            "SELECT a, b FROM `{project_id}.us_xx_dataset.table`"
        )
        self.assertEqual(
            set(), get_state_code_literal_references(state_specific_dataset_query)
        )

        state_specific_table_query = self._parse_query(
            "SELECT a, b FROM `{project_id}.us_xx_dataset.table`"
        )
        self.assertEqual(
            set(), get_state_code_literal_references(state_specific_table_query)
        )

        state_code_equals_filter_query = self._parse_query(
            "SELECT a, b FROM `{project_id}.dataset.table` WHERE state = 'US_XX'"
        )
        self.assertEqual(
            {StateCode.US_XX},
            get_state_code_literal_references(state_code_equals_filter_query),
        )

        state_code_in_filter_query = self._parse_query(
            "SELECT a, b FROM `{project_id}.dataset.table` "
            "WHERE state IN ('US_XX', 'US_YY')"
        )
        self.assertEqual(
            {StateCode.US_XX, StateCode.US_YY},
            get_state_code_literal_references(state_code_in_filter_query),
        )

        state_code_column_clause_query = self._parse_query(
            "SELECT 'US_XX' AS a, b FROM `{project_id}.dataset.table`"
        )
        self.assertEqual(
            {StateCode.US_XX},
            get_state_code_literal_references(state_code_column_clause_query),
        )

        state_code_case_clause_query = self._parse_query(
            "SELECT CASE WHEN state = 'US_XX' THEN x END AS a, b "
            "FROM `{project_id}.dataset.table`"
        )
        self.assertEqual(
            {StateCode.US_XX},
            get_state_code_literal_references(state_code_case_clause_query),
        )

        state_code_subquery_query = self._parse_query(
            "WITH cte AS ("
            "  SELECT * FROM `{project_id}.dataset.table` WHERE state = 'US_XX'"
            ") SELECT a, 'US_YY' AS b FROM cte"
        )
        self.assertEqual(
            {StateCode.US_XX, StateCode.US_YY},
            get_state_code_literal_references(state_code_subquery_query),
        )

    def test_get_undocumented_ctes(self) -> None:
        query = """
        WITH
            -- This is a good comment in the right place
            cte_1 AS (SELECT * FROM a),
            -- This is another good comment,
            -- it's even on two lines!
            cte_2 AS (SELECT * FROM b)
            SELECT * FROM cte_1 JOIN cte_2 USING(a, b, c)
        """
        self.assertEqual(set(), get_undocumented_ctes(query))

        query = """
        WITH
            -- This is a good comment in the right place
            cte_1 AS (SELECT * FROM a),
            cte_2 AS (SELECT * FROM b)
            SELECT * FROM cte_1 JOIN cte_2 USING(a, b, c)
        """
        self.assertEqual({"cte_2"}, get_undocumented_ctes(query))
