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
"""Tests our sqlglot_helpers utility functions."""
import unittest

from recidiviz.tests.big_query.sqlglot_helpers import (
    check_query_is_not_ordered_outside_of_windows,
    get_undocumented_ctes,
)


class TestSqlglotHelpers(unittest.TestCase):
    """Tests all of our helper functions using sqlglot."""

    def test_check_query_is_not_ordered_outside_of_windows(self) -> None:

        qualified = "SELECT * FROM table_a QUALIFY ROW_NUMBER() OVER(PARTITION BY a ORDER BY b DESC) = 1"
        check_query_is_not_ordered_outside_of_windows(qualified)

        invalid = "SELECT * FROM table_a ORDER BY b"
        with self.assertRaisesRegex(
            ValueError,
            "Found ORDER BY statement that isn't applied to window or aggregation: 'ORDER BY b'",
        ):
            check_query_is_not_ordered_outside_of_windows(invalid)

        invalid_multiple_cols = "SELECT * FROM table_a ORDER BY a, b"
        with self.assertRaisesRegex(
            ValueError,
            "Found ORDER BY statement that isn't applied to window or aggregation: 'ORDER BY a, b'",
        ):
            check_query_is_not_ordered_outside_of_windows(invalid_multiple_cols)

        invalid_qualified = """
            SELECT * 
            FROM table_a 
            QUALIFY ROW_NUMBER() OVER(PARTITION BY a ORDER BY b DESC) = 1
            ORDER BY invalid
        """
        with self.assertRaisesRegex(
            ValueError,
            "Found ORDER BY statement that isn't applied to window or aggregation: 'ORDER BY invalid'",
        ):
            check_query_is_not_ordered_outside_of_windows(invalid_qualified)

        multiple_windows = """
            SELECT 
                ROW_NUMBER() OVER(PARTITION BY a ORDER BY b DESC) AS row_num,
                FIRST_VALUE(a) OVER(PARTITION BY a ORDER BY b, c) AS first_val,
                MAX(a) OVER(PARTITION BY a ORDER BY b) AS max_Val
            FROM table_a 
        """
        check_query_is_not_ordered_outside_of_windows(multiple_windows)

        multiple_windows_invalid = """
            SELECT 
                ROW_NUMBER() OVER(PARTITION BY a ORDER BY b DESC) AS row_num,
                FIRST_VALUE(a) OVER(PARTITION BY a ORDER BY b, c) AS first_val,
                MAX(a) OVER(PARTITION BY a ORDER BY b) AS max_Val
            FROM table_a 
            ORDER BY row_num DESC
        """
        with self.assertRaisesRegex(
            ValueError,
            "Found ORDER BY statement that isn't applied to window or aggregation: 'ORDER BY row_num DESC'",
        ):
            check_query_is_not_ordered_outside_of_windows(multiple_windows_invalid)

        string_agg = """
            SELECT 
                STRING_AGG(DISTINCT control_number, ',' ORDER BY control_number) AS control_numbers,
            FROM table
        """
        check_query_is_not_ordered_outside_of_windows(string_agg)

        array_agg_structs = """
        SELECT
          ARRAY_AGG(STRUCT<outcome_type string, outcome_data string>(VIOLATIONOUTCOME,OUTCOMEDETAILS) ORDER BY VIOLATIONOUTCOME, OUTCOMEDETAILS)
        FROM
            table
        """
        check_query_is_not_ordered_outside_of_windows(array_agg_structs)

        json_from_structs = """
        WITH tmp AS (
            SELECT 1 AS a, 'b' AS b UNION ALL
            SELECT 2 AS a, 'c' AS b UNION ALL
            SELECT 3 AS a, 'd' AS b
        )
            SELECT
              TO_JSON(
                ARRAY_AGG(
                  STRUCT<a int64, b string>(a, b)
                  ORDER BY a DESC
                )
              )
            FROM tmp
        """
        check_query_is_not_ordered_outside_of_windows(json_from_structs)

        order_struct_outside_of_aggregation_invalid = """
        WITH tmp AS (
          SELECT 1 AS a, 'b' AS b UNION ALL
          SELECT 2 AS a, 'c' AS b UNION ALL
          SELECT 3 AS a, 'd' AS b
        )
        SELECT
          STRUCT<a int64, b string>(a, b) AS rec
        FROM tmp
          ORDER BY rec.a DESC
        """
        with self.assertRaisesRegex(
            ValueError,
            "Found ORDER BY statement that isn't applied to window or aggregation: 'ORDER BY rec.a DESC'",
        ):
            check_query_is_not_ordered_outside_of_windows(
                order_struct_outside_of_aggregation_invalid
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
