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
"""Tests for query_complexity_score_2025.py"""
import unittest

import sqlglot

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.utils.types import assert_type
from recidiviz.view_registry.query_complexity_score_2025 import (
    get_query_complexity_score_2025,
)

_TABLE_1_ADDRESS = BigQueryAddress.from_str("dataset.table_1")
_TABLE_2_ADDRESS = BigQueryAddress.from_str("dataset.table_2")
_TABLE_3_ADDRESS = BigQueryAddress.from_str("dataset.table_3")
_STATE_SPECIFIC_TABLE_ADDRESS = BigQueryAddress.from_str("us_xx_dataset.table")
_RAW_TABLE_ADDRESS = BigQueryAddress.from_str("us_xx_raw_data.table")


class TestQueryComplexityScore2025(unittest.TestCase):
    """Tests for get_query_complexity_score_2025()"""

    address_to_table_complexity_score: dict[BigQueryAddress, int]

    @classmethod
    def setUpClass(cls) -> None:
        cls.address_to_table_complexity_score = {
            _TABLE_1_ADDRESS: 1,
            _TABLE_2_ADDRESS: 1,
            _TABLE_3_ADDRESS: 1,
            _STATE_SPECIFIC_TABLE_ADDRESS: 5,
            _RAW_TABLE_ADDRESS: 10,
        }

    def _get_query_score(self, query: str) -> int:
        parsed_query = assert_type(
            sqlglot.parse_one(query, dialect="bigquery"), sqlglot.expressions.Query
        )
        return get_query_complexity_score_2025(
            parsed_query, self.address_to_table_complexity_score
        )

    def test_simplest_query(self) -> None:
        query = f"""
        SELECT a, b 
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """

        self.assertEqual(0, self._get_query_score(query))

        query = f"""
        SELECT a, b 
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        LIMIT 10
        """

        self.assertEqual(0, self._get_query_score(query))

    def test_distinct(self) -> None:
        query = f"""
        SELECT DISTINCT a, b 
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """

        self.assertEqual(1, self._get_query_score(query))

    def test_except_columns(self) -> None:
        query = f"""
        SELECT * EXCEPT(a)
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """

        self.assertEqual(0, self._get_query_score(query))

    def test_unpivot(self) -> None:
        query = f"""
        SELECT 
          a,
          b
        FROM 
          `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        UNPIVOT (
          b FOR x IN (c, d, e)
        )
        """

        # 1 point for IN expression, 1 point for UNPIVOT
        self.assertEqual(2, self._get_query_score(query))

    def test_aggregation(self) -> None:
        query = f"""
        SELECT a, b, COUNT(*)
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        GROUP BY a, b
        """

        # 1 point for GROUP BY, 1 point for COUNT
        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        GROUP BY a, b
        HAVING COUNT(*) < 1
        """

        # 1 point for GROUP BY, 1 point for COUNT, 1 point for boolean expression, 1
        # point for HAVING
        self.assertEqual(4, self._get_query_score(query))

        query = f"""
        SELECT a, b, FIRST_VALUE(a)
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        GROUP BY a, b
        """

        # 1 point for GROUP BY, 1 point for FIRST_VALUE
        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        SELECT a, b, FIRST_VALUE(a IGNORE NULLS)
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        GROUP BY a, b
        """

        # 1 point for GROUP BY, 1 point for FIRST_VALUE, 1 point for IGNORE NULLS
        self.assertEqual(3, self._get_query_score(query))

    def test_binary_ops(self) -> None:
        query = f"""
        SELECT a - b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """

        self.assertEqual(1, self._get_query_score(query))

        query = f"""
        SELECT a * b - c
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """

        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        SELECT a || b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """

        self.assertEqual(2, self._get_query_score(query))

    def test_table_references(self) -> None:
        query = f"""
        SELECT a, b 
        FROM 
            `recidiviz-staging.{_STATE_SPECIFIC_TABLE_ADDRESS.to_str()}`
        """

        self.assertEqual(4, self._get_query_score(query))

        query = f"""
        SELECT a, b 
        FROM 
            `recidiviz-staging.{_RAW_TABLE_ADDRESS.to_str()}`
        """

        self.assertEqual(9, self._get_query_score(query))

    def test_boolean_expressions(self) -> None:
        query = f"""
        SELECT a, b, a < b AS a_is_less_than_b
        FROM `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """

        # 1 point for a < b
        self.assertEqual(1, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        WHERE a < b
        """

        # 1 point for a < b, 1 point for WHERE clause
        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        WHERE (a < b OR b IS NULL) AND c >= d
        """

        # 1 point for WHERE clause, 5 point for boolean expression (3 predicates + 2
        # connectors).
        self.assertEqual(6, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        WHERE a BETWEEN b AND c
        """

        # 1 point for a BETWEEN b AND c, 1 point for WHERE clause
        self.assertEqual(2, self._get_query_score(query))

    def test_joins(self) -> None:
        query = f"""
        SELECT a, b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        JOIN
            `recidiviz-staging.{_TABLE_2_ADDRESS.to_str()}`
        USING (a)
        """

        # 1 point for extra table reference, 1 point for JOIN, 0 points for USING
        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        INNER JOIN
            `recidiviz-staging.{_TABLE_2_ADDRESS.to_str()}`
        USING (a)
        """

        # 1 point for extra table reference, 1 point for INNER JOIN, 0 points for USING
        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        LEFT OUTER JOIN
            `recidiviz-staging.{_TABLE_2_ADDRESS.to_str()}`
        USING (a)
        """

        # 1 point for extra table reference, 2 point for LEFT OUTER JOIN,
        # 0 points for USING
        self.assertEqual(3, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}` t1
        FULL OUTER JOIN
            `recidiviz-staging.{_TABLE_2_ADDRESS.to_str()}` t2
        ON t1.a = t2.a_other AND t1.b = t2.b_other
        """

        # 1 point for extra table reference, 2 point for FULL OUTER JOIN,
        # 3 points for ON boolean expression
        self.assertEqual(6, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}` t1
        FULL OUTER JOIN
            `recidiviz-staging.{_TABLE_2_ADDRESS.to_str()}` t2
        ON t1.a < t2.a_other OR t1.b < t2.b_other
        """

        # 1 point for extra table reference, 2 point for FULL OUTER JOIN,
        # 3 points for boolean expression in ON (x2 for using non-standard boolean
        # logic).
        self.assertEqual(9, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        LEFT OUTER JOIN
            `recidiviz-staging.{_TABLE_2_ADDRESS.to_str()}`
        USING (a)
        LEFT OUTER JOIN
            `recidiviz-staging.{_TABLE_3_ADDRESS.to_str()}`
        USING (b)
        """

        # 2 point for extra table reference2, 2*2 point for LEFT OUTER JOIN,
        # 0 points for USING
        self.assertEqual(6, self._get_query_score(query))

    def test_windows(self) -> None:
        query = f"""
        SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS c
        FROM
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """

        # 1 point for ROW_NUMBER function, 3 points for window
        self.assertEqual(4, self._get_query_score(query))

        query = f"""
        SELECT a, b, ROW_NUMBER() OVER w AS c
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        WINDOW w AS (PARTITION BY a ORDER BY b)
        """

        # 1 point for ROW_NUMBER function, 3 points for window
        self.assertEqual(4, self._get_query_score(query))

        query = f"""
        SELECT a, b, ROW_NUMBER() OVER w AS c, RANK() OVER w AS d
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        WINDOW w AS (PARTITION BY a ORDER BY b)
        """

        # 1 point for ROW_NUMBER function, 1 point for RANK function,
        # 3 points for window (not double counted)
        self.assertEqual(5, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) > 1
        """

        # 1 point for ROW_NUMBER function, 3 points for window, 1 point for boolean
        # expression, 1 point for QUALIFY
        self.assertEqual(6, self._get_query_score(query))

    def test_subqueries(self) -> None:
        query = f"""
        WITH my_cte AS (
            SELECT a, b 
            FROM 
                `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        )
        SELECT *
        FROM 
            my_cte
        """

        # 2 points for subquery in CTE
        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        WITH 
        -- This explains my_cte
        my_cte AS (
            SELECT a, b 
            FROM 
                `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        )
        SELECT *
        FROM 
            my_cte
        """

        # 2 points for subquery in CTE, -1 for the CTE comment.
        self.assertEqual(1, self._get_query_score(query))

        query = f"""
        SELECT *
        FROM (
            SELECT a, b 
            FROM 
                `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        )
        """

        # 3 points for subquery in FROM clause
        self.assertEqual(3, self._get_query_score(query))

        query = f"""
        SELECT *
        FROM `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        WHERE a IN (
            SELECT a
            FROM 
                `recidiviz-staging.{_TABLE_2_ADDRESS.to_str()}`
        )
        """

        # 4 points for subquery in WHERE clause, 1 point for second table reference,
        # 1 point for boolean expression, 1 point for WHERE clause
        self.assertEqual(6, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM (
            SELECT a, b, COUNT(*) AS cnt
            FROM 
                `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
            GROUP BY a, b
        )
        WHERE cnt < 1
        """

        # 1 point for GROUP BY, 1 point for COUNT, 1 point for boolean expression,
        # 3 points for subquery in FROM clause, 1 point for WHERE clause
        self.assertEqual(7, self._get_query_score(query))

        query = f"""
        SELECT a, b 
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        UNION ALL
        SELECT a, b 
        FROM 
            `recidiviz-staging.{_TABLE_2_ADDRESS.to_str()}`
        """

        # 2 points for each subquery in a UNION, 1 point for the second table reference.
        self.assertEqual(5, self._get_query_score(query))

        query = f"""
        SELECT a, b 
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        EXCEPT DISTINCT
        SELECT a, b 
        FROM 
            `recidiviz-staging.{_TABLE_2_ADDRESS.to_str()}`
        """

        # 2 points for each subquery, 1 point for the second table reference.
        self.assertEqual(5, self._get_query_score(query))

    def test_functions(self) -> None:
        query = f"""
        SELECT COUNTIF(a = 'A' AND b = 'B') 
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 1 point for COUNT, 3 points for boolean expression
        self.assertEqual(4, self._get_query_score(query))

        query = f"""
        SELECT 
            CASE 
                WHEN a = 'A1' THEN b 
                WHEN (a + b - c) = 2 THEN 'B1' 
                ELSE CONCAT(c, d) 
            END
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 1 point for CASE
        # 2 point for CONCAT
        # 1 point for = (x2)
        # 2 point for a + b - c
        # 1 point for each WHEN branch (x2)
        self.assertEqual(9, self._get_query_score(query))

        query = f"""
        SELECT a, b
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        WHERE REGEXP_CONTAINS(supervision_level_raw_text, r'_')
        """
        # 10 points for REGEXP_CONTAINS, 1 point for WHERE clause
        self.assertEqual(11, self._get_query_score(query))

        query = f"""
        SELECT DATE_DIFF(a, b, DAY) 
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 3 point for DATE_DIFF
        self.assertEqual(3, self._get_query_score(query))

        query = f"""
        SELECT CONCAT(a, '-', b) 
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 2 point for CONCAT
        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        SELECT a, b, ARRAY_AGG(c)
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        GROUP BY a, b
        """

        # 1 point for GROUP BY, 1 point for ARRAY_AGG
        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        SELECT a, b, ARRAY_AGG(c ORDER BY d)
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        GROUP BY a, b
        """

        # 1 point for GROUP BY, 1 point for ARRAY_AGG
        self.assertEqual(2, self._get_query_score(query))

        query = """
        SELECT a, b
        FROM 
            UNNEST([STRUCT(1 AS a, 2 AS b)])
        """

        # 1 point for UNNEST, 1 point for STRUCT
        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        SELECT TO_JSON()
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 1 point for TO_JSON
        self.assertEqual(1, self._get_query_score(query))

        query = f"""
        SELECT TO_JSON_STRING(STRUCT())
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 1 point for STRUCT, 1 point for TO_JSON_STRING
        self.assertEqual(2, self._get_query_score(query))

        query = f"""
        SELECT TO_JSON_STRING(STRUCT(a - b))
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 1 point for STRUCT, 1 point for TO_JSON_STRING, 1 point for minus
        self.assertEqual(3, self._get_query_score(query))

        query = f"""
        SELECT TO_JSON_STRING(STRUCT(a - b AS c))
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 1 point for STRUCT, 1 point for TO_JSON_STRING, 1 point for minus
        self.assertEqual(3, self._get_query_score(query))

        query = f"""
        SELECT PARSE_DATE("%y%j", a)
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 10 point for PARSE_DATE
        self.assertEqual(10, self._get_query_score(query))

        query = f"""
        SELECT SAFE.PARSE_DATE("%y%j", a)
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 10 point for PARSE_DATE, 2 for SAFE
        self.assertEqual(12, self._get_query_score(query))

        query = f"""
        SELECT
            DATE_DIFF(
                LEAST(
                    DATE_ADD(date_1, INTERVAL 365 DAY),
                    COALESCE(date_2, DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))
                ),
                GREATEST(
                    date_3,
                    IF(date_4 <= DATE_ADD(date_5, INTERVAL 365 DAY), date_6, NULL)
                ),
                DAY
            )
        FROM 
            `recidiviz-staging.{_TABLE_1_ADDRESS.to_str()}`
        """
        # 3 point for DATE_DIFF
        # 1 point for LEAST
        # 1 point for GREATEST
        # 3 point for DATE_ADD (x3)
        # 1 point for IF
        # 1 point for COALESCE
        # 1 point for <=
        # 1 point for CURRENT_DATE
        self.assertEqual(18, self._get_query_score(query))
