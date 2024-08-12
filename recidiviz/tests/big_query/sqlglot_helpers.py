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
"""
Utility functions that use the sqlglot library to do advanced checks
of SQL queries using their ASTs.
"""
from typing import Set

import sqlglot
import sqlglot.expressions as expr

ORDER_BY_ALLOWED_IN = (expr.Window, expr.GroupConcat, expr.ArrayAgg)


def check_query_is_not_ordered_outside_of_windows(query: str) -> None:
    """
    Raises a value error if a query uses an ORDER BY statement outside of a window function.

    Valid queries have no ORDER BY, or:

    Have an ORDER BY in a window function like:

        SELECT *
        FROM table_a
        QUALIFY ROW_NUMBER() OVER(PARTITION BY a ORDER BY b DESC) = 1

    Use an ORDER BY in aggregation functions like STRING_AGG:

        SELECT
          ARRAY_AGG(
            STRUCT<outcome_type STRING, outcome_data STRING>
                (violationoutcome, outcomedetails)
            ORDER BY violationoutcome, outcomedetails
            )
        FROM
            table

    Invalid queries have an ORDER BY on the outside like:
        SELECT * FROM table_a ORDER BY b
    """
    tree = sqlglot.parse_one(query, dialect="bigquery")
    if not isinstance(tree, expr.Query):
        raise ValueError("Non-Query SQL expression built from ViewBuilder")
    allowed_order_by_statements = set()
    for window in tree.find_all(*ORDER_BY_ALLOWED_IN):
        for order in window.find_all(expr.Order):
            allowed_order_by_statements.add(order)
    for order in tree.find_all(expr.Order):
        if order not in allowed_order_by_statements:
            raise ValueError(
                f"Found ORDER BY statement that isn't applied to window or aggregation: '{order}'"
            )


def get_undocumented_ctes(query: str) -> Set[str]:
    """
    Returns the names of CTEs that do not have a comment.

    We expect to have CTEs in queries documented like:
        WITH
        -- this explains table 1
        table_1 AS (
            SELECT * FROM A
        ),
        -- this explains table 2
        table_2 AS (
            SELECT * FROM B JOIN C USING(col)
        )
        SELECT * FROM table_1 UNION ALL SELECT * FROM table_2

    """
    tree = sqlglot.parse_one(query, dialect="bigquery")
    if not isinstance(tree, expr.Query):
        raise ValueError("Non-Query SQL expression built from ViewBuilder")
    return {
        cte.alias
        for cte in tree.ctes
        # TODO(#29272) Update DirectIngestViewQueryBuilder to self document generated views
        if "generated_view" not in cte.alias and not cte.args["alias"].comments
    }
