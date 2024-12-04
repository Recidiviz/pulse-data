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
import sqlglot
import sqlglot.expressions as expr

from recidiviz.big_query.big_query_sqlglot_helpers import (
    get_state_code_literal_references,
)
from recidiviz.big_query.big_query_view import BigQueryViewBuilder

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


def check_query_selects_output_columns(
    query: str, expected_output_columns: set[str]
) -> None:
    """Raises a value error if the top-level SELECT statement in the provided query does
    not explicitly select the provided expected_output_columns and only those columns.

    For example, this would be a valid query for expected columns {a, b}:
    WITH cte AS (SELECT * FROM table)
    SELECT a, b
    FROM cte

    This would also be a valid query:
    SELECT x AS a, b
    FROM table

    However, any query with a top-level SELECT * statement will fail.
    """

    if not expected_output_columns:
        raise ValueError("Expected non-empty expected_output_columns")

    tree = sqlglot.parse_one(query, dialect="bigquery")
    if not isinstance(tree, expr.Select):
        raise ValueError("Query does not have a valid top-level SELECT statement")
    if tree.is_star:
        raise ValueError(
            f"May not use a wildcard (*) in the query SELECT statement. You should "
            f"explicitly list the expected columns: {expected_output_columns}"
        )

    found_output_columns = set()
    for expression in tree.expressions:
        if isinstance(expression, expr.Column):
            found_output_columns.add(expression.name)
        elif isinstance(expression, expr.Alias):
            found_output_columns.add(expression.alias)
        else:
            raise ValueError(f"Unexpected column expression type [{type(expression)}].")

    if missing := expected_output_columns - found_output_columns:
        raise ValueError(
            f"Missing expected top-level selected columns: {sorted(missing)}"
        )
    if extra := found_output_columns - expected_output_columns:
        raise ValueError(
            f"Found unexpected top-level selected columns: {sorted(extra)}"
        )


def check_view_has_no_state_specific_logic(view_builder: BigQueryViewBuilder) -> None:
    """Raises a value error if the view associated with the provided builder encodes
    state-specific logic. State-specific logic is categorized in two ways:
        1) Querying from state-specific tables
        2) Explicitly referencing a state code (e.g. "US_XX") in the query
    """
    try:
        view = view_builder.build(sandbox_context=None)
    except Exception as e:
        raise ValueError(
            f"Unable to build view [{view_builder.address.to_str()}]"
        ) from e

    state_specific_addresses = [
        a.to_str() for a in view.parent_tables if a.is_state_specific_address()
    ]

    if state_specific_addresses:
        raise ValueError(
            f"Cannot query from state-specific tables in this view. Found view "
            f"[{view_builder.address.to_str()}] with state-specific parent tables: "
            f"{state_specific_addresses}"
        )

    view_query = view_builder.build(sandbox_context=None).view_query
    tree = sqlglot.parse_one(view_query, dialect="bigquery")
    if not isinstance(tree, sqlglot.expressions.Query):
        raise ValueError(f"Unexpected query expression type [{type(tree)}]")
    found_state_codes = get_state_code_literal_references(tree)

    if found_state_codes:
        raise ValueError(
            f"Cannot include state-specific logic this view. Found reference "
            f"to state_code(s) {[s.value for s in found_state_codes]}."
        )
