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
"""Query parsing helpers that are used in production code to understand BigQuery view
queries.
"""
import re

import sqlglot
import sqlglot.expressions as expr

from recidiviz.common.constants.states import StateCode


def get_state_code_literal_references(
    parsed_syntax_tree: expr.Query,
) -> set[StateCode]:
    """Returns any state code literals referenced by this query expression (e.g. "US_XX"
    written in the SQL).
    """

    found_states = set()
    for literal in parsed_syntax_tree.find_all(expr.Literal):
        if not literal.is_string:
            continue
        str_literal_value = literal.this

        if match := re.match(r"^US_[A-Z]{2}$", str_literal_value):
            found_states.add(StateCode(match.string))

    return found_states


def does_cte_expression_have_docstring(cte_expression: expr.CTE) -> bool:
    """
    Returns True if the given CTE expression has a comment, False otherwise.

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
    return bool(cte_expression.args["alias"].comments)


def get_undocumented_ctes(query: str) -> set[str]:
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
        if "generated_view" not in cte.alias
        and not does_cte_expression_have_docstring(cte)
    }
