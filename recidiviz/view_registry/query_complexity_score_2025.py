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
"""Functionality for computing a score that can be used to rank BigQuery SQL queries
according to their conceptual complexity.
"""

import sqlglot.expressions as expr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_sqlglot_helpers import (
    does_cte_expression_have_docstring,
)


def _join_type_complexity_score(join_expression: expr.Join) -> int:
    """Returns the complexity score for a JOIN expression of a given type. JOINs are 1
    point by default, with any join other than a JOIN/INNER JOIN getting an extra
    point.

    Complexity points for the JOIN condition are awarded separately as part of the
    condition_expressions_score.
    """
    score = 1
    if join_expression.kind and join_expression.kind != "INNER":
        # Extra points for OUTER joins
        score += 1
    return score


def _top_level_condition_expression_score(
    top_level_condition_expression: expr.Condition,
) -> int:
    """Returns the complexity score for a Condition expression that is not part of a
    larger Condition expression. The term "condition" is used liberally in sqlglot,
    meaning anything, more or less, that can produce a value result. This includes
    functions, boolean expressions, other binary arithmetic operators and window
    expressions.

    For example, this produces a score for a top-level boolean expression in a WHERE
    clause OR an expression that produces an output column value.
    """
    score = sum(
        _condition_type_complexity_score(e)
        for e in top_level_condition_expression.find_all(expr.Condition)
    )

    parent = top_level_condition_expression.parent

    if isinstance(parent, (expr.Where, expr.Having, expr.Qualify)):
        # This expression is modifying the set of returned rows - increase point
        # value.
        return score + 1

    if isinstance(parent, expr.Join):
        # This expression is impacting a join condition - increase point
        # value if there's complex logic other than simple AND / equals operations.
        # value if there's complex logic other than simple AND / equals operations.
        has_complex_operations = bool(
            {
                cond_e
                for cond_e in top_level_condition_expression.find_all(expr.Condition)
                if not isinstance(cond_e, (expr.And, expr.EQ, expr.Column, expr.Dot))
            }
        )
        return score * 2 if has_complex_operations else score

    if isinstance(parent, (expr.HavingMax, expr.IgnoreNulls, expr.RespectNulls)):
        return score + 1

    if isinstance(
        parent,
        (
            expr.Alias,
            # Extra points awarded separately as part of the distincts_score
            expr.Distinct,
            # Extra points awarded separately as part of the group_by_score
            expr.Group,
            # Extra points awarded separately as part of the pivots_score
            expr.Pivot,
            expr.Interval,
            expr.Limit,
            expr.Order,
            expr.Ordered,
            expr.Select,
            expr.Star,
            expr.Tuple,
            expr.Unnest,
            expr.WindowSpec,
            expr.JSONKeyValue,
        ),
    ):
        return score

    raise ValueError(
        f"Unexpected parent type for top-level Condition expression: [{type(parent)}]. "
        f"Expression: [{top_level_condition_expression.sql()}]"
    )


def _condition_type_complexity_score(cond_exp: expr.Condition) -> int:
    """Returns the complexity score for the provided condition expression. Higher
    complexity scores are awarded to condition expression types that are more
    error-prone, harder to understand, or whose logic should not be repeated often. This
    does not compute the complexity of all the sub-expressions referenced by this
    condition, just the cost to use a condition expression of this type.
    """
    condition_type = type(cond_exp)
    if condition_type in {
        expr.Column,
        expr.Null,
        expr.Literal,
        expr.Boolean,
        expr.RawString,
    }:
        # sqlglot treats simple columns / NULL values as potential boolean values -
        # exclude these from the complexity score.
        return 0

    if isinstance(cond_exp, expr.Func):
        return _function_complexity_score(cond_exp)

    if condition_type in {
        # Do not assign points for assigning an alias / STRUCT property name
        expr.PropertyEQ,
        expr.Paren,
    }:

        return 0

    if isinstance(cond_exp, expr.Dot):
        first_part = cond_exp.this
        if (
            isinstance(first_part, expr.Identifier)
            and first_part.name == "SAFE"
            and isinstance(cond_exp.expression, expr.Func)
        ):
            # "SAFE" functions (e.g. SAFE.PARSE_DATE) are hard to reason about because
            # they could be silently not working.
            return 2
        # Referencing columns qualified by a table name should not add to the score
        # ( e.g. t.col)
        return 0
    if condition_type is expr.DPipe:
        # Extra point for || which does string concatenation - ideally we wouldn't
        # do
        # string manipulation.
        return 2

    if issubclass(condition_type, expr.Window):
        # Don't assign points to window aliases - these will be scored as separate
        # top-level condition expressions
        return 0 if cond_exp.alias else 3

    if issubclass(
        condition_type,
        (
            expr.Unary,
            expr.Binary,
            expr.Between,
            expr.In,
            expr.Bracket,
            expr.SubqueryPredicate,
        ),
    ):
        return 1

    raise ValueError(f"Unexpected function expression type [{condition_type}]")


def _function_complexity_score(function_expression: expr.Func) -> int:
    """Returns the complexity score for a function. Functions each are awarded at least
    1 point. Higher complexity scores are awarded to functions that are more
    error-prone, harder to understand, or whose logic should not be repeated often. This
    does not compute the complexity of all the sub-expressions referenced by this
    function, just the cost to use a function of this type.
    """
    function_type = type(function_expression)
    function_name = function_expression.name.upper()

    if function_type in {
        expr.Max,
        expr.Min,
        expr.Avg,
        expr.AnyValue,
        expr.Least,
        expr.Greatest,
        expr.Sum,
        expr.LogicalOr,
        expr.LogicalAnd,
        expr.Abs,
        expr.Floor,
        expr.Ceil,
        expr.Pow,
        expr.SafeDivide,
        expr.Round,
        expr.Count,
        expr.CountIf,
        expr.Lead,
        expr.Lag,
        expr.FirstValue,
        expr.LastValue,
        expr.RowNumber,
        expr.Coalesce,
        expr.Case,
        expr.If,
        expr.Nullif,
        expr.CurrentDate,
        expr.CurrentDatetime,
        expr.CurrentTime,
        expr.CurrentTimestamp,
        expr.Array,
        expr.GenerateSeries,
        expr.GenerateDateArray,
        expr.Struct,
        expr.ArrayAgg,
        expr.ArrayConcat,
        expr.GroupConcat,  # STRING_AGG
        expr.ArrayToString,
        expr.ArraySize,
        expr.Length,
        expr.SHA2,
        expr.Hex,
        expr.ToBase64,
        expr.StartsWith,
        expr.JSONFormat,
        expr.JSONObject,
    } or (
        function_type == expr.Anonymous
        and function_name
        in {
            "TO_JSON",
            "DENSE_RANK",
            "RANK",
            "APPROX_QUANTILES",
            "TRUNC",
            "IEEE_DIVIDE",
            "ENDS_WITH",
            "ERROR",
            "CHAR_LENGTH",
            "ARRAY_CONCAT_AGG",
            "ARRAY_REVERSE",
        }
    ):
        return 1

    # Simple date manipulation
    if function_type in {
        expr.Extract,
        expr.DateAdd,
        expr.DateSub,
        expr.DateDiff,
        expr.DateTrunc,
        expr.DatetimeAdd,
        expr.DatetimeDiff,
        expr.DatetimeSub,
        expr.DatetimeTrunc,
        expr.LastDay,
        expr.DateFromParts,
        expr.TimestampDiff,
        expr.TimestampTrunc,
        expr.TimestampAdd,
    }:
        return 3

    # Ideally we wouldn't be doing so much string manipulation - make simple string
    # manipulation slightly more expensive.
    if function_type in {
        expr.Trim,
        expr.Upper,
        expr.Lower,
        expr.Concat,
        expr.Initcap,
    } or (function_type == expr.Anonymous and function_name in {"LTRIM", "RTRIM"}):
        return 2

    # More complex string manipulation gets a higher score.
    if function_type in {expr.Substring, expr.Left, expr.Right, expr.Split} or (
        function_type == expr.Anonymous
        and function_name
        in {"REPLACE", "FORMAT", "SUBSTR", "CONTAINS_SUBSTR", "LPAD", "INSTR", "STRPOS"}
    ):
        return 5

    # Regex operations are hard to understand at a glance - make these quite
    # expensive.
    if function_type in {
        expr.RegexpExtract,
        expr.RegexpReplace,
        expr.RegexpLike,
        expr.RegexpSplit,
        expr.RegexpILike,
    } or (
        function_type == expr.Anonymous
        and function_name in {"REGEXP_SUBSTR", "REGEXP_EXTRACT_ALL"}
    ):
        return 10

    # Date parsing is error-prone and ideally we'd be doing this in very few places
    # and propagating parsed dates through our code - make this very expensive.
    if function_type in {expr.StrToDate, expr.StrToTime} or (
        function_type in {expr.Anonymous, expr.TryCast}
        and function_name
        in {
            "PARSE_DATE",
            "PARSE_TIME",
            "PARSE_DATETIME",
            "PARSE_TIMESTAMP",
            "FORMAT_DATETIME",
        }
    ):
        return 10

    # JSON parsing is super brittle - make this very expensive.
    if function_type in {expr.JSONExtract, expr.JSONExtractScalar, expr.ParseJSON,} or (
        function_type in {expr.Anonymous, expr.TryCast}
        and function_name
        in {
            "JSON_QUERY",
            "JSON_QUERY_ARRAY",
            "JSON_EXTRACT_ARRAY",
            "JSON_EXTRACT_STRING_ARRAY",
            "JSON_VALUE",
            "JSON_VALUE_ARRAY",
            "JSON_EXTRACT_KEYS",
            "PARSE_JSON",
        }
    ):
        return 10

    # Ideally we wouldn't to a ton of casting - we should be storing data in its most
    # useful format (e.g. storing dates as dates not strings).
    if function_type in {
        expr.Cast,
        expr.TryCast,
        expr.Date,
        expr.Timestamp,
        expr.TimeToStr,
        expr.TsOrDsToDate,
    } or (
        function_type == expr.Anonymous
        and function_name
        in {
            "STRING",
            "DATETIME",
            "TIMESTAMP",
        }
    ):
        return 3

    raise ValueError(
        f"Found function with unknown complexity [{function_type=}, {function_name=}]. "
        f"You are using a function that hasn't been used in any view query before. "
        f"Consult in #platform-team to determine the appropriate complexity score."
    )


def _table_complexity_score(
    table_expression: expr.Table,
    address_to_complexity_score_mapping: dict[BigQueryAddress, int],
) -> int:
    """ ""Returns the complexity score for a single table reference. References to named
    table expressions (e.g. CTEs) are given 1 point. Scores for any other reference are
    taken from the |address_to_complexity_score_mapping|.
    """
    table_name = table_expression.name
    if not table_name:
        raise ValueError(
            f"Found table expression with no table name: {table_expression}"
        )
    if not table_expression.db:
        # Do not penalize for referencing tables generated by CTEs / subqueries - that
        # complexity is captured in the query_expressions_score.
        return 0

    address = BigQueryAddress(dataset_id=table_expression.db, table_id=table_name)
    if address not in address_to_complexity_score_mapping:
        raise ValueError(f"No known complexity score for table [{address.to_str()}]")

    return address_to_complexity_score_mapping[address]


def _tables_complexity_score(
    full_query_syntax_tree: expr.Query,
    address_to_complexity_score_mapping: dict[BigQueryAddress, int],
) -> int:
    """Returns the complexity score for all table references in the query."""
    score = sum(
        _table_complexity_score(e, address_to_complexity_score_mapping)
        for e in full_query_syntax_tree.find_all(expr.Table)
    )

    # We generally will have at least 1 table reference, do not penalize for the
    #  first one.
    return max(score - 1, 0)


def _query_expression_complexity_score(query_expression: expr.Query) -> int:
    """Returns the complexity score that should be added for this query expression.

    Cost of sub-expressions (e.g. joins, conditions) are capture elsewhere. No points
    awarded for the top-level expression.
    """
    if query_expression.parent is None:
        # We expect to always see one top-level query expression, do not penalize for
        # that one.
        return 0

    if isinstance(query_expression, expr.Subquery):
        # We will capture the complexity from the Query that is the child of this
        # subquery.
        return 0

    cost = 3

    if isinstance(query_expression.parent, expr.CTE):
        # Minus 1 complexity for moving subquery to a CTE
        cost += -1

        if does_cte_expression_have_docstring(query_expression.parent):
            # Minus 1 complexity for documenting the CTE
            cost += -1

    elif isinstance(query_expression.parent, expr.Union):
        # Sub-expressions in UNION ALL statements have to follow the same output
        # structure so are somewhat less conceptually complex - give one point credit.
        cost += -1
    elif isinstance(query_expression.parent, (expr.Subquery, expr.SubqueryPredicate)):
        # No credits for queries in the context of a plain subquery
        # (e.g. referenced by a FROM or other non-CTE context).
        pass
    elif isinstance(query_expression.parent, expr.Array):
        # A subquery inside an ARRAY is extra hard to conceptualize
        cost += 1
    else:
        raise ValueError(
            f"Unexpected query parent type [{type(query_expression.parent)}]"
        )

    return cost


def get_query_complexity_score_2025(
    full_query_syntax_tree: expr.Query,
    address_to_complexity_score_mapping: dict[BigQueryAddress, int],
) -> int:
    """Returns a score that can be used to rank BigQuery SQL queries according to their
    conceptual complexity. In essence, this score tries to get at "how easy is it to
    look at this query and understand what it is doing?". This score is imperfect, but
    will help us track how our view complexity changes over time.

    Args:
        full_query_syntax_tree: The parsed syntax tree for the SQL query to analyze
        address_to_complexity_score_mapping: A mapping of address to point value for all
            possible valid table / view addresses referenced by this query. This
            function will crash if a referenced table is not in this map.

    NOTE: THE ALGORITHM FOR COMPUTING THIS SCORE IS LOCKED FOR 2025 - DO NOT MAKE ANY
    MODIFICATIONS TO THE LOGIC, OTHERWISE WE WON'T BE ABLE TO LEGITIMATELY COMPARE HOW
    COMPLEXITY CHANGES OVER THE COURSE OF THE YEAR.
    """
    query_expressions_score = sum(
        _query_expression_complexity_score(e)
        for e in full_query_syntax_tree.find_all(expr.Query)
    )
    tables_score = _tables_complexity_score(
        full_query_syntax_tree, address_to_complexity_score_mapping
    )

    top_level_condition_expressions = {
        e
        for e in full_query_syntax_tree.find_all(expr.Condition)
        if not isinstance(e.parent, expr.Condition)
    }
    condition_expressions_score = sum(
        _top_level_condition_expression_score(e)
        for e in top_level_condition_expressions
    )

    joins_score = sum(
        _join_type_complexity_score(e)
        for e in full_query_syntax_tree.find_all(expr.Join)
    )

    group_by_score = len(list(full_query_syntax_tree.find_all(expr.Group)))
    distincts_score = len(list(full_query_syntax_tree.find_all(expr.Distinct)))
    pivots_score = len(list(full_query_syntax_tree.find_all(expr.Pivot)))

    return (
        query_expressions_score
        + tables_score
        + condition_expressions_score
        + joins_score
        + group_by_score
        + distincts_score
        + pivots_score
    )
