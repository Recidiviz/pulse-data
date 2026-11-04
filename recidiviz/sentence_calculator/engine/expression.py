# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Evaluates the arithmetic expressions in law regime rules without eval().

Rule expressions are written in law regime YAML files, for example a credit cap
of "sentence_length_days * 0.58" or a date formula of
"start_date + adjusted_min_days". Given the variables

    sentence_length_days = TrackedValue(value=1000, label="S1.original_min_days")

evaluate_days_expression returns, for the credit cap expression,

    TrackedValue(
        value=580,
        label="S1.credit_cap",
        sources=(sentence_length_days,),
        rule_name="credit_cap",
        formula="sentence_length_days * 0.58",
    )
"""

import ast
import math
import operator
from collections.abc import Callable, Mapping
from datetime import date, timedelta
from fractions import Fraction
from typing import Any

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.sentence_calculator.models.rules import EXPRESSION_DURATION_FUNCTIONS
from recidiviz.sentence_calculator.models.tracked import TrackedValue

_NUMERIC_BINARY_OPERATORS: dict[type[ast.operator], Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
}


@attr.define(frozen=True, kw_only=True)
class _PendingDate:
    """A date plus the days an expression has added to or subtracted from it so
    far. The days stay exact until the expression is fully evaluated, so
    start_date + 0.5 + 0.5 is the day after start_date rather than start_date
    plus two half days truncated to zero."""

    base_date: date = attr.ib(validator=attr.validators.instance_of(date))
    """The date before the pending days."""

    pending_days: Fraction = attr.ib(validator=attr.validators.instance_of(Fraction))
    """Days to add to base_date, possibly fractional or negative."""

    def plus_days(self, days: Fraction) -> "_PendingDate":
        """Returns this date with days added to its pending days."""
        return _PendingDate(
            base_date=self.base_date, pending_days=self.pending_days + days
        )

    def plus_duration(self, duration: relativedelta) -> "_PendingDate":
        """Returns this date moved by its pending whole days and then by
        duration, carrying the fractional rest of the pending days. Moving by
        the whole days first keeps them in the order the expression adds them,
        which matters at the end of a month: 2024-01-30 + 1 + months(1) is
        2024-02-29, while 2024-01-30 + months(1) + 1 is 2024-03-01."""
        whole_days = int(self.pending_days)
        return _PendingDate(
            base_date=self.base_date + timedelta(days=whole_days) + duration,
            pending_days=self.pending_days - whole_days,
        )

    def resolve(self) -> date:
        """Returns base_date moved by the pending days, truncated toward zero."""
        return self.base_date + timedelta(days=int(self.pending_days))

    def __str__(self) -> str:
        if not self.pending_days:
            return str(self.base_date)
        return f"{self.base_date} + {self.pending_days} days"


def evaluate_days_expression(
    *,
    expression: str,
    variables: Mapping[str, TrackedValue[int | float | date]],
    label: str,
    rule_name: str,
) -> TrackedValue[int]:
    """Returns the number of days expression evaluates to, tracked back to the
    variables it used. Any fractional result is truncated, since every value
    rules compute is a whole number of days. The arithmetic is exact, so
    "sentence_length_days * 0.58" with 100 days is 58, not the 57 that
    truncating the float product 57.99999999999999 would give.

    Args:
        expression: The expression to evaluate, e.g. "sentence_length_days * 0.58".
        variables: Values the expression may reference, keyed by name.
        label: Label for the resulting TrackedValue.
        rule_name: Name of the rule the expression belongs to.
    """
    result, used_variables = _evaluate_expression(
        expression=expression, variables=variables, rule_name=rule_name
    )
    if isinstance(result, date):
        raise _expression_error(
            expression,
            rule_name,
            f"produced the date [{result}], but this rule must produce a number "
            f"of days.",
        )
    if isinstance(result, relativedelta):
        raise _expression_error(
            expression,
            rule_name,
            "produced a duration, but this rule must produce a number of days.",
        )
    # TODO(NV-47): Truncation or non-truncation logic is state-specific. We'll
    # need to revisit this after checking with NV and when adding AZ support.
    return TrackedValue(
        value=int(result),
        label=label,
        sources=used_variables,
        rule_name=rule_name,
        formula=expression,
    )


def evaluate_date_expression(
    *,
    expression: str,
    variables: Mapping[str, TrackedValue[int | float | date]],
    label: str,
    rule_name: str,
) -> TrackedValue[date]:
    """Returns the date expression evaluates to, tracked back to the variables
    it used. The days added to or subtracted from a date are summed exactly and
    truncated toward zero once, so "start_date + adjusted_min_days * 0.5 +
    jail_credit_days * 0.5" with 365 and 3 days is 184 days after start_date.

    Args:
        expression: The expression to evaluate, e.g. "start_date + adjusted_min_days".
        variables: Values the expression may reference, keyed by name.
        label: Label for the resulting TrackedValue.
        rule_name: Name of the rule the expression belongs to.
    """
    result, used_variables = _evaluate_expression(
        expression=expression, variables=variables, rule_name=rule_name
    )
    if isinstance(result, relativedelta):
        raise _expression_error(
            expression,
            rule_name,
            "produced a duration, but this rule must produce a date.",
        )
    if not isinstance(result, date):
        raise _expression_error(
            expression,
            rule_name,
            f"produced the number [{result}], but this rule must produce a date.",
        )
    return TrackedValue(
        value=result,
        label=label,
        sources=used_variables,
        rule_name=rule_name,
        formula=expression,
    )


def _evaluate_expression(
    *,
    expression: str,
    variables: Mapping[str, TrackedValue[int | float | date]],
    rule_name: str,
) -> tuple[Fraction | date | relativedelta, tuple[TrackedValue[Any], ...]]:
    """Returns the value of expression and the variables it used, in the order
    it first used them.

    Expressions support numeric constants, the variables named in variables,
    parentheses, unary minus, and the operators +, -, *, /, and //. A date plus
    or minus a number of days is a date, with the days kept exact until the
    expression is fully evaluated. Numbers are evaluated as exact
    fractions, with float constants and variables read as the decimal they
    print as (0.58 is 58/100). Infinite and NaN values raise, as does a date
    before date.min or after date.max.

    The duration functions years(n) and months(n) take a whole number, and a
    date plus or minus one moves by that many calendar years or months, so
    2024-02-29 - months(12) is 2023-02-28. Whole days already added to a date
    move it before a duration does. A duration can't be used any other way.
    """
    # Keyed by name, in the order the expression first uses each one.
    used_variables: dict[str, TrackedValue[Any]] = {}

    def expression_error(problem: str) -> ValueError:
        """Returns the error for this expression, ending with problem."""
        return _expression_error(expression, rule_name, problem)

    def evaluate_duration(node: ast.Call) -> relativedelta:
        """Returns the duration a years(n) or months(n) call evaluates to."""
        if not (
            isinstance(node.func, ast.Name)
            and node.func.id in EXPRESSION_DURATION_FUNCTIONS
        ):
            raise expression_error(
                f"calls [{ast.unparse(node.func)}], but the only functions are "
                f"{list(EXPRESSION_DURATION_FUNCTIONS)}."
            )
        unit = node.func.id
        count = (
            node.args[0].value
            if len(node.args) == 1 and isinstance(node.args[0], ast.Constant)
            else None
        )
        if node.keywords or isinstance(count, bool) or not isinstance(count, int):
            raise expression_error(
                f"calls [{ast.unparse(node)}], but {unit}() takes one whole number, "
                f"e.g. {unit}(12)."
            )
        if unit == "years":
            return relativedelta(years=count)
        if unit == "months":
            return relativedelta(months=count)
        raise ValueError(f"Unexpected duration function: [{unit}]")

    def to_finite_fraction(value: int | float, *, description: str) -> Fraction:
        """Returns value as an exact fraction, raising if it is infinite or NaN.
        description says where value came from in the error message."""
        if isinstance(value, float) and not math.isfinite(value):
            raise expression_error(
                f"{description} [{value!r}], but expressions only support finite "
                f"numbers."
            )
        return _to_fraction(value)

    def evaluate_node(node: ast.AST) -> Fraction | _PendingDate | relativedelta:
        """Returns the value of node, recording each variable it reads in
        used_variables."""
        if isinstance(node, ast.Expression):
            return evaluate_node(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
                raise expression_error(
                    f"has constant [{node.value!r}]; only numeric constants are "
                    f"supported."
                )
            return to_finite_fraction(node.value, description="has constant")
        if isinstance(node, ast.Name):
            if node.id not in variables:
                raise expression_error(
                    f"references undefined variable [{node.id}]. Available "
                    f"variables: {sorted(variables)}."
                )
            variable = variables[node.id]
            used_variables[node.id] = variable
            if isinstance(variable.value, date):
                return _PendingDate(base_date=variable.value, pending_days=Fraction(0))
            return to_finite_fraction(
                variable.value, description=f"reads variable [{node.id}] as"
            )
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            operand = evaluate_node(node.operand)
            if isinstance(operand, _PendingDate):
                raise expression_error("negates a date.")
            if isinstance(operand, relativedelta):
                raise expression_error(
                    "negates a duration. Subtract it from a date instead."
                )
            return -operand
        if isinstance(node, ast.BinOp):
            return evaluate_binary_operation(
                node.op, evaluate_node(node.left), evaluate_node(node.right)
            )
        if isinstance(node, ast.Call):
            return evaluate_duration(node)
        raise expression_error(f"uses unsupported syntax [{type(node).__name__}].")

    def out_of_range_date_error(pending_date: _PendingDate) -> ValueError:
        """Returns the error for a date moved before date.min or after date.max."""
        return expression_error(
            f"moves the date [{pending_date}] outside the range dates support, "
            f"[{date.min}] to [{date.max}]."
        )

    def move_by_duration(
        pending_date: _PendingDate, duration: relativedelta
    ) -> _PendingDate:
        """Returns pending_date moved by duration, raising if that leaves the
        range dates support."""
        try:
            return pending_date.plus_duration(duration)
        except (OverflowError, ValueError) as e:
            # Moving past date.max or before date.min raises OverflowError for
            # whole days and ValueError for a duration, e.g. "year 10024 is out
            # of range".
            raise out_of_range_date_error(pending_date) from e

    def misused_duration_error(op: ast.operator) -> ValueError:
        """Returns the error for any use of op on a duration other than adding
        it to or subtracting it from a date."""
        return expression_error(
            f"applies [{type(op).__name__}] to a duration. A duration can only be "
            f"added to or subtracted from a date."
        )

    def evaluate_binary_operation(
        op: ast.operator,
        left: Fraction | _PendingDate | relativedelta,
        right: Fraction | _PendingDate | relativedelta,
    ) -> Fraction | _PendingDate | relativedelta:
        """Returns the result of applying op to left and right. Days and
        durations can only be added to or subtracted from a date, and a
        duration can only move a date."""
        if isinstance(right, relativedelta):
            if isinstance(left, _PendingDate) and isinstance(op, ast.Add):
                return move_by_duration(left, right)
            if isinstance(left, _PendingDate) and isinstance(op, ast.Sub):
                return move_by_duration(left, -right)
            raise misused_duration_error(op)
        if isinstance(left, relativedelta):
            if isinstance(right, _PendingDate) and isinstance(op, ast.Add):
                return move_by_duration(right, left)
            raise misused_duration_error(op)
        if isinstance(left, _PendingDate) and isinstance(right, Fraction):
            if isinstance(op, ast.Add):
                return left.plus_days(right)
            if isinstance(op, ast.Sub):
                return left.plus_days(-right)
        if isinstance(right, _PendingDate) and isinstance(left, Fraction):
            if isinstance(op, ast.Add):
                return right.plus_days(left)
        if isinstance(left, _PendingDate) or isinstance(right, _PendingDate):
            raise expression_error(
                f"applies [{type(op).__name__}] to [{left}] and [{right}]. Dates "
                f"only support adding or subtracting a number of days."
            )
        if type(op) not in _NUMERIC_BINARY_OPERATORS:
            raise expression_error(f"uses unsupported operator [{type(op).__name__}].")
        # Floor division of two fractions returns an int, so converting the
        # result back to a fraction keeps the rest of the expression exact.
        try:
            return Fraction(_NUMERIC_BINARY_OPERATORS[type(op)](left, right))
        except ZeroDivisionError as e:
            raise expression_error("divides by zero.") from e

    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as e:
        raise expression_error("is not valid syntax.") from e

    result = evaluate_node(tree)
    if isinstance(result, _PendingDate):
        try:
            resolved_date = result.resolve()
        except OverflowError as e:
            raise out_of_range_date_error(result) from e
        return resolved_date, tuple(used_variables.values())
    return result, tuple(used_variables.values())


def _expression_error(expression: str, rule_name: str, problem: str) -> ValueError:
    """Returns the error for an expression in a law regime rule that can't be
    evaluated, naming the expression and its rule so the YAML is easy to find.
    problem finishes the sentence, e.g. "divides by zero."."""
    return ValueError(f"Expression [{expression}] in rule [{rule_name}] {problem}")


def _to_fraction(value: int | float) -> Fraction:
    """Returns value as an exact fraction. A float becomes the decimal it prints
    as rather than its binary approximation, so 0.58 is exactly 58/100."""
    return Fraction(repr(value)) if isinstance(value, float) else Fraction(value)
