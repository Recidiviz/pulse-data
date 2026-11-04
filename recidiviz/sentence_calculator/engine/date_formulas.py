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
"""Computes the projected dates a law regime's projected_dates rules define.

Each regime defines some projected dates, like expiration date (release), first
parole eligibility, or mandatory parole release. This class evaluates the
formula to produce a calendar date for each projected date type that applies.

A formula may reference the values on DateCalculationContext and any other
projected date; dates are evaluated so that every date is computed before the
formulas that use it. A formula that uses a date whose conditions don't hold
raises, since that date has no value for the calculation.
"""

from datetime import date

import attr

from recidiviz.sentence_calculator.engine.expression import evaluate_date_expression
from recidiviz.sentence_calculator.models.conditions import Condition
from recidiviz.sentence_calculator.models.rules import (
    LawRegime,
    expression_variable_names,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue


@attr.define(frozen=True, kw_only=True)
class DateCalculationContext:
    """The values projected date rules can reference, aggregated over the
    sentences on the controlling path. Each attribute's name is the name
    formulas and conditions use for it, and the names and types match
    PROJECTED_DATE_VALUE_TYPES, which regimes are checked against when they
    load."""

    start_date: TrackedValue[date] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Date the first sentence on the controlling path starts."""

    unadjusted_min_days: TrackedValue[int] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Sum of the original min terms on the controlling path, minus any overlap."""

    unadjusted_max_days: TrackedValue[int] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Sum of the original max terms on the controlling path, minus any overlap."""

    unadjusted_max_years: TrackedValue[int] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Whole years in unadjusted_max_days, counted the way the regime converts
    terms to days."""

    adjusted_min_days: TrackedValue[int] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Sum of the adjusted min terms on the controlling path, minus any overlap."""

    adjusted_max_days: TrackedValue[int] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Sum of the adjusted max terms on the controlling path, minus any overlap."""

    jail_credit_days: TrackedValue[int] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Sum of the jail credit on the controlling path."""

    good_time_days: TrackedValue[int] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Days the regime's reduction took off the controlling path's max terms."""

    def formula_variables(self) -> dict[str, TrackedValue[int | float | date]]:
        """Returns the aggregated values keyed by the names formulas use."""
        return {
            "start_date": self.start_date,
            "unadjusted_min_days": self.unadjusted_min_days,
            "unadjusted_max_days": self.unadjusted_max_days,
            "unadjusted_max_years": self.unadjusted_max_years,
            "adjusted_min_days": self.adjusted_min_days,
            "adjusted_max_days": self.adjusted_max_days,
            "jail_credit_days": self.jail_credit_days,
            "good_time_days": self.good_time_days,
        }

    def condition_values(self) -> dict[str, int | float | date]:
        """Returns the raw aggregated values keyed by the names conditions use."""
        return {name: v.value for name, v in self.formula_variables().items()}


def _projected_date_conditions_hold(
    *,
    date_name: str,
    condition: Condition,
    condition_values: dict[str, int | float | date],
) -> bool:
    """Returns whether 'condition', the conditions on the projected date
    date_name, holds for condition_values. Any error is raised again with
    date_name in its message."""
    try:
        return condition.is_met(condition_values.__getitem__)
    except ValueError as e:
        raise ValueError(
            f"Could not check the conditions in projected date [{date_name}] "
            f"against the calculation's values: {e}"
        ) from e


def evaluate_date_formulas(
    *, regime: LawRegime, context: DateCalculationContext
) -> dict[str, TrackedValue[date]]:
    """Returns each of the regime's projected dates. A date is left out when its
    rule's conditions on calculation values (e.g. unadjusted_max_days) don't hold,
    and a date with a formula using a different date that was dropped (e.g. because
    its conditions didn't hold) raises."""
    condition_values = context.condition_values()
    variables: dict[str, TrackedValue[int | float | date]] = context.formula_variables()
    calculated_dates: dict[str, TrackedValue[date]] = {}
    skipped_date_names: set[str] = set()
    for date_name in regime.projected_date_evaluation_order:
        rule = regime.projected_dates[date_name]
        if rule.when is not None and not _projected_date_conditions_hold(
            date_name=date_name, condition=rule.when, condition_values=condition_values
        ):
            skipped_date_names.add(date_name)
            continue
        used_skipped_date_names = skipped_date_names & expression_variable_names(
            expression=rule.formula, rule_name=rule.name
        )
        if used_skipped_date_names:
            raise ValueError(
                f"Projected date [{date_name}] uses {sorted(used_skipped_date_names)} "
                f"in its formula, but those dates were left out because their "
                f"conditions don't hold for this calculation. Give [{date_name}] "
                f"conditions that hold only when theirs do, or stop using them in "
                f"its formula."
            )
        tracked_date = evaluate_date_expression(
            expression=rule.formula,
            variables=variables,
            label=date_name,
            rule_name=rule.name,
        )
        calculated_dates[date_name] = tracked_date
        variables[date_name] = tracked_date
    return calculated_dates
