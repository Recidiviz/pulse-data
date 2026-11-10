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
formulas and floors that use it. A formula or floor that uses a date whose
conditions don't hold raises, since that date has no value for the calculation.
"""

from datetime import date

import attr

from recidiviz.common import attr_validators
from recidiviz.sentence_calculator.engine.expression import evaluate_date_expression
from recidiviz.sentence_calculator.engine.rule_engine import sentence_meets_condition
from recidiviz.sentence_calculator.models.conditions import (
    EARLIEST_START_DATE_VALUE,
    Condition,
)
from recidiviz.sentence_calculator.models.inputs import Sentence
from recidiviz.sentence_calculator.models.rules import (
    DateFormulaRule,
    LawRegime,
    ProjectedDateCondition,
    expression_variable_names,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue
from recidiviz.utils.types import assert_type


@attr.define(frozen=True, kw_only=True)
class DateCalculationContext:
    """The values projected date rules can reference, aggregated over the
    sentences on the controlling path, and the path's sentences themselves.
    Every attribute except sentences is a value that formulas and conditions use
    by the attribute's name, with the name and type PROJECTED_DATE_VALUE_TYPES
    gives it, which regimes are checked against when they load. sentences is
    what each rule's sentence_condition is checked against."""

    earliest_start_date: TrackedValue[date] = attr.ib(
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
    terms to days. With calendar conversion they count from the RETRO date, where
    the terms start counting."""

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

    max_ped: TrackedValue[date] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Latest date any sentence, controlling or not, becomes eligible to go before
    the parole board. Unlike the values above, it isn't limited to the controlling
    path, since a sentence that never controls the max can have the latest PED."""

    sentences: list[Sentence] = attr.ib(
        validator=attr_validators.is_list_of(Sentence)  # type: ignore[type-abstract]
    )
    """The sentences on the controlling path, which each rule's sentence_condition
    is checked against."""

    def formula_variables(self) -> dict[str, TrackedValue[int | float | date]]:
        """Returns the aggregated values keyed by the names formulas use."""
        return {
            EARLIEST_START_DATE_VALUE: self.earliest_start_date,
            "unadjusted_min_days": self.unadjusted_min_days,
            "unadjusted_max_days": self.unadjusted_max_days,
            "unadjusted_max_years": self.unadjusted_max_years,
            "adjusted_min_days": self.adjusted_min_days,
            "adjusted_max_days": self.adjusted_max_days,
            "jail_credit_days": self.jail_credit_days,
            "good_time_days": self.good_time_days,
            "max_ped": self.max_ped,
        }

    def condition_values(self) -> dict[str, int | float | date]:
        """Returns the raw aggregated values keyed by the names conditions use."""
        return {name: v.value for name, v in self.formula_variables().items()}


def _projected_date_conditions_hold(
    *,
    location: str,
    condition: Condition,
    condition_values: dict[str, int | float | date],
) -> bool:
    """Returns whether 'condition', a condition on calculation values, holds for
    condition_values. An error checking condition is raised again with the location
    where the condition appears (e.g. projected date [parole_eligibility_date] entry
    [parole_eligibility_date]), in its message."""
    try:
        return condition.is_met(condition_values.__getitem__)
    except ValueError as e:
        raise ValueError(
            f"Could not check the conditions in {location} against the "
            f"calculation's values: {e}"
        ) from e


def evaluate_date_formulas(
    *, regime: LawRegime, context: DateCalculationContext
) -> dict[str, TrackedValue[date]]:
    """Returns each of the regime's projected dates. A date is left out when
    none of its rules' conditions hold. If a date uses an omitted date in its
    formula (or floor, when the floor's conditions hold), this raises.

    Each date is computed with the first rule whose conditions hold. A rule's
    conditions on calculation values (e.g. unadjusted_max_days) must all hold,
    and if it has a sentence_condition at least one sentence on the controlling
    path must meet it.
    """
    rules_by_date = regime.projected_dates
    variables: dict[str, TrackedValue[int | float | date]] = context.formula_variables()
    calculated_dates: dict[str, TrackedValue[date]] = {}
    skipped_date_names: set[str] = set()
    for date_name in regime.projected_date_evaluation_order:
        matching_rule = next(
            (
                rule
                for rule in rules_by_date[date_name]
                if _condition_holds(
                    rule.when,
                    context=context,
                    location=_rule_location(date_name=date_name, rule=rule),
                )
            ),
            None,
        )
        if matching_rule is None:
            skipped_date_names.add(date_name)
            continue
        used_skipped_date_names = skipped_date_names & expression_variable_names(
            expression=matching_rule.formula, rule_name=matching_rule.name
        )
        if used_skipped_date_names:
            raise ValueError(
                f"Projected date [{date_name}] uses {sorted(used_skipped_date_names)} "
                f"in its formula, but those dates were left out because none of "
                f"their rules' conditions hold for this calculation. Give "
                f"[{date_name}] conditions that hold only when theirs do, or stop "
                f"using them in its formula."
            )
        tracked_date = _evaluate_rule(
            date_name=date_name,
            rule=matching_rule,
            context=context,
            variables=variables,
            skipped_date_names=skipped_date_names,
        )
        calculated_dates[date_name] = tracked_date
        variables[date_name] = tracked_date
    return calculated_dates


def _rule_location(*, date_name: str, rule: DateFormulaRule) -> str:
    """Returns where the rule appears in its regime. Used for error messages."""
    return f"projected date [{date_name}] entry [{rule.name}]"


def _condition_holds(
    condition: ProjectedDateCondition | None,
    *,
    context: DateCalculationContext,
    location: str,
) -> bool:
    """Returns whether the condition holds for the calculation, which it always does
    when 'condition' is None. location names where the condition appears for use in
    error messages."""
    if condition is None:
        return True
    if condition.values_condition is not None and not _projected_date_conditions_hold(
        location=location,
        condition=condition.values_condition,
        condition_values=context.condition_values(),
    ):
        return False
    if condition.sentence_condition is None:
        return True
    return any(
        sentence_meets_condition(
            condition.sentence_condition, sentence, location=location
        )
        for sentence in context.sentences
    )


def _evaluate_rule(
    *,
    date_name: str,
    rule: DateFormulaRule,
    context: DateCalculationContext,
    variables: dict[str, TrackedValue[int | float | date]],
    skipped_date_names: set[str],
) -> TrackedValue[date]:
    """Returns the date 'rule' computes for date_name. This is the date given
    by its formula unless it has a valid floor condition. In that case, this
    returns the later of the floored date and the date from the formula.

    A floor that applies is always a source of the result, even when the
    formula's date is already later, so the derivation tree shows that the floor
    was checked. The formula's date is then labeled <date_name>.before_floor.

    Args:
        date_name: The projected date rule computes, which labels the result.
        rule: The rule that matched for date_name.
        context: The calculation's values and controlling path sentences.
        variables: The values and already computed projected dates the formula
            and floor can use.
        skipped_date_names: Projected dates left out of this calculation because
            none of their rules' conditions held.
    """
    floor = rule.floor
    location = _rule_location(date_name=date_name, rule=rule)
    if floor is None or not _condition_holds(
        floor.when, context=context, location=f"the floor of {location}"
    ):
        return evaluate_date_expression(
            expression=rule.formula,
            variables=variables,
            label=date_name,
            rule_name=rule.name,
        )
    if floor.date in skipped_date_names:
        raise ValueError(
            f"Projected date [{date_name}] has a floor on [{floor.date}] in rule "
            f"[{rule.name}], but [{floor.date}] was left out because none of its "
            f"rules' conditions hold for this calculation. Give the floor "
            f"conditions that hold only when [{floor.date}]'s do, or remove the "
            f"floor."
        )
    formula_date = evaluate_date_expression(
        expression=rule.formula,
        variables=variables,
        label=f"{date_name}.before_floor",
        rule_name=rule.name,
    )
    floor_date = variables[floor.date]
    later_date = max(formula_date.value, assert_type(floor_date.value, date))
    return TrackedValue(
        value=later_date,
        label=date_name,
        sources=(formula_date, floor_date),
        rule_name=rule.name,
        formula=f"max({rule.formula}, {floor.date})",
    )
