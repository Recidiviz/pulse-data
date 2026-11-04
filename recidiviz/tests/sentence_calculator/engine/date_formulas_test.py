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
"""Tests for recidiviz/sentence_calculator/engine/date_formulas.py."""

import unittest
from datetime import date
from unittest import mock

from recidiviz.sentence_calculator.engine.date_formulas import (
    DateCalculationContext,
    evaluate_date_formulas,
)
from recidiviz.sentence_calculator.models.conditions import (
    Condition,
    ConditionOperator,
    FieldCondition,
)
from recidiviz.sentence_calculator.models.rules import (
    PROJECTED_DATE_VALUE_TYPES,
    DateFormulaRule,
    LawRegime,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue

_START_DATE = TrackedValue.input(date(2025, 1, 1), "start_date")
_ADJUSTED_MIN_DAYS = TrackedValue.input(474, "adjusted_min_days")
_ADJUSTED_MAX_DAYS = TrackedValue.input(1186, "adjusted_max_days")


def _context(
    *, unadjusted_max_days: int, unadjusted_max_years: int
) -> DateCalculationContext:
    return DateCalculationContext(
        start_date=_START_DATE,
        unadjusted_min_days=TrackedValue.input(730, "unadjusted_min_days"),
        unadjusted_max_days=TrackedValue.input(
            unadjusted_max_days, "unadjusted_max_days"
        ),
        unadjusted_max_years=TrackedValue.input(
            unadjusted_max_years, "unadjusted_max_years"
        ),
        adjusted_min_days=_ADJUSTED_MIN_DAYS,
        adjusted_max_days=_ADJUSTED_MAX_DAYS,
        jail_credit_days=TrackedValue.input(0, "jail_credit_days"),
        good_time_days=TrackedValue.input(639, "good_time_days"),
        max_ped=TrackedValue.input(date(2026, 4, 19), "max_ped"),
    )


def _date_rule(*, name: str, when: Condition | None, formula: str) -> DateFormulaRule:
    return DateFormulaRule(
        name=name,
        description=f"Computes {name}",
        when=when,
        formula=formula,
        statute=None,
    )


def _regime(projected_dates: dict[str, DateFormulaRule]) -> LawRegime:
    return LawRegime(
        name="TEST_REGIME",
        description="Test regime",
        effective_date=date(2020, 1, 1),
        use_calendar_term_conversion=False,
        days_per_month=30.0,
        days_per_year=365.0,
        term_multipliers=[],
        credit_rates=[],
        credit_caps=[],
        projected_dates=projected_dates,
    )


_PROJECTED_EXPIRATION_DATE_RULE = _date_rule(
    name="projected_expiration_date",
    when=None,
    formula="start_date + adjusted_max_days",
)
# Applies only to an original max term of 3 years or more.
_MANDATORY_PAROLE_RELEASE_DATE_RULE = _date_rule(
    name="mandatory_parole_release_date",
    when=FieldCondition(
        field="unadjusted_max_years",
        operator=ConditionOperator.GREATER_THAN_OR_EQUAL,
        value=3,
    ),
    formula="projected_expiration_date - months(12)",
)


class TestDateCalculationContext(unittest.TestCase):
    """Tests for DateCalculationContext."""

    def test_values_match_the_names_regimes_are_checked_against(self) -> None:
        context = _context(unadjusted_max_days=1826, unadjusted_max_years=5)

        self.assertEqual(
            PROJECTED_DATE_VALUE_TYPES,
            {name: type(value) for name, value in context.condition_values().items()},
        )


class TestEvaluateDateFormulas(unittest.TestCase):
    """Tests for evaluate_date_formulas."""

    def test_formula_using_another_date_is_evaluated_after_it(self) -> None:
        # Listed before the date it depends on.
        regime = _regime(
            {
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULE,
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULE,
            }
        )

        dates = evaluate_date_formulas(
            regime=regime,
            context=_context(unadjusted_max_days=1826, unadjusted_max_years=5),
        )

        projected_expiration_date = TrackedValue(
            value=date(2028, 4, 1),
            label="projected_expiration_date",
            sources=(_START_DATE, _ADJUSTED_MAX_DAYS),
            rule_name="projected_expiration_date",
            formula="start_date + adjusted_max_days",
        )
        self.assertEqual(
            {
                "projected_expiration_date": projected_expiration_date,
                "mandatory_parole_release_date": TrackedValue(
                    value=date(2027, 4, 1),
                    label="mandatory_parole_release_date",
                    sources=(projected_expiration_date,),
                    rule_name="mandatory_parole_release_date",
                    formula="projected_expiration_date - months(12)",
                ),
            },
            dates,
        )

    def test_date_whose_conditions_fail_is_left_out(self) -> None:
        regime = _regime(
            {
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULE,
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULE,
            }
        )

        dates = evaluate_date_formulas(
            regime=regime,
            context=_context(unadjusted_max_days=1094, unadjusted_max_years=2),
        )

        self.assertEqual(["projected_expiration_date"], list(dates))

    def test_formula_using_a_left_out_date_raises(self) -> None:
        regime = _regime(
            {
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULE,
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULE,
                "parole_review_date": _date_rule(
                    name="parole_review_date",
                    when=None,
                    formula="mandatory_parole_release_date - 30",
                ),
            }
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Projected date \[parole_review_date\] uses "
            r"\['mandatory_parole_release_date'\] in its formula, but those dates "
            r"were left out because their conditions don't hold for this "
            r"calculation\. Give \[parole_review_date\] conditions that hold only "
            r"when theirs do, or stop using them in its formula\.$",
        ):
            evaluate_date_formulas(
                regime=regime,
                context=_context(unadjusted_max_days=1094, unadjusted_max_years=2),
            )

    def test_date_using_a_left_out_date_with_the_same_conditions_is_left_out(
        self,
    ) -> None:
        regime = _regime(
            {
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULE,
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULE,
                "parole_review_date": _date_rule(
                    name="parole_review_date",
                    when=FieldCondition(
                        field="unadjusted_max_years",
                        operator=ConditionOperator.GREATER_THAN_OR_EQUAL,
                        value=3,
                    ),
                    formula="mandatory_parole_release_date - 30",
                ),
            }
        )

        dates = evaluate_date_formulas(
            regime=regime,
            context=_context(unadjusted_max_days=1094, unadjusted_max_years=2),
        )

        self.assertEqual(["projected_expiration_date"], list(dates))

    def test_error_checking_conditions_names_the_date(self) -> None:
        """No calculation value can be missing today, so is_met is patched to
        raise the error a missing value would."""
        regime = _regime(
            {
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULE,
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULE,
            }
        )

        with mock.patch.object(
            FieldCondition,
            "is_met",
            side_effect=ValueError(
                "Condition compares field [unadjusted_max_years] with operator "
                "[greater_than_or_equal], but the field has no value."
            ),
        ), self.assertRaisesRegex(
            ValueError,
            r"^Could not check the conditions in projected date "
            r"\[mandatory_parole_release_date\] against the calculation's values: "
            r"Condition compares field \[unadjusted_max_years\] with operator "
            r"\[greater_than_or_equal\], but the field has no value\.$",
        ):
            evaluate_date_formulas(
                regime=regime,
                context=_context(unadjusted_max_days=1826, unadjusted_max_years=5),
            )

    def test_date_name_inside_another_name_is_not_a_dependency(self) -> None:
        """base_parole_eligibility_date contains parole_eligibility_date as a
        substring, but referencing one must not make the other a dependency."""
        regime = _regime(
            {
                "parole_eligibility_date": _date_rule(
                    name="parole_eligibility_date",
                    when=None,
                    formula="base_parole_eligibility_date + 10",
                ),
                "base_parole_eligibility_date": _date_rule(
                    name="base_parole_eligibility_date",
                    when=None,
                    formula="start_date + adjusted_min_days",
                ),
            }
        )

        dates = evaluate_date_formulas(
            regime=regime,
            context=_context(unadjusted_max_days=1826, unadjusted_max_years=5),
        )

        self.assertEqual(date(2026, 4, 20), dates["base_parole_eligibility_date"].value)
        self.assertEqual(date(2026, 4, 30), dates["parole_eligibility_date"].value)


if __name__ == "__main__":
    unittest.main()
