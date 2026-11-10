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
    AllOf,
    ConditionOperator,
    FieldCondition,
)
from recidiviz.sentence_calculator.models.inputs import Sentence, Term
from recidiviz.sentence_calculator.models.rules import (
    DateFloor,
    DateFormulaRule,
    LawRegime,
    ProjectedDateCondition,
)
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NevadaOffenseCategory,
    NevadaOffenseGroup,
    NevadaSentence,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue

_VIOLENT_SENTENCE = NevadaSentence(  # pylint: disable=abstract-class-instantiated
    sentence_id="S1",
    offense_id=9999,
    min_term=Term(years=1),
    max_term=Term(years=2),
    offense_category=NevadaOffenseCategory.CATEGORY_C,
    offense_group=NevadaOffenseGroup.VIOLENCE,
)

_EARLIEST_START_DATE = TrackedValue.input(date(2025, 1, 1), "earliest_start_date")
_ADJUSTED_MIN_DAYS = TrackedValue.input(474, "adjusted_min_days")
_ADJUSTED_MAX_DAYS = TrackedValue.input(1186, "adjusted_max_days")


def _context(
    *,
    unadjusted_max_days: int,
    unadjusted_max_years: int,
    sentences: list[Sentence],
) -> DateCalculationContext:
    return DateCalculationContext(
        earliest_start_date=_EARLIEST_START_DATE,
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
        sentences=sentences,
    )


def _date_rule(
    *,
    name: str,
    when: ProjectedDateCondition | None,
    formula: str,
    floor: DateFloor | None,
) -> DateFormulaRule:
    return DateFormulaRule(
        name=name,
        description=f"Computes {name}",
        when=when,
        formula=formula,
        floor=floor,
        statute=None,
    )


def _regime(projected_dates: dict[str, list[DateFormulaRule]]) -> LawRegime:
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


_PROJECTED_EXPIRATION_DATE_RULES = [
    _date_rule(
        name="projected_expiration_date",
        when=None,
        formula="earliest_start_date + adjusted_max_days",
        floor=None,
    )
]
# Applies only to an original max term of 3 years or more.
_MANDATORY_PAROLE_RELEASE_DATE_RULES = [
    _date_rule(
        name="mandatory_parole_release_date",
        when=ProjectedDateCondition(
            values_condition=FieldCondition(
                field="unadjusted_max_years",
                operator=ConditionOperator.GREATER_THAN_OR_EQUAL,
                value=3,
            ),
            sentence_condition=None,
        ),
        formula="projected_expiration_date - months(12)",
        floor=None,
    )
]


class TestEvaluateDateFormulas(unittest.TestCase):
    """Tests for evaluate_date_formulas."""

    def test_formula_using_another_date_is_evaluated_after_it(self) -> None:
        # Listed before the date it depends on.
        regime = _regime(
            {
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULES,
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULES,
            }
        )

        dates = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1826,
                unadjusted_max_years=5,
                sentences=[_VIOLENT_SENTENCE],
            ),
        )

        projected_expiration_date = TrackedValue(
            value=date(2028, 4, 1),
            label="projected_expiration_date",
            sources=(_EARLIEST_START_DATE, _ADJUSTED_MAX_DAYS),
            rule_name="projected_expiration_date",
            formula="earliest_start_date + adjusted_max_days",
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
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULES,
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULES,
            }
        )

        dates = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1094,
                unadjusted_max_years=2,
                sentences=[_VIOLENT_SENTENCE],
            ),
        )

        self.assertEqual(["projected_expiration_date"], list(dates))

    def test_formula_using_a_left_out_date_raises(self) -> None:
        regime = _regime(
            {
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULES,
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULES,
                "parole_review_date": [
                    _date_rule(
                        name="parole_review_date",
                        when=None,
                        formula="mandatory_parole_release_date - 30",
                        floor=None,
                    )
                ],
            }
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Projected date \[parole_review_date\] uses "
            r"\['mandatory_parole_release_date'\] in its formula, but those dates "
            r"were left out because none of their rules' conditions hold for this "
            r"calculation\. Give \[parole_review_date\] conditions that hold only "
            r"when theirs do, or stop using them in its formula\.$",
        ):
            evaluate_date_formulas(
                regime=regime,
                context=_context(
                    unadjusted_max_days=1094,
                    unadjusted_max_years=2,
                    sentences=[_VIOLENT_SENTENCE],
                ),
            )

    def test_date_using_a_left_out_date_with_the_same_conditions_is_left_out(
        self,
    ) -> None:
        regime = _regime(
            {
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULES,
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULES,
                "parole_review_date": [
                    _date_rule(
                        name="parole_review_date",
                        when=ProjectedDateCondition(
                            values_condition=FieldCondition(
                                field="unadjusted_max_years",
                                operator=ConditionOperator.GREATER_THAN_OR_EQUAL,
                                value=3,
                            ),
                            sentence_condition=None,
                        ),
                        formula="mandatory_parole_release_date - 30",
                        floor=None,
                    )
                ],
            }
        )

        dates = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1094,
                unadjusted_max_years=2,
                sentences=[_VIOLENT_SENTENCE],
            ),
        )

        self.assertEqual(["projected_expiration_date"], list(dates))

    def test_error_checking_conditions_names_the_date(self) -> None:
        """No calculation value can be missing today, so is_met is patched to
        raise the error a missing value would."""
        regime = _regime(
            {
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULES,
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULES,
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
            r"\[mandatory_parole_release_date\] entry "
            r"\[mandatory_parole_release_date\] against the calculation's values: "
            r"Condition compares field \[unadjusted_max_years\] with operator "
            r"\[greater_than_or_equal\], but the field has no value\.$",
        ):
            evaluate_date_formulas(
                regime=regime,
                context=_context(
                    unadjusted_max_days=1826,
                    unadjusted_max_years=5,
                    sentences=[_VIOLENT_SENTENCE],
                ),
            )

    def test_later_rule_applies_when_earlier_conditions_fail(self) -> None:
        regime = _regime(
            {
                "release_date": [
                    _date_rule(
                        name="nonviolent_release_date",
                        when=ProjectedDateCondition(
                            values_condition=None,
                            sentence_condition=FieldCondition(
                                field="offense_group",
                                operator=ConditionOperator.EQUALS,
                                value=NevadaOffenseGroup.OTHER,
                            ),
                        ),
                        formula="earliest_start_date + adjusted_min_days",
                        floor=None,
                    ),
                    _date_rule(
                        name="default_release_date",
                        when=None,
                        formula="earliest_start_date + adjusted_max_days",
                        floor=None,
                    ),
                ]
            }
        )

        # The context's only sentence is violent, so the nonviolent rule fails.
        release_date = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1825,
                unadjusted_max_years=4,
                sentences=[_VIOLENT_SENTENCE],
            ),
        )["release_date"]

        self.assertEqual("default_release_date", release_date.rule_name)
        self.assertEqual(date(2028, 4, 1), release_date.value)

    def test_floor_raises_earlier_date(self) -> None:
        regime = _regime(
            {
                "probable_release_date": [
                    _date_rule(
                        name="probable_release_date",
                        when=None,
                        formula="earliest_start_date + 100",
                        floor=DateFloor(date="parole_eligibility_date", when=None),
                    )
                ],
                "parole_eligibility_date": [
                    _date_rule(
                        name="parole_eligibility_date",
                        when=None,
                        formula="earliest_start_date + adjusted_min_days",
                        floor=None,
                    )
                ],
            }
        )

        dates = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1825,
                unadjusted_max_years=4,
                sentences=[_VIOLENT_SENTENCE],
            ),
        )

        parole_eligibility_date = TrackedValue(
            value=date(2026, 4, 20),
            label="parole_eligibility_date",
            sources=(_EARLIEST_START_DATE, _ADJUSTED_MIN_DAYS),
            rule_name="parole_eligibility_date",
            formula="earliest_start_date + adjusted_min_days",
        )
        self.assertEqual(
            {
                "parole_eligibility_date": parole_eligibility_date,
                # earliest_start_date + 474 days, rather than
                # earliest_start_date + 100 days.
                "probable_release_date": TrackedValue(
                    value=date(2026, 4, 20),
                    label="probable_release_date",
                    sources=(
                        TrackedValue(
                            value=date(2025, 4, 11),
                            label="probable_release_date.before_floor",
                            sources=(_EARLIEST_START_DATE,),
                            rule_name="probable_release_date",
                            formula="earliest_start_date + 100",
                        ),
                        parole_eligibility_date,
                    ),
                    rule_name="probable_release_date",
                    formula="max(earliest_start_date + 100, parole_eligibility_date)",
                ),
            },
            dates,
        )

    def test_floor_whose_conditions_fail_has_no_effect(self) -> None:
        regime = _regime(
            {
                "probable_release_date": [
                    _date_rule(
                        name="probable_release_date",
                        when=None,
                        formula="earliest_start_date + 100",
                        floor=DateFloor(
                            date="parole_eligibility_date",
                            when=ProjectedDateCondition(
                                values_condition=FieldCondition(
                                    field="unadjusted_max_days",
                                    operator=ConditionOperator.GREATER_THAN,
                                    value=1825,
                                ),
                                sentence_condition=None,
                            ),
                        ),
                    )
                ],
                "parole_eligibility_date": [
                    _date_rule(
                        name="parole_eligibility_date",
                        when=None,
                        formula="earliest_start_date + adjusted_min_days",
                        floor=None,
                    )
                ],
            }
        )

        probable_release_date = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1825,
                unadjusted_max_years=4,
                sentences=[_VIOLENT_SENTENCE],
            ),
        )["probable_release_date"]

        self.assertEqual(date(2025, 4, 11), probable_release_date.value)

    def test_date_name_inside_another_name_is_not_a_dependency(self) -> None:
        """base_parole_eligibility_date contains parole_eligibility_date as a
        substring, but referencing one must not make the other a dependency."""
        regime = _regime(
            {
                "parole_eligibility_date": [
                    _date_rule(
                        name="parole_eligibility_date",
                        when=None,
                        formula="base_parole_eligibility_date + 10",
                        floor=None,
                    )
                ],
                "base_parole_eligibility_date": [
                    _date_rule(
                        name="base_parole_eligibility_date",
                        when=None,
                        formula="earliest_start_date + adjusted_min_days",
                        floor=None,
                    )
                ],
            }
        )

        dates = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1826,
                unadjusted_max_years=5,
                sentences=[_VIOLENT_SENTENCE],
            ),
        )

        self.assertEqual(date(2026, 4, 20), dates["base_parole_eligibility_date"].value)
        self.assertEqual(date(2026, 4, 30), dates["parole_eligibility_date"].value)

    def test_floor_below_formula_date_stays_in_derivation(self) -> None:
        regime = _regime(
            {
                "probable_release_date": [
                    _date_rule(
                        name="probable_release_date",
                        when=None,
                        formula="earliest_start_date + adjusted_min_days",
                        floor=DateFloor(date="parole_eligibility_date", when=None),
                    )
                ],
                "parole_eligibility_date": [
                    _date_rule(
                        name="parole_eligibility_date",
                        when=None,
                        formula="earliest_start_date + 100",
                        floor=None,
                    )
                ],
            }
        )

        probable_release_date = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1825,
                unadjusted_max_years=4,
                sentences=[_VIOLENT_SENTENCE],
            ),
        )["probable_release_date"]

        # earliest_start_date + 474 days is already after
        # earliest_start_date + 100 days, so the date doesn't move, but the floor
        # stays a source of it.
        self.assertEqual(
            TrackedValue(
                value=date(2026, 4, 20),
                label="probable_release_date",
                sources=(
                    TrackedValue(
                        value=date(2026, 4, 20),
                        label="probable_release_date.before_floor",
                        sources=(_EARLIEST_START_DATE, _ADJUSTED_MIN_DAYS),
                        rule_name="probable_release_date",
                        formula="earliest_start_date + adjusted_min_days",
                    ),
                    TrackedValue(
                        value=date(2025, 4, 11),
                        label="parole_eligibility_date",
                        sources=(_EARLIEST_START_DATE,),
                        rule_name="parole_eligibility_date",
                        formula="earliest_start_date + 100",
                    ),
                ),
                rule_name="probable_release_date",
                formula=(
                    "max(earliest_start_date + adjusted_min_days, "
                    "parole_eligibility_date)"
                ),
            ),
            probable_release_date,
        )

    def test_floor_on_left_out_date_raises(self) -> None:
        regime = _regime(
            {
                "projected_expiration_date": _PROJECTED_EXPIRATION_DATE_RULES,
                "mandatory_parole_release_date": _MANDATORY_PAROLE_RELEASE_DATE_RULES,
                "probable_release_date": [
                    _date_rule(
                        name="probable_release_date",
                        when=None,
                        formula="earliest_start_date + 100",
                        floor=DateFloor(
                            date="mandatory_parole_release_date", when=None
                        ),
                    )
                ],
            }
        )

        # An unadjusted max of 2 years leaves out mandatory_parole_release_date.
        with self.assertRaisesRegex(
            ValueError,
            r"^Projected date \[probable_release_date\] has a floor on "
            r"\[mandatory_parole_release_date\] in rule \[probable_release_date\], "
            r"but \[mandatory_parole_release_date\] was left out because none of "
            r"its rules' conditions hold for this calculation\. Give the floor "
            r"conditions that hold only when \[mandatory_parole_release_date\]'s "
            r"do, or remove the floor\.$",
        ):
            evaluate_date_formulas(
                regime=regime,
                context=_context(
                    unadjusted_max_days=1094,
                    unadjusted_max_years=2,
                    sentences=[_VIOLENT_SENTENCE],
                ),
            )

    def test_floor_with_sentence_condition_applies_when_a_sentence_meets_it(
        self,
    ) -> None:
        regime = _regime(
            {
                "probable_release_date": [
                    _date_rule(
                        name="probable_release_date",
                        when=None,
                        formula="earliest_start_date + 100",
                        floor=DateFloor(
                            date="parole_eligibility_date",
                            when=ProjectedDateCondition(
                                values_condition=None,
                                sentence_condition=FieldCondition(
                                    field="offense_group",
                                    operator=ConditionOperator.EQUALS,
                                    value=NevadaOffenseGroup.VIOLENCE,
                                ),
                            ),
                        ),
                    )
                ],
                "parole_eligibility_date": [
                    _date_rule(
                        name="parole_eligibility_date",
                        when=None,
                        formula="earliest_start_date + adjusted_min_days",
                        floor=None,
                    )
                ],
            }
        )

        probable_release_date = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1825,
                unadjusted_max_years=4,
                sentences=[_VIOLENT_SENTENCE],
            ),
        )["probable_release_date"]

        # The path's violent sentence meets the floor's condition, so the date
        # moves up from earliest_start_date + 100 days to
        # earliest_start_date + 474 days.
        self.assertEqual(date(2026, 4, 20), probable_release_date.value)

    def test_rule_needs_both_value_and_sentence_conditions(self) -> None:
        regime = _regime(
            {
                "release_date": [
                    _date_rule(
                        name="long_violent_release_date",
                        when=ProjectedDateCondition(
                            values_condition=FieldCondition(
                                field="unadjusted_max_days",
                                operator=ConditionOperator.GREATER_THAN,
                                value=1825,
                            ),
                            sentence_condition=FieldCondition(
                                field="offense_group",
                                operator=ConditionOperator.EQUALS,
                                value=NevadaOffenseGroup.VIOLENCE,
                            ),
                        ),
                        formula="earliest_start_date + adjusted_min_days",
                        floor=None,
                    ),
                    _date_rule(
                        name="default_release_date",
                        when=None,
                        formula="earliest_start_date + adjusted_max_days",
                        floor=None,
                    ),
                ]
            }
        )

        # The path's sentence is violent, but unadjusted_max_days isn't over
        # 1825, so the first rule doesn't apply.
        release_date = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1825,
                unadjusted_max_years=4,
                sentences=[_VIOLENT_SENTENCE],
            ),
        )["release_date"]

        self.assertEqual("default_release_date", release_date.rule_name)

    def test_sentence_conditions_must_hold_for_the_same_sentence(self) -> None:
        """S1 is violent but not category A, and S2 is category A but not violent,
        so no single sentence satisfies both conditions."""
        category_a = NevadaSentence(  # pylint: disable=abstract-class-instantiated
            sentence_id="S2",
            offense_id=9999,
            min_term=Term(years=1),
            max_term=Term(years=2),
            offense_category=NevadaOffenseCategory.CATEGORY_A,
            offense_group=NevadaOffenseGroup.OTHER,
        )
        regime = _regime(
            {
                "release_date": [
                    _date_rule(
                        name="violent_category_a_release_date",
                        when=ProjectedDateCondition(
                            values_condition=None,
                            sentence_condition=AllOf(
                                conditions=[
                                    FieldCondition(
                                        field="offense_category",
                                        operator=ConditionOperator.EQUALS,
                                        value=NevadaOffenseCategory.CATEGORY_A,
                                    ),
                                    FieldCondition(
                                        field="offense_group",
                                        operator=ConditionOperator.EQUALS,
                                        value=NevadaOffenseGroup.VIOLENCE,
                                    ),
                                ]
                            ),
                        ),
                        formula="earliest_start_date + adjusted_min_days",
                        floor=None,
                    ),
                    _date_rule(
                        name="default_release_date",
                        when=None,
                        formula="earliest_start_date + adjusted_max_days",
                        floor=None,
                    ),
                ]
            }
        )

        release_date = evaluate_date_formulas(
            regime=regime,
            context=_context(
                unadjusted_max_days=1825,
                unadjusted_max_years=4,
                sentences=[_VIOLENT_SENTENCE, category_a],
            ),
        )["release_date"]

        self.assertEqual("default_release_date", release_date.rule_name)


if __name__ == "__main__":
    unittest.main()
