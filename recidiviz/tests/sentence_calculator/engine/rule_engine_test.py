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
"""Tests for the sentence-level rule lookups in RuleEngine."""

import unittest
from datetime import date

from recidiviz.sentence_calculator.engine.rule_engine import (
    RuleEngine,
    sentence_meets_condition,
)
from recidiviz.sentence_calculator.models.conditions import (
    AllOf,
    AnyOf,
    ConditionOperator,
    FieldCondition,
)
from recidiviz.sentence_calculator.models.inputs import Term
from recidiviz.sentence_calculator.models.outputs import (
    CreditCap,
    ProjectedCreditRate,
    TermMultiplier,
)
from recidiviz.sentence_calculator.models.rules import (
    CreditCapRule,
    CreditRateRule,
    LawRegime,
    ReductionTarget,
    TermMultiplierRule,
    TermType,
)
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NevadaOffenseCategory,
    NevadaOffenseGroup,
    NevadaSentence,
)
from recidiviz.sentence_calculator.models.tracked import TrackedValue

_VIOLENT_SENTENCE = NevadaSentence(  # pylint: disable=abstract-class-instantiated
    sentence_id="S1",
    min_term=Term(years=1),
    max_term=Term(years=3),
    offense_category=NevadaOffenseCategory.CATEGORY_C,
    offense_group=NevadaOffenseGroup.VIOLENCE,
)
_NONVIOLENT_SENTENCE = NevadaSentence(  # pylint: disable=abstract-class-instantiated
    sentence_id="S2",
    min_term=Term(years=1),
    max_term=Term(years=3),
    offense_category=NevadaOffenseCategory.CATEGORY_C,
    offense_group=NevadaOffenseGroup.OTHER,
)

_IS_VIOLENT = FieldCondition(
    field="offense_group",
    operator=ConditionOperator.EQUALS,
    value=NevadaOffenseGroup.VIOLENCE,
)
_IS_NOT_VIOLENT = FieldCondition(
    field="offense_group",
    operator=ConditionOperator.NOT_EQUALS,
    value=NevadaOffenseGroup.VIOLENCE,
)
# The test sentences have no start_date, so checking this condition raises.
_STARTS_BEFORE_2014 = FieldCondition(
    field="start_date",
    operator=ConditionOperator.BEFORE,
    value=date(2014, 7, 1),
)
_VIOLENT_TERM_REDUCTION = TermMultiplierRule(
    name="violent_term_reduction",
    description="15% reduction of violent offenses' terms",
    when=_IS_VIOLENT,
    multiplier=0.85,
    applies_to=[
        ReductionTarget(term=TermType.MAX_TERM, when=None),
        ReductionTarget(term=TermType.MIN_TERM, when=None),
    ],
    statute=None,
)
_STANDARD_TERM_REDUCTION = TermMultiplierRule(
    name="standard_term_reduction",
    description=(
        "35% reduction of every other offense's max term, and of its min term "
        "unless it is violent"
    ),
    when=None,
    multiplier=0.65,
    applies_to=[
        ReductionTarget(term=TermType.MAX_TERM, when=None),
        ReductionTarget(term=TermType.MIN_TERM, when=_IS_NOT_VIOLENT),
    ],
    statute=None,
)
_STAT_TIME = CreditRateRule(
    name="stat_time",
    description=(
        "Stat time off every offense's max term, and off its min term unless it "
        "is violent"
    ),
    when=None,
    rate=20.0,
    applies_to=[
        ReductionTarget(term=TermType.MAX_TERM, when=None),
        ReductionTarget(term=TermType.MIN_TERM, when=_IS_NOT_VIOLENT),
    ],
    statute="Test Statute 1",
)


def _engine(
    *,
    term_multipliers: list[TermMultiplierRule],
    credit_rates: list[CreditRateRule],
    credit_caps: list[CreditCapRule],
) -> RuleEngine:
    return RuleEngine(
        LawRegime(
            name="TEST_REGIME",
            description="Test regime",
            effective_date=date(2020, 1, 1),
            use_calendar_term_conversion=False,
            days_per_month=30.0,
            days_per_year=365.0,
            term_multipliers=term_multipliers,
            credit_rates=credit_rates,
            credit_caps=credit_caps,
            projected_dates={},
        )
    )


class TestGetTermMultiplier(unittest.TestCase):
    """Tests for RuleEngine.get_term_multiplier."""

    def test_unmet_min_term_condition_keeps_reduction_off_min_term(self) -> None:
        engine = _engine(
            term_multipliers=[_STANDARD_TERM_REDUCTION], credit_rates=[], credit_caps=[]
        )

        self.assertEqual(
            TermMultiplier(
                multiplier=TrackedValue(
                    value=0.65,
                    label="S1.term_multiplier",
                    rule_name="standard_term_reduction",
                    formula=(
                        "multiplier=0.65 because standard_term_reduction applies to "
                        "every sentence; doesn't reduce min_term because applies_to "
                        "min_term doesn't match offense_group=VIOLENCE; "
                        "reduces max_term because applies_to lists max_term with "
                        "no conditions"
                    ),
                ),
                reduced_terms=frozenset({TermType.MAX_TERM}),
            ),
            engine.get_term_multiplier(_VIOLENT_SENTENCE),
        )

    def test_met_min_term_condition_reduces_both_terms(self) -> None:
        engine = _engine(
            term_multipliers=[_STANDARD_TERM_REDUCTION], credit_rates=[], credit_caps=[]
        )

        self.assertEqual(
            TermMultiplier(
                multiplier=TrackedValue(
                    value=0.65,
                    label="S2.term_multiplier",
                    rule_name="standard_term_reduction",
                    formula=(
                        "multiplier=0.65 because standard_term_reduction applies to "
                        "every sentence; reduces min_term because applies_to "
                        "min_term matches offense_group=OTHER; reduces max_term "
                        "because applies_to lists max_term with no conditions"
                    ),
                ),
                reduced_terms=frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            ),
            engine.get_term_multiplier(_NONVIOLENT_SENTENCE),
        )

    def test_first_matching_rule_applies(self) -> None:
        engine = _engine(
            term_multipliers=[_VIOLENT_TERM_REDUCTION, _STANDARD_TERM_REDUCTION],
            credit_rates=[],
            credit_caps=[],
        )

        self.assertEqual(
            TermMultiplier(
                multiplier=TrackedValue(
                    value=0.85,
                    label="S1.term_multiplier",
                    rule_name="violent_term_reduction",
                    formula=(
                        "multiplier=0.85 because violent_term_reduction matches "
                        "offense_group=VIOLENCE; reduces min_term because "
                        "applies_to lists min_term with no conditions; "
                        "reduces max_term because applies_to lists max_term with "
                        "no conditions"
                    ),
                ),
                reduced_terms=frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            ),
            engine.get_term_multiplier(_VIOLENT_SENTENCE),
        )
        self.assertEqual(
            TermMultiplier(
                multiplier=TrackedValue(
                    value=0.65,
                    label="S2.term_multiplier",
                    rule_name="standard_term_reduction",
                    formula=(
                        "multiplier=0.65 because standard_term_reduction applies to "
                        "every sentence; reduces min_term because applies_to "
                        "min_term matches offense_group=OTHER; reduces max_term "
                        "because applies_to lists max_term with no conditions"
                    ),
                ),
                reduced_terms=frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            ),
            engine.get_term_multiplier(_NONVIOLENT_SENTENCE),
        )

    def test_term_missing_from_applies_to_is_not_reduced(self) -> None:
        engine = _engine(
            term_multipliers=[
                TermMultiplierRule(
                    name="min_term_reduction",
                    description="35% reduction of every offense's min term",
                    when=None,
                    multiplier=0.65,
                    applies_to=[ReductionTarget(term=TermType.MIN_TERM, when=None)],
                    statute=None,
                )
            ],
            credit_rates=[],
            credit_caps=[],
        )

        self.assertEqual(
            TermMultiplier(
                multiplier=TrackedValue(
                    value=0.65,
                    label="S2.term_multiplier",
                    rule_name="min_term_reduction",
                    formula=(
                        "multiplier=0.65 because min_term_reduction applies to every "
                        "sentence; reduces min_term because applies_to lists "
                        "min_term with no conditions; doesn't reduce max_term because "
                        "applies_to doesn't list max_term"
                    ),
                ),
                reduced_terms=frozenset({TermType.MIN_TERM}),
            ),
            engine.get_term_multiplier(_NONVIOLENT_SENTENCE),
        )

    def test_matching_rule_can_reduce_no_terms(self) -> None:
        engine = _engine(
            term_multipliers=[
                TermMultiplierRule(
                    name="nonviolent_max_term_reduction",
                    description="35% reduction of nonviolent offenses' max terms",
                    when=None,
                    multiplier=0.65,
                    applies_to=[
                        ReductionTarget(term=TermType.MAX_TERM, when=_IS_NOT_VIOLENT)
                    ],
                    statute=None,
                )
            ],
            credit_rates=[],
            credit_caps=[],
        )

        self.assertEqual(
            TermMultiplier(
                multiplier=TrackedValue(
                    value=0.65,
                    label="S1.term_multiplier",
                    rule_name="nonviolent_max_term_reduction",
                    formula=(
                        "multiplier=0.65 because nonviolent_max_term_reduction "
                        "applies to every sentence; doesn't reduce min_term because "
                        "applies_to doesn't list min_term; doesn't reduce max_term "
                        "because applies_to max_term doesn't match "
                        "offense_group=VIOLENCE"
                    ),
                ),
                reduced_terms=frozenset(),
            ),
            engine.get_term_multiplier(_VIOLENT_SENTENCE),
        )

    def test_formula_lists_each_field_of_nested_conditions_once(self) -> None:
        engine = _engine(
            term_multipliers=[
                TermMultiplierRule(
                    name="lower_category_reduction",
                    description=(
                        "40% reduction of lower-category offenses that are not "
                        "violent or are category C"
                    ),
                    when=AllOf(
                        conditions=[
                            FieldCondition(
                                field="offense_category",
                                operator=ConditionOperator.NOT_IN,
                                value=[
                                    NevadaOffenseCategory.CATEGORY_A,
                                    NevadaOffenseCategory.CATEGORY_B,
                                ],
                            ),
                            AnyOf(
                                conditions=[
                                    _IS_NOT_VIOLENT,
                                    FieldCondition(
                                        field="offense_category",
                                        operator=ConditionOperator.EQUALS,
                                        value=NevadaOffenseCategory.CATEGORY_C,
                                    ),
                                ]
                            ),
                        ]
                    ),
                    multiplier=0.6,
                    applies_to=[
                        ReductionTarget(term=TermType.MAX_TERM, when=None),
                        ReductionTarget(term=TermType.MIN_TERM, when=None),
                    ],
                    statute=None,
                ),
                _STANDARD_TERM_REDUCTION,
            ],
            credit_rates=[],
            credit_caps=[],
        )

        self.assertEqual(
            TermMultiplier(
                multiplier=TrackedValue(
                    value=0.6,
                    label="S2.term_multiplier",
                    rule_name="lower_category_reduction",
                    formula=(
                        # offense_category appears twice in the conditions but
                        # once here, in the order the conditions first read it.
                        "multiplier=0.6 because lower_category_reduction matches "
                        "offense_category=C, offense_group=OTHER; "
                        "reduces min_term because applies_to lists min_term "
                        "with no conditions; reduces max_term because "
                        "applies_to lists max_term with no conditions"
                    ),
                ),
                reduced_terms=frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            ),
            engine.get_term_multiplier(_NONVIOLENT_SENTENCE),
        )

    def test_regime_without_term_multipliers_means_no_reduction(self) -> None:
        engine = _engine(term_multipliers=[], credit_rates=[], credit_caps=[])

        self.assertEqual(
            TermMultiplier(
                multiplier=TrackedValue(
                    value=1.0,
                    label="S2.term_multiplier",
                    formula="No term_multipliers rule; default 1.0",
                ),
                reduced_terms=frozenset(),
            ),
            engine.get_term_multiplier(_NONVIOLENT_SENTENCE),
        )

    def test_unchecked_rule_condition_names_the_rule(self) -> None:
        engine = _engine(
            term_multipliers=[
                TermMultiplierRule(
                    name="early_sentence_reduction",
                    description="50% reduction of sentences that start before 2014",
                    when=_STARTS_BEFORE_2014,
                    multiplier=0.5,
                    applies_to=[
                        ReductionTarget(term=TermType.MAX_TERM, when=None),
                        ReductionTarget(term=TermType.MIN_TERM, when=None),
                    ],
                    statute=None,
                ),
                _STANDARD_TERM_REDUCTION,
            ],
            credit_rates=[],
            credit_caps=[],
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Could not check the conditions in term_multipliers entry "
            r"\[early_sentence_reduction\] against sentence \[S1\]: Condition "
            r"compares field \[start_date\] with operator \[before\], but the field "
            r"has no value\.$",
        ):
            engine.get_term_multiplier(_VIOLENT_SENTENCE)

    def test_unchecked_applies_to_condition_names_the_rule_and_term(self) -> None:
        engine = _engine(
            term_multipliers=[
                TermMultiplierRule(
                    name="early_min_term_reduction",
                    description=(
                        "35% reduction of every max term, and of the min term of "
                        "sentences that start before 2014"
                    ),
                    when=None,
                    multiplier=0.65,
                    applies_to=[
                        ReductionTarget(term=TermType.MAX_TERM, when=None),
                        ReductionTarget(
                            term=TermType.MIN_TERM, when=_STARTS_BEFORE_2014
                        ),
                    ],
                    statute=None,
                )
            ],
            credit_rates=[],
            credit_caps=[],
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Could not check the conditions in term_multipliers entry "
            r"\[early_min_term_reduction\] applies_to \[min_term\] against sentence "
            r"\[S1\]: Condition compares field \[start_date\] with operator "
            r"\[before\], but the field has no value\.$",
        ):
            engine.get_term_multiplier(_VIOLENT_SENTENCE)


class TestGetCreditRate(unittest.TestCase):
    """Tests for RuleEngine.get_credit_rate."""

    def test_unmet_min_term_condition_keeps_credits_off_min_term(self) -> None:
        engine = _engine(term_multipliers=[], credit_rates=[_STAT_TIME], credit_caps=[])

        self.assertEqual(
            ProjectedCreditRate(
                rate=TrackedValue(
                    value=20.0,
                    label="S1.credit_rate",
                    rule_name="stat_time",
                    formula=(
                        "rate=20.0 days/month because stat_time applies to every "
                        "sentence; doesn't reduce min_term because applies_to min_term "
                        "doesn't match offense_group=VIOLENCE; reduces max_term "
                        "because applies_to lists max_term with no conditions"
                    ),
                ),
                reduced_terms=frozenset({TermType.MAX_TERM}),
            ),
            engine.get_credit_rate(_VIOLENT_SENTENCE),
        )

    def test_met_min_term_condition_gives_credits_on_both_terms(self) -> None:
        engine = _engine(term_multipliers=[], credit_rates=[_STAT_TIME], credit_caps=[])

        self.assertEqual(
            ProjectedCreditRate(
                rate=TrackedValue(
                    value=20.0,
                    label="S2.credit_rate",
                    rule_name="stat_time",
                    formula=(
                        "rate=20.0 days/month because stat_time applies to every "
                        "sentence; reduces min_term because applies_to min_term "
                        "matches offense_group=OTHER; reduces max_term because "
                        "applies_to lists max_term with no conditions"
                    ),
                ),
                reduced_terms=frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            ),
            engine.get_credit_rate(_NONVIOLENT_SENTENCE),
        )

    def test_first_matching_rule_applies(self) -> None:
        engine = _engine(
            term_multipliers=[],
            credit_rates=[
                CreditRateRule(
                    name="violent_credit_rate",
                    description="Lower credits for violent offenses",
                    when=_IS_VIOLENT,
                    rate=10.0,
                    applies_to=[
                        ReductionTarget(term=TermType.MAX_TERM, when=None),
                        ReductionTarget(term=TermType.MIN_TERM, when=None),
                    ],
                    statute=None,
                ),
                _STAT_TIME,
            ],
            credit_caps=[],
        )

        self.assertEqual(
            "violent_credit_rate",
            engine.get_credit_rate(_VIOLENT_SENTENCE).rate.rule_name,
        )
        self.assertEqual(
            "stat_time", engine.get_credit_rate(_NONVIOLENT_SENTENCE).rate.rule_name
        )

    def test_regime_without_credit_rates_means_no_credits(self) -> None:
        engine = _engine(term_multipliers=[], credit_rates=[], credit_caps=[])

        self.assertEqual(
            ProjectedCreditRate(
                rate=TrackedValue(
                    value=0.0,
                    label="S2.credit_rate",
                    formula="No credit_rates rule; default 0.0 days/month",
                ),
                reduced_terms=frozenset(),
            ),
            engine.get_credit_rate(_NONVIOLENT_SENTENCE),
        )


class TestGetCreditCap(unittest.TestCase):
    """Tests for RuleEngine.get_credit_cap."""

    def test_cap_limits_term_it_binds(self) -> None:
        engine = _engine(
            term_multipliers=[],
            credit_rates=[_STAT_TIME],
            credit_caps=[
                CreditCapRule(
                    name="min_term_credit_cap",
                    description="Credits against the min term are capped",
                    when=None,
                    max_credit_days="sentence_length_days * 0.58",
                    binds=frozenset({TermType.MIN_TERM}),
                    statute=None,
                )
            ],
        )
        min_term_days = TrackedValue.input(1000, "S1.original_min_days")

        cap = engine.get_credit_cap(
            sentence=_VIOLENT_SENTENCE,
            term=TermType.MIN_TERM,
            sentence_length_days=min_term_days,
        )

        self.assertEqual(
            CreditCap(
                max_credit_days=TrackedValue(
                    value=580,
                    label="S1.credit_cap_min_term",
                    sources=(min_term_days,),
                    rule_name="min_term_credit_cap",
                    formula="sentence_length_days * 0.58",
                ),
                binds=frozenset({TermType.MIN_TERM}),
            ),
            cap,
        )

    def test_cap_does_not_limit_term_it_does_not_bind(self) -> None:
        engine = _engine(
            term_multipliers=[],
            credit_rates=[_STAT_TIME],
            credit_caps=[
                CreditCapRule(
                    name="min_term_credit_cap",
                    description="Credits against the min term are capped",
                    when=None,
                    max_credit_days="sentence_length_days * 0.58",
                    binds=frozenset({TermType.MIN_TERM}),
                    statute=None,
                )
            ],
        )

        self.assertIsNone(
            engine.get_credit_cap(
                sentence=_VIOLENT_SENTENCE,
                term=TermType.MAX_TERM,
                sentence_length_days=TrackedValue.input(2000, "S1.original_max_days"),
            )
        )

    def test_sentence_that_meets_no_cap_is_uncapped(self) -> None:
        engine = _engine(
            term_multipliers=[],
            credit_rates=[_STAT_TIME],
            credit_caps=[
                CreditCapRule(
                    name="violent_credit_cap",
                    description="Credits for violent offenses are capped",
                    when=_IS_VIOLENT,
                    max_credit_days="sentence_length_days * 0.58",
                    binds=frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
                    statute=None,
                )
            ],
        )

        self.assertIsNone(
            engine.get_credit_cap(
                sentence=_NONVIOLENT_SENTENCE,
                term=TermType.MIN_TERM,
                sentence_length_days=TrackedValue.input(1000, "S2.original_min_days"),
            )
        )


class TestSentenceMeetsCondition(unittest.TestCase):
    """Tests for sentence_meets_condition."""

    def test_enum_field(self) -> None:
        self.assertTrue(
            sentence_meets_condition(
                FieldCondition(
                    field="offense_category",
                    operator=ConditionOperator.NOT_IN,
                    value=[
                        NevadaOffenseCategory.CATEGORY_A,
                        NevadaOffenseCategory.CATEGORY_B,
                    ],
                ),
                _VIOLENT_SENTENCE,
                location="term_multipliers entry [standard_term_reduction]",
            )
        )

    def test_ordered_comparison_on_missing_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Could not check the conditions in term_multipliers entry "
            r"\[early_sentence_reduction\] against sentence \[S1\]: Condition "
            r"compares field \[start_date\] with operator \[before\], but the field "
            r"has no value\.$",
        ):
            sentence_meets_condition(
                FieldCondition(
                    field="start_date",
                    operator=ConditionOperator.BEFORE,
                    value=date(2014, 7, 1),
                ),
                _VIOLENT_SENTENCE,
                location="term_multipliers entry [early_sentence_reduction]",
            )


if __name__ == "__main__":
    unittest.main()
