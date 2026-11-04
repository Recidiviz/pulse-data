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
from recidiviz.sentence_calculator.models.outputs import TermMultiplier
from recidiviz.sentence_calculator.models.rules import (
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


def _engine(*, term_multipliers: list[TermMultiplierRule]) -> RuleEngine:
    return RuleEngine(
        LawRegime(
            name="TEST_REGIME",
            description="Test regime",
            effective_date=date(2020, 1, 1),
            use_calendar_term_conversion=False,
            days_per_month=30.0,
            days_per_year=365.0,
            term_multipliers=term_multipliers,
            credit_rates=[],
            credit_caps=[],
            projected_dates={},
        )
    )


class TestGetTermMultiplier(unittest.TestCase):
    """Tests for RuleEngine.get_term_multiplier."""

    def test_unmet_min_term_condition_keeps_reduction_off_min_term(self) -> None:
        engine = _engine(term_multipliers=[_STANDARD_TERM_REDUCTION])

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
        engine = _engine(term_multipliers=[_STANDARD_TERM_REDUCTION])

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
            term_multipliers=[_VIOLENT_TERM_REDUCTION, _STANDARD_TERM_REDUCTION]
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
            ]
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
            ]
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
            ]
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
        engine = _engine(term_multipliers=[])

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
            ]
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
            ]
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Could not check the conditions in term_multipliers entry "
            r"\[early_min_term_reduction\] applies_to \[min_term\] against sentence "
            r"\[S1\]: Condition compares field \[start_date\] with operator "
            r"\[before\], but the field has no value\.$",
        ):
            engine.get_term_multiplier(_VIOLENT_SENTENCE)


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
