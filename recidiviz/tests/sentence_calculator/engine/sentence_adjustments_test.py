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
"""Tests for recidiviz/sentence_calculator/engine/sentence_adjustments.py."""

import unittest
from datetime import date

from recidiviz.sentence_calculator.engine.rule_engine import RuleEngine
from recidiviz.sentence_calculator.engine.sentence_adjustments import (
    _jail_credit_to_days,
    _term_to_days,
    calculate_sentence_adjustment,
)
from recidiviz.sentence_calculator.models.conditions import (
    ConditionOperator,
    FieldCondition,
)
from recidiviz.sentence_calculator.models.inputs import Term
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

_START_DATE = date(2025, 1, 1)

# Identical except offense_group, which _STANDARD_TERM_REDUCTION's min term
# condition keys on.
_VIOLENT_SENTENCE = NevadaSentence(  # pylint: disable=abstract-class-instantiated
    sentence_id="S1",
    min_term=Term(years=2),
    max_term=Term(years=5),
    initial_time_served=Term(days=30),
    offense_category=NevadaOffenseCategory.CATEGORY_C,
    offense_group=NevadaOffenseGroup.VIOLENCE,
)
_NONVIOLENT_SENTENCE = NevadaSentence(  # pylint: disable=abstract-class-instantiated
    sentence_id="S2",
    min_term=Term(years=2),
    max_term=Term(years=5),
    initial_time_served=Term(days=30),
    offense_category=NevadaOffenseCategory.CATEGORY_C,
    offense_group=NevadaOffenseGroup.OTHER,
)

_STANDARD_TERM_REDUCTION = TermMultiplierRule(
    name="standard_term_reduction",
    description=(
        "35% reduction of every offense's max term, and of its min term unless "
        "it is violent"
    ),
    when=None,
    multiplier=0.65,
    applies_to=[
        ReductionTarget(term=TermType.MAX_TERM, when=None),
        ReductionTarget(
            term=TermType.MIN_TERM,
            when=FieldCondition(
                field="offense_group",
                operator=ConditionOperator.NOT_EQUALS,
                value=NevadaOffenseGroup.VIOLENCE,
            ),
        ),
    ],
    statute=None,
)


def _regime(
    *,
    term_multipliers: list[TermMultiplierRule],
    use_calendar_term_conversion: bool,
) -> LawRegime:
    return LawRegime(
        name="TEST_REGIME",
        description="Test regime",
        effective_date=date(2020, 1, 1),
        use_calendar_term_conversion=use_calendar_term_conversion,
        days_per_month=30.0,
        days_per_year=365.0,
        term_multipliers=term_multipliers,
        credit_rates=[],
        credit_caps=[],
        projected_dates={},
    )


class TestTermToDays(unittest.TestCase):
    """Tests for _term_to_days and _jail_credit_to_days."""

    def test_flat_conversion(self) -> None:
        regime = _regime(
            term_multipliers=[],
            use_calendar_term_conversion=False,
        )

        days = _term_to_days(
            term=Term(years=1, months=2, days=3),
            label="S1.original_max_days",
            start_date=TrackedValue.input(date(2024, 1, 1), "S1.retro_date"),
            regime=regime,
        )

        self.assertEqual(428, days.value)
        self.assertEqual("1y × 365.0 + 2m × 30.0 + 3d = 428 days", days.formula)

    def test_calendar_conversion_counts_leap_day(self) -> None:
        regime = _regime(
            term_multipliers=[],
            use_calendar_term_conversion=True,
        )

        days = _term_to_days(
            term=Term(years=1),
            label="S1.original_max_days",
            start_date=TrackedValue.input(date(2024, 1, 1), "S1.retro_date"),
            regime=regime,
        )

        # Flat conversion would give 365.
        self.assertEqual(366, days.value)
        self.assertEqual("2024-01-01 + 1y 0m 0d = 2025-01-01 (366 days)", days.formula)

    def test_calendar_conversion_depends_on_start_date(self) -> None:
        regime = _regime(
            term_multipliers=[],
            use_calendar_term_conversion=True,
        )

        self.assertEqual(
            29,
            _term_to_days(
                term=Term(months=1),
                label="S1.original_max_days",
                start_date=TrackedValue.input(date(2024, 2, 1), "S1.retro_date"),
                regime=regime,
            ).value,
        )
        self.assertEqual(
            31,
            _term_to_days(
                term=Term(months=1),
                label="S1.original_max_days",
                start_date=TrackedValue.input(date(2024, 3, 1), "S1.retro_date"),
                regime=regime,
            ).value,
        )

    def test_calendar_jail_credit_counts_back_from_start_date(self) -> None:
        regime = _regime(
            term_multipliers=[],
            use_calendar_term_conversion=True,
        )

        jail_credit = _jail_credit_to_days(
            term=Term(months=1),
            label="S1.jail_credit",
            start_date=TrackedValue.input(date(2024, 3, 1), "S1.start_date"),
            regime=regime,
        )

        # The month before 2024-03-01 is February 2024, which has 29 days. Counting
        # forward from 2024-03-01 would give 31.
        self.assertEqual(
            {
                "label": "S1.jail_credit",
                "value": 29,
                "formula": "2024-03-01 - 0y 1m 0d = 2024-02-01 (29 days)",
                "derived_from": [
                    {"label": "S1.jail_credit.years", "value": 0},
                    {"label": "S1.jail_credit.months", "value": 1},
                    {"label": "S1.jail_credit.days", "value": 0},
                    {"label": "S1.start_date", "value": "2024-03-01"},
                ],
            },
            jail_credit.to_derivation_tree(),
        )


class TestCalendarTermConversion(unittest.TestCase):
    """Tests for calculate_sentence_adjustment under a regime with calendar term
    conversion."""

    def test_terms_count_from_retro_date(self) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                use_calendar_term_conversion=True,
            )
        )
        sentence = NevadaSentence(  # pylint: disable=abstract-class-instantiated
            sentence_id="S1",
            min_term=Term(years=1),
            max_term=Term(years=3),
            initial_time_served=Term(days=30),
            offense_category=NevadaOffenseCategory.CATEGORY_C,
            offense_group=NevadaOffenseGroup.OTHER,
        )

        adjustment = calculate_sentence_adjustment(
            sentence=sentence, start_date=date(2024, 3, 15), engine=engine
        )

        # The terms count from the RETRO date, 2024-02-14, so both span February
        # 29, 2024. Counted from the 2024-03-15 start date they would be 365 and
        # 1095 days.
        self.assertEqual(
            {
                "label": "S1.original_min_days",
                "value": 366,
                "formula": "2024-02-14 + 1y 0m 0d = 2025-02-14 (366 days)",
                "derived_from": [
                    {"label": "S1.original_min_days.years", "value": 1},
                    {"label": "S1.original_min_days.months", "value": 0},
                    {"label": "S1.original_min_days.days", "value": 0},
                    {
                        "label": "S1.retro_date",
                        "value": "2024-02-14",
                        "formula": "2024-03-15 - 30 days = 2024-02-14",
                        "derived_from": [
                            {"label": "S1.start_date", "value": "2024-03-15"},
                            {
                                "label": "S1.jail_credit",
                                "value": 30,
                                "formula": (
                                    "2024-03-15 - 0y 0m 30d = 2024-02-14 (30 days)"
                                ),
                                "derived_from": [
                                    {"label": "S1.jail_credit.years", "value": 0},
                                    {"label": "S1.jail_credit.months", "value": 0},
                                    {"label": "S1.jail_credit.days", "value": 30},
                                    {"label": "S1.start_date", "value": "2024-03-15"},
                                ],
                            },
                        ],
                    },
                ],
            },
            adjustment.original_min_days.to_derivation_tree(),
        )
        self.assertEqual(1096, adjustment.original_max_days.value)
        # 366 - 30 and 1096 - 30
        self.assertEqual(336, adjustment.adjusted_min_days.value)
        self.assertEqual(1066, adjustment.adjusted_max_days.value)


class TestTermMultiplierAdjustment(unittest.TestCase):
    """Tests for calculate_sentence_adjustment under a regime with
    term_multipliers."""

    def setUp(self) -> None:
        self.engine = RuleEngine(
            _regime(
                term_multipliers=[_STANDARD_TERM_REDUCTION],
                use_calendar_term_conversion=False,
            )
        )

    def test_reduction_applies_to_both_terms(self) -> None:
        adjustment = calculate_sentence_adjustment(
            sentence=_NONVIOLENT_SENTENCE, start_date=_START_DATE, engine=self.engine
        )

        self.assertEqual(730, adjustment.original_min_days.value)
        self.assertEqual(1825, adjustment.original_max_days.value)
        # int(730 * 0.65) - 30
        self.assertEqual(444, adjustment.adjusted_min_days.value)
        # int(1825 * 0.65) - 30
        self.assertEqual(1156, adjustment.adjusted_max_days.value)
        # 1825 - int(1825 * 0.65)
        self.assertEqual(639, adjustment.max_term_reduction_days.value)
        self.assertEqual(0.65, adjustment.term_multiplier.multiplier.value)
        self.assertEqual(30, adjustment.jail_credit.value)
        self.assertEqual(
            frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            adjustment.term_multiplier.reduced_terms,
        )

    def test_unmet_min_term_condition_reduces_max_term_only(self) -> None:
        adjustment = calculate_sentence_adjustment(
            sentence=_VIOLENT_SENTENCE, start_date=_START_DATE, engine=self.engine
        )

        # 730 - 30, with no reduction
        self.assertEqual(700, adjustment.adjusted_min_days.value)
        self.assertEqual(1156, adjustment.adjusted_max_days.value)
        self.assertEqual(
            frozenset({TermType.MAX_TERM}), adjustment.term_multiplier.reduced_terms
        )

    def test_adjusted_min_derivation_tree(self) -> None:
        adjustment = calculate_sentence_adjustment(
            sentence=_VIOLENT_SENTENCE, start_date=_START_DATE, engine=self.engine
        )

        self.assertEqual(
            {
                "label": "S1.adjusted_min_days",
                "value": 700,
                "formula": "max(0, 730 - 30) = 700 (S1.term_multiplier not applied)",
                "derived_from": [
                    {
                        "label": "S1.original_min_days",
                        "value": 730,
                        "formula": "2y × 365.0 + 0m × 30.0 + 0d = 730 days",
                        "derived_from": [
                            {"label": "S1.original_min_days.years", "value": 2},
                            {"label": "S1.original_min_days.months", "value": 0},
                            {"label": "S1.original_min_days.days", "value": 0},
                        ],
                    },
                    {
                        "label": "S1.term_multiplier",
                        "value": 0.65,
                        "formula": (
                            "multiplier=0.65 because standard_term_reduction "
                            "applies to every sentence; doesn't reduce min_term "
                            "because applies_to min_term doesn't match "
                            "offense_group=VIOLENCE; reduces max_term because "
                            "applies_to lists max_term with no conditions"
                        ),
                        "rule": "standard_term_reduction",
                    },
                    {
                        "label": "S1.jail_credit",
                        "value": 30,
                        "formula": "0y × 365.0 + 0m × 30.0 + 30d = 30 days",
                        "derived_from": [
                            {"label": "S1.jail_credit.years", "value": 0},
                            {"label": "S1.jail_credit.months", "value": 0},
                            {"label": "S1.jail_credit.days", "value": 30},
                        ],
                    },
                ],
            },
            adjustment.adjusted_min_days.to_derivation_tree(),
        )

    def test_reduced_min_derivation_tree(self) -> None:
        adjustment = calculate_sentence_adjustment(
            sentence=_NONVIOLENT_SENTENCE, start_date=_START_DATE, engine=self.engine
        )

        self.assertEqual(
            {
                "label": "S2.adjusted_min_days",
                "value": 444,
                "formula": "max(0, int(730 × 0.65) - 30) = 444",
                "derived_from": [
                    {
                        "label": "S2.original_min_days",
                        "value": 730,
                        "formula": "2y × 365.0 + 0m × 30.0 + 0d = 730 days",
                        "derived_from": [
                            {"label": "S2.original_min_days.years", "value": 2},
                            {"label": "S2.original_min_days.months", "value": 0},
                            {"label": "S2.original_min_days.days", "value": 0},
                        ],
                    },
                    {
                        "label": "S2.term_multiplier",
                        "value": 0.65,
                        "formula": (
                            "multiplier=0.65 because standard_term_reduction "
                            "applies to every sentence; reduces min_term because "
                            "applies_to min_term matches offense_group=OTHER; "
                            "reduces max_term because applies_to lists max_term "
                            "with no conditions"
                        ),
                        "rule": "standard_term_reduction",
                    },
                    {
                        "label": "S2.jail_credit",
                        "value": 30,
                        "formula": "0y × 365.0 + 0m × 30.0 + 30d = 30 days",
                        "derived_from": [
                            {"label": "S2.jail_credit.years", "value": 0},
                            {"label": "S2.jail_credit.months", "value": 0},
                            {"label": "S2.jail_credit.days", "value": 30},
                        ],
                    },
                ],
            },
            adjustment.adjusted_min_days.to_derivation_tree(),
        )

    def test_multiplier_is_applied_exactly(self) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[
                    TermMultiplierRule(
                        name="thirty_percent_reduction",
                        description="30% reduction of every offense's terms",
                        when=None,
                        multiplier=0.7,
                        applies_to=[
                            ReductionTarget(term=TermType.MIN_TERM, when=None),
                            ReductionTarget(term=TermType.MAX_TERM, when=None),
                        ],
                        statute=None,
                    )
                ],
                use_calendar_term_conversion=False,
            )
        )
        sentence = NevadaSentence(  # pylint: disable=abstract-class-instantiated
            sentence_id="S3",
            min_term=Term(days=90),
            max_term=Term(days=170),
            initial_time_served=Term(),
            offense_category=NevadaOffenseCategory.CATEGORY_C,
            offense_group=NevadaOffenseGroup.OTHER,
        )

        adjustment = calculate_sentence_adjustment(
            sentence=sentence, start_date=_START_DATE, engine=engine
        )

        # 90 × 0.7 and 170 × 0.7 are exactly 63 and 119. Multiplying the floats
        # gives 62.99999999999999 and 118.99999999999999, which truncate to 62
        # and 118.
        self.assertEqual(63, adjustment.adjusted_min_days.value)
        self.assertEqual(119, adjustment.adjusted_max_days.value)

    def test_jail_credit_cannot_make_term_negative(self) -> None:
        sentence = NevadaSentence(  # pylint: disable=abstract-class-instantiated
            sentence_id="S3",
            min_term=Term(days=10),
            max_term=Term(days=20),
            initial_time_served=Term(days=30),
            offense_category=NevadaOffenseCategory.CATEGORY_C,
            offense_group=NevadaOffenseGroup.OTHER,
        )

        adjustment = calculate_sentence_adjustment(
            sentence=sentence, start_date=_START_DATE, engine=self.engine
        )

        self.assertEqual(0, adjustment.adjusted_min_days.value)
        self.assertEqual(0, adjustment.adjusted_max_days.value)
        # 20 - int(20 * 0.65), counted in full though jail credit covers the term
        self.assertEqual(7, adjustment.max_term_reduction_days.value)


class TestReductionOffMinTermOnly(unittest.TestCase):
    """Tests for calculate_sentence_adjustment under a term multiplier whose
    applies_to doesn't list the max term."""

    def test_reduction_skips_unlisted_max_term(self) -> None:
        engine = RuleEngine(
            _regime(
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
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_NONVIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        # int(730 * 0.65) - 30
        self.assertEqual(444, adjustment.adjusted_min_days.value)
        # 1825 - 30, with no reduction
        self.assertEqual(1795, adjustment.adjusted_max_days.value)
        self.assertEqual(0, adjustment.max_term_reduction_days.value)
        self.assertEqual(
            frozenset({TermType.MIN_TERM}), adjustment.term_multiplier.reduced_terms
        )


class TestReductionReachingNoTerms(unittest.TestCase):
    """Tests for calculate_sentence_adjustment under a term multiplier that
    matches the sentence but whose applies_to reaches neither of its terms."""

    def test_matching_rule_that_reaches_no_terms_subtracts_only_jail_credit(
        self,
    ) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[
                    TermMultiplierRule(
                        name="nonviolent_max_term_reduction",
                        description="35% reduction of nonviolent offenses' max terms",
                        when=None,
                        multiplier=0.65,
                        applies_to=[
                            ReductionTarget(
                                term=TermType.MAX_TERM,
                                when=FieldCondition(
                                    field="offense_group",
                                    operator=ConditionOperator.NOT_EQUALS,
                                    value=NevadaOffenseGroup.VIOLENCE,
                                ),
                            )
                        ],
                        statute=None,
                    )
                ],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_VIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        # The rule matched, so its multiplier is reported, but it reduced no terms.
        self.assertEqual(0.65, adjustment.term_multiplier.multiplier.value)
        self.assertEqual(frozenset(), adjustment.term_multiplier.reduced_terms)
        # 730 - 30 and 1825 - 30, with no reduction
        self.assertEqual(700, adjustment.adjusted_min_days.value)
        self.assertEqual(1795, adjustment.adjusted_max_days.value)


class TestNoReduction(unittest.TestCase):
    """Tests for calculate_sentence_adjustment under a regime without
    term_multipliers."""

    def test_regime_without_reduction_subtracts_only_jail_credit(self) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_NONVIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        self.assertEqual(1.0, adjustment.term_multiplier.multiplier.value)
        self.assertEqual(frozenset(), adjustment.term_multiplier.reduced_terms)
        # Only the 30 days of jail credit come off each term.
        self.assertEqual(700, adjustment.adjusted_min_days.value)
        self.assertEqual(1795, adjustment.adjusted_max_days.value)


if __name__ == "__main__":
    unittest.main()
