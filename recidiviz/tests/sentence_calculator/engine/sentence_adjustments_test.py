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

_START_DATE = date(2025, 1, 1)

# The two sentences differ only in offense_group, the field the min term's
# reduction condition checks.
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

# Every max term, and the min term unless the offense is violent.
_MAX_TERM_AND_NONVIOLENT_MIN_TERM = [
    ReductionTarget(term=TermType.MAX_TERM, when=None),
    ReductionTarget(
        term=TermType.MIN_TERM,
        when=FieldCondition(
            field="offense_group",
            operator=ConditionOperator.NOT_EQUALS,
            value=NevadaOffenseGroup.VIOLENCE,
        ),
    ),
]
_STANDARD_TERM_REDUCTION = TermMultiplierRule(
    name="standard_term_reduction",
    description=(
        "35% reduction of every offense's max term, and of its min term unless "
        "it is violent"
    ),
    when=None,
    multiplier=0.65,
    applies_to=_MAX_TERM_AND_NONVIOLENT_MIN_TERM,
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
    applies_to=_MAX_TERM_AND_NONVIOLENT_MIN_TERM,
    statute=None,
)
_MIN_TERM_CREDIT_CAP = CreditCapRule(
    name="min_term_credit_cap",
    description="Credits against the min term are capped",
    when=None,
    max_credit_days="sentence_length_days * 0.1",
    binds=frozenset({TermType.MIN_TERM}),
    statute=None,
)


def _regime(
    *,
    term_multipliers: list[TermMultiplierRule],
    credit_rates: list[CreditRateRule],
    credit_caps: list[CreditCapRule],
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
        credit_rates=credit_rates,
        credit_caps=credit_caps,
        projected_dates={},
    )


class TestTermToDays(unittest.TestCase):
    """Tests for _term_to_days and _jail_credit_to_days."""

    def test_flat_conversion(self) -> None:
        regime = _regime(
            term_multipliers=[],
            credit_rates=[],
            credit_caps=[],
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
            credit_rates=[],
            credit_caps=[],
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
            credit_rates=[],
            credit_caps=[],
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
            credit_rates=[],
            credit_caps=[],
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
                credit_rates=[],
                credit_caps=[],
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
                credit_rates=[],
                credit_caps=[],
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
        assert adjustment.term_multiplier is not None
        self.assertEqual(0.65, adjustment.term_multiplier.multiplier.value)
        self.assertEqual(30, adjustment.jail_credit.value)
        self.assertEqual(
            frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            adjustment.reduced_terms,
        )

    def test_unmet_min_term_condition_reduces_max_term_only(self) -> None:
        adjustment = calculate_sentence_adjustment(
            sentence=_VIOLENT_SENTENCE, start_date=_START_DATE, engine=self.engine
        )

        # 730 - 30, with no reduction
        self.assertEqual(700, adjustment.adjusted_min_days.value)
        self.assertEqual(1156, adjustment.adjusted_max_days.value)
        self.assertEqual(frozenset({TermType.MAX_TERM}), adjustment.reduced_terms)

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
                credit_rates=[],
                credit_caps=[],
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


class TestCreditRateAdjustment(unittest.TestCase):
    """Tests for calculate_sentence_adjustment under a regime with credit_rates."""

    def test_credits_apply_to_both_terms(self) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[_STAT_TIME],
                credit_caps=[],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_NONVIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        # The jail credit earns int(30 / 30 * 20) = 20 days, leaving 730 - 30 - 20
        # = 680 days to cover. 13 months of 30 days cover 13 * 50 = 650 of them,
        # and the last 30 take ceil(30 / (1 + 20 / 30)) = 18 days: 390 + 18.
        self.assertEqual(408, adjustment.adjusted_min_days.value)
        # 1825 - 30 - 20 = 1775 days to cover. 35 months cover 1750, and the last
        # 25 take ceil(25 / (1 + 20 / 30)) = 15 days: 1050 + 15.
        self.assertEqual(1065, adjustment.adjusted_max_days.value)
        # 1825 - 30 - 1065
        self.assertEqual(730, adjustment.max_term_reduction_days.value)
        self.assertIsNone(adjustment.term_multiplier)
        self.assertEqual(
            frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            adjustment.reduced_terms,
        )

    def test_unmet_min_term_condition_earns_credits_against_max_term_only(
        self,
    ) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[_STAT_TIME],
                credit_caps=[],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_VIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        self.assertEqual(700, adjustment.adjusted_min_days.value)
        self.assertEqual(1065, adjustment.adjusted_max_days.value)
        self.assertEqual(frozenset({TermType.MAX_TERM}), adjustment.reduced_terms)

    def test_unreduced_min_term_derivation_keeps_credit_rate(self) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[_STAT_TIME],
                credit_caps=[],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_VIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        self.assertEqual(
            {
                "label": "S1.adjusted_min_days",
                "value": 700,
                "formula": "max(0, 730 - 30) = 700 (S1.credit_rate not applied)",
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
                        "label": "S1.credit_rate",
                        "value": 20.0,
                        "formula": (
                            "rate=20.0 days/month because stat_time applies to "
                            "every sentence; doesn't reduce min_term because "
                            "applies_to min_term doesn't match "
                            "offense_group=VIOLENCE; reduces max_term because "
                            "applies_to lists max_term with no conditions"
                        ),
                        "rule": "stat_time",
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

    def test_cap_binds_min_term_only(self) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[_STAT_TIME],
                credit_caps=[_MIN_TERM_CREDIT_CAP],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_NONVIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        # Credits capped at int(730 * 0.1) = 73, so 730 - 30 - 73.
        self.assertEqual(627, adjustment.adjusted_min_days.value)
        # The cap doesn't bind the max term, so this matches the uncapped case.
        self.assertEqual(1065, adjustment.adjusted_max_days.value)

    def test_cap_above_uncapped_credits_has_no_effect(self) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[_STAT_TIME],
                credit_caps=[
                    CreditCapRule(
                        name="min_term_credit_cap",
                        description="Credits against the min term are capped",
                        when=None,
                        max_credit_days="sentence_length_days * 0.9",
                        binds=frozenset({TermType.MIN_TERM}),
                        statute=None,
                    )
                ],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_NONVIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        # The 730 - 30 - 408 = 292 days of credits fall below the int(730 * 0.9) =
        # 657 day cap, so the min term matches the uncapped case.
        self.assertEqual(408, adjustment.adjusted_min_days.value)
        self.assertEqual(1065, adjustment.adjusted_max_days.value)

    def test_capped_min_term_derivation_tree(self) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[_STAT_TIME],
                credit_caps=[_MIN_TERM_CREDIT_CAP],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_NONVIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        original_min_days = {
            "label": "S2.original_min_days",
            "value": 730,
            "formula": "2y × 365.0 + 0m × 30.0 + 0d = 730 days",
            "derived_from": [
                {"label": "S2.original_min_days.years", "value": 2},
                {"label": "S2.original_min_days.months", "value": 0},
                {"label": "S2.original_min_days.days", "value": 0},
            ],
        }
        jail_credit = {
            "label": "S2.jail_credit",
            "value": 30,
            "formula": "0y × 365.0 + 0m × 30.0 + 30d = 30 days",
            "derived_from": [
                {"label": "S2.jail_credit.years", "value": 0},
                {"label": "S2.jail_credit.months", "value": 0},
                {"label": "S2.jail_credit.days", "value": 30},
            ],
        }
        self.assertEqual(
            {
                "label": "S2.adjusted_min_days",
                "value": 627,
                "formula": "max(0, 730 - 30 - 73) = 627",
                "derived_from": [
                    original_min_days,
                    jail_credit,
                    {
                        "label": "S2.good_time_credits_min",
                        "value": 73,
                        "formula": "min(292, 73) = 73",
                        "derived_from": [
                            {
                                "label": "S2.uncapped_good_time_credits_min",
                                "value": 292,
                                "formula": (
                                    "20.0 days/month on 30 days of jail credit and "
                                    "month by month from 2025-01-01 = 292"
                                ),
                                "derived_from": [
                                    original_min_days,
                                    {
                                        "label": "S2.credit_rate",
                                        "value": 20.0,
                                        "formula": (
                                            "rate=20.0 days/month because stat_time "
                                            "applies to every sentence; reduces "
                                            "min_term because applies_to min_term "
                                            "matches offense_group=OTHER; reduces "
                                            "max_term because applies_to lists "
                                            "max_term with no conditions"
                                        ),
                                        "rule": "stat_time",
                                    },
                                    jail_credit,
                                ],
                            },
                            {
                                "label": "S2.credit_cap_min_term",
                                "value": 73,
                                "formula": "sentence_length_days * 0.1",
                                "rule": "min_term_credit_cap",
                                "derived_from": [original_min_days],
                            },
                        ],
                    },
                ],
            },
            adjustment.adjusted_min_days.to_derivation_tree(),
        )

    def test_credits_accrue_by_calendar_month(self) -> None:
        sentence = NevadaSentence(  # pylint: disable=abstract-class-instantiated
            sentence_id="S3",
            min_term=Term(days=99),
            max_term=Term(days=99),
            initial_time_served=Term(),
            offense_category=NevadaOffenseCategory.CATEGORY_C,
            offense_group=NevadaOffenseGroup.OTHER,
        )
        calendar_engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[_STAT_TIME],
                credit_caps=[],
                use_calendar_term_conversion=True,
            )
        )
        flat_engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[_STAT_TIME],
                credit_caps=[],
                use_calendar_term_conversion=False,
            )
        )

        calendar_adjustment = calculate_sentence_adjustment(
            sentence=sentence, start_date=date(2023, 2, 1), engine=calendar_engine
        )
        flat_adjustment = calculate_sentence_adjustment(
            sentence=sentence, start_date=date(2023, 2, 1), engine=flat_engine
        )

        # February 2023 (28 days) covers 48 of the 99 days, and March (31 days)
        # covers the remaining 51 exactly: 28 + 31.
        self.assertEqual(59, calendar_adjustment.adjusted_max_days.value)
        # The first 30-day month covers 50, and the last 49 take
        # ceil(49 / (1 + 20 / 30)) = 30 days: 30 + 30.
        self.assertEqual(60, flat_adjustment.adjusted_max_days.value)

    def test_jail_credit_longer_than_term_earns_no_credits(self) -> None:
        """Someone can arrive with more jail credit than days to serve and be
        released right away. Their terms take no credits, rather than
        negative ones."""
        sentence = NevadaSentence(  # pylint: disable=abstract-class-instantiated
            sentence_id="S4",
            min_term=Term(days=10),
            max_term=Term(days=20),
            initial_time_served=Term(days=30),
            offense_category=NevadaOffenseCategory.CATEGORY_C,
            offense_group=NevadaOffenseGroup.OTHER,
        )
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[_STAT_TIME],
                credit_caps=[],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=sentence, start_date=_START_DATE, engine=engine
        )

        self.assertEqual(
            {
                "label": "S4.adjusted_max_days",
                "value": 0,
                "formula": "max(0, 20 - 30 - 0) = 0",
                "derived_from": [
                    {
                        "label": "S4.original_max_days",
                        "value": 20,
                        "formula": "0y × 365.0 + 0m × 30.0 + 20d = 20 days",
                        "derived_from": [
                            {"label": "S4.original_max_days.years", "value": 0},
                            {"label": "S4.original_max_days.months", "value": 0},
                            {"label": "S4.original_max_days.days", "value": 20},
                        ],
                    },
                    {
                        "label": "S4.jail_credit",
                        "value": 30,
                        "formula": "0y × 365.0 + 0m × 30.0 + 30d = 30 days",
                        "derived_from": [
                            {"label": "S4.jail_credit.years", "value": 0},
                            {"label": "S4.jail_credit.months", "value": 0},
                            {"label": "S4.jail_credit.days", "value": 30},
                        ],
                    },
                    {
                        "label": "S4.good_time_credits_max",
                        "value": 0,
                        "formula": (
                            "20.0 days/month on 30 days of jail credit and month "
                            "by month from 2025-01-01 = 0"
                        ),
                        "derived_from": [
                            {
                                "label": "S4.original_max_days",
                                "value": 20,
                                "formula": "0y × 365.0 + 0m × 30.0 + 20d = 20 days",
                                "derived_from": [
                                    {
                                        "label": "S4.original_max_days.years",
                                        "value": 0,
                                    },
                                    {
                                        "label": "S4.original_max_days.months",
                                        "value": 0,
                                    },
                                    {
                                        "label": "S4.original_max_days.days",
                                        "value": 20,
                                    },
                                ],
                            },
                            {
                                "label": "S4.credit_rate",
                                "value": 20.0,
                                "formula": (
                                    "rate=20.0 days/month because stat_time "
                                    "applies to every sentence; reduces min_term "
                                    "because applies_to min_term matches "
                                    "offense_group=OTHER; reduces max_term because "
                                    "applies_to lists max_term with no conditions"
                                ),
                                "rule": "stat_time",
                            },
                            {
                                "label": "S4.jail_credit",
                                "value": 30,
                                "formula": "0y × 365.0 + 0m × 30.0 + 30d = 30 days",
                                "derived_from": [
                                    {"label": "S4.jail_credit.years", "value": 0},
                                    {"label": "S4.jail_credit.months", "value": 0},
                                    {"label": "S4.jail_credit.days", "value": 30},
                                ],
                            },
                        ],
                    },
                ],
            },
            adjustment.adjusted_max_days.to_derivation_tree(),
        )
        self.assertEqual(0, adjustment.adjusted_min_days.value)
        self.assertEqual(0, adjustment.max_term_reduction_days.value)


class TestReductionOffMinTermOnly(unittest.TestCase):
    """Tests for calculate_sentence_adjustment under reductions whose applies_to
    doesn't list the max term."""

    def test_term_multiplier_skips_unlisted_max_term(self) -> None:
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
                credit_rates=[],
                credit_caps=[],
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
        self.assertEqual(frozenset({TermType.MIN_TERM}), adjustment.reduced_terms)


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
                credit_rates=[],
                credit_caps=[],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_VIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        # The rule matched, so its multiplier is reported, but it reduced no terms.
        assert adjustment.term_multiplier is not None
        self.assertEqual(0.65, adjustment.term_multiplier.multiplier.value)
        self.assertEqual(frozenset(), adjustment.reduced_terms)
        # 730 - 30 and 1825 - 30, with no reduction
        self.assertEqual(700, adjustment.adjusted_min_days.value)
        self.assertEqual(1795, adjustment.adjusted_max_days.value)

    def test_credit_rate_skips_unlisted_max_term(self) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[
                    CreditRateRule(
                        name="min_term_stat_time",
                        description="Stat time off every offense's min term",
                        when=None,
                        rate=20.0,
                        applies_to=[ReductionTarget(term=TermType.MIN_TERM, when=None)],
                        statute=None,
                    )
                ],
                credit_caps=[],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_NONVIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        # 1825 - 30, with no credits
        self.assertEqual(1795, adjustment.adjusted_max_days.value)
        self.assertEqual(0, adjustment.max_term_reduction_days.value)


class TestNoReduction(unittest.TestCase):
    """Tests for calculate_sentence_adjustment under a regime with neither
    term_multipliers nor credit_rates."""

    def test_regime_without_reduction_subtracts_only_jail_credit(self) -> None:
        engine = RuleEngine(
            _regime(
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[],
                use_calendar_term_conversion=False,
            )
        )

        adjustment = calculate_sentence_adjustment(
            sentence=_NONVIOLENT_SENTENCE, start_date=_START_DATE, engine=engine
        )

        assert adjustment.term_multiplier is not None
        self.assertEqual(1.0, adjustment.term_multiplier.multiplier.value)
        self.assertEqual(frozenset(), adjustment.reduced_terms)
        # Only the 30 days of jail credit come off each term.
        self.assertEqual(700, adjustment.adjusted_min_days.value)
        self.assertEqual(1795, adjustment.adjusted_max_days.value)


if __name__ == "__main__":
    unittest.main()
