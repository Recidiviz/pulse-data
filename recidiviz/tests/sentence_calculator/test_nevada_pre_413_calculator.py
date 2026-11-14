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
"""Tests for the Nevada pre-SB 413 regime (NRS 209.4465).

The rules under test are in recidiviz/sentence_calculator/rules/us_nv/pre_413.yaml.
The behavior these tests pin down most carefully is the NRS 209.4465(8) carve-out:
credits come off the maximum term for everyone, but off the minimum term only for
offenders who were not convicted of a violent felony, a felony sexual offense, a
felony DUI, or a category A or B felony.
"""

import unittest
from datetime import date

from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator.engine.calculator import calculate_sentences
from recidiviz.sentence_calculator.engine.rule_engine import RuleEngine
from recidiviz.sentence_calculator.models.inputs import (
    SentenceCalculationInput,
    SentenceType,
    Term,
    TermType,
)
from recidiviz.sentence_calculator.models.outputs import TrackedSentenceAdjustment
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NevadaOffenseCategory,
    NevadaOffenseGroup,
    NevadaSentence,
)
from recidiviz.sentence_calculator.rules.loader import load_law_regime

_REGIME_NAME = "PRE_413"
_START_DATE = date(2020, 1, 1)

# A felony DUI offense id from the NRS 484C carve-out list.
_DUI_OFFENSE_ID = 1577
# An offense id outside the NRS 484C carve-out list.
_NON_DUI_OFFENSE_ID = 9999

# Each ground on which NRS 209.4465(8) withholds credits from the minimum term, as
# (name, offense_category, offense_group, offense_id).
_CARVE_OUT_CASES = [
    (
        "category_a",
        NevadaOffenseCategory.CATEGORY_A,
        NevadaOffenseGroup.OTHER,
        _NON_DUI_OFFENSE_ID,
    ),
    (
        "category_b",
        NevadaOffenseCategory.CATEGORY_B,
        NevadaOffenseGroup.OTHER,
        _NON_DUI_OFFENSE_ID,
    ),
    (
        "violence",
        NevadaOffenseCategory.CATEGORY_C,
        NevadaOffenseGroup.VIOLENCE,
        _NON_DUI_OFFENSE_ID,
    ),
    (
        "sex_offense",
        NevadaOffenseCategory.CATEGORY_C,
        NevadaOffenseGroup.SEX,
        _NON_DUI_OFFENSE_ID,
    ),
    (
        "felony_dui",
        NevadaOffenseCategory.CATEGORY_C,
        NevadaOffenseGroup.OTHER,
        _DUI_OFFENSE_ID,
    ),
]


def _two_to_five_year_sentence(
    *,
    offense_category: NevadaOffenseCategory,
    offense_group: NevadaOffenseGroup,
    offense_id: int,
    qualified_for_enhanced_good_time: bool,
    initial_time_served: Term,
) -> NevadaSentence:
    """Returns a two-to-five year sentence starting 2020-01-01. Under the regime's
    calendar conversion, its min term is 731 days and its max term 1827 days."""
    return NevadaSentence(  # pylint: disable=abstract-class-instantiated
        sentence_id="S1",
        start_date=_START_DATE,
        min_term=Term(years=2),
        max_term=Term(years=5),
        initial_time_served=initial_time_served,
        offense_category=offense_category,
        offense_group=offense_group,
        offense_id=offense_id,
        qualified_for_enhanced_good_time=qualified_for_enhanced_good_time,
        sentence_type=SentenceType.OTHER,
    )


def _calculate_adjustment(sentence: NevadaSentence) -> TrackedSentenceAdjustment:
    [adjustment] = calculate_sentences(
        SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_REGIME_NAME,
            sentences=[sentence],
        )
    ).sentence_adjustments
    return adjustment


class TestNevadaPre413Rules(unittest.TestCase):
    """Tests for the pre-SB 413 regime settings and credit rate selection."""

    def setUp(self) -> None:
        self.engine = RuleEngine(
            load_law_regime(state_code=StateCode.US_NV, regime_name=_REGIME_NAME)
        )

    def test_regime_settings(self) -> None:
        self.assertEqual(
            ["work_time", "stat_time"],
            [rule.name for rule in self.engine.regime.credit_rates],
        )
        self.assertEqual([], self.engine.regime.term_multipliers)
        self.assertTrue(self.engine.regime.use_calendar_term_conversion)

    def test_stat_time_is_the_default_rate(self) -> None:
        """NRS 209.4465(1) grants 20 days per month to every offender, so the
        catch-all rule must match a sentence with no distinguishing features."""
        credit_rate = self.engine.get_credit_rate(
            _two_to_five_year_sentence(
                offense_category=NevadaOffenseCategory.CATEGORY_C,
                offense_group=NevadaOffenseGroup.OTHER,
                offense_id=_NON_DUI_OFFENSE_ID,
                qualified_for_enhanced_good_time=False,
                initial_time_served=Term(),
            )
        )

        self.assertEqual(20.0, credit_rate.rate.value)
        self.assertEqual("stat_time", credit_rate.rate.rule_name)
        self.assertEqual(
            frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            credit_rate.reduced_terms,
        )

    def test_carve_outs_withhold_stat_time_from_the_min_term(self) -> None:
        for name, offense_category, offense_group, offense_id in _CARVE_OUT_CASES:
            with self.subTest(carve_out=name):
                credit_rate = self.engine.get_credit_rate(
                    _two_to_five_year_sentence(
                        offense_category=offense_category,
                        offense_group=offense_group,
                        offense_id=offense_id,
                        qualified_for_enhanced_good_time=False,
                        initial_time_served=Term(),
                    )
                )
                self.assertEqual(20.0, credit_rate.rate.value)
                self.assertEqual(
                    frozenset({TermType.MAX_TERM}), credit_rate.reduced_terms
                )

    def test_carve_outs_withhold_work_time_from_the_min_term(self) -> None:
        """A carve-out offender still earns the higher work time rate against the
        maximum term, the carve-out only changes whether the min term gets credits."""
        for name, offense_category, offense_group, offense_id in _CARVE_OUT_CASES:
            with self.subTest(carve_out=name):
                credit_rate = self.engine.get_credit_rate(
                    _two_to_five_year_sentence(
                        offense_category=offense_category,
                        offense_group=offense_group,
                        offense_id=offense_id,
                        qualified_for_enhanced_good_time=True,
                        initial_time_served=Term(),
                    )
                )
                self.assertEqual(30.0, credit_rate.rate.value)
                self.assertEqual(
                    frozenset({TermType.MAX_TERM}), credit_rate.reduced_terms
                )

    def test_offense_id_outside_the_dui_list_is_not_carved_out(self) -> None:
        credit_rate = self.engine.get_credit_rate(
            _two_to_five_year_sentence(
                offense_category=NevadaOffenseCategory.CATEGORY_C,
                offense_group=NevadaOffenseGroup.OTHER,
                offense_id=_NON_DUI_OFFENSE_ID,
                qualified_for_enhanced_good_time=False,
                initial_time_served=Term(),
            )
        )

        self.assertEqual("stat_time", credit_rate.rate.rule_name)
        self.assertEqual(
            frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            credit_rate.reduced_terms,
        )


class TestNevadaPre413Calculator(unittest.TestCase):
    """Tests that the pre-SB 413 rules reach the calculated terms and dates."""

    def test_stat_time_reduces_both_terms(self) -> None:
        """At 20 days of credit per month served, each month covers about 50 days
        of the term, so a term is served in about 60% of its length. That stays
        below NRS 209.4465(9)'s 58% cap on the minimum term's reduction."""
        adjustment = _calculate_adjustment(
            _two_to_five_year_sentence(
                offense_category=NevadaOffenseCategory.CATEGORY_C,
                offense_group=NevadaOffenseGroup.OTHER,
                offense_id=_NON_DUI_OFFENSE_ID,
                qualified_for_enhanced_good_time=False,
                initial_time_served=Term(),
            )
        )

        self.assertEqual(731, adjustment.original_min_days.value)
        # The calendar months of 2020 cover 366 days served plus 240 days of
        # credit, January and February 2021 cover 99 more, and the last 26 days
        # take 16 days of March: 366 + 31 + 28 + 16. That earns 290 days of
        # credit, under the cap of int(731 * 0.58) = 423.
        self.assertEqual(441, adjustment.adjusted_min_days.value)
        self.assertEqual(1827, adjustment.original_max_days.value)
        self.assertEqual(1103, adjustment.adjusted_max_days.value)

    def test_carve_outs_leave_the_min_term_unreduced(self) -> None:
        """The maximum term is still reduced under NRS 209.4465(7)(a); only the
        minimum term is left whole."""
        for name, offense_category, offense_group, offense_id in _CARVE_OUT_CASES:
            with self.subTest(carve_out=name):
                adjustment = _calculate_adjustment(
                    _two_to_five_year_sentence(
                        offense_category=offense_category,
                        offense_group=offense_group,
                        offense_id=offense_id,
                        qualified_for_enhanced_good_time=False,
                        initial_time_served=Term(),
                    )
                )

                self.assertEqual(731, adjustment.adjusted_min_days.value)
                self.assertEqual(1103, adjustment.adjusted_max_days.value)

    def test_carve_out_delays_parole_eligibility_but_not_expiration(self) -> None:
        """Parole eligibility runs off the minimum term, so withholding credits
        from it pushes PED out while PEXD is unchanged."""
        standard = calculate_sentences(
            SentenceCalculationInput(
                state_code=StateCode.US_NV,
                law_regime_id=_REGIME_NAME,
                sentences=[
                    _two_to_five_year_sentence(
                        offense_category=NevadaOffenseCategory.CATEGORY_C,
                        offense_group=NevadaOffenseGroup.OTHER,
                        offense_id=_NON_DUI_OFFENSE_ID,
                        qualified_for_enhanced_good_time=False,
                        initial_time_served=Term(),
                    )
                ],
            )
        ).projected_dates
        carved_out = calculate_sentences(
            SentenceCalculationInput(
                state_code=StateCode.US_NV,
                law_regime_id=_REGIME_NAME,
                sentences=[
                    _two_to_five_year_sentence(
                        offense_category=NevadaOffenseCategory.CATEGORY_A,
                        offense_group=NevadaOffenseGroup.OTHER,
                        offense_id=_NON_DUI_OFFENSE_ID,
                        qualified_for_enhanced_good_time=False,
                        initial_time_served=Term(),
                    )
                ],
            )
        ).projected_dates

        assert standard is not None and carved_out is not None
        # 2020-01-01 + 441 days - 1, the last day of the reduced minimum term.
        self.assertEqual(
            date(2021, 3, 16), standard.dates["parole_eligibility_date"].value
        )
        # 2020-01-01 + 731 days - 1
        self.assertEqual(
            date(2021, 12, 31), carved_out.dates["parole_eligibility_date"].value
        )
        # 2020-01-01 + 1103 days - 1, for both.
        self.assertEqual(
            date(2023, 1, 7), standard.dates["projected_expiration_date"].value
        )
        self.assertEqual(
            date(2023, 1, 7), carved_out.dates["projected_expiration_date"].value
        )

    def test_jail_credit_still_comes_off_a_carved_out_min_term(self) -> None:
        """Withholding good time from the minimum must not also withhold jail
        credit, which is not a good time credit."""
        adjustment = _calculate_adjustment(
            _two_to_five_year_sentence(
                offense_category=NevadaOffenseCategory.CATEGORY_A,
                offense_group=NevadaOffenseGroup.OTHER,
                offense_id=_NON_DUI_OFFENSE_ID,
                qualified_for_enhanced_good_time=False,
                initial_time_served=Term(days=100),
            )
        )

        # 731 - 100
        self.assertEqual(631, adjustment.adjusted_min_days.value)

    def test_work_time_serves_about_half_of_each_term(self) -> None:
        """At the 30 days per month of NRS 209.4465(1)-(2), each month served
        covers about twice its length, so each term is served in about half its
        length."""
        adjustment = _calculate_adjustment(
            _two_to_five_year_sentence(
                offense_category=NevadaOffenseCategory.CATEGORY_C,
                offense_group=NevadaOffenseGroup.OTHER,
                offense_id=_NON_DUI_OFFENSE_ID,
                qualified_for_enhanced_good_time=True,
                initial_time_served=Term(),
            )
        )

        self.assertEqual(369, adjustment.adjusted_min_days.value)
        self.assertEqual(920, adjustment.adjusted_max_days.value)


if __name__ == "__main__":
    unittest.main()
