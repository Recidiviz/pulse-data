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
"""Tests for recidiviz/sentence_calculator/engine/calculator.py, under Nevada's
SB_413 regime."""

import unittest
from datetime import date
from unittest import mock

from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator.engine import calculator
from recidiviz.sentence_calculator.engine.calculator import calculate_sentences
from recidiviz.sentence_calculator.models.inputs import (
    SentenceCalculationInput,
    SentenceType,
    Term,
)
from recidiviz.sentence_calculator.models.rules import TermType
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NevadaOffenseCategory,
    NevadaOffenseGroup,
    NevadaSentence,
)

_SB_413 = "SB_413"


class TestCalculateSentences(unittest.TestCase):
    """Tests for calculate_sentences with determinate sentences."""

    def test_single_sentence_standard_reduction(self) -> None:
        calculation_input = SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_SB_413,
            sentences=[
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S1",
                    offense_id=9999,
                    start_date=date(2025, 1, 1),
                    min_term=Term(days=1000),
                    max_term=Term(days=2000),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                )
            ],
        )

        result = calculate_sentences(calculation_input)

        self.assertIsNone(result.indeterminate_sentence_type)
        assert result.longest_path is not None
        self.assertEqual(["S1"], result.longest_path.path_sentence_ids)
        [adjustment] = result.sentence_adjustments
        # int(1000 * 0.65) and int(2000 * 0.65)
        self.assertEqual(650, adjustment.adjusted_min_days.value)
        self.assertEqual(1300, adjustment.adjusted_max_days.value)
        self.assertEqual(
            frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            adjustment.reduced_terms,
        )
        assert result.projected_dates is not None
        self.assertEqual(
            {
                # 2025-01-01 + 650 days - 1
                "parole_eligibility_date": date(2026, 10, 12),
                # 2025-01-01 + 1300 days - 1
                "projected_expiration_date": date(2028, 7, 23),
                # projected_expiration_date - 12 months, since the unadjusted max
                # term is at least three years.
                "mandatory_parole_release_date": date(2027, 7, 23),
                # 2025-01-01 + the unadjusted 1000 day min term - 1
                "base_parole_eligibility_date": date(2027, 9, 27),
            },
            {
                name: tracked.value
                for name, tracked in result.projected_dates.dates.items()
            },
        )

    def test_category_a_sentence_reduces_max_term_only(self) -> None:
        calculation_input = SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_SB_413,
            sentences=[
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S1",
                    offense_id=9999,
                    start_date=date(2025, 1, 1),
                    min_term=Term(days=1000),
                    max_term=Term(days=2000),
                    offense_category=NevadaOffenseCategory.CATEGORY_A,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                )
            ],
        )

        [adjustment] = calculate_sentences(calculation_input).sentence_adjustments

        self.assertEqual(1000, adjustment.adjusted_min_days.value)
        self.assertEqual(1300, adjustment.adjusted_max_days.value)
        self.assertEqual(frozenset({TermType.MAX_TERM}), adjustment.reduced_terms)

    def test_jail_credit_comes_off_both_terms(self) -> None:
        calculation_input = SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_SB_413,
            sentences=[
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S1",
                    offense_id=9999,
                    start_date=date(2025, 1, 1),
                    min_term=Term(days=1000),
                    max_term=Term(days=2000),
                    initial_time_served=Term(days=100),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                )
            ],
        )

        [adjustment] = calculate_sentences(calculation_input).sentence_adjustments

        self.assertEqual(550, adjustment.adjusted_min_days.value)
        self.assertEqual(1200, adjustment.adjusted_max_days.value)

    def test_consecutive_chain_sums_adjusted_terms(self) -> None:
        calculation_input = SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_SB_413,
            sentences=[
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S1",
                    offense_id=9999,
                    start_date=date(2025, 1, 1),
                    min_term=Term(days=400),
                    max_term=Term(days=1000),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                ),
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S2",
                    offense_id=9999,
                    parent_sentence_id="S1",
                    min_term=Term(days=200),
                    max_term=Term(days=500),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                ),
            ],
        )

        result = calculate_sentences(calculation_input)

        assert result.longest_path is not None
        self.assertEqual(["S1", "S2"], result.longest_path.path_sentence_ids)
        # 650 + 325
        self.assertEqual(975, result.longest_path.total_days.value)
        assert result.projected_dates is not None
        # S1 starts 2025-01-01, so the path's adjusted max of 975 days ends on
        # 2027-09-02.
        self.assertEqual(
            date(2027, 9, 2),
            result.projected_dates.dates["projected_expiration_date"].value,
        )

    def test_good_time_days_counts_the_reduction_jail_credit_covers(self) -> None:
        """Jail credit covers all of the reduced max term, so the 128 days the
        reduction took off it still count as good time."""
        calculation_input = SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_SB_413,
            sentences=[
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S1",
                    offense_id=9999,
                    start_date=date(2025, 1, 1),
                    min_term=Term(days=100),
                    max_term=Term(days=365),
                    initial_time_served=Term(days=300),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                )
            ],
        )

        with mock.patch.object(
            calculator,
            "evaluate_date_formulas",
            wraps=calculator.evaluate_date_formulas,
        ) as mock_evaluate_date_formulas:
            calculate_sentences(calculation_input)

        context = mock_evaluate_date_formulas.call_args.kwargs["context"]
        # max(0, int(365 * 0.65) - 300)
        self.assertEqual(0, context.adjusted_max_days.value)
        # 365 - int(365 * 0.65)
        self.assertEqual(128, context.good_time_days.value)

    def test_concurrent_sentences_longer_one_controls(self) -> None:
        calculation_input = SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_SB_413,
            sentences=[
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S1",
                    offense_id=9999,
                    start_date=date(2025, 1, 1),
                    min_term=Term(days=400),
                    max_term=Term(days=1000),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                ),
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S2",
                    offense_id=9999,
                    start_date=date(2025, 1, 1),
                    min_term=Term(days=400),
                    max_term=Term(days=2000),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                ),
            ],
        )

        result = calculate_sentences(calculation_input)

        assert result.longest_path is not None
        self.assertEqual(["S2"], result.longest_path.path_sentence_ids)
        self.assertEqual(1300, result.longest_path.total_days.value)

    def test_parole_eligibility_date_can_come_from_sentence_that_never_controls(
        self,
    ) -> None:
        """S2's reduced max term ends later, so S2 controls, but S1's min term
        isn't reduced because S1 is a category A felony. S1 has the later PED."""
        calculation_input = SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_SB_413,
            sentences=[
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S1",
                    offense_id=9999,
                    start_date=date(2025, 1, 1),
                    min_term=Term(days=1000),
                    max_term=Term(days=2000),
                    offense_category=NevadaOffenseCategory.CATEGORY_A,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                ),
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S2",
                    offense_id=9999,
                    start_date=date(2025, 1, 1),
                    min_term=Term(days=400),
                    max_term=Term(days=2200),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                ),
            ],
        )

        result = calculate_sentences(calculation_input)

        assert result.longest_path is not None
        self.assertEqual(["S2"], result.longest_path.path_sentence_ids)
        assert result.projected_dates is not None
        self.assertEqual(
            {
                # S1's PED, 2025-01-01 + its unreduced 1000 day min term - 1. S2's
                # PED, 2025-01-01 + 260 days - 1, is earlier.
                "parole_eligibility_date": date(2027, 9, 27),
                # 2025-01-01 + S2's adjusted 1430 day max term - 1
                "projected_expiration_date": date(2028, 11, 30),
                "mandatory_parole_release_date": date(2027, 11, 30),
                # 2025-01-01 + S2's unadjusted 400 day min term - 1
                "base_parole_eligibility_date": date(2026, 2, 4),
            },
            {
                name: tracked.value
                for name, tracked in result.projected_dates.dates.items()
            },
        )
        self.assertEqual(
            (result.longest_path.max_ped,),
            result.projected_dates.dates["parole_eligibility_date"].sources,
        )

    def test_mandatory_parole_release_left_out_for_short_max_term(self) -> None:
        calculation_input = SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_SB_413,
            sentences=[
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S1",
                    offense_id=9999,
                    start_date=date(2025, 1, 1),
                    min_term=Term(days=400),
                    # A day short of the 3 years the mandatory parole release
                    # rule needs.
                    max_term=Term(days=1094),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                )
            ],
        )

        projected_dates = calculate_sentences(calculation_input).projected_dates

        assert projected_dates is not None
        self.assertEqual(
            [
                "parole_eligibility_date",
                "projected_expiration_date",
                "base_parole_eligibility_date",
            ],
            list(projected_dates.dates),
        )

    def test_mandatory_parole_release_counts_calendar_years(self) -> None:
        """From 2024-01-01, a max term of 2 years and 364 days is 1095 days
        because 2024 is a leap year. That's as many days as 3 years from
        2025-01-01, but it's still short of 3 years."""
        calculation_input = SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_SB_413,
            sentences=[
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S1",
                    offense_id=9999,
                    start_date=date(2024, 1, 1),
                    min_term=Term(days=400),
                    max_term=Term(years=2, days=364),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                )
            ],
        )

        projected_dates = calculate_sentences(calculation_input).projected_dates

        assert projected_dates is not None
        self.assertEqual(
            [
                "parole_eligibility_date",
                "projected_expiration_date",
                "base_parole_eligibility_date",
            ],
            list(projected_dates.dates),
        )

    def test_mandatory_parole_release_counts_years_from_retro_date(self) -> None:
        """100 days of jail credit puts the RETRO date at 2024-01-01, so the max
        term of 2 years and 364 days is 1095 days, as in
        test_mandatory_parole_release_counts_calendar_years. Counted from the
        2024-04-10 start date instead, 1095 days would be exactly 3 years."""
        calculation_input = SentenceCalculationInput(
            state_code=StateCode.US_NV,
            law_regime_id=_SB_413,
            sentences=[
                NevadaSentence(  # pylint: disable=abstract-class-instantiated
                    sentence_id="S1",
                    offense_id=9999,
                    start_date=date(2024, 4, 10),
                    min_term=Term(days=400),
                    max_term=Term(years=2, days=364),
                    initial_time_served=Term(days=100),
                    offense_category=NevadaOffenseCategory.CATEGORY_C,
                    offense_group=NevadaOffenseGroup.OTHER,
                    sentence_type=SentenceType.OTHER,
                )
            ],
        )

        projected_dates = calculate_sentences(calculation_input).projected_dates

        assert projected_dates is not None
        self.assertEqual(
            [
                "parole_eligibility_date",
                "projected_expiration_date",
                "base_parole_eligibility_date",
            ],
            list(projected_dates.dates),
        )

    def test_no_sentences(self) -> None:
        result = calculate_sentences(
            SentenceCalculationInput(
                state_code=StateCode.US_NV, law_regime_id=_SB_413, sentences=[]
            )
        )

        self.assertIsNone(result.indeterminate_sentence_type)
        self.assertIsNone(result.longest_path)
        self.assertIsNone(result.projected_dates)
        self.assertEqual([], result.sentence_adjustments)

    def test_unknown_regime(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^No law regime \[SB_999\] defined for state \[US_NV\]\."
        ):
            calculate_sentences(
                SentenceCalculationInput(
                    state_code=StateCode.US_NV, law_regime_id="SB_999", sentences=[]
                )
            )


class TestCalculateIndeterminateSentences(unittest.TestCase):
    """Tests for calculate_sentences with DEATH, LWO, and LIFE sentences."""

    def test_death_sentence_has_no_path(self) -> None:
        result = calculate_sentences(
            SentenceCalculationInput(
                state_code=StateCode.US_NV,
                law_regime_id=_SB_413,
                sentences=[
                    NevadaSentence(  # pylint: disable=abstract-class-instantiated
                        sentence_id="S1",
                        offense_id=9999,
                        start_date=date(2025, 1, 1),
                        min_term=Term(),
                        max_term=Term(),
                        offense_category=NevadaOffenseCategory.CATEGORY_A,
                        offense_group=NevadaOffenseGroup.VIOLENCE,
                        sentence_type=SentenceType.DEATH,
                    )
                ],
            )
        )

        self.assertEqual(SentenceType.DEATH, result.indeterminate_sentence_type)
        self.assertIsNone(result.longest_path)
        self.assertIsNone(result.projected_dates)

    # TODO(NV-9): Fix to match realistic life sentence input
    def test_lwo_outranks_life(self) -> None:
        result = calculate_sentences(
            SentenceCalculationInput(
                state_code=StateCode.US_NV,
                law_regime_id=_SB_413,
                sentences=[
                    NevadaSentence(  # pylint: disable=abstract-class-instantiated
                        sentence_id="S1",
                        offense_id=9999,
                        start_date=date(2025, 1, 1),
                        min_term=Term(years=20),
                        max_term=Term(years=50),
                        offense_category=NevadaOffenseCategory.CATEGORY_A,
                        offense_group=NevadaOffenseGroup.VIOLENCE,
                        sentence_type=SentenceType.LIFE,
                    ),
                    NevadaSentence(  # pylint: disable=abstract-class-instantiated
                        sentence_id="S2",
                        offense_id=9999,
                        start_date=date(2025, 1, 1),
                        min_term=Term(),
                        max_term=Term(),
                        offense_category=NevadaOffenseCategory.CATEGORY_A,
                        offense_group=NevadaOffenseGroup.VIOLENCE,
                        sentence_type=SentenceType.LWO,
                    ),
                ],
            )
        )

        self.assertEqual(SentenceType.LWO, result.indeterminate_sentence_type)
        self.assertIsNone(result.longest_path)

    # TODO(NV-9): Fix to match realistic life sentence input
    def test_life_sentence_has_path_but_no_projected_dates(self) -> None:
        result = calculate_sentences(
            SentenceCalculationInput(
                state_code=StateCode.US_NV,
                law_regime_id=_SB_413,
                sentences=[
                    NevadaSentence(  # pylint: disable=abstract-class-instantiated
                        sentence_id="S1",
                        offense_id=9999,
                        start_date=date(2025, 1, 1),
                        min_term=Term(years=20),
                        max_term=Term(years=50),
                        offense_category=NevadaOffenseCategory.CATEGORY_A,
                        offense_group=NevadaOffenseGroup.VIOLENCE,
                        sentence_type=SentenceType.LIFE,
                    )
                ],
            )
        )

        self.assertEqual(SentenceType.LIFE, result.indeterminate_sentence_type)
        assert result.longest_path is not None
        self.assertEqual(["S1"], result.longest_path.path_sentence_ids)
        self.assertIsNone(result.projected_dates)


if __name__ == "__main__":
    unittest.main()
