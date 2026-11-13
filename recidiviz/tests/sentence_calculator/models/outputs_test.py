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
"""Tests for recidiviz/sentence_calculator/models/outputs.py."""

import unittest
from datetime import date

from recidiviz.sentence_calculator.models.outputs import (
    ProjectedCreditRate,
    TermMultiplier,
    TrackedSentenceAdjustment,
)
from recidiviz.sentence_calculator.models.rules import TermType
from recidiviz.sentence_calculator.models.tracked import TrackedValue

# Term lengths shared by every adjustment below. The tests only vary which
# reduction the adjustment holds.
_ORIGINAL_MIN_DAYS = TrackedValue.input(730, "S1.original_min_days")
_ORIGINAL_MAX_DAYS = TrackedValue.input(1825, "S1.original_max_days")
_ADJUSTED_MIN_DAYS = TrackedValue.input(700, "S1.adjusted_min_days")
_ADJUSTED_MAX_DAYS = TrackedValue.input(1156, "S1.adjusted_max_days")
_MAX_TERM_REDUCTION_DAYS = TrackedValue.input(639, "S1.max_term_reduction_days")
_JAIL_CREDIT = TrackedValue.input(30, "S1.jail_credit")
_RETRO_DATE = TrackedValue.input(date(2024, 2, 14), "S1.retro_date")


class TestTrackedSentenceAdjustment(unittest.TestCase):
    """Tests for TrackedSentenceAdjustment's reduction fields."""

    def test_reduced_terms_come_from_term_multiplier(self) -> None:
        adjustment = TrackedSentenceAdjustment(
            sentence_id="S1",
            original_min_days=_ORIGINAL_MIN_DAYS,
            original_max_days=_ORIGINAL_MAX_DAYS,
            adjusted_min_days=_ADJUSTED_MIN_DAYS,
            adjusted_max_days=_ADJUSTED_MAX_DAYS,
            max_term_reduction_days=_MAX_TERM_REDUCTION_DAYS,
            term_multiplier=TermMultiplier(
                multiplier=TrackedValue.input(0.65, "S1.term_multiplier"),
                reduced_terms=frozenset({TermType.MAX_TERM}),
            ),
            credit_rate=None,
            jail_credit=_JAIL_CREDIT,
            retro_date=_RETRO_DATE,
        )

        self.assertEqual(frozenset({TermType.MAX_TERM}), adjustment.reduced_terms)

    def test_reduced_terms_come_from_credit_rate(self) -> None:
        adjustment = TrackedSentenceAdjustment(
            sentence_id="S1",
            original_min_days=_ORIGINAL_MIN_DAYS,
            original_max_days=_ORIGINAL_MAX_DAYS,
            adjusted_min_days=_ADJUSTED_MIN_DAYS,
            adjusted_max_days=_ADJUSTED_MAX_DAYS,
            max_term_reduction_days=_MAX_TERM_REDUCTION_DAYS,
            term_multiplier=None,
            credit_rate=ProjectedCreditRate(
                rate=TrackedValue.input(20.0, "S1.credit_rate"),
                reduced_terms=frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            ),
            jail_credit=_JAIL_CREDIT,
            retro_date=_RETRO_DATE,
        )

        self.assertEqual(
            frozenset({TermType.MIN_TERM, TermType.MAX_TERM}),
            adjustment.reduced_terms,
        )

    def test_rejects_neither_term_multiplier_nor_credit_rate(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Sentence adjustment \[S1\] must have exactly one of a term "
            r"multiplier or a credit rate\. Found term_multiplier \[None\] and "
            r"credit_rate \[None\]\.$",
        ):
            TrackedSentenceAdjustment(
                sentence_id="S1",
                original_min_days=_ORIGINAL_MIN_DAYS,
                original_max_days=_ORIGINAL_MAX_DAYS,
                adjusted_min_days=_ADJUSTED_MIN_DAYS,
                adjusted_max_days=_ADJUSTED_MAX_DAYS,
                max_term_reduction_days=_MAX_TERM_REDUCTION_DAYS,
                term_multiplier=None,
                credit_rate=None,
                jail_credit=_JAIL_CREDIT,
                retro_date=_RETRO_DATE,
            )

    def test_rejects_both_term_multiplier_and_credit_rate(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Sentence adjustment \[S1\] must have exactly one of a term "
            r"multiplier or a credit rate\. Found term_multiplier "
            r"\[TermMultiplier\(.*\)\] and credit_rate \[ProjectedCreditRate\(.*\)\]\.$",
        ):
            TrackedSentenceAdjustment(
                sentence_id="S1",
                original_min_days=_ORIGINAL_MIN_DAYS,
                original_max_days=_ORIGINAL_MAX_DAYS,
                adjusted_min_days=_ADJUSTED_MIN_DAYS,
                adjusted_max_days=_ADJUSTED_MAX_DAYS,
                max_term_reduction_days=_MAX_TERM_REDUCTION_DAYS,
                term_multiplier=TermMultiplier(
                    multiplier=TrackedValue.input(0.65, "S1.term_multiplier"),
                    reduced_terms=frozenset({TermType.MAX_TERM}),
                ),
                credit_rate=ProjectedCreditRate(
                    rate=TrackedValue.input(20.0, "S1.credit_rate"),
                    reduced_terms=frozenset({TermType.MAX_TERM}),
                ),
                jail_credit=_JAIL_CREDIT,
                retro_date=_RETRO_DATE,
            )


if __name__ == "__main__":
    unittest.main()
