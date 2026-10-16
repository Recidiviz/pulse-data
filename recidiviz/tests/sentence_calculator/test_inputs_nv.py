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
"""Tests for NvSentenceWithSortableEndDate's comparators and _is_more_severe."""

import unittest
from datetime import date

from recidiviz.sentence_calculator.models.inputs import Term
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NevadaOffenseCategory,
    NevadaOffenseGroup,
    NevadaSentence,
    NvSentenceWithSortableEndDate,
)


def _sentence(
    sentence_id: str,
    *,
    end_date: date,
    offense_category: NevadaOffenseCategory,
    offense_group: NevadaOffenseGroup,
) -> NvSentenceWithSortableEndDate:
    return NvSentenceWithSortableEndDate(
        sentence=NevadaSentence(  # pylint: disable=abstract-class-instantiated
            sentence_id=sentence_id,
            min_term=Term(),
            max_term=Term(),
            offense_category=offense_category,
            offense_group=offense_group,
        ),
        end_date=end_date,
    )


class TestSentenceWithSortableEndDateComparators(unittest.TestCase):
    """Tests for NvSentenceWithSortableEndDate's comparators, which
    also exercise _is_more_severe as their tie-breaker."""

    def test_different_end_dates_decide_regardless_of_severity(self) -> None:
        """The earlier-ending sentence is "less than" the later-ending one even
        though it's the more severe sentence -- end date is checked first."""
        earlier_but_more_severe = _sentence(
            "A",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_A,
            offense_group=NevadaOffenseGroup.VIOLENCE,
        )
        later_but_less_severe = _sentence(
            "B",
            end_date=date(2025, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_E,
            offense_group=NevadaOffenseGroup.OTHER,
        )

        self.assertLess(earlier_but_more_severe, later_but_less_severe)
        self.assertGreater(later_but_less_severe, earlier_but_more_severe)

    def test_tied_end_date_falls_back_to_group_severity(self) -> None:
        """VIOLENCE outranks PROPERTY even when the PROPERTY sentence has the
        more severe category -- group is checked before category."""
        violent_low_category = _sentence(
            "A",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_E,
            offense_group=NevadaOffenseGroup.VIOLENCE,
        )
        property_high_category = _sentence(
            "B",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_A,
            offense_group=NevadaOffenseGroup.PROPERTY,
        )

        self.assertGreater(violent_low_category, property_high_category)
        self.assertLess(property_high_category, violent_low_category)

    def test_violence_and_sex_are_tied_in_group_severity(self) -> None:
        """VIOLENCE and SEX rank equally, so a tie between them falls through
        to category rather than being decided by group."""
        violence_high_category = _sentence(
            "A",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_A,
            offense_group=NevadaOffenseGroup.VIOLENCE,
        )
        sex_low_category = _sentence(
            "B",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_E,
            offense_group=NevadaOffenseGroup.SEX,
        )

        self.assertGreater(violence_high_category, sex_low_category)
        self.assertLess(sex_low_category, violence_high_category)

    def test_tied_group_falls_back_to_category_severity(self) -> None:
        """Same group on both sides, so category breaks the tie: CATEGORY_A is
        more severe than CATEGORY_E."""
        category_a = _sentence(
            "A",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_A,
            offense_group=NevadaOffenseGroup.OTHER,
        )
        category_e = _sentence(
            "B",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_E,
            offense_group=NevadaOffenseGroup.OTHER,
        )

        self.assertGreater(category_a, category_e)
        self.assertLess(category_e, category_a)

    def test_tied_group_and_category_falls_back_to_sentence_id(self) -> None:
        """Same group and category on both sides, so the lower sentence_id wins,
        purely to keep the ordering deterministic."""
        lower_id = _sentence(
            "A",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_A,
            offense_group=NevadaOffenseGroup.OTHER,
        )
        higher_id = _sentence(
            "B",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_A,
            offense_group=NevadaOffenseGroup.OTHER,
        )

        self.assertGreater(lower_id, higher_id)
        self.assertLess(higher_id, lower_id)

    def test_identical_sentences_are_equal_not_strictly_ordered(self) -> None:
        """Two sentences tied on end date, group, category, and sentence_id are
        not strictly less than or greater than each other, but <= and >= both
        hold. __lt__ calls _is_more_severe on the other instance, so this
        exercises the edge case where that call must return False for an
        identical sentence rather than treating the tie as "more severe". <=
        holds only because __eq__ treats the two as equal."""
        first = _sentence(
            "A",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_A,
            offense_group=NevadaOffenseGroup.OTHER,
        )
        second = _sentence(
            "A",
            end_date=date(2024, 1, 1),
            offense_category=NevadaOffenseCategory.CATEGORY_A,
            offense_group=NevadaOffenseGroup.OTHER,
        )

        self.assertFalse(first < second)
        self.assertFalse(first > second)
        self.assertTrue(first <= second)
        self.assertTrue(first >= second)


if __name__ == "__main__":
    unittest.main()
