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
"""Tests for ArizonaSentence and its registration as Arizona's sentence class."""

import unittest
from datetime import date

from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator.models.conditions import (
    sentence_condition_field_types,
)
from recidiviz.sentence_calculator.models.inputs import SentenceType, Term
from recidiviz.sentence_calculator.models.state_inputs import sentence_class
from recidiviz.sentence_calculator.models.state_inputs.inputs_az import (
    ArizonaOffenseCategory,
    ArizonaOffenseGroup,
    ArizonaSentence,
)


class TestArizonaSentence(unittest.TestCase):
    """Tests for ArizonaSentence."""

    def test_construct_sentence_with_every_field(self) -> None:
        sentence = ArizonaSentence(  # pylint: disable=abstract-class-instantiated
            sentence_id="CR2019-001234-001",
            min_term=Term(years=5),
            max_term=Term(years=5),
            start_date=date(2020, 3, 12),
            initial_time_served=Term(days=47),
            offense_category=ArizonaOffenseCategory.CLASS_3,
            offense_group=ArizonaOffenseGroup.OTHER,
            offense_date=date(2019, 8, 2),
            date_sentenced=date(2020, 3, 12),
            statute="13-1802",
            subsection=None,
            is_flat=False,
            is_community_supervision_imposed=True,
            is_prison_a_condition_of_probation=False,
        )

        self.assertEqual(ArizonaOffenseCategory.CLASS_3, sentence.offense_category)
        self.assertEqual(ArizonaOffenseGroup.OTHER, sentence.offense_group)
        self.assertEqual(date(2019, 8, 2), sentence.offense_date)
        self.assertEqual("13-1802", sentence.statute)
        self.assertIsNone(sentence.sentence_type)
        self.assertIsNone(sentence.parent_sentence_id)

    def test_fields_conditions_can_compare(self) -> None:
        """The regime files' when conditions can only compare these fields, so
        a field dropped from this list silently stops being usable in a rule."""
        self.assertEqual(
            {
                "sentence_id": str,
                "sentence_type": SentenceType,
                "start_date": date,
                "parent_sentence_id": str,
                "offense_category": ArizonaOffenseCategory,
                "offense_group": ArizonaOffenseGroup,
                "offense_date": date,
                "date_sentenced": date,
                "statute": str,
                "subsection": str,
                "is_flat": bool,
                "is_community_supervision_imposed": bool,
                "is_prison_a_condition_of_probation": bool,
            },
            sentence_condition_field_types(ArizonaSentence),
        )


class TestArizonaSentenceRegistration(unittest.TestCase):
    """Tests that US_AZ dispatches to ArizonaSentence."""

    def test_sentence_class_for_arizona(self) -> None:
        self.assertIs(ArizonaSentence, sentence_class(StateCode.US_AZ))


if __name__ == "__main__":
    unittest.main()
