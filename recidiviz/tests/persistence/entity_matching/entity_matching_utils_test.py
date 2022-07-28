# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for entity_matching_utils.py."""
from datetime import datetime
from unittest import TestCase

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity_matching.entity_matching_utils import get_only_match

_DATE = datetime(2018, 12, 13)
_DATE_OTHER = datetime(2017, 12, 13)


class TestEntityMatchingUtils(TestCase):
    """Tests for entity matching logic"""

    def test_get_only_match_duplicates(self) -> None:
        def match(
            db_entity,
            ingested_entity,
            field_index,  # pylint: disable=unused-argument
        ) -> bool:
            return db_entity.birthdate == ingested_entity.birthdate

        person = state_entities.StatePerson.new_with_defaults(
            state_code=StateCode.US_XX.value, person_id=1, birthdate=_DATE
        )
        person_2 = state_entities.StatePerson.new_with_defaults(
            state_code=StateCode.US_XX.value, person_id=2, birthdate=_DATE_OTHER
        )

        ing_person = state_entities.StatePerson.new_with_defaults(
            state_code=StateCode.US_XX.value, birthdate=_DATE
        )

        self.assertEqual(
            get_only_match(
                ing_person,
                [person, person_2, person],
                matcher=match,
                field_index=CoreEntityFieldIndex(),
            ),
            person,
        )
