# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for root_entity_utils.py"""
import unittest

from recidiviz.persistence.entity.root_entity_utils import (
    get_entity_class_name_to_root_entity_class_name,
    get_root_entity_class_for_entity,
    get_root_entity_id,
)
from recidiviz.persistence.entity.state import entities, normalized_entities


class RootEntityUtilsTest(unittest.TestCase):
    """Tests for root_entity_utils.py"""

    def test_get_root_entity_class(self) -> None:
        self.assertEqual(
            entities.StatePerson, get_root_entity_class_for_entity(entities.StatePerson)
        )
        self.assertEqual(
            entities.StatePerson,
            get_root_entity_class_for_entity(entities.StateAssessment),
        )
        self.assertEqual(
            entities.StateStaff,
            get_root_entity_class_for_entity(entities.StateStaffRolePeriod),
        )

    def test_get_root_entity_class_normalized(self) -> None:
        self.assertEqual(
            normalized_entities.NormalizedStatePerson,
            get_root_entity_class_for_entity(normalized_entities.NormalizedStatePerson),
        )
        self.assertEqual(
            normalized_entities.NormalizedStatePerson,
            get_root_entity_class_for_entity(
                normalized_entities.NormalizedStateAssessment
            ),
        )
        self.assertEqual(
            normalized_entities.NormalizedStatePerson,
            get_root_entity_class_for_entity(
                normalized_entities.NormalizedStateAssessment
            ),
        )
        self.assertEqual(
            normalized_entities.NormalizedStateStaff,
            get_root_entity_class_for_entity(
                normalized_entities.NormalizedStateStaffRolePeriod
            ),
        )

    def test_get_root_entity_id_person(self) -> None:
        person = entities.StatePerson.new_with_defaults(
            person_id=123, state_code="US_XX"
        )
        external_id = entities.StatePersonExternalId.new_with_defaults(
            person_external_id_id=345,
            external_id="11111",
            id_type="US_XX_TYPE",
            state_code="US_XX",
            person=person,
        )
        person.external_ids.append(external_id)

        self.assertEqual(123, get_root_entity_id(external_id))
        self.assertEqual(123, get_root_entity_id(person))

    def test_get_root_entity_id_staff(self) -> None:
        staff = entities.StateStaff.new_with_defaults(staff_id=789, state_code="US_XX")
        staff_external_id = entities.StateStaffExternalId.new_with_defaults(
            staff_external_id_id=910,
            external_id="11111",
            id_type="US_ND_TYPE",
            state_code="US_ND",
            staff=staff,
        )
        staff.external_ids.append(staff_external_id)

        self.assertEqual(789, get_root_entity_id(staff_external_id))
        self.assertEqual(789, get_root_entity_id(staff))

    def test_get_entity_class_name_to_root_entity_class_name(self) -> None:
        root_entity_mapping = get_entity_class_name_to_root_entity_class_name(
            entities_module=entities
        )

        self.assertEqual("state_person", root_entity_mapping["state_assessment"])
        self.assertEqual("state_person", root_entity_mapping["state_person"])

        self.assertEqual("state_staff", root_entity_mapping["state_staff_role_period"])
        self.assertEqual("state_staff", root_entity_mapping["state_staff"])
