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
"""Tests for generating primary keys based on external IDs."""
import unittest
from typing import Set

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import get_all_entities_from_tree
from recidiviz.persistence.entity.generate_primary_key import generate_primary_key
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonExternalId,
)
from recidiviz.pipelines.ingest.state.generate_primary_keys import (
    generate_primary_keys_for_root_entity_tree,
    string_representation,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
    generate_full_graph_state_staff,
)


class TestGeneratePrimaryKey(unittest.TestCase):
    """Tests for generating primary keys based on external IDs."""

    def test_generate_primary_key_consistent(self) -> None:
        external_id_1 = ("ID1", "TYPE1")
        generated_primary_keys: Set[int] = set()
        for _ in range(10000):
            generated_primary_keys.add(
                generate_primary_key(
                    string_representation({external_id_1}), StateCode.US_DD
                )
            )
        self.assertEqual(len(generated_primary_keys), 1)

        external_id_2 = ("ID2", "TYPE2")
        external_id_3 = ("ID3", "TYPE3")
        generated_primary_keys.clear()
        for _ in range(2000):
            generated_primary_keys.add(
                generate_primary_key(
                    string_representation(
                        {external_id_1, external_id_2, external_id_3}
                    ),
                    StateCode.US_DD,
                )
            )
        self.assertEqual(len(generated_primary_keys), 1)

    def test_generate_primary_key_always_starts_with_fips_mask(self) -> None:
        external_id = ("ID", "TYPE")
        for state_code in StateCode:
            if state_code == StateCode.US_OZ:
                # OZ has a fips mask of 00, so we skip it.
                continue
            primary_key = generate_primary_key(
                string_representation({external_id}), state_code
            )
            self.assertTrue(
                str(primary_key).startswith(str(int(state_code.get_state().fips)))
            )

    def test_generate_primary_key_deterministic(self) -> None:
        external_id = ("ID", "TYPE")
        self.assertEqual(
            generate_primary_key(string_representation({external_id}), StateCode.US_MO),
            2925259285447670540,
        )
        self.assertEqual(
            generate_primary_key(string_representation({external_id}), StateCode.US_PA),
            4225259285447670540,
        )

    def test_generate_primary_keys_for_root_entity_tree_person(self) -> None:
        person = generate_full_graph_state_person(
            set_back_edges=True,
            include_person_back_edges=True,
            set_ids=False,
        )
        state_code = StateCode(person.state_code)
        all_entities = get_all_entities_from_tree(person)
        person_primary_key = generate_primary_key(
            string_representation(
                {
                    (external_id.external_id, external_id.id_type)
                    for external_id in person.external_ids
                }
            ),
            state_code=state_code,
        )
        _ = generate_primary_keys_for_root_entity_tree(
            root_primary_key=person_primary_key,
            root_entity=person,
            state_code=state_code,
        )
        for entity in all_entities:
            if isinstance(entity, person.__class__):
                self.assertEqual(entity.get_id(), person_primary_key)
            self.assertIsNotNone(entity.get_id())
            self.assertTrue(
                str(entity.get_id()).startswith(str(int(state_code.get_state().fips)))
            )

    def test_generate_primary_keys_for_root_entity_tree_staff(self) -> None:
        staff = generate_full_graph_state_staff(set_back_edges=True, set_ids=False)
        state_code = StateCode(staff.state_code)
        all_entities = get_all_entities_from_tree(staff)
        staff_primary_key = generate_primary_key(
            string_representation(
                {
                    (external_id.external_id, external_id.id_type)
                    for external_id in staff.external_ids
                }
            ),
            state_code=state_code,
        )
        _ = generate_primary_keys_for_root_entity_tree(
            root_primary_key=staff_primary_key,
            root_entity=staff,
            state_code=state_code,
        )
        for entity in all_entities:
            if isinstance(entity, staff.__class__):
                self.assertEqual(entity.get_id(), staff_primary_key)
            self.assertIsNotNone(entity.get_id())
            self.assertTrue(
                str(entity.get_id()).startswith(str(int(state_code.get_state().fips)))
            )

    def test_generate_primary_keys_for_root_entity_external_id_entities_share_same_id_value(
        self,
    ) -> None:
        """There are certain circumstances where two ExternalIdEntity objects have the same
        external_id value but not the same id_type. We should make primary key generation
        resilient to that."""
        external_id_1 = StatePersonExternalId.new_with_defaults(
            state_code=StateCode.US_XX.value, external_id="ID", id_type="TYPE1"
        )
        external_id_2 = StatePersonExternalId.new_with_defaults(
            state_code=StateCode.US_XX.value, external_id="ID", id_type="TYPE2"
        )
        person = StatePerson.new_with_defaults(
            state_code=StateCode.US_XX.value,
            external_ids=[external_id_1, external_id_2],
        )
        external_id_1.person = person
        external_id_2.person = person

        person_primary_key = generate_primary_key(
            string_representation(
                {
                    (external_id.external_id, external_id.id_type)
                    for external_id in person.external_ids
                }
            ),
            state_code=StateCode.US_XX,
        )
        _ = generate_primary_keys_for_root_entity_tree(
            root_primary_key=person_primary_key,
            root_entity=person,
            state_code=StateCode.US_XX,
        )
        self.assertEqual(person.get_id(), person_primary_key)
        self.assertNotEqual(external_id_1.get_id(), external_id_2.get_id())
        self.assertTrue(
            str(external_id_1.get_id()).startswith(
                str(int(StateCode.US_XX.get_state().fips))
            )
        )
        self.assertTrue(
            str(external_id_2.get_id()).startswith(
                str(int(StateCode.US_XX.get_state().fips))
            )
        )
        self.assertTrue(
            str(external_id_1.get_id()).startswith(
                str(int(StateCode.US_XX.get_state().fips))
            )
        )
        self.assertTrue(
            str(person.get_id()).startswith(str(int(StateCode.US_XX.get_state().fips)))
        )
