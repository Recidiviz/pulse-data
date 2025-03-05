# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for entity_explore_builder.py."""
import unittest

from recidiviz.tests.persistence.entity import fake_entities
from recidiviz.tests.persistence.entity.fake_entities_module_context import (
    FakeEntitiesModuleContext,
)
from recidiviz.tools.looker.entity.entity_explore_builder import (
    EntityLookMLExploreBuilder,
)


class TestEntityLookMLExploreBuilder(unittest.TestCase):
    """Tests for EntityLookMLExploreBuilder."""

    def test_build_explore(self) -> None:
        expected_explore = """
explore: fake_person_template {
  extension: required
  extends: [
    fake_entity,
    fake_person_external_id
  ]

  view_name: fake_person
  view_label: "fake_person"

  group_label: "Fake"
  join: fake_entity {
    sql_on: ${fake_person.fake_person_id} = ${fake_entity.fake_person_id};;
    relationship: one_to_many
  }

  join: fake_person_external_id {
    sql_on: ${fake_person.fake_person_id} = ${fake_person_external_id.fake_person_id};;
    relationship: one_to_many
  }

}
explore: fake_entity {
  extension: required
  extends: [
    fake_another_entity_fake_entity_association
  ]

  join: fake_another_entity_fake_entity_association {
    sql_on: ${fake_entity.fake_entity_id} = ${fake_another_entity_fake_entity_association.fake_entity_id};;
    relationship: one_to_many
  }

}
explore: fake_another_entity_fake_entity_association {
  extension: required

  join: fake_another_entity {
    sql_on: ${fake_another_entity_fake_entity_association.fake_another_entity_id} = ${fake_another_entity.fake_another_entity_id};;
    relationship: many_to_one
  }

}
explore: fake_person_external_id {
  extension: required

}
"""

        explores = EntityLookMLExploreBuilder(
            module_context=FakeEntitiesModuleContext(),
            root_entity_cls=fake_entities.FakePerson,
            group_label="Fake",
        ).build()
        explore_str = "\n".join([e.build() for e in explores])

        self.assertEqual(expected_explore.strip(), explore_str.strip())
