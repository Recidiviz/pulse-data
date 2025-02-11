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
"""Tests for SchemaToEntityClassMapper."""
import datetime
import unittest

from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_entity_converter.schema_to_entity_class_mapper import (
    SchemaToEntityClassMapper,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.tests.persistence.database.schema_entity_converter import (
    fake_entities,
    fake_schema,
)
from recidiviz.tests.persistence.database.schema_entity_converter.fake_entities_module_context import (
    FakeEntitiesModuleContext,
)


class TestSchemaToEntityClassMapper(unittest.TestCase):
    """Tests for SchemaToEntityClassMapper."""

    def test_schema_cls_for_entity_cls(self) -> None:
        class_mapper = SchemaToEntityClassMapper(
            schema_module=fake_schema,
            entities_module_context=FakeEntitiesModuleContext(),
        )

        self.assertEqual(
            fake_schema.Root, class_mapper.schema_cls_for_entity_cls(fake_entities.Root)
        )
        self.assertEqual(
            fake_schema.Parent,
            class_mapper.schema_cls_for_entity_cls(fake_entities.Parent),
        )

    def test_schema_cls_for_entity_cls_does_not_exist(self) -> None:
        class_mapper = SchemaToEntityClassMapper(
            schema_module=fake_schema,
            entities_module_context=FakeEntitiesModuleContext(),
        )

        with self.assertRaisesRegex(ValueError, "Invalid entity class: StatePerson"):
            class_mapper.schema_cls_for_entity_cls(state_entities.StatePerson)

    def test_entity_cls_for_schema_cls(self) -> None:
        class_mapper = SchemaToEntityClassMapper(
            schema_module=fake_schema,
            entities_module_context=FakeEntitiesModuleContext(),
        )

        self.assertEqual(
            fake_entities.Root, class_mapper.entity_cls_for_schema_cls(fake_schema.Root)
        )
        self.assertEqual(
            fake_entities.Parent,
            class_mapper.entity_cls_for_schema_cls(fake_schema.Parent),
        )

    def test_entity_cls_for_schema_cls_does_not_exist(self) -> None:
        class_mapper = SchemaToEntityClassMapper(
            schema_module=fake_schema,
            entities_module_context=FakeEntitiesModuleContext(),
        )

        # StatePerson is not present in fake_schema
        with self.assertRaisesRegex(ValueError, "Invalid schema class: StatePerson"):
            class_mapper.entity_cls_for_schema_cls(state_schema.StatePerson)

        # While SchemaToEntityClassMapper construction shouldn't crash if there is
        # a schema table not in entities.py, we will crash if you try to map that schema
        # class to an entity class.
        with self.assertRaisesRegex(ValueError, "Invalid schema class: NotInEntities"):
            class_mapper.entity_cls_for_schema_cls(fake_schema.NotInEntities)

    def test_get_mapper(self) -> None:
        SchemaToEntityClassMapper.clear_cache()

        state_mapper = SchemaToEntityClassMapper.get(
            schema_module=state_schema, entities_module=state_entities
        )
        fake_mapper = SchemaToEntityClassMapper(
            schema_module=fake_schema,
            entities_module_context=FakeEntitiesModuleContext(),
        )
        self.assertEqual(
            fake_entities.Root, fake_mapper.entity_cls_for_schema_cls(fake_schema.Root)
        )
        self.assertEqual(
            fake_schema.Root, fake_mapper.schema_cls_for_entity_cls(fake_entities.Root)
        )

        self.assertEqual(
            state_entities.StatePerson,
            state_mapper.entity_cls_for_schema_cls(state_schema.StatePerson),
        )
        self.assertEqual(
            state_schema.StatePerson,
            state_mapper.schema_cls_for_entity_cls(state_entities.StatePerson),
        )

    def test_get_mapper_perf(self) -> None:
        """Ensures the .get() factory is fast when run in a loop."""
        SchemaToEntityClassMapper.clear_cache()

        start = datetime.datetime.now()
        for _ in range(10**5):
            _ = SchemaToEntityClassMapper.get(
                schema_module=state_schema, entities_module=state_entities
            )
        end = datetime.datetime.now()
        self.assertLess((end - start).total_seconds(), 0.5)
