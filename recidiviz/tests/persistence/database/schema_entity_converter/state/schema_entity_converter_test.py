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
"""Tests for state/schema_entity_converter.py."""
from typing import List, Type
from unittest import TestCase

from recidiviz.common.attr_mixins import attr_field_name_storing_referenced_cls_name
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema_entity_converter.state.schema_entity_converter import (
    StateEntityToSchemaConverter,
    StateSchemaToEntityConverter,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import (
    entities_have_direct_relationship,
    print_entity_trees,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.persistence.entity.walk_entity_dag import EntityDagEdge, walk_entity_dag
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
)


class TestStateSchemaEntityConverter(TestCase):
    """Tests for state/schema_entity_converter.py."""

    def _get_schema_class_for_objects(
        self, schema_objects: List[DatabaseEntity]
    ) -> Type:
        schema_classes = list({obj.__class__ for obj in schema_objects})

        self.assertTrue(len(schema_classes), 1)
        return schema_classes[0]

    def _run_convert_test(
        self,
        start_entities: List[Entity],
        expected_entities: List[Entity],
        *,
        populate_back_edges: bool,
        debug: bool = False,
    ) -> None:
        """
        Runs a test that takes in entities,
        converts them to schema objects and back, optionally writing to the DB,
        then checks that the result is expected.

        Args:
            start_entities: A list of entities to convert.
            expected_entities: A list of entities with expected final structure
                after conversion to schema objects and back.
        """

        self.assertEqual(len(start_entities), len(expected_entities))
        expected_length = len(start_entities)

        # Convert entities to schema objects
        schema_objects = StateEntityToSchemaConverter().convert_all(
            start_entities, populate_back_edges=populate_back_edges
        )
        self.assertEqual(len(schema_objects), expected_length)

        # Get the list of schema objects we want to convert back to entities
        schema_objects_to_convert = schema_objects

        # Convert schema objects back to entities
        result = StateSchemaToEntityConverter().convert_all(
            schema_objects_to_convert, populate_back_edges=populate_back_edges
        )
        entities_module_context = entities_module_context_for_module(state_entities)
        self.assertEqual(len(result), len(expected_entities))
        if debug:
            print("============== EXPECTED WITH BACKEDGES ==============")
            print_entity_trees(expected_entities, entities_module_context)
            print("============== CONVERTED MATCHED ==============")
            print_entity_trees(result, entities_module_context)

        self.assertCountEqual(result, expected_entities)

    def test_convertPerson_withBackEdges_populateBackEdges(self) -> None:
        input_person = generate_full_graph_state_person(set_back_edges=True)
        expected_person = generate_full_graph_state_person(set_back_edges=True)
        self._run_convert_test(
            [input_person], [expected_person], populate_back_edges=True
        )

    def test_convertPerson_withBackEdges_dontPopulateBackEdges(self) -> None:
        input_person = generate_full_graph_state_person(set_back_edges=True)
        expected_person = generate_full_graph_state_person(set_back_edges=False)
        self._run_convert_test(
            [input_person], [expected_person], populate_back_edges=False
        )

    def test_convertPerson_noBackEdges_populateBackEdges(self) -> None:
        input_person = generate_full_graph_state_person(set_back_edges=False)

        # The converter will convert direct backedges but not indirect / root entity
        # backedges - strip these from the expected output.
        def _remove_person_backedges(
            entity: Entity, _dag_edges: list[EntityDagEdge]
        ) -> None:
            person_field = attr_field_name_storing_referenced_cls_name(
                type(entity), StatePerson.__name__
            )

            if person_field and not entities_have_direct_relationship(
                type(entity), StatePerson
            ):
                entity.clear_field(person_field)

        expected_person = generate_full_graph_state_person(set_back_edges=True)
        walk_entity_dag(
            entities_module_context=entities_module_context_for_entity(expected_person),
            dag_root_entity=expected_person,
            node_processing_fn=_remove_person_backedges,
        )

        self._run_convert_test(
            [input_person], [expected_person], populate_back_edges=True, debug=True
        )

    def test_convertPerson_noBackEdges_dontPopulateBackEdges(self) -> None:
        input_person = generate_full_graph_state_person(set_back_edges=False)
        expected_person = generate_full_graph_state_person(set_back_edges=False)
        self._run_convert_test(
            [input_person], [expected_person], populate_back_edges=False
        )
