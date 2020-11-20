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
from typing import List, Optional
from unittest import TestCase


from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema_entity_converter.\
    state.schema_entity_converter import (
        StateEntityToSchemaConverter,
        StateSchemaToEntityConverter,
    )
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import print_entity_trees
from recidiviz.tests.persistence.entity.state.entities_test_utils import \
    generate_full_graph_state_person
from recidiviz.tools.postgres import local_postgres_helpers


class TestStateSchemaEntityConverter(TestCase):
    """Tests for state/schema_entity_converter.py."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        local_postgres_helpers.use_on_disk_postgresql_database(StateBase)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(StateBase)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(cls.temp_db_dir)

    def _get_schema_class_for_objects(
            self, schema_objects: List[DatabaseEntity]):
        schema_classes = \
            list({obj.__class__ for obj in schema_objects})

        self.assertTrue(len(schema_classes), 1)
        return schema_classes[0]

    def _run_convert_test(self,
                          start_entities: List[Entity],
                          expected_entities: List[Entity],
                          populate_back_edges: bool,
                          debug: bool = False):
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
        schema_objects = \
            StateEntityToSchemaConverter().convert_all(
                start_entities, populate_back_edges)
        self.assertEqual(len(schema_objects), expected_length)

        # Get the list of schema objects we want to convert back to entities
        schema_objects_to_convert = schema_objects

        # Convert schema objects back to entities
        result = StateSchemaToEntityConverter().convert_all(
            schema_objects_to_convert, populate_back_edges)
        self.assertEqual(len(result), len(expected_entities))
        if debug:
            print('============== EXPECTED WITH BACKEDGES ==============')
            print_entity_trees(expected_entities)
            print('============== CONVERTED MATCHED ==============')
            print_entity_trees(result)

        self.assertCountEqual(result, expected_entities)

    def test_convertPerson_withBackEdges_populateBackEdges(self):
        input_person = generate_full_graph_state_person(set_back_edges=True)
        expected_person = generate_full_graph_state_person(set_back_edges=True)
        self._run_convert_test([input_person], [expected_person], populate_back_edges=True)

    def test_convertPerson_withBackEdges_dontPopulateBackEdges(self):
        input_person = generate_full_graph_state_person(set_back_edges=True)
        expected_person = generate_full_graph_state_person(set_back_edges=False)
        self._run_convert_test([input_person], [expected_person], populate_back_edges=False)

    def test_convertPerson_noBackEdges_populateBackEdges(self):
        input_person = generate_full_graph_state_person(set_back_edges=False)
        expected_person = generate_full_graph_state_person(set_back_edges=True)
        self._run_convert_test([input_person], [expected_person], populate_back_edges=True)

    def test_convertPerson_noBackEdges_dontPopulateBackEdges(self):
        input_person = generate_full_graph_state_person(set_back_edges=False)
        expected_person = generate_full_graph_state_person(set_back_edges=False)
        self._run_convert_test([input_person], [expected_person], populate_back_edges=False)
