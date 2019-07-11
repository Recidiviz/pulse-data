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
from mock import patch

from recidiviz import Session
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.state_base_schema import \
    StateBase
from recidiviz.persistence.persistence_utils import primary_key_name_from_cls
from recidiviz.persistence.database.schema_entity_converter.\
    state.schema_entity_converter import (
        StateEntityToSchemaConverter,
        StateSchemaToEntityConverter,
    )
from recidiviz.persistence.entity.base_entity import Entity, entity_graph_eq
from recidiviz.tests.persistence.entity.state.entities_test_utils import \
    generate_full_graph_state_person
from recidiviz.tests.utils import fakes


class TestStateSchemaEntityConverter(TestCase):
    """Tests for state/schema_entity_converter.py."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database(StateBase)

    @staticmethod
    def _eq_ignore_primary_keys(*args):
        return entity_graph_eq(
            args[0],
            args[1],
            should_ignore_field_cb=
            TestStateSchemaEntityConverter._is_field_a_primary_key)

    @staticmethod
    def _is_field_a_primary_key(cls_: Type, field: str):
        return primary_key_name_from_cls(cls_) == field

    def _patch_entity_eq_to_ignore_primary_keys(self):
        patcher = \
            patch('recidiviz.persistence.entity.base_entity.Entity.__eq__',
                  new=self._eq_ignore_primary_keys)
        patcher.start()
        return patcher

    def _get_schema_class_for_objects(
            self, schema_objects: List[DatabaseEntity]):
        schema_classes = \
            list({obj.__class__ for obj in schema_objects})

        self.assertTrue(len(schema_classes), 1)
        return schema_classes[0]

    def _run_convert_test(self,
                          start_entities: List[Entity],
                          expected_entities: List[Entity],
                          write_to_db: bool):
        """
        Runs a test that takes in entities,
        converts them to schema objects and back, optionally writing to the DB,
        then checks that the result is expected.

        Args:
            start_entities: A list of entities to convert.
            expected_entities: A list of entities with expected final structure
                after conversion to schema objects and back.
            write_to_db: Whether to commit the |start_entities| to the database
                before converting back to entities.
        """

        self.assertEqual(len(start_entities), len(expected_entities))
        expected_length = len(start_entities)

        # Convert entities to schema objects
        schema_objects = \
            StateEntityToSchemaConverter().convert_all(start_entities)
        self.assertEqual(len(schema_objects), expected_length)

        # Optionally commit the schema objects to the DB
        if write_to_db:
            session = Session()
            for schema_obj in schema_objects:
                session.add(schema_obj)
            session.commit()

        # Get the list of schema objects we want to convert back to entities
        schema_objects_to_convert = schema_objects
        if write_to_db:
            session = Session()
            schema_class = self._get_schema_class_for_objects(schema_objects)
            schema_objects_to_convert = session.query(schema_class).all()
            self.assertEqual(len(schema_objects), expected_length)

        # Convert schema objects back to entities
        result = StateSchemaToEntityConverter().convert_all(
            schema_objects_to_convert)
        self.assertEqual(len(result), len(expected_entities))

        # Check converted entities match original entities
        if write_to_db:
            # Writing to the DB will add primary keys if there aren't any - we
            # ignore these when we've committed to the DB.
            with self._patch_entity_eq_to_ignore_primary_keys():
                self.assertEqual(result, expected_entities)
        else:
            self.assertEqual(result, expected_entities)

    def test_convert_person_no_commit_no_explicit_backedges(self):
        input_person = generate_full_graph_state_person(set_back_edges=False)
        expected_person = generate_full_graph_state_person(set_back_edges=True)
        self._run_convert_test([input_person],
                               [expected_person],
                               write_to_db=False)

    def test_convert_person_no_commit_explicit_backedges(self):
        input_person = generate_full_graph_state_person(set_back_edges=True)
        expected_person = input_person
        self._run_convert_test([input_person],
                               [expected_person],
                               write_to_db=False)

    def test_convert_person_with_commit_no_explicit_backedges(self):
        input_person = generate_full_graph_state_person(set_back_edges=False)
        expected_person = generate_full_graph_state_person(set_back_edges=True)
        self._run_convert_test([input_person],
                               [expected_person],
                               write_to_db=True)

    def test_convert_person_with_commit_explicit_backedges(self):
        input_person = generate_full_graph_state_person(set_back_edges=True)
        expected_person = input_person
        self._run_convert_test([input_person],
                               [expected_person],
                               write_to_db=True)
