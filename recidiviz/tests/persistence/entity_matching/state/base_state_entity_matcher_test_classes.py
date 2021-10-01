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
"""Base classes for various state entity matcher test classes."""
from typing import List, Optional, Sequence
from unittest.case import TestCase

import pytest

from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.state import dao, schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    print_entity_trees,
)
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    assert_no_unexpected_entities_in_db,
    clear_db_ids,
)
from recidiviz.tests.utils.test_utils import print_visible_header_label
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class BaseStateEntityMatcherTest(TestCase):
    """Base class for testing state specific entity matching logic."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def to_entity(self, schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False
        )

    def assert_no_errors(self, matched_entities):
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(0, matched_entities.database_cleanup_error_count)

    def assert_people_match_pre_and_post_commit(
        self,
        expected_people,
        matched_people,
        match_session,
        expected_unmatched_db_people=None,
        debug=False,
    ):
        self._assert_people_match(expected_people, matched_people, debug)

        # Sanity check that committing and reading the people from the DB
        # doesn't break/update any fields (except for DB ids).
        match_session.commit()
        match_session.close()

        session = self._session()
        result_db_people = dao.read_people(session)
        if expected_unmatched_db_people:
            expected_people.extend(expected_unmatched_db_people)
        self._assert_people_match(expected_people, result_db_people)
        assert_no_unexpected_entities_in_db(result_db_people, session)

    def _assert_people_match(self, expected_people, matched_people, debug=False):
        converted_matched = converter.convert_schema_objects_to_entity(matched_people)
        db_expected_with_backedges = converter.convert_entity_people_to_schema_people(
            expected_people
        )
        expected_with_backedges = converter.convert_schema_objects_to_entity(
            db_expected_with_backedges
        )

        clear_db_ids(converted_matched)
        clear_db_ids(expected_with_backedges)

        if debug:
            print("============== EXPECTED WITH BACKEDGES ==============")
            print_entity_trees(expected_with_backedges)
            print("============== CONVERTED MATCHED ==============")
            print_entity_trees(converted_matched)
        self.assertCountEqual(expected_with_backedges, converted_matched)

    def _session(self):
        # TODO(#8046): Figure out whether it makes sense to use `using_database` instead
        # and what downstream would have to be refactored.
        return SessionFactory.deprecated__for_database(self.database_key)

    def _commit_to_db(self, *persons: schema.StatePerson) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            for person in persons:
                session.add(person)


@pytest.mark.uses_db
class BaseStateMatchingUtilsTest(TestCase):
    """Base class for testing state matching utils"""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)
        self.field_index = CoreEntityFieldIndex()

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def to_entity(self, schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False
        )

    def to_entities(self, schema_objects):
        return converter.convert_schema_objects_to_entity(
            schema_objects, populate_back_edges=False
        )

    def assert_schema_objects_equal(self, expected: StateBase, actual: StateBase):
        self.assertEqual(
            converter.convert_schema_object_to_entity(expected),
            converter.convert_schema_object_to_entity(actual),
        )

    def assert_schema_object_lists_equal(
        self, expected: List[StateBase], actual: List[StateBase]
    ):
        self.assertCountEqual(
            converter.convert_schema_objects_to_entity(expected),
            converter.convert_schema_objects_to_entity(actual),
        )

    def assert_people_match(
        self,
        expected_people: List[StatePerson],
        matched_people: Sequence[schema.StatePerson],
        debug: bool = False,
    ):
        converted_matched = converter.convert_schema_objects_to_entity(matched_people)
        db_expected_with_backedges = converter.convert_entity_people_to_schema_people(
            expected_people
        )
        expected_with_backedges = converter.convert_schema_objects_to_entity(
            db_expected_with_backedges
        )
        if debug:
            print_visible_header_label("EXPECTED")
            print_entity_trees(expected_with_backedges)
            print_visible_header_label("FINAL")
            print_entity_trees(converted_matched)
        self.assertEqual(expected_with_backedges, converted_matched)
