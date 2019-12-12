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
"""Base class for all state entity matcher test classes."""
from unittest import TestCase
from mock import patch, create_autospec

from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.schema.state import dao
from recidiviz.tests.persistence.entity.state.entities_test_utils import \
    clear_db_ids, assert_no_unexpected_entities_in_db
from recidiviz.tests.utils import fakes

from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.utils.regions import Region


class BaseStateEntityMatcherTest(TestCase):
    """Tests for state specific entity matching logic."""

    def setUp(self) -> None:
        fakes.use_in_memory_sqlite_database(StateBase)
        self.get_region_patcher = patch(
            "recidiviz.persistence.entity_matching.state."
            "base_state_matching_delegate.get_region", new=self.get_fake_region)
        self.get_region_patcher.start()
        self.addCleanup(self.get_region_patcher.stop)

    def get_fake_region(self, **_kwargs):
        return create_autospec(Region)

    def to_entity(self, schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False)

    def assert_no_errors(self, matched_entities):
        self.assertEqual(0, matched_entities.error_count)
        self.assertEqual(0, matched_entities.database_cleanup_error_count)

    def assert_people_match_pre_and_post_commit(
            self, expected_people, matched_people, match_session,
            expected_unmatched_db_people=None, debug=False):
        self._assert_people_match(expected_people, matched_people, debug)

        # Sanity check that committing and reading the people from the DB
        # doesn't break/update any fields (except for DB ids).
        match_session.commit()
        match_session.close()

        session = self._session()
        result_db_people = dao.read_people(session)
        if expected_unmatched_db_people:
            expected_people.extend(expected_unmatched_db_people)
        self._assert_people_match(
            expected_people, result_db_people)
        assert_no_unexpected_entities_in_db(result_db_people, session)

    def _assert_people_match(
            self, expected_people, matched_people, debug=False):
        converted_matched = \
            converter.convert_schema_objects_to_entity(matched_people)
        db_expected_with_backedges = \
            converter.convert_entity_people_to_schema_people(expected_people)
        expected_with_backedges = \
            converter.convert_schema_objects_to_entity(
                db_expected_with_backedges)

        clear_db_ids(converted_matched)
        clear_db_ids(expected_with_backedges)

        if debug:
            print('============== EXPECTED WITH BACKEDGES ==============')
            for p in expected_with_backedges:
                print(p)
            print('============== CONVERTED MATCHED ==============')
            for p in converted_matched:
                print(p)
        self.assertCountEqual(expected_with_backedges, converted_matched)

    def _session(self):
        return SessionFactory.for_schema_base(StateBase)

    def _commit_to_db(self, *persons):
        session = self._session()
        for person in persons:
            session.add(person)
        session.commit()
