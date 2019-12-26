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
"""Tests for entity_utils.py"""
from unittest import TestCase

from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.entity_utils import EntityFieldType, \
    get_set_entity_field_names, is_standalone_class, SchemaEdgeDirectionChecker
from recidiviz.persistence.entity.state.entities import StateSentenceGroup, \
    StateFine, StatePerson, StateSupervisionViolation

_ID = 1
_STATE_CODE = 'NC'
_EXTERNAL_ID = 'EXTERNAL_ID-1'
_ID_TYPE = 'ID_TYPE'


class TestEntityUtils(TestCase):
    """Tests the functionality of our entity utils."""

    def test_getEntityRelationshipFieldNames_children(self):
        entity = StateSentenceGroup.new_with_defaults(
            fines=[StateFine.new_with_defaults()],
            person=[StatePerson.new_with_defaults()],
            sentence_group_id=_ID)
        self.assertEqual(
            {'fines'},
            get_set_entity_field_names(entity, EntityFieldType.FORWARD_EDGE))

    def test_getDbEntityRelationshipFieldNames_children(self):
        entity = schema.StateSentenceGroup(
            fines=[schema.StateFine()],
            person=schema.StatePerson(),
            person_id=_ID,
            sentence_group_id=_ID)
        self.assertEqual(
            {'fines'},
            get_set_entity_field_names(entity, EntityFieldType.FORWARD_EDGE))

    def test_getEntityRelationshipFieldNames_backedges(self):
        entity = schema.StateSentenceGroup(
            fines=[schema.StateFine()],
            person=schema.StatePerson(),
            person_id=_ID,
            sentence_group_id=_ID)
        self.assertEqual(
            {'person'},
            get_set_entity_field_names(entity, EntityFieldType.BACK_EDGE))

    def test_getEntityRelationshipFieldNames_flatFields(self):
        entity = schema.StateSentenceGroup(
            fines=[schema.StateFine()],
            person=schema.StatePerson(),
            person_id=_ID,
            sentence_group_id=_ID)
        self.assertEqual(
            {'sentence_group_id'},
            get_set_entity_field_names(entity, EntityFieldType.FLAT_FIELD))

    def test_getEntityRelationshipFieldNames_foreignKeys(self):
        entity = schema.StateSentenceGroup(
            fines=[schema.StateFine()],
            person=schema.StatePerson(),
            person_id=_ID,
            sentence_group_id=_ID)
        self.assertEqual(
            {'person_id'},
            get_set_entity_field_names(entity, EntityFieldType.FOREIGN_KEYS))

    def test_getEntityRelationshipFieldNames_all(self):
        entity = schema.StateSentenceGroup(
            fines=[schema.StateFine()],
            person=schema.StatePerson(),
            person_id=_ID,
            sentence_group_id=_ID)
        self.assertEqual(
            {'fines', 'person', 'person_id', 'sentence_group_id'},
            get_set_entity_field_names(entity, EntityFieldType.ALL))

    def test_isStandaloneClass(self):
        for cls in schema_utils.get_non_history_state_database_entities():
            if cls == schema.StateAgent:
                self.assertTrue(is_standalone_class(cls))
            else:
                self.assertFalse(is_standalone_class(cls))

    def test_schemaEdgeDirectionChecker_isHigherRanked_higherRank(self):
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
        self.assertTrue(direction_checker.is_higher_ranked(
            StatePerson, StateSentenceGroup))
        self.assertTrue(direction_checker.is_higher_ranked(
            StateSentenceGroup, StateSupervisionViolation))

    def test_schemaEdgeDirectionChecker_isHigherRanked_lowerRank(self):
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
        self.assertFalse(direction_checker.is_higher_ranked(
            StateSentenceGroup, StatePerson))
        self.assertFalse(direction_checker.is_higher_ranked(
            StateSupervisionViolation, StateSentenceGroup))

    def test_schemaEdgeDirectionChecker_isHigherRanked_sameRank(self):
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
        self.assertFalse(direction_checker.is_higher_ranked(
            StatePerson, StatePerson))
        self.assertFalse(direction_checker.is_higher_ranked(
            StateSupervisionViolation, StateSupervisionViolation))
