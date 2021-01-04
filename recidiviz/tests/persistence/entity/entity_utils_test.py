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

import attr

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.entity_utils import EntityFieldType, \
    get_set_entity_field_names, is_standalone_class, \
    SchemaEdgeDirectionChecker, prune_dangling_placeholders_from_tree
from recidiviz.persistence.entity.state.entities import StateSentenceGroup, \
    StateFine, StatePerson, StateSupervisionViolation
from recidiviz.tests.persistence.database.schema.state.schema_test_utils \
    import generate_person, generate_sentence_group
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter)

_ID = 1
_STATE_CODE = 'NC'
_EXTERNAL_ID = 'EXTERNAL_ID-1'
_ID_TYPE = 'ID_TYPE'


class TestEntityUtils(TestCase):
    """Tests the functionality of our entity utils."""

    @staticmethod
    def to_entity(schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False)

    def test_getEntityRelationshipFieldNames_children(self) -> None:
        entity = StateSentenceGroup.new_with_defaults(
            state_code='US_XX',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            fines=[StateFine.new_with_defaults(state_code='US_XX')],
            person=[StatePerson.new_with_defaults(state_code='US_XX')],
            sentence_group_id=_ID)
        self.assertEqual(
            {'fines'},
            get_set_entity_field_names(entity, EntityFieldType.FORWARD_EDGE))

    def test_getDbEntityRelationshipFieldNames_children(self) -> None:
        entity = schema.StateSentenceGroup(
            state_code='US_XX',
            fines=[schema.StateFine()],
            person=schema.StatePerson(),
            person_id=_ID,
            sentence_group_id=_ID)
        self.assertEqual(
            {'fines'},
            get_set_entity_field_names(entity, EntityFieldType.FORWARD_EDGE))

    def test_getEntityRelationshipFieldNames_backedges(self) -> None:
        entity = schema.StateSentenceGroup(
            state_code='US_XX',
            fines=[schema.StateFine()],
            person=schema.StatePerson(),
            person_id=_ID,
            sentence_group_id=_ID)
        self.assertEqual(
            {'person'},
            get_set_entity_field_names(entity, EntityFieldType.BACK_EDGE))

    def test_getEntityRelationshipFieldNames_flatFields(self) -> None:
        entity = schema.StateSentenceGroup(
            state_code='US_XX',
            fines=[schema.StateFine(state_code='US_XX')],
            person=schema.StatePerson(state_code='US_XX'),
            person_id=_ID,
            sentence_group_id=_ID)
        self.assertEqual(
            {'state_code', 'sentence_group_id'},
            get_set_entity_field_names(entity, EntityFieldType.FLAT_FIELD))

    def test_getEntityRelationshipFieldNames_foreignKeys(self) -> None:
        entity = schema.StateSentenceGroup(
            state_code='US_XX',
            fines=[schema.StateFine()],
            person=schema.StatePerson(),
            person_id=_ID,
            sentence_group_id=_ID)
        self.assertEqual(
            {'person_id'},
            get_set_entity_field_names(entity, EntityFieldType.FOREIGN_KEYS))

    def test_getEntityRelationshipFieldNames_all(self) -> None:
        entity = schema.StateSentenceGroup(
            state_code='US_XX',
            fines=[schema.StateFine()],
            person=schema.StatePerson(),
            person_id=_ID,
            sentence_group_id=_ID)
        self.assertEqual(
            {'state_code', 'fines', 'person', 'person_id', 'sentence_group_id'},
            get_set_entity_field_names(entity, EntityFieldType.ALL))

    def test_isStandaloneClass(self) -> None:
        for cls in schema_utils.get_non_history_state_database_entities():
            if cls == schema.StateAgent:
                self.assertTrue(is_standalone_class(cls))
            else:
                self.assertFalse(is_standalone_class(cls))

    def test_schemaEdgeDirectionChecker_isHigherRanked_higherRank(self) -> None:
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
        self.assertTrue(direction_checker.is_higher_ranked(
            StatePerson, StateSentenceGroup))
        self.assertTrue(direction_checker.is_higher_ranked(
            StateSentenceGroup, StateSupervisionViolation))

    def test_schemaEdgeDirectionChecker_isHigherRanked_lowerRank(self) -> None:
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
        self.assertFalse(direction_checker.is_higher_ranked(
            StateSentenceGroup, StatePerson))
        self.assertFalse(direction_checker.is_higher_ranked(
            StateSupervisionViolation, StateSentenceGroup))

    def test_schemaEdgeDirectionChecker_isHigherRanked_sameRank(self) -> None:
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
        self.assertFalse(direction_checker.is_higher_ranked(
            StatePerson, StatePerson))
        self.assertFalse(direction_checker.is_higher_ranked(
            StateSupervisionViolation, StateSupervisionViolation))

    def test_pruneDanglingPlaceholders_isDangling(self) -> None:
        # Arrange
        dangling_placeholder_person = generate_person()
        dangling_placeholder_sg = generate_sentence_group()

        # Act
        pruned_person = \
            prune_dangling_placeholders_from_tree(dangling_placeholder_person)
        pruned_sentence_group = \
            prune_dangling_placeholders_from_tree(dangling_placeholder_sg)

        # Assert
        self.assertIsNone(pruned_person)
        self.assertIsNone(pruned_sentence_group)

    def test_pruneDanglingPlaceholders_placeholderHasNonPlaceholderChildren(
            self):
        # Arrange
        non_placeholder_sg = generate_sentence_group(external_id='external_id')
        placeholder_person = \
            generate_person(sentence_groups=[non_placeholder_sg])

        expected_non_placeholder_sg = \
            generate_sentence_group(external_id='external_id')
        expected_placeholder_person = \
            generate_person(sentence_groups=[expected_non_placeholder_sg])

        # Act
        pruned_tree = prune_dangling_placeholders_from_tree(placeholder_person)

        # Assert
        self.assertIsNotNone(pruned_tree)
        self.assertEqual(
            attr.evolve(self.to_entity(pruned_tree)),
            attr.evolve(self.to_entity(expected_placeholder_person))
        )

    def test_pruneDanglingPlaceholders_placeholderHasMixedChildren(
            self):
        # Arrange
        non_placeholder_sg = generate_sentence_group(external_id='external_id')
        placeholder_sg = generate_sentence_group()
        placeholder_person = generate_person(
            sentence_groups=[non_placeholder_sg, placeholder_sg])

        expected_non_placeholder_sg = \
            generate_sentence_group(external_id='external_id')
        expected_placeholder_person = \
            generate_person(sentence_groups=[expected_non_placeholder_sg])

        # Act
        pruned_tree = prune_dangling_placeholders_from_tree(placeholder_person)

        # Assert
        self.assertIsNotNone(pruned_tree)
        self.assertEqual(
            attr.evolve(self.to_entity(pruned_tree)),
            attr.evolve(self.to_entity(expected_placeholder_person))
        )
