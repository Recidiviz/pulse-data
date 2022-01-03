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

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    SchemaEdgeDirectionChecker,
    is_standalone_class,
    prune_dangling_placeholders_from_tree,
)
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionSentence,
    StateSupervisionViolation,
)
from recidiviz.tests.persistence.database.schema.state.schema_test_utils import (
    generate_incarceration_sentence,
    generate_person,
)

_ID = 1
_STATE_CODE = "US_XX"
_EXTERNAL_ID = "EXTERNAL_ID-1"
_ID_TYPE = "ID_TYPE"


class TestCoreEntityFieldIndex(TestCase):
    """Tests the functionality of CoreEntityFieldIndex."""

    def setUp(self) -> None:
        self.field_index = CoreEntityFieldIndex()

    def test_getEntityRelationshipFieldNames_children(self) -> None:
        entity = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX", status=ChargeStatus.PRESENT_WITHOUT_INFO
                )
            ],
            person=[StatePerson.new_with_defaults(state_code="US_XX")],
            supervision_sentence_id=_ID,
        )
        self.assertEqual(
            {"charges"},
            self.field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.FORWARD_EDGE
            ),
        )

    def test_getDbEntityRelationshipFieldNames_children(self) -> None:
        entity = schema.StateSupervisionSentence(
            state_code="US_XX",
            charges=[schema.StateCharge()],
            person=schema.StatePerson(),
            person_id=_ID,
            supervision_sentence_id=_ID,
        )
        self.assertEqual(
            {"charges"},
            self.field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.FORWARD_EDGE
            ),
        )

    def test_getEntityRelationshipFieldNames_backedges(self) -> None:
        entity = schema.StateSupervisionSentence(
            state_code="US_XX",
            charges=[schema.StateCharge()],
            person=schema.StatePerson(),
            person_id=_ID,
            supervision_sentence_id=_ID,
        )
        self.assertEqual(
            {"person"},
            self.field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.BACK_EDGE
            ),
        )

    def test_getEntityRelationshipFieldNames_flatFields(self) -> None:
        entity = schema.StateSupervisionSentence(
            state_code="US_XX",
            charges=[schema.StateCharge()],
            person=schema.StatePerson(),
            person_id=_ID,
            supervision_sentence_id=_ID,
        )
        self.assertEqual(
            {"state_code", "supervision_sentence_id"},
            self.field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.FLAT_FIELD
            ),
        )

    def test_getEntityRelationshipFieldNames_foreignKeys(self) -> None:
        entity = schema.StateSupervisionSentence(
            state_code="US_XX",
            charges=[schema.StateCharge()],
            person=schema.StatePerson(),
            person_id=_ID,
            supervision_sentence_id=_ID,
        )
        self.assertEqual(
            {"person_id"},
            self.field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.FOREIGN_KEYS
            ),
        )

    def test_getEntityRelationshipFieldNames_all(self) -> None:
        entity = schema.StateSupervisionSentence(
            state_code="US_XX",
            charges=[schema.StateCharge()],
            person=schema.StatePerson(),
            person_id=_ID,
            supervision_sentence_id=_ID,
        )
        self.assertEqual(
            {"state_code", "charges", "person", "person_id", "supervision_sentence_id"},
            self.field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.ALL
            ),
        )


class TestEntityUtils(TestCase):
    """Tests the functionality of our entity utils."""

    def setUp(self) -> None:
        self.field_index = CoreEntityFieldIndex()

    @staticmethod
    def to_entity(schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False
        )

    def test_isStandaloneClass(self) -> None:
        for cls in schema_utils.get_non_history_state_database_entities():
            if cls == schema.StateAgent:
                self.assertTrue(is_standalone_class(cls))
            else:
                self.assertFalse(is_standalone_class(cls))

    def test_schemaEdgeDirectionChecker_isHigherRanked_higherRank(self) -> None:
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
        self.assertTrue(
            direction_checker.is_higher_ranked(StatePerson, StateIncarcerationSentence)
        )
        self.assertTrue(
            direction_checker.is_higher_ranked(StatePerson, StateSupervisionViolation)
        )

    def test_schemaEdgeDirectionChecker_isHigherRanked_lowerRank(self) -> None:
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
        self.assertFalse(
            direction_checker.is_higher_ranked(StateSupervisionSentence, StatePerson)
        )
        self.assertFalse(
            direction_checker.is_higher_ranked(StateSupervisionViolation, StatePerson)
        )

    def test_schemaEdgeDirectionChecker_isHigherRanked_sameRank(self) -> None:
        direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
        self.assertFalse(direction_checker.is_higher_ranked(StatePerson, StatePerson))
        self.assertFalse(
            direction_checker.is_higher_ranked(
                StateSupervisionViolation, StateSupervisionViolation
            )
        )

    def test_pruneDanglingPlaceholders_isDangling(self) -> None:
        # Arrange
        dangling_placeholder_person = generate_person()
        dangling_placeholder_is = generate_incarceration_sentence(
            person=dangling_placeholder_person
        )

        # Act
        pruned_person = prune_dangling_placeholders_from_tree(
            dangling_placeholder_person, field_index=self.field_index
        )
        pruned_incarceration_sentence = prune_dangling_placeholders_from_tree(
            dangling_placeholder_is, field_index=self.field_index
        )

        # Assert
        self.assertIsNone(pruned_person)
        self.assertIsNone(pruned_incarceration_sentence)

    def test_pruneDanglingPlaceholders_placeholderHasNonPlaceholderChildren(self):
        # Arrange
        placeholder_person = generate_person()
        non_placeholder_is = generate_incarceration_sentence(
            person=placeholder_person, external_id="external_id"
        )
        placeholder_person.incarceration_sentences = [non_placeholder_is]

        expected_placeholder_person = generate_person()
        expected_non_placeholder_is = generate_incarceration_sentence(
            person=expected_placeholder_person, external_id="external_id"
        )
        expected_placeholder_person.incarceration_sentences = [
            expected_non_placeholder_is
        ]

        # Act
        pruned_tree = prune_dangling_placeholders_from_tree(
            placeholder_person, field_index=self.field_index
        )

        # Assert
        self.assertIsNotNone(pruned_tree)
        self.assertEqual(
            attr.evolve(self.to_entity(pruned_tree)),
            attr.evolve(self.to_entity(expected_placeholder_person)),
        )

    def test_pruneDanglingPlaceholders_placeholderHasMixedChildren(self):
        # Arrange
        placeholder_person = generate_person()
        non_placeholder_is = generate_incarceration_sentence(
            person=placeholder_person, external_id="external_id"
        )
        placeholder_is = generate_incarceration_sentence(person=placeholder_person)
        placeholder_person.incarceration_sentences = [
            non_placeholder_is,
            placeholder_is,
        ]

        expected_placeholder_person = generate_person()
        expected_non_placeholder_is = generate_incarceration_sentence(
            person=expected_placeholder_person, external_id="external_id"
        )
        expected_placeholder_person.incarceration_sentences = [
            expected_non_placeholder_is
        ]

        # Act
        pruned_tree = prune_dangling_placeholders_from_tree(
            placeholder_person, field_index=self.field_index
        )

        # Assert
        self.assertIsNotNone(pruned_tree)
        self.assertEqual(
            attr.evolve(self.to_entity(pruned_tree)),
            attr.evolve(self.to_entity(expected_placeholder_person)),
        )
