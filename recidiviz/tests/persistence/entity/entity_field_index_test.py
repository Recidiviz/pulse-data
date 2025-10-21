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
"""Tests the functionality of EntityFieldIndex."""
from unittest import TestCase
from unittest.mock import call, patch

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.entity import entity_field_index
from recidiviz.persistence.entity.entities_module_context_factory import (
    clear_entities_module_context_cache,
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_field_index import (
    EntityFieldIndex,
    EntityFieldType,
    clear_entity_field_index_cache,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StatePerson,
    StateSupervisionSentence,
)
from recidiviz.persistence.entity.state.normalized_entities import NormalizedStatePerson

_ID = 1
_EXTERNAL_ID = "EXTERNAL_ID-1"


class TestEntityFieldIndex(TestCase):
    """Tests the functionality of EntityFieldIndex."""

    def test_getEntityRelationshipFieldNames_children(self) -> None:
        field_index = entities_module_context_for_module(state_entities).field_index()
        entity = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            external_id="ss1",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    external_id="c1",
                    status=StateChargeStatus.PRESENT_WITHOUT_INFO,
                )
            ],
            person=[StatePerson.new_with_defaults(state_code="US_XX")],
            supervision_sentence_id=_ID,
        )
        self.assertEqual(
            {"charges"},
            field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.FORWARD_EDGE
            ),
        )

    def test_getEntityRelationshipFieldNames_backedges(self) -> None:
        field_index = entities_module_context_for_module(state_entities).field_index()

        entity = state_entities.StateSupervisionSentence(
            state_code="US_XX",
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    external_id="c1",
                    status=StateChargeStatus.PRESENT_WITHOUT_INFO,
                )
            ],
            person=StatePerson.new_with_defaults(state_code="US_XX"),
            supervision_sentence_id=_ID,
        )
        self.assertEqual(
            {"person"},
            field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.BACK_EDGE
            ),
        )

    def test_getEntityRelationshipFieldNames_flatFields(self) -> None:
        field_index = entities_module_context_for_module(state_entities).field_index()

        entity = state_entities.StateSupervisionSentence(
            state_code="US_XX",
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    external_id="c1",
                    status=StateChargeStatus.PRESENT_WITHOUT_INFO,
                )
            ],
            person=StatePerson.new_with_defaults(state_code="US_XX"),
            supervision_sentence_id=_ID,
        )
        self.assertEqual(
            {"state_code", "external_id", "status", "supervision_sentence_id"},
            field_index.get_fields_with_non_empty_values(
                entity, EntityFieldType.FLAT_FIELD
            ),
        )

    def test_getEntityRelationshipFieldNames_all(self) -> None:
        field_index = entities_module_context_for_module(state_entities).field_index()
        entity = state_entities.StateSupervisionSentence(
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code="US_XX",
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    external_id="c1",
                    status=StateChargeStatus.PRESENT_WITHOUT_INFO,
                )
            ],
            person=StatePerson.new_with_defaults(state_code="US_XX"),
            supervision_sentence_id=_ID,
        )
        self.assertEqual(
            {
                "state_code",
                "charges",
                "external_id",
                "person",
                "status",
                "supervision_sentence_id",
            },
            field_index.get_fields_with_non_empty_values(entity, EntityFieldType.ALL),
        )

    def test_caching_behavior_entity(self) -> None:
        clear_entities_module_context_cache()
        clear_entity_field_index_cache()
        with patch(
            f"{entity_field_index.__name__}.attribute_field_type_reference_for_class"
        ) as mock_get_field_type_ref:
            mock_get_field_type_ref.side_effect = (
                attribute_field_type_reference_for_class
            )

            state_entities_module_context = entities_module_context_for_module(
                state_entities
            )
            field_index_state = EntityFieldIndex.get(
                state_entities_module_context.entities_module(),
                state_entities_module_context.direction_checker(),
            )
            self.assertEqual(
                set(),
                field_index_state.get_all_entity_fields(
                    StatePerson, EntityFieldType.BACK_EDGE
                ),
            )
            # The slow function is called once
            mock_get_field_type_ref.assert_called_once_with(StatePerson)
            field_index_state.get_all_entity_fields(
                StatePerson, EntityFieldType.BACK_EDGE
            )
            # The slow function still should only have been called once
            mock_get_field_type_ref.assert_called_once_with(StatePerson)
            self.assertEqual(
                {
                    "birthdate",
                    "current_address",
                    "current_email_address",
                    "current_phone_number",
                    "full_name",
                    "gender",
                    "gender_raw_text",
                    "person_id",
                    "residency_status",
                    "residency_status_raw_text",
                    "sex",
                    "sex_raw_text",
                    "state_code",
                },
                field_index_state.get_all_entity_fields(
                    StatePerson, EntityFieldType.FLAT_FIELD
                ),
            )
            # Still only once
            mock_get_field_type_ref.assert_called_once_with(StatePerson)

            field_index_normalized_state = entities_module_context_for_module(
                normalized_entities
            ).field_index()
            self.assertEqual(
                {
                    "birthdate",
                    "current_address",
                    "current_email_address",
                    "current_phone_number",
                    "full_name",
                    "gender",
                    "gender_raw_text",
                    "person_id",
                    "residency_status",
                    "residency_status_raw_text",
                    "sex",
                    "sex_raw_text",
                    "state_code",
                },
                field_index_normalized_state.get_all_entity_fields(
                    NormalizedStatePerson, EntityFieldType.FLAT_FIELD
                ),
            )

            # Now a new call
            mock_get_field_type_ref.assert_has_calls(
                [call(StatePerson), call(NormalizedStatePerson)]
            )

            field_index_state_2 = entities_module_context_for_module(
                state_entities
            ).field_index()
            # These should be the exact same object
            self.assertEqual(id(field_index_state), id(field_index_state_2))

            # Call a function we've called already
            field_index_state_2.get_all_entity_fields(
                StatePerson, EntityFieldType.FLAT_FIELD
            )

            # Still only two calls
            mock_get_field_type_ref.assert_has_calls(
                [call(StatePerson), call(NormalizedStatePerson)]
            )
