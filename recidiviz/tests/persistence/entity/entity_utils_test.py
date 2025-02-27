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
import datetime
from typing import List
from unittest import TestCase

import attr

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.database.schema_utils import is_association_table
from recidiviz.persistence.entity.base_entity import Entity, HasExternalIdEntity
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.entity_utils import (
    deep_entity_update,
    entities_have_direct_relationship,
    get_all_entities_from_tree,
    get_all_entity_classes_in_module,
    get_all_many_to_many_relationships_in_module,
    get_association_table_id,
    get_child_entity_classes,
    get_entities_by_association_table_id,
    get_entity_class_in_module_with_table_id,
    get_many_to_many_relationships,
    group_has_external_id_entities_by_function,
    is_many_to_many_relationship,
    is_many_to_one_relationship,
    is_one_to_many_relationship,
    set_backedges,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateCharge,
    StateChargeV2,
    StateEarlyDischarge,
    StateIncarcerationSentence,
    StatePerson,
    StateSentence,
    StateStaff,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateCharge,
    NormalizedStateChargeV2,
    NormalizedStateIncarcerationSentence,
    NormalizedStatePerson,
    NormalizedStateSentence,
    NormalizedStateSupervisionSentence,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
    generate_full_graph_state_staff,
)

_STATE_CODE = "US_XX"


class TestEntityUtils(TestCase):
    """Tests the functionality of our entity utils."""

    def test_get_entity_class_in_module_with_table_id(self) -> None:
        self.assertEqual(
            StatePerson,
            get_entity_class_in_module_with_table_id(state_entities, "state_person"),
        )
        self.assertEqual(
            StateAssessment,
            get_entity_class_in_module_with_table_id(
                state_entities, "state_assessment"
            ),
        )
        self.assertEqual(
            NormalizedStateAssessment,
            get_entity_class_in_module_with_table_id(
                normalized_entities, "state_assessment"
            ),
        )

    def test_is_many_to_many_relationship(self) -> None:
        self.assertTrue(is_many_to_many_relationship(StateSentence, StateChargeV2))
        self.assertTrue(is_many_to_many_relationship(StateChargeV2, StateSentence))
        self.assertFalse(
            is_many_to_many_relationship(
                StateSupervisionViolation, StateSupervisionViolationResponse
            )
        )
        self.assertFalse(is_many_to_many_relationship(StatePerson, StateAssessment))

        with self.assertRaisesRegex(
            ValueError,
            r"Entities \[StatePerson\] and \[StateSupervisionViolationResponse\] are "
            "not directly related",
        ):
            _ = is_many_to_one_relationship(
                StatePerson, StateSupervisionViolationResponse
            )

    def test_is_many_to_many_relationship_normalized(self) -> None:
        self.assertTrue(
            is_many_to_many_relationship(
                NormalizedStateSentence, NormalizedStateChargeV2
            )
        )
        self.assertTrue(
            is_many_to_many_relationship(
                NormalizedStateChargeV2, NormalizedStateSentence
            )
        )
        self.assertFalse(
            is_many_to_many_relationship(
                NormalizedStateSupervisionViolation,
                NormalizedStateSupervisionViolationResponse,
            )
        )
        self.assertFalse(
            is_many_to_many_relationship(
                NormalizedStatePerson, NormalizedStateAssessment
            )
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Entities \[NormalizedStatePerson\] and \[NormalizedStateSupervisionViolationResponse\] are "
            "not directly related",
        ):
            _ = is_many_to_one_relationship(
                NormalizedStatePerson, NormalizedStateSupervisionViolationResponse
            )

    def test_is_one_to_many_relationship(self) -> None:
        self.assertFalse(is_one_to_many_relationship(StateSentence, StateChargeV2))
        self.assertFalse(is_one_to_many_relationship(StateChargeV2, StateSentence))
        self.assertTrue(
            is_one_to_many_relationship(
                StateSupervisionViolation, StateSupervisionViolationResponse
            )
        )
        self.assertFalse(
            is_one_to_many_relationship(
                StateSupervisionViolationResponse, StateSupervisionViolation
            )
        )
        self.assertTrue(is_one_to_many_relationship(StatePerson, StateAssessment))

        with self.assertRaisesRegex(
            ValueError,
            r"Entities \[StatePerson\] and \[StateSupervisionViolationResponse\] are "
            "not directly related",
        ):
            _ = is_many_to_one_relationship(
                StatePerson, StateSupervisionViolationResponse
            )

    def test_is_one_to_many_relationship_normalized(self) -> None:
        self.assertFalse(
            is_one_to_many_relationship(
                NormalizedStateSentence, NormalizedStateChargeV2
            )
        )
        self.assertFalse(
            is_one_to_many_relationship(
                NormalizedStateChargeV2, NormalizedStateSentence
            )
        )
        self.assertTrue(
            is_one_to_many_relationship(
                NormalizedStateSupervisionViolation,
                NormalizedStateSupervisionViolationResponse,
            )
        )
        self.assertFalse(
            is_one_to_many_relationship(
                NormalizedStateSupervisionViolationResponse,
                NormalizedStateSupervisionViolation,
            )
        )
        self.assertTrue(
            is_one_to_many_relationship(
                NormalizedStatePerson, NormalizedStateAssessment
            )
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Entities \[NormalizedStatePerson\] and \[NormalizedStateSupervisionViolationResponse\] are "
            "not directly related",
        ):
            _ = is_many_to_one_relationship(
                NormalizedStatePerson, NormalizedStateSupervisionViolationResponse
            )

    def test_is_many_to_one_relationship(self) -> None:
        self.assertFalse(is_many_to_one_relationship(StateSentence, StateChargeV2))
        self.assertFalse(is_many_to_one_relationship(StateChargeV2, StateSentence))
        self.assertFalse(
            is_many_to_one_relationship(
                StateSupervisionViolation, StateSupervisionViolationResponse
            )
        )
        self.assertTrue(
            is_many_to_one_relationship(
                StateSupervisionViolationResponse, StateSupervisionViolation
            )
        )
        self.assertFalse(is_many_to_one_relationship(StatePerson, StateAssessment))

        with self.assertRaisesRegex(
            ValueError,
            r"Entities \[StatePerson\] and \[StateSupervisionViolationResponse\] are "
            "not directly related",
        ):
            _ = is_many_to_one_relationship(
                StatePerson, StateSupervisionViolationResponse
            )

    def test_is_many_to_one_relationship_normalized(self) -> None:
        self.assertFalse(
            is_many_to_one_relationship(
                NormalizedStateSentence, NormalizedStateChargeV2
            )
        )
        self.assertFalse(
            is_many_to_one_relationship(
                NormalizedStateChargeV2, NormalizedStateSentence
            )
        )
        self.assertFalse(
            is_many_to_one_relationship(
                NormalizedStateSupervisionViolation,
                NormalizedStateSupervisionViolationResponse,
            )
        )
        self.assertTrue(
            is_many_to_one_relationship(
                NormalizedStateSupervisionViolationResponse,
                NormalizedStateSupervisionViolation,
            )
        )
        self.assertFalse(
            is_many_to_one_relationship(
                NormalizedStatePerson, NormalizedStateAssessment
            )
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Entities \[NormalizedStatePerson\] and \[NormalizedStateSupervisionViolationResponse\] are "
            "not directly related",
        ):
            _ = is_many_to_one_relationship(
                NormalizedStatePerson, NormalizedStateSupervisionViolationResponse
            )

    def test_entities_have_direct_relationship(self) -> None:
        self.assertTrue(entities_have_direct_relationship(StateSentence, StateChargeV2))
        self.assertTrue(entities_have_direct_relationship(StateChargeV2, StateSentence))
        self.assertTrue(
            entities_have_direct_relationship(
                StateSupervisionViolation, StateSupervisionViolationResponse
            )
        )
        self.assertTrue(
            entities_have_direct_relationship(
                StateSupervisionViolationResponse, StateSupervisionViolation
            )
        )
        self.assertTrue(entities_have_direct_relationship(StatePerson, StateAssessment))

        self.assertFalse(
            entities_have_direct_relationship(
                StatePerson, StateSupervisionViolationResponse
            )
        )
        # No direct relationship because these are part of different schemas
        self.assertFalse(
            entities_have_direct_relationship(
                StateSupervisionViolation, NormalizedStateSupervisionViolationResponse
            )
        )

        self.assertTrue(
            entities_have_direct_relationship(
                NormalizedStateSentence, NormalizedStateChargeV2
            )
        )
        self.assertTrue(
            entities_have_direct_relationship(
                NormalizedStateChargeV2, NormalizedStateSentence
            )
        )
        self.assertTrue(
            entities_have_direct_relationship(
                NormalizedStateSupervisionViolation,
                NormalizedStateSupervisionViolationResponse,
            )
        )
        self.assertTrue(
            entities_have_direct_relationship(
                NormalizedStateSupervisionViolationResponse,
                NormalizedStateSupervisionViolation,
            )
        )
        self.assertTrue(
            entities_have_direct_relationship(
                NormalizedStatePerson, NormalizedStateAssessment
            )
        )

        self.assertFalse(
            entities_have_direct_relationship(
                NormalizedStatePerson, NormalizedStateSupervisionViolationResponse
            )
        )

    def test_get_association_table_id(self) -> None:
        entities_module_context = entities_module_context_for_module(state_entities)
        self.assertEqual(
            "state_charge_v2_state_sentence_association",
            get_association_table_id(
                StateSentence, StateChargeV2, entities_module_context
            ),
        )
        self.assertEqual(
            "state_charge_v2_state_sentence_association",
            get_association_table_id(
                StateChargeV2, StateSentence, entities_module_context
            ),
        )
        self.assertEqual(
            "state_charge_supervision_sentence_association",
            get_association_table_id(
                StateSupervisionSentence, StateCharge, entities_module_context
            ),
        )
        self.assertEqual(
            "state_charge_incarceration_sentence_association",
            get_association_table_id(
                StateCharge, StateIncarcerationSentence, entities_module_context
            ),
        )

    def test_get_association_table_id_normalized(self) -> None:
        entities_module_context = entities_module_context_for_module(
            normalized_entities
        )
        self.assertEqual(
            "state_charge_v2_state_sentence_association",
            get_association_table_id(
                NormalizedStateSentence,
                NormalizedStateChargeV2,
                entities_module_context,
            ),
        )
        self.assertEqual(
            "state_charge_v2_state_sentence_association",
            get_association_table_id(
                NormalizedStateChargeV2,
                NormalizedStateSentence,
                entities_module_context,
            ),
        )
        self.assertEqual(
            "state_charge_supervision_sentence_association",
            get_association_table_id(
                NormalizedStateSupervisionSentence,
                NormalizedStateCharge,
                entities_module_context,
            ),
        )
        self.assertEqual(
            "state_charge_incarceration_sentence_association",
            get_association_table_id(
                NormalizedStateCharge,
                NormalizedStateIncarcerationSentence,
                entities_module_context,
            ),
        )

    def test_get_association_table_id_agrees_with_is_association_table(self) -> None:
        entities_module_context = entities_module_context_for_module(state_entities)
        state_table_names = list(
            get_bq_schema_for_entities_module(state_entities).keys()
        )
        schema_association_tables = {
            table_name
            for table_name in state_table_names
            if is_association_table(table_name)
        }

        association_tables = set()
        for entity_cls_a in get_all_entity_classes_in_module(state_entities):
            for entity_cls_b in get_all_entity_classes_in_module(state_entities):
                if entity_cls_a == entity_cls_b:
                    continue
                if not entities_have_direct_relationship(entity_cls_a, entity_cls_b):
                    continue
                if not is_many_to_many_relationship(entity_cls_a, entity_cls_b):
                    continue
                association_tables.add(
                    get_association_table_id(
                        entity_cls_a, entity_cls_b, entities_module_context
                    )
                )

        # Check that the set of association tables defined in state_entities.py matches the set
        #  of association tables defined in state_entities.py
        self.assertEqual(schema_association_tables, association_tables)

    def test_get_association_table_id_agrees_with_is_association_table__normalized(
        self,
    ) -> None:
        entities_module_context = entities_module_context_for_module(
            normalized_entities
        )
        state_table_names = list(
            get_bq_schema_for_entities_module(normalized_entities).keys()
        )
        schema_association_tables = {
            table_name
            for table_name in state_table_names
            if is_association_table(table_name)
        }

        association_tables = set()
        for entity_cls_a in get_all_entity_classes_in_module(normalized_entities):
            for entity_cls_b in get_all_entity_classes_in_module(normalized_entities):
                if entity_cls_a == entity_cls_b:
                    continue
                if not entities_have_direct_relationship(entity_cls_a, entity_cls_b):
                    continue
                if not is_many_to_many_relationship(entity_cls_a, entity_cls_b):
                    continue
                association_tables.add(
                    get_association_table_id(
                        entity_cls_a, entity_cls_b, entities_module_context
                    )
                )

        # Check that the set of association tables defined in state_entities.py matches the set
        #  of association tables defined in state_entities.py
        self.assertEqual(schema_association_tables, association_tables)

    def test_get_entities_by_association_table_id(self) -> None:
        entities_module_context = entities_module_context_for_module(state_entities)
        self.assertEqual(
            (StateChargeV2, StateSentence),
            get_entities_by_association_table_id(
                entities_module_context, "state_charge_v2_state_sentence_association"
            ),
        )
        self.assertEqual(
            (StateCharge, StateSupervisionSentence),
            get_entities_by_association_table_id(
                entities_module_context, "state_charge_supervision_sentence_association"
            ),
        )

        self.assertEqual(
            (StateCharge, StateIncarcerationSentence),
            get_entities_by_association_table_id(
                entities_module_context,
                "state_charge_incarceration_sentence_association",
            ),
        )

    def test_get_entities_by_association_table_id_normalized(self) -> None:
        entities_module_context = entities_module_context_for_module(
            normalized_entities
        )
        self.assertEqual(
            (NormalizedStateChargeV2, NormalizedStateSentence),
            get_entities_by_association_table_id(
                entities_module_context, "state_charge_v2_state_sentence_association"
            ),
        )
        self.assertEqual(
            (NormalizedStateCharge, NormalizedStateSupervisionSentence),
            get_entities_by_association_table_id(
                entities_module_context, "state_charge_supervision_sentence_association"
            ),
        )

        self.assertEqual(
            (NormalizedStateCharge, NormalizedStateIncarcerationSentence),
            get_entities_by_association_table_id(
                entities_module_context,
                "state_charge_incarceration_sentence_association",
            ),
        )

    def test_get_entities_by_association_table_id_works_for_all_schema_tables(
        self,
    ) -> None:
        schema_association_tables = {
            table_name
            for table_name in get_bq_schema_for_entities_module(state_entities)
            if is_association_table(table_name)
        }

        for table_id in schema_association_tables:
            # These shouldn't crash
            _ = get_entities_by_association_table_id(
                entities_module_context_for_module(state_entities), table_id
            )
            _ = get_entities_by_association_table_id(
                entities_module_context_for_module(normalized_entities), table_id
            )

    def test_get_child_entity_classes(self) -> None:
        entities_module_context = entities_module_context_for_module(state_entities)

        self.assertEqual(
            {StateCharge, StateEarlyDischarge},
            get_child_entity_classes(
                StateIncarcerationSentence, entities_module_context
            ),
        )
        self.assertEqual(
            set(),
            get_child_entity_classes(StateCharge, entities_module_context),
        )


class TestBidirectionalUpdates(TestCase):
    """Tests the deep_entity_update function."""

    def test_build_new_entity_with_bidirectionally_updated_attributes(self) -> None:
        sp = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=123,
            external_id="123",
            state_code=_STATE_CODE,
            start_date=datetime.date(2020, 1, 1),
        )

        new_case_type_entries = [
            StateSupervisionCaseTypeEntry.new_with_defaults(
                supervision_case_type_entry_id=123,
                state_code=_STATE_CODE,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            ),
            StateSupervisionCaseTypeEntry.new_with_defaults(
                supervision_case_type_entry_id=456,
                state_code=_STATE_CODE,
                case_type=StateSupervisionCaseType.MENTAL_HEALTH_COURT,
            ),
        ]

        updated_entity = deep_entity_update(
            original_entity=sp, case_type_entries=new_case_type_entries
        )

        updated_case_type_entries = [attr.evolve(c) for c in new_case_type_entries]

        expected_sp = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=123,
            external_id="123",
            state_code=_STATE_CODE,
            start_date=datetime.date(2020, 1, 1),
            case_type_entries=updated_case_type_entries,
        )

        for c in updated_case_type_entries:
            c.supervision_period = expected_sp

        self.assertEqual(expected_sp, updated_entity)

    def test_build_new_entity_with_bidirectionally_updated_attributes_flat_fields_only(
        self,
    ) -> None:
        sp = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=123,
            external_id="123",
            state_code=_STATE_CODE,
            start_date=datetime.date(2020, 1, 1),
        )

        updated_entity = deep_entity_update(
            original_entity=sp,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        expected_sp = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=123,
            external_id="123",
            state_code=_STATE_CODE,
            start_date=datetime.date(2020, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        self.assertEqual(expected_sp, updated_entity)

    def test_build_new_entity_with_bidirectionally_updated_attributes_flat_and_refs(
        self,
    ) -> None:
        """Tests when there are related entities on the entity, but the only attribute
        being updated is a flat field."""
        sp = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=123,
            external_id="123",
            state_code=_STATE_CODE,
            start_date=datetime.date(2020, 1, 1),
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    supervision_case_type_entry_id=123,
                    state_code=_STATE_CODE,
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                ),
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    supervision_case_type_entry_id=456,
                    state_code=_STATE_CODE,
                    case_type=StateSupervisionCaseType.MENTAL_HEALTH_COURT,
                ),
            ],
        )

        for c in sp.case_type_entries:
            c.supervision_period = sp

        updated_entity = deep_entity_update(
            original_entity=sp,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        expected_sp = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=123,
            external_id="123",
            state_code=_STATE_CODE,
            start_date=datetime.date(2020, 1, 1),
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    supervision_case_type_entry_id=123,
                    state_code=_STATE_CODE,
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                ),
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    supervision_case_type_entry_id=456,
                    state_code=_STATE_CODE,
                    case_type=StateSupervisionCaseType.MENTAL_HEALTH_COURT,
                ),
            ],
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        for c in expected_sp.case_type_entries:
            c.supervision_period = expected_sp

        self.assertEqual(expected_sp, updated_entity)

    def test_build_new_entity_with_bidirectionally_updated_attributes_add_to_list(
        self,
    ) -> None:
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            external_id="123",
            state_code=_STATE_CODE,
        )

        ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="456",
            supervision_violation_response_id=456,
            supervision_violation=supervision_violation,
            response_date=datetime.date(2008, 12, 1),
        )

        supervision_violation.supervision_violation_responses = [ssvr]

        supervision_violation_2 = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            external_id="123",
            state_code=_STATE_CODE,
        )

        ssvr_2 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="789",
            supervision_violation_response_id=789,
            supervision_violation=supervision_violation_2,
            response_date=datetime.date(2010, 1, 5),
        )

        supervision_violation_2.supervision_violation_responses = [ssvr_2]

        # Setting supervision_violation_2 as the violation on ssvr
        updated_entity = deep_entity_update(
            original_entity=ssvr, supervision_violation=supervision_violation_2
        )

        copy_of_violation_2 = attr.evolve(supervision_violation_2)

        expected_updated_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="456",
            supervision_violation_response_id=456,
            supervision_violation=copy_of_violation_2,
            response_date=datetime.date(2008, 12, 1),
        )

        # The updated version of copy_of_violation_2 now has references to both
        # responses
        copy_of_violation_2.supervision_violation_responses = [
            ssvr_2,
            expected_updated_ssvr,
        ]

        self.assertEqual(expected_updated_ssvr, updated_entity)

    def test_build_new_entity_with_bidirectionally_updated_attributes_replace_in_list(
        self,
    ) -> None:
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            external_id="123",
            state_code=_STATE_CODE,
        )

        ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="456",
            supervision_violation_response_id=456,
            supervision_violation=supervision_violation,
            response_date=datetime.date(2008, 12, 1),
        )

        supervision_violation.supervision_violation_responses = [ssvr]

        new_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            external_id="123",
            state_code=_STATE_CODE,
        )

        # Setting new_violation as the violation on ssvr, and adding a new
        # response_subtype
        updated_entity = deep_entity_update(
            original_entity=ssvr,
            supervision_violation=new_violation,
            response_subtype="SUBTYPE",
        )

        copy_of_new_violation = attr.evolve(new_violation)

        expected_updated_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="456",
            supervision_violation_response_id=456,
            supervision_violation=copy_of_new_violation,
            response_date=datetime.date(2008, 12, 1),
            response_subtype="SUBTYPE",
        )

        # The updated version of copy_of_violation_2 only references the new version
        # of ssvr
        copy_of_new_violation.supervision_violation_responses = [
            expected_updated_ssvr,
        ]

        self.assertEqual(expected_updated_ssvr, updated_entity)

    def test_set_backedges_person(self) -> None:
        person = generate_full_graph_state_person(
            set_back_edges=False, include_person_back_edges=False, set_ids=False
        )
        entities_module_context = entities_module_context_for_module(state_entities)
        _ = set_backedges(person, entities_module_context)
        all_entities = get_all_entities_from_tree(person, entities_module_context)
        for entity in all_entities:
            entities_module_context = entities_module_context_for_entity(entity)
            field_index = entities_module_context.field_index()
            if isinstance(entity, StatePerson):
                continue
            related_entities: List[Entity] = []
            for field in field_index.get_all_entity_fields(
                entity.__class__, EntityFieldType.BACK_EDGE
            ):
                related_entities += entity.get_field_as_list(field)
            self.assertGreater(len(related_entities), 0)

    def test_set_backedges_staff(self) -> None:
        staff = generate_full_graph_state_staff(set_back_edges=False, set_ids=True)
        entities_module_context = entities_module_context_for_module(state_entities)
        _ = set_backedges(staff, entities_module_context)
        all_entities = get_all_entities_from_tree(staff, entities_module_context)
        for entity in all_entities:
            entities_module_context = entities_module_context_for_entity(entity)
            field_index = entities_module_context.field_index()
            if isinstance(entity, StateStaff):
                continue
            related_entities: List[Entity] = []
            for field in field_index.get_all_entity_fields(
                entity.__class__, EntityFieldType.BACK_EDGE
            ):
                related_entities += entity.get_field_as_list(field)
            self.assertGreater(len(related_entities), 0)

    def test_get_many_to_many_relationships_person(self) -> None:
        entities_module_context = entities_module_context_for_module(state_entities)
        person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=True, set_ids=True
        )
        all_entities = get_all_entities_from_tree(person, entities_module_context)
        for entity in all_entities:
            many_to_many_relationships = get_many_to_many_relationships(
                entity.__class__, entities_module_context
            )
            if isinstance(entity, StateCharge):
                self.assertSetEqual(
                    many_to_many_relationships,
                    {"incarceration_sentences", "supervision_sentences"},
                )
            elif isinstance(entity, StateChargeV2):
                self.assertSetEqual(
                    many_to_many_relationships,
                    {"sentences"},
                )
            else:
                self.assertEqual(len(many_to_many_relationships), 0)

    def test_get_many_to_many_relationships_staff(self) -> None:
        entities_module_context = entities_module_context_for_module(state_entities)
        staff = generate_full_graph_state_staff(set_back_edges=True, set_ids=True)
        all_entities = get_all_entities_from_tree(staff, entities_module_context)
        for entity in all_entities:
            many_to_many_relationships = get_many_to_many_relationships(
                entity.__class__, entities_module_context
            )
            self.assertEqual(len(many_to_many_relationships), 0)

    def test_get_all_many_to_many_relationships_in_module(self) -> None:
        all_many_to_many_relationships = get_all_many_to_many_relationships_in_module(
            state_entities
        )
        self.assertEqual(
            all_many_to_many_relationships,
            {
                (StateCharge, StateIncarcerationSentence),
                (StateCharge, StateSupervisionSentence),
                (StateChargeV2, StateSentence),
            },
        )


def test_group_has_external_id_entities_by_function() -> None:
    class Example(HasExternalIdEntity):
        """only has external id"""

    A = Example(external_id="A")
    B = Example(external_id="B")
    C = Example(external_id="C")
    D = Example(external_id="D")

    # These are all external IDs
    GROUPINGS = {
        "A": {"C"},
        "B": {"C", "D"},
        "C": {"A", "B"},
        "D": {"B"},
    }

    def _grouping_func(a: HasExternalIdEntity, b: HasExternalIdEntity) -> bool:
        if not (a and b):
            return False
        return b.external_id in GROUPINGS[a.external_id]

    result = group_has_external_id_entities_by_function([A, B, C, D], _grouping_func)
    assert result[0] == {"A", "B", "C", "D"}

    # Two inferred group from sentences, D all alone
    GROUPINGS = {
        "A": {"C"},
        "B": {"C"},
        "C": {"A", "B"},
        "D": set(),
    }
    result = group_has_external_id_entities_by_function([A, B, C, D], _grouping_func)
    assert len(result) == 2
    assert {"A", "B", "C"} in result
    assert {"D"} in result

    # # Two inferred group from sentences, B and D together
    GROUPINGS = {
        "A": {"C"},
        "B": {"D"},
        "C": {"A"},
        "D": {"B"},
    }
    result = group_has_external_id_entities_by_function([A, B, C, D], _grouping_func)
    assert len(result) == 2
    assert {"B", "D"} in result
    assert {"A", "C"} in result

    # No groups, each sentence is their own group.
    GROUPINGS = {
        "A": set(),
        "B": set(),
        "C": set(),
        "D": set(),
    }
    result = group_has_external_id_entities_by_function([A, B, C, D], _grouping_func)
    assert sorted(result) == [
        {"A"},
        {"B"},
        {"C"},
        {"D"},
    ]
