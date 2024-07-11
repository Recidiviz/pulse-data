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
from typing import Dict, List, Set, Type
from unittest import TestCase
from unittest.mock import call, patch

import attr
import pytz

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_drug_screen import (
    StateDrugScreenResult,
    StateDrugScreenSampleType,
)
from recidiviz.common.constants.state.state_employment_period import (
    StateEmploymentPeriodEmploymentStatus,
    StateEmploymentPeriodEndReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.state.state_person_address_period import (
    StatePersonAddressType,
)
from recidiviz.common.constants.state.state_person_housing_status_period import (
    StatePersonHousingStatusType,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
)
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)
from recidiviz.common.constants.state.state_staff_role_period import StateStaffRoleType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violated_condition import (
    StateSupervisionViolatedConditionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import get_state_database_entities
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    SchemaEdgeDirectionChecker,
    deep_entity_update,
    get_all_entities_from_tree,
    get_many_to_many_relationships,
    is_reference_only_entity,
    set_backedges,
)
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateChargeV2,
    StateIncarcerationSentence,
    StatePerson,
    StateStaff,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
    generate_full_graph_state_staff,
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

    def test_caching_behavior_entity(self) -> None:
        def mock_get_fields_fn(
            entity_cls: Type[Entity], entity_field_type: EntityFieldType
        ) -> Set[str]:
            if (entity_cls, entity_field_type) == (
                StatePerson,
                EntityFieldType.BACK_EDGE,
            ):
                return set()
            if (entity_cls, entity_field_type) == (
                StatePerson,
                EntityFieldType.FLAT_FIELD,
            ):
                return {"full_name", "birthdate"}
            raise ValueError(f"Unexpected {entity_cls=} and {entity_field_type=}")

        with patch.object(
            CoreEntityFieldIndex, "_get_entity_fields_with_type_slow"
        ) as mock_get_fields:
            mock_get_fields.side_effect = mock_get_fields_fn
            fields = self.field_index.get_all_core_entity_fields(
                StatePerson, EntityFieldType.BACK_EDGE
            )
            self.assertEqual(set(), fields)
            mock_get_fields.assert_called_once_with(
                StatePerson, EntityFieldType.BACK_EDGE
            )
            self.field_index.get_all_core_entity_fields(
                StatePerson, EntityFieldType.BACK_EDGE
            )
            # The slow function still should only have been called once
            mock_get_fields.assert_called_once_with(
                StatePerson, EntityFieldType.BACK_EDGE
            )
            fields = self.field_index.get_all_core_entity_fields(
                StatePerson, EntityFieldType.FLAT_FIELD
            )
            self.assertEqual({"full_name", "birthdate"}, fields)
            mock_get_fields.assert_has_calls(
                [
                    call(StatePerson, EntityFieldType.BACK_EDGE),
                    call(StatePerson, EntityFieldType.FLAT_FIELD),
                ]
            )

    def test_caching_behavior_database_entity(self) -> None:
        def mock_get_fields_fn(
            entity_cls: Type[Entity], entity_field_type: EntityFieldType
        ) -> Set[str]:
            if (entity_cls, entity_field_type) == (
                schema.StatePerson,
                EntityFieldType.BACK_EDGE,
            ):
                return set()
            if (entity_cls, entity_field_type) == (
                schema.StatePerson,
                EntityFieldType.FLAT_FIELD,
            ):
                return {"full_name", "birthdate"}
            raise ValueError(f"Unexpected {entity_cls=} and {entity_field_type=}")

        with patch.object(
            CoreEntityFieldIndex, "_get_all_database_entity_fields_slow"
        ) as mock_get_fields:
            mock_get_fields.side_effect = mock_get_fields_fn
            fields = self.field_index.get_all_core_entity_fields(
                schema.StatePerson, EntityFieldType.BACK_EDGE
            )
            self.assertEqual(set(), fields)
            mock_get_fields.assert_called_once_with(
                schema.StatePerson, EntityFieldType.BACK_EDGE
            )
            self.field_index.get_all_core_entity_fields(
                schema.StatePerson, EntityFieldType.BACK_EDGE
            )
            # The slow function still should only have been called once
            mock_get_fields.assert_called_once_with(
                schema.StatePerson, EntityFieldType.BACK_EDGE
            )
            fields = self.field_index.get_all_core_entity_fields(
                schema.StatePerson, EntityFieldType.FLAT_FIELD
            )
            self.assertEqual({"full_name", "birthdate"}, fields)
            mock_get_fields.assert_has_calls(
                [
                    call(schema.StatePerson, EntityFieldType.BACK_EDGE),
                    call(schema.StatePerson, EntityFieldType.FLAT_FIELD),
                ]
            )

    def test_caching_behavior_pre_hydrated_entity_cache(self) -> None:
        field_index = CoreEntityFieldIndex(
            entity_fields_by_field_type={
                "state_person": {
                    EntityFieldType.BACK_EDGE: set(),
                    EntityFieldType.FLAT_FIELD: {"full_name", "birthdate"},
                }
            }
        )
        with patch.object(
            CoreEntityFieldIndex, "_get_entity_fields_with_type_slow"
        ) as mock_get_fields:
            fields = field_index.get_all_core_entity_fields(
                StatePerson, EntityFieldType.BACK_EDGE
            )
            self.assertEqual(set(), fields)
            mock_get_fields.assert_not_called()
            fields = field_index.get_all_core_entity_fields(
                StatePerson, EntityFieldType.FLAT_FIELD
            )
            self.assertEqual({"full_name", "birthdate"}, fields)
            mock_get_fields.assert_not_called()
            _ = field_index.get_all_core_entity_fields(
                StatePerson, EntityFieldType.FORWARD_EDGE
            )
            # This value wasn't cached so we had to call the slow function
            mock_get_fields.assert_called_once_with(
                StatePerson, EntityFieldType.FORWARD_EDGE
            )

    def test_caching_behavior_pre_hydrated_database_entity_cache(self) -> None:
        field_index = CoreEntityFieldIndex(
            database_entity_fields_by_field_type={
                "state_person": {
                    EntityFieldType.BACK_EDGE: set(),
                    EntityFieldType.FLAT_FIELD: {"full_name", "birthdate"},
                }
            }
        )
        with patch.object(
            CoreEntityFieldIndex, "_get_all_database_entity_fields_slow"
        ) as mock_get_fields:
            fields = field_index.get_all_core_entity_fields(
                schema.StatePerson, EntityFieldType.BACK_EDGE
            )
            self.assertEqual(set(), fields)
            mock_get_fields.assert_not_called()
            fields = field_index.get_all_core_entity_fields(
                schema.StatePerson, EntityFieldType.FLAT_FIELD
            )
            self.assertEqual({"full_name", "birthdate"}, fields)
            mock_get_fields.assert_not_called()
            _ = field_index.get_all_core_entity_fields(
                schema.StatePerson, EntityFieldType.FORWARD_EDGE
            )
            # This value wasn't cached so we had to call the slow function
            mock_get_fields.assert_called_once_with(
                schema.StatePerson, EntityFieldType.FORWARD_EDGE
            )


PLACEHOLDER_ENTITY_EXAMPLES: Dict[Type[DatabaseEntity], List[DatabaseEntity]] = {
    schema.StateAssessment: [schema.StateAssessment(state_code=StateCode.US_XX.value)],
    schema.StateCharge: [
        schema.StateCharge(
            state_code=StateCode.US_XX.value,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO.value,
        )
    ],
    schema.StateDrugScreen: [schema.StateDrugScreen(state_code=StateCode.US_XX.value)],
    schema.StateEmploymentPeriod: [
        schema.StateEmploymentPeriod(state_code=StateCode.US_XX.value)
    ],
    schema.StateEarlyDischarge: [
        schema.StateEarlyDischarge(state_code=StateCode.US_XX.value)
    ],
    schema.StateIncarcerationIncident: [
        schema.StateIncarcerationIncident(state_code=StateCode.US_XX.value)
    ],
    schema.StateIncarcerationIncidentOutcome: [
        schema.StateIncarcerationIncidentOutcome(state_code=StateCode.US_XX.value)
    ],
    schema.StateIncarcerationPeriod: [
        schema.StateIncarcerationPeriod(state_code=StateCode.US_XX.value)
    ],
    schema.StateIncarcerationSentence: [
        schema.StateIncarcerationSentence(
            state_code=StateCode.US_XX.value,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        )
    ],
    schema.StatePerson: [
        schema.StatePerson(state_code=StateCode.US_XX.value),
        schema.StatePerson(
            state_code=StateCode.US_XX.value,
            assessments=[
                schema.StateAssessment(
                    state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
                )
            ],
        ),
    ],
    schema.StatePersonAddressPeriod: [
        schema.StatePersonAddressPeriod(state_code=StateCode.US_XX)
    ],
    schema.StatePersonHousingStatusPeriod: [
        schema.StatePersonHousingStatusPeriod(state_code=StateCode.US_XX)
    ],
    schema.StatePersonAlias: [
        schema.StatePersonAlias(state_code=StateCode.US_XX.value)
    ],
    schema.StatePersonEthnicity: [
        schema.StatePersonEthnicity(state_code=StateCode.US_XX.value)
    ],
    schema.StatePersonExternalId: [],
    schema.StatePersonRace: [schema.StatePersonRace(state_code=StateCode.US_XX.value)],
    schema.StateProgramAssignment: [
        schema.StateProgramAssignment(
            state_code=StateCode.US_XX.value,
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO.value,
        )
    ],
    schema.StateStaff: [schema.StateStaff(state_code=StateCode.US_XX.value)],
    schema.StateStaffExternalId: [],
    schema.StateStaffCaseloadTypePeriod: [
        # StateStaffCaseloadTypePeriod cannot be placeholders - must always have an
        # external_id and start_date.
    ],
    schema.StateStaffLocationPeriod: [
        # StateStaffSupervisorPeriod cannot be placeholders - must always have an
        # external_id and start_date.
    ],
    schema.StateStaffRolePeriod: [
        # StateStaffRolePeriod cannot be placeholders - must always have an external_id
        # and start_date.
    ],
    schema.StateStaffSupervisorPeriod: [
        # StateStaffSupervisorPeriod cannot be placeholders - must always have an
        # external_id and start_date.
    ],
    schema.StateSupervisionCaseTypeEntry: [
        schema.StateSupervisionCaseTypeEntry(state_code=StateCode.US_XX.value)
    ],
    schema.StateSupervisionContact: [
        schema.StateSupervisionContact(state_code=StateCode.US_XX.value)
    ],
    schema.StateSupervisionPeriod: [
        schema.StateSupervisionPeriod(state_code=StateCode.US_XX.value)
    ],
    schema.StateSupervisionSentence: [
        schema.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        ),
        schema.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            charges=[
                schema.StateCharge(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    status=StateChargeStatus.PRESENT_WITHOUT_INFO.value,
                )
            ],
        ),
    ],
    schema.StateSupervisionViolatedConditionEntry: [
        schema.StateSupervisionViolatedConditionEntry(state_code=StateCode.US_XX.value)
    ],
    schema.StateSupervisionViolation: [
        schema.StateSupervisionViolation(state_code=StateCode.US_XX.value)
    ],
    schema.StateSupervisionViolationResponse: [
        schema.StateSupervisionViolationResponse(state_code=StateCode.US_XX.value)
    ],
    schema.StateSupervisionViolationResponseDecisionEntry: [
        schema.StateSupervisionViolationResponseDecisionEntry(
            state_code=StateCode.US_XX.value
        )
    ],
    schema.StateSupervisionViolationTypeEntry: [
        schema.StateSupervisionViolationTypeEntry(state_code=StateCode.US_XX.value)
    ],
    schema.StateTaskDeadline: [
        schema.StateTaskDeadline(state_code=StateCode.US_XX.value)
    ],
}

# Entities in this dict should not contain any meaningful information.
# Instead, they only identify the entity for referencing.
# Concretely, this means the object has an external_id but no other set fields.
REFERENCE_ENTITY_EXAMPLES: Dict[Type[DatabaseEntity], List[DatabaseEntity]] = {
    schema.StateSentenceGroup: [
        schema.StateSentenceGroup(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
        )
    ],
    schema.StateSentenceGroupLength: [],
    schema.StateSentenceLength: [],
    schema.StateSentenceStatusSnapshot: [],
    schema.StateChargeV2: [
        schema.StateChargeV2(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
        )
    ],
    schema.StateSentenceServingPeriod: [
        schema.StateSentenceServingPeriod(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateSentence: [
        schema.StateSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
        ),
    ],
    schema.StateAssessment: [
        schema.StateAssessment(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateCharge: [
        schema.StateCharge(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO.value,
        )
    ],
    schema.StateDrugScreen: [
        schema.StateDrugScreen(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateEmploymentPeriod: [
        schema.StateEmploymentPeriod(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateEarlyDischarge: [
        schema.StateEarlyDischarge(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateIncarcerationIncident: [
        schema.StateIncarcerationIncident(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateIncarcerationIncidentOutcome: [
        schema.StateIncarcerationIncidentOutcome(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateIncarcerationPeriod: [
        schema.StateIncarcerationPeriod(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateIncarcerationSentence: [
        schema.StateIncarcerationSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        ),
    ],
    schema.StatePerson: [
        schema.StatePerson(
            state_code=StateCode.US_XX.value,
            external_ids=[
                schema.StatePersonExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
        ),
        schema.StatePerson(
            state_code=StateCode.US_XX.value,
            assessments=[
                schema.StateAssessment(
                    state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
                )
            ],
            external_ids=[
                schema.StatePersonExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
        ),
    ],
    schema.StatePersonAddressPeriod: [],
    schema.StatePersonHousingStatusPeriod: [],
    schema.StatePersonAlias: [],
    schema.StatePersonEthnicity: [],
    schema.StatePersonExternalId: [],
    schema.StatePersonRace: [],
    schema.StateProgramAssignment: [
        schema.StateProgramAssignment(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO.value,
        )
    ],
    schema.StateStaff: [
        schema.StateStaff(
            state_code=StateCode.US_XX.value,
            external_ids=[
                schema.StateStaffExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
        )
    ],
    schema.StateStaffExternalId: [],
    schema.StateStaffCaseloadTypePeriod: [
        # StateStaffCaseloadTypePeriod cannot be reference entities - must always have a
        # start_date.
    ],
    schema.StateStaffLocationPeriod: [
        # StateStaffLocationPeriod cannot be reference entities - must always have a
        # start_date.
    ],
    schema.StateStaffRolePeriod: [
        # StateStaffRolePeriod cannot be reference entities - must always have a
        # start_date.
    ],
    schema.StateStaffSupervisorPeriod: [
        # StateStaffSupervisorPeriod cannot be reference entities - must always have a
        # start_date.
    ],
    schema.StateSupervisionCaseTypeEntry: [],
    schema.StateSupervisionContact: [
        schema.StateSupervisionContact(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateSupervisionPeriod: [
        schema.StateSupervisionPeriod(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateSupervisionSentence: [
        schema.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        ),
        schema.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            charges=[
                schema.StateCharge(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    status=StateChargeStatus.PRESENT_WITHOUT_INFO.value,
                )
            ],
        ),
    ],
    schema.StateSupervisionViolatedConditionEntry: [],
    schema.StateSupervisionViolation: [
        schema.StateSupervisionViolation(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateSupervisionViolationResponse: [
        schema.StateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    schema.StateSupervisionViolationResponseDecisionEntry: [],
    schema.StateSupervisionViolationTypeEntry: [],
    schema.StateTaskDeadline: [],
}

HAS_MEANINGFUL_DATA_ENTITIES: Dict[Type[DatabaseEntity], List[DatabaseEntity]] = {
    schema.StateSentenceGroup: [],
    schema.StateSentenceGroupLength: [
        schema.StateSentenceGroupLength(
            state_code=StateCode.US_XX.value,
            group_update_datetime=datetime.datetime(2022, 1, 1),
        )
    ],
    schema.StateSentenceLength: [
        schema.StateSentenceLength(
            state_code=StateCode.US_XX.value,
            length_update_datetime=datetime.datetime(2022, 1, 1),
        )
    ],
    schema.StateSentenceStatusSnapshot: [
        schema.StateSentenceStatusSnapshot(
            state_code=StateCode.US_XX.value,
            status=StateSentenceStatus.SERVING.value,
            status_update_datetime=datetime.datetime(2022, 1, 1),
        )
    ],
    schema.StateChargeV2: [
        schema.StateChargeV2(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO.value,
            date_charged=datetime.datetime(2022, 1, 1),
        )
    ],
    schema.StateSentenceServingPeriod: [
        schema.StateSentenceServingPeriod(
            state_code=StateCode.US_XX.value,
            person_id=1,
            external_id=_EXTERNAL_ID,
            serving_start_date=datetime.date(2021, 1, 1),
        ),
    ],
    schema.StateSentence: [
        schema.StateSentence(
            state_code=StateCode.US_XX.value,
            sentence_type=StateSentenceType.STATE_PRISON,
            external_id=_EXTERNAL_ID,
            person_id=42,
            imposed_date=datetime.date(2022, 1, 1),
        ),
    ],
    schema.StateAssessment: [
        schema.StateAssessment(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            assessment_date=datetime.date(2021, 1, 1),
        ),
        schema.StateAssessment(
            state_code=StateCode.US_XX.value,
            assessment_date=datetime.date(2021, 1, 1),
        ),
    ],
    schema.StateCharge: [
        schema.StateCharge(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO.value,
            statute="1234a",
        ),
        schema.StateCharge(
            state_code=StateCode.US_XX.value,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO.value,
            statute="1234a",
        ),
    ],
    schema.StateDrugScreen: [
        schema.StateDrugScreen(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            drug_screen_date=datetime.date(2022, 5, 8),
            drug_screen_result=StateDrugScreenResult.NEGATIVE.value,
            drug_screen_result_raw_text="DRUN",
            sample_type=StateDrugScreenSampleType.BREATH.value,
            sample_type_raw_text="BREATH",
        )
    ],
    schema.StateEmploymentPeriod: [
        schema.StateEmploymentPeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            last_verified_date=datetime.date(2022, 5, 1),
            employment_status=StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME.value,
            employment_status_raw_text="PT",
            end_reason=StateEmploymentPeriodEndReason.QUIT.value,
            end_reason_raw_text="PERSONAL",
            employer_name="ACME, INC.",
            employer_address="123 FAKE ST, ANYTOWN, XX, 00000",
            job_title=None,
        )
    ],
    schema.StateEarlyDischarge: [
        schema.StateEarlyDischarge(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            decision_date=datetime.date(2021, 1, 1),
        ),
        schema.StateEarlyDischarge(
            state_code=StateCode.US_XX.value, decision_date=datetime.date(2021, 1, 1)
        ),
    ],
    schema.StateIncarcerationIncident: [
        schema.StateIncarcerationIncident(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            incident_date=datetime.date(2021, 1, 1),
        ),
        schema.StateIncarcerationIncident(
            state_code=StateCode.US_XX.value,
            incident_date=datetime.date(2021, 1, 1),
        ),
    ],
    schema.StateIncarcerationIncidentOutcome: [
        schema.StateIncarcerationIncidentOutcome(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            report_date=datetime.date(2021, 1, 1),
        ),
        schema.StateIncarcerationIncidentOutcome(
            state_code=StateCode.US_XX.value,
            report_date=datetime.date(2021, 1, 1),
        ),
    ],
    schema.StateIncarcerationPeriod: [
        schema.StateIncarcerationPeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            admission_reason=StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        ),
        schema.StateIncarcerationPeriod(
            state_code=StateCode.US_XX.value,
            admission_reason=StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        ),
    ],
    schema.StateIncarcerationSentence: [
        schema.StateIncarcerationSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            date_imposed=datetime.date(2021, 1, 1),
        ),
        schema.StateIncarcerationSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.SERVING.value,
        ),
        schema.StateIncarcerationSentence(
            state_code=StateCode.US_XX.value,
            status=StateSentenceStatus.SERVING.value,
        ),
    ],
    schema.StatePerson: [
        schema.StatePerson(
            state_code=StateCode.US_XX.value,
            external_ids=[
                schema.StatePersonExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
            races=[
                schema.StatePersonRace(
                    state_code=StateCode.US_XX.value,
                    race=StateRace.WHITE,
                    race_raw_text="W",
                ),
            ],
        ),
        schema.StatePerson(
            state_code=StateCode.US_XX.value,
            gender=StateGender.MALE,
            external_ids=[
                schema.StatePersonExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
        ),
        schema.StatePerson(
            state_code=StateCode.US_XX.value,
            gender=StateGender.MALE,
        ),
    ],
    schema.StatePersonAddressPeriod: [
        schema.StatePersonAddressPeriod(
            state_code=StateCode.US_XX.value,
            address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
        ),
    ],
    schema.StatePersonHousingStatusPeriod: [
        schema.StatePersonHousingStatusPeriod(
            state_code=StateCode.US_XX.value,
            housing_status_type=StatePersonHousingStatusType.PERMANENT_RESIDENCE,
        ),
    ],
    schema.StatePersonAlias: [
        schema.StatePersonAlias(state_code=StateCode.US_XX.value, full_name="Name"),
    ],
    schema.StatePersonEthnicity: [
        schema.StatePersonEthnicity(
            state_code=StateCode.US_XX.value,
            ethnicity=StateEthnicity.HISPANIC,
            ethnicity_raw_text="H",
        ),
        schema.StatePersonEthnicity(
            state_code=StateCode.US_XX.value, ethnicity=StateEthnicity.NOT_HISPANIC
        ),
    ],
    schema.StatePersonExternalId: [
        schema.StatePersonExternalId(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
    ],
    schema.StatePersonRace: [
        schema.StatePersonRace(
            state_code=StateCode.US_XX.value, race=StateRace.WHITE, race_raw_text="W"
        ),
        schema.StatePersonRace(state_code=StateCode.US_XX.value, race=StateRace.WHITE),
    ],
    schema.StateProgramAssignment: [
        schema.StateProgramAssignment(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO.value,
            start_date=datetime.date(2021, 1, 1),
        ),
        schema.StateProgramAssignment(
            state_code=StateCode.US_XX.value,
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO.value,
            start_date=datetime.date(2021, 1, 1),
        ),
    ],
    schema.StateStaff: [
        schema.StateStaff(
            state_code=StateCode.US_XX.value,
            email="foo@bar.com",
            external_ids=[
                schema.StateStaffExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
        ),
        schema.StateStaff(
            state_code=StateCode.US_XX.value,
            email="foo@bar.com",
            external_ids=[
                schema.StateStaffExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
            role_periods=[
                schema.StateStaffRolePeriod(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    start_date=datetime.date(2022, 5, 8),
                    end_date=datetime.date(2022, 5, 10),
                    role_type=StateStaffRoleType.SUPERVISION_OFFICER.value,
                )
            ],
        ),
    ],
    schema.StateStaffExternalId: [
        schema.StateStaffExternalId(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
    ],
    schema.StateStaffCaseloadTypePeriod: [
        schema.StateStaffCaseloadTypePeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            caseload_type=StateStaffCaseloadType.ADMINISTRATIVE_SUPERVISION,
            caseload_type_raw_text="ADMINISTRATIVE",
            start_date=datetime.date(2023, 1, 2),
            end_date=datetime.date(2023, 3, 12),
        )
    ],
    schema.StateStaffLocationPeriod: [
        schema.StateStaffLocationPeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            location_external_id="ABC123",
        )
    ],
    schema.StateStaffRolePeriod: [
        schema.StateStaffRolePeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            role_type=StateStaffRoleType.SUPERVISION_OFFICER.value,
        )
    ],
    schema.StateStaffSupervisorPeriod: [
        schema.StateStaffSupervisorPeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            supervisor_staff_external_id="ABC",
            supervisor_staff_external_id_type="US_XX_STAFF_ID",
        )
    ],
    schema.StateSupervisionCaseTypeEntry: [
        schema.StateSupervisionCaseTypeEntry(
            state_code=StateCode.US_XX.value,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
        ),
        schema.StateSupervisionCaseTypeEntry(
            state_code=StateCode.US_XX.value,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
        ),
    ],
    schema.StateSupervisionContact: [
        schema.StateSupervisionContact(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            location=StateSupervisionContactLocation.SUPERVISION_OFFICE,
        ),
        schema.StateSupervisionContact(
            state_code=StateCode.US_XX.value,
            location=StateSupervisionContactLocation.SUPERVISION_OFFICE,
        ),
    ],
    schema.StateSupervisionPeriod: [
        schema.StateSupervisionPeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            start_date=datetime.date(2021, 1, 1),
        ),
        schema.StateSupervisionPeriod(
            state_code=StateCode.US_XX.value, start_date=datetime.date(2021, 1, 1)
        ),
    ],
    schema.StateSupervisionSentence: [
        schema.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.SERVING.value,
        ),
        schema.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            date_imposed=datetime.date(2021, 1, 1),
            charges=[
                schema.StateCharge(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    status=StateChargeStatus.PRESENT_WITHOUT_INFO.value,
                )
            ],
        ),
        schema.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            status=StateSentenceStatus.SERVING.value,
        ),
    ],
    schema.StateSupervisionViolatedConditionEntry: [
        schema.StateSupervisionViolatedConditionEntry(
            state_code=StateCode.US_XX.value,
            condition=StateSupervisionViolatedConditionType.SPECIAL_CONDITIONS,
            condition_raw_text="DRG",
        )
    ],
    schema.StateSupervisionViolation: [
        schema.StateSupervisionViolation(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            violation_date=datetime.date(2021, 1, 1),
        ),
        schema.StateSupervisionViolation(
            state_code=StateCode.US_XX.value,
            violation_date=datetime.date(2021, 1, 1),
        ),
    ],
    schema.StateSupervisionViolationResponse: [
        schema.StateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            response_date=datetime.date(2021, 1, 1),
        ),
        schema.StateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            response_date=datetime.date(2021, 1, 1),
        ),
    ],
    schema.StateSupervisionViolationResponseDecisionEntry: [
        schema.StateSupervisionViolationResponseDecisionEntry(
            state_code=StateCode.US_XX.value,
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
    ],
    schema.StateSupervisionViolationTypeEntry: [
        schema.StateSupervisionViolationTypeEntry(
            state_code=StateCode.US_XX.value,
            violation_type=StateSupervisionViolationType.FELONY,
        )
    ],
    schema.StateTaskDeadline: [
        schema.StateTaskDeadline(
            state_code=StateCode.US_XX.value,
            task_type=StateTaskType.DRUG_SCREEN,
            eligible_date=None,
            due_date=datetime.date(2021, 1, 1),
            update_datetime=datetime.datetime.now(tz=pytz.UTC),
        ),
        schema.StateTaskDeadline(
            state_code=StateCode.US_XX.value,
            task_type=StateTaskType.DISCHARGE_FROM_SUPERVISION,
            task_subtype="SOME_SUBTYPE",
            eligible_date=datetime.date(2021, 1, 1),
            due_date=None,
            update_datetime=datetime.datetime.now(tz=pytz.UTC),
        ),
    ],
}


class TestEntityUtils(TestCase):
    """Tests the functionality of our entity utils."""

    def setUp(self) -> None:
        self.field_index = CoreEntityFieldIndex()

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

    def test_is_reference_only_entity(self) -> None:
        field_index = CoreEntityFieldIndex()
        for db_entity_cls in get_state_database_entities():
            if db_entity_cls not in REFERENCE_ENTITY_EXAMPLES:
                self.fail(
                    f"Expected to find [{db_entity_cls}] in REFERENCE_ENTITY_EXAMPLES"
                )
            for entity in REFERENCE_ENTITY_EXAMPLES[db_entity_cls]:
                self.assertIsInstance(entity, db_entity_cls)
                self.assertTrue(is_reference_only_entity(entity, field_index))

            if db_entity_cls not in HAS_MEANINGFUL_DATA_ENTITIES:
                self.fail(
                    f"Expected to find [{db_entity_cls}] in NON_REFERENCE_ENTITY_EXAMPLES"
                )

            for entity in HAS_MEANINGFUL_DATA_ENTITIES[db_entity_cls]:
                self.assertIsInstance(entity, db_entity_cls)
                self.assertFalse(is_reference_only_entity(entity, field_index))


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
        field_index = CoreEntityFieldIndex()
        _ = set_backedges(person, field_index)
        all_entities = get_all_entities_from_tree(person, field_index)
        for entity in all_entities:
            if isinstance(entity, StatePerson):
                continue
            related_entities: List[Entity] = []
            for field in field_index.get_all_core_entity_fields(
                entity.__class__, EntityFieldType.BACK_EDGE
            ):
                related_entities += entity.get_field_as_list(field)
            self.assertGreater(len(related_entities), 0)

    def test_set_backedges_staff(self) -> None:
        staff = generate_full_graph_state_staff(set_back_edges=False, set_ids=True)
        field_index = CoreEntityFieldIndex()
        _ = set_backedges(staff, field_index)
        all_entities = get_all_entities_from_tree(staff, field_index)
        for entity in all_entities:
            if isinstance(entity, StateStaff):
                continue
            related_entities: List[Entity] = []
            for field in field_index.get_all_core_entity_fields(
                entity.__class__, EntityFieldType.BACK_EDGE
            ):
                related_entities += entity.get_field_as_list(field)
            self.assertGreater(len(related_entities), 0)

    def test_get_many_to_many_relationships_person(self) -> None:
        person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=True, set_ids=True
        )
        field_index = CoreEntityFieldIndex()
        all_entities = get_all_entities_from_tree(person, field_index)
        for entity in all_entities:
            many_to_many_relationships = get_many_to_many_relationships(
                entity.__class__, field_index
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
        staff = generate_full_graph_state_staff(set_back_edges=True, set_ids=True)
        field_index = CoreEntityFieldIndex()
        all_entities = get_all_entities_from_tree(staff, field_index)
        for entity in all_entities:
            many_to_many_relationships = get_many_to_many_relationships(
                entity.__class__, field_index
            )
            self.assertEqual(len(many_to_many_relationships), 0)
