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
from functools import cmp_to_key
from typing import Dict, List, Type
from unittest import TestCase
from unittest.mock import call, patch

import attr
import pytz

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import (
    StateChargeStatus,
    StateChargeV2Status,
)
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
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    is_association_table,
)
from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    EntityFieldIndex,
    EntityFieldType,
    clear_entity_field_index_cache,
    deep_entity_update,
    entities_have_direct_relationship,
    get_all_entities_from_tree,
    get_all_entity_classes_in_module,
    get_association_table_id,
    get_entities_by_association_table_id,
    get_entity_class_in_module_with_name,
    get_entity_class_in_module_with_table_id,
    get_many_to_many_relationships,
    get_module_for_entity_class,
    is_many_to_many_relationship,
    is_many_to_one_relationship,
    is_one_to_many_relationship,
    is_reference_only_entity,
    set_backedges,
)
from recidiviz.persistence.entity.schema_edge_direction_checker import (
    direction_checker_for_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateCharge,
    StateChargeV2,
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
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
    generate_full_graph_state_staff,
)
from recidiviz.utils.types import assert_subclass

_ID = 1
_STATE_CODE = "US_XX"
_EXTERNAL_ID = "EXTERNAL_ID-1"
_ID_TYPE = "ID_TYPE"


class TestEntityFieldIndex(TestCase):
    """Tests the functionality of EntityFieldIndex."""

    def test_getEntityRelationshipFieldNames_children(self) -> None:
        field_index = EntityFieldIndex.for_entities_module(state_entities)
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
        field_index = EntityFieldIndex.for_entities_module(state_entities)

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
        field_index = EntityFieldIndex.for_entities_module(state_entities)

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
        field_index = EntityFieldIndex.for_entities_module(state_entities)
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
        clear_entity_field_index_cache()
        with patch(
            f"{entity_utils.__name__}.attribute_field_type_reference_for_class"
        ) as mock_get_field_type_ref:
            mock_get_field_type_ref.side_effect = (
                attribute_field_type_reference_for_class
            )

            field_index_state = EntityFieldIndex.for_entities_module(state_entities)
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
                    "state_code",
                },
                field_index_state.get_all_entity_fields(
                    StatePerson, EntityFieldType.FLAT_FIELD
                ),
            )
            # Still only once
            mock_get_field_type_ref.assert_called_once_with(StatePerson)

            field_index_normalized_state = EntityFieldIndex.for_entities_module(
                normalized_entities
            )
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

            field_index_state_2 = EntityFieldIndex.for_entities_module(state_entities)
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


# Entities in this dict should not contain any meaningful information.
# Instead, they only identify the entity for referencing.
# Concretely, this means the object has an external_id but no other set fields.
REFERENCE_ENTITY_EXAMPLES: Dict[Type[Entity], List[Entity]] = {
    state_entities.StateSentenceGroup: [
        state_entities.StateSentenceGroup(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
        )
    ],
    state_entities.StateSentenceGroupLength: [],
    state_entities.StateSentenceLength: [],
    state_entities.StateSentenceStatusSnapshot: [],
    state_entities.StateChargeV2: [
        state_entities.StateChargeV2(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        )
    ],
    state_entities.StateSentenceServingPeriod: [],
    state_entities.StateSentence: [
        state_entities.StateSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
        ),
    ],
    state_entities.StateAssessment: [
        state_entities.StateAssessment(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    state_entities.StateCharge: [
        state_entities.StateCharge(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
        )
    ],
    state_entities.StateDrugScreen: [],
    state_entities.StateEmploymentPeriod: [],
    state_entities.StateEarlyDischarge: [
        state_entities.StateEarlyDischarge(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    state_entities.StateIncarcerationIncident: [
        state_entities.StateIncarcerationIncident(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    state_entities.StateIncarcerationIncidentOutcome: [
        state_entities.StateIncarcerationIncidentOutcome(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    state_entities.StateIncarcerationPeriod: [
        state_entities.StateIncarcerationPeriod(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    state_entities.StateIncarcerationSentence: [
        state_entities.StateIncarcerationSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        ),
    ],
    state_entities.StatePerson: [
        state_entities.StatePerson(
            state_code=StateCode.US_XX.value,
            external_ids=[
                state_entities.StatePersonExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
        ),
        state_entities.StatePerson(
            state_code=StateCode.US_XX.value,
            assessments=[
                state_entities.StateAssessment(
                    state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
                )
            ],
            external_ids=[
                state_entities.StatePersonExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
        ),
    ],
    state_entities.StatePersonAddressPeriod: [],
    state_entities.StatePersonHousingStatusPeriod: [],
    state_entities.StatePersonAlias: [],
    state_entities.StatePersonEthnicity: [],
    state_entities.StatePersonExternalId: [],
    state_entities.StatePersonRace: [],
    state_entities.StateProgramAssignment: [
        state_entities.StateProgramAssignment(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO,
        )
    ],
    state_entities.StateStaff: [
        state_entities.StateStaff(
            state_code=StateCode.US_XX.value,
            external_ids=[
                state_entities.StateStaffExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
        )
    ],
    state_entities.StateStaffExternalId: [],
    state_entities.StateStaffCaseloadTypePeriod: [
        # StateStaffCaseloadTypePeriod cannot be reference entities - must always have a
        # start_date.
    ],
    state_entities.StateStaffLocationPeriod: [
        # StateStaffLocationPeriod cannot be reference entities - must always have a
        # start_date.
    ],
    state_entities.StateStaffRolePeriod: [
        # StateStaffRolePeriod cannot be reference entities - must always have a
        # start_date.
    ],
    state_entities.StateStaffSupervisorPeriod: [
        # StateStaffSupervisorPeriod cannot be reference entities - must always have a
        # start_date.
    ],
    state_entities.StateSupervisionCaseTypeEntry: [],
    state_entities.StateSupervisionContact: [
        state_entities.StateSupervisionContact(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    state_entities.StateSupervisionPeriod: [],
    state_entities.StateSupervisionSentence: [
        state_entities.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        ),
        state_entities.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[
                state_entities.StateCharge(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    status=StateChargeStatus.PRESENT_WITHOUT_INFO,
                )
            ],
        ),
    ],
    state_entities.StateSupervisionViolatedConditionEntry: [],
    state_entities.StateSupervisionViolation: [
        state_entities.StateSupervisionViolation(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    state_entities.StateSupervisionViolationResponse: [
        state_entities.StateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
    state_entities.StateSupervisionViolationResponseDecisionEntry: [],
    state_entities.StateSupervisionViolationTypeEntry: [],
    state_entities.StateTaskDeadline: [],
}

HAS_MEANINGFUL_DATA_ENTITIES: Dict[Type[Entity], List[Entity]] = {
    state_entities.StateSentenceGroup: [],
    state_entities.StateSentenceGroupLength: [
        state_entities.StateSentenceGroupLength(
            state_code=StateCode.US_XX.value,
            group_update_datetime=datetime.datetime(2022, 1, 1),
        )
    ],
    state_entities.StateSentenceLength: [
        state_entities.StateSentenceLength(
            state_code=StateCode.US_XX.value,
            length_update_datetime=datetime.datetime(2022, 1, 1),
        )
    ],
    state_entities.StateSentenceStatusSnapshot: [
        state_entities.StateSentenceStatusSnapshot(
            state_code=StateCode.US_XX.value,
            status=StateSentenceStatus.SERVING,
            status_update_datetime=datetime.datetime(2022, 1, 1),
        )
    ],
    state_entities.StateChargeV2: [
        state_entities.StateChargeV2(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
            date_charged=datetime.datetime(2022, 1, 1),
        )
    ],
    state_entities.StateSentenceServingPeriod: [
        state_entities.StateSentenceServingPeriod(
            sentence_serving_period_id=None,
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            serving_start_date=datetime.date(2021, 1, 1),
            serving_end_date=None,
        ),
    ],
    state_entities.StateSentence: [
        state_entities.StateSentence(
            state_code=StateCode.US_XX.value,
            sentence_type=StateSentenceType.STATE_PRISON,
            external_id=_EXTERNAL_ID,
            imposed_date=datetime.date(2022, 1, 1),
        ),
    ],
    state_entities.StateAssessment: [
        state_entities.StateAssessment(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            assessment_date=datetime.date(2021, 1, 1),
        ),
        state_entities.StateAssessment(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            assessment_date=datetime.date(2021, 1, 1),
        ),
    ],
    state_entities.StateCharge: [
        state_entities.StateCharge(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
            statute="1234a",
        ),
        state_entities.StateCharge(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
            statute="1234a",
        ),
    ],
    state_entities.StateDrugScreen: [
        state_entities.StateDrugScreen(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            drug_screen_date=datetime.date(2022, 5, 8),
            drug_screen_result=StateDrugScreenResult.NEGATIVE,
            drug_screen_result_raw_text="DRUN",
            sample_type=StateDrugScreenSampleType.BREATH,
            sample_type_raw_text="BREATH",
        )
    ],
    state_entities.StateEmploymentPeriod: [
        state_entities.StateEmploymentPeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            last_verified_date=datetime.date(2022, 5, 1),
            employment_status=StateEmploymentPeriodEmploymentStatus.EMPLOYED_PART_TIME,
            employment_status_raw_text="PT",
            end_reason=StateEmploymentPeriodEndReason.QUIT,
            end_reason_raw_text="PERSONAL",
            employer_name="ACME, INC.",
            employer_address="123 FAKE ST, ANYTOWN, XX, 00000",
            job_title=None,
        )
    ],
    state_entities.StateEarlyDischarge: [
        state_entities.StateEarlyDischarge(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            decision_date=datetime.date(2021, 1, 1),
        ),
    ],
    state_entities.StateIncarcerationIncident: [
        state_entities.StateIncarcerationIncident(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            incident_date=datetime.date(2021, 1, 1),
        ),
    ],
    state_entities.StateIncarcerationIncidentOutcome: [
        state_entities.StateIncarcerationIncidentOutcome(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            report_date=datetime.date(2021, 1, 1),
        ),
    ],
    state_entities.StateIncarcerationPeriod: [
        state_entities.StateIncarcerationPeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            admission_reason=StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        ),
    ],
    state_entities.StateIncarcerationSentence: [
        state_entities.StateIncarcerationSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            date_imposed=datetime.date(2021, 1, 1),
        ),
        state_entities.StateIncarcerationSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.SERVING,
        ),
    ],
    state_entities.StatePerson: [
        state_entities.StatePerson(
            state_code=StateCode.US_XX.value,
            external_ids=[
                state_entities.StatePersonExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
            races=[
                state_entities.StatePersonRace(
                    state_code=StateCode.US_XX.value,
                    race=StateRace.WHITE,
                    race_raw_text="W",
                ),
            ],
        ),
        state_entities.StatePerson(
            state_code=StateCode.US_XX.value,
            gender=StateGender.MALE,
            external_ids=[
                state_entities.StatePersonExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
        ),
        state_entities.StatePerson(
            state_code=StateCode.US_XX.value,
            gender=StateGender.MALE,
        ),
    ],
    state_entities.StatePersonAddressPeriod: [
        state_entities.StatePersonAddressPeriod(
            state_code=StateCode.US_XX.value,
            address_start_date=datetime.date(2020, 1, 1),
            address_type=StatePersonAddressType.PHYSICAL_RESIDENCE,
        ),
    ],
    state_entities.StatePersonHousingStatusPeriod: [
        state_entities.StatePersonHousingStatusPeriod(
            state_code=StateCode.US_XX.value,
            housing_status_type=StatePersonHousingStatusType.PERMANENT_RESIDENCE,
        ),
    ],
    state_entities.StatePersonAlias: [
        state_entities.StatePersonAlias(
            state_code=StateCode.US_XX.value, full_name="Name"
        ),
    ],
    state_entities.StatePersonEthnicity: [
        state_entities.StatePersonEthnicity(
            state_code=StateCode.US_XX.value,
            ethnicity=StateEthnicity.HISPANIC,
            ethnicity_raw_text="H",
        ),
        state_entities.StatePersonEthnicity(
            state_code=StateCode.US_XX.value, ethnicity=StateEthnicity.NOT_HISPANIC
        ),
    ],
    state_entities.StatePersonExternalId: [
        state_entities.StatePersonExternalId(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
    ],
    state_entities.StatePersonRace: [
        state_entities.StatePersonRace(
            state_code=StateCode.US_XX.value, race=StateRace.WHITE, race_raw_text="W"
        ),
        state_entities.StatePersonRace(
            state_code=StateCode.US_XX.value, race=StateRace.WHITE
        ),
    ],
    state_entities.StateProgramAssignment: [
        state_entities.StateProgramAssignment(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO,
            start_date=datetime.date(2021, 1, 1),
        ),
    ],
    state_entities.StateStaff: [
        state_entities.StateStaff(
            state_code=StateCode.US_XX.value,
            email="foo@bar.com",
            external_ids=[
                state_entities.StateStaffExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
        ),
        state_entities.StateStaff(
            state_code=StateCode.US_XX.value,
            email="foo@bar.com",
            external_ids=[
                state_entities.StateStaffExternalId(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    id_type=_ID_TYPE,
                )
            ],
            role_periods=[
                state_entities.StateStaffRolePeriod(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    start_date=datetime.date(2022, 5, 8),
                    end_date=datetime.date(2022, 5, 10),
                    role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                )
            ],
        ),
    ],
    state_entities.StateStaffExternalId: [
        state_entities.StateStaffExternalId(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
        )
    ],
    state_entities.StateStaffCaseloadTypePeriod: [
        state_entities.StateStaffCaseloadTypePeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            caseload_type=StateStaffCaseloadType.ADMINISTRATIVE_SUPERVISION,
            caseload_type_raw_text="ADMINISTRATIVE",
            start_date=datetime.date(2023, 1, 2),
            end_date=datetime.date(2023, 3, 12),
        )
    ],
    state_entities.StateStaffLocationPeriod: [
        state_entities.StateStaffLocationPeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            location_external_id="ABC123",
        )
    ],
    state_entities.StateStaffRolePeriod: [
        state_entities.StateStaffRolePeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            role_type=StateStaffRoleType.SUPERVISION_OFFICER,
        )
    ],
    state_entities.StateStaffSupervisorPeriod: [
        state_entities.StateStaffSupervisorPeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            start_date=datetime.date(2022, 5, 8),
            end_date=datetime.date(2022, 5, 10),
            supervisor_staff_external_id="ABC",
            supervisor_staff_external_id_type="US_XX_STAFF_ID",
        )
    ],
    state_entities.StateSupervisionCaseTypeEntry: [
        state_entities.StateSupervisionCaseTypeEntry(
            state_code=StateCode.US_XX.value,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
        ),
    ],
    state_entities.StateSupervisionContact: [
        state_entities.StateSupervisionContact(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            location=StateSupervisionContactLocation.SUPERVISION_OFFICE,
        ),
    ],
    state_entities.StateSupervisionPeriod: [
        state_entities.StateSupervisionPeriod(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            start_date=datetime.date(2021, 1, 1),
        ),
    ],
    state_entities.StateSupervisionSentence: [
        state_entities.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.SERVING,
        ),
        state_entities.StateSupervisionSentence(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            date_imposed=datetime.date(2021, 1, 1),
            charges=[
                state_entities.StateCharge(
                    state_code=StateCode.US_XX.value,
                    external_id=_EXTERNAL_ID,
                    status=StateChargeStatus.PRESENT_WITHOUT_INFO,
                )
            ],
        ),
    ],
    state_entities.StateSupervisionViolatedConditionEntry: [
        state_entities.StateSupervisionViolatedConditionEntry(
            state_code=StateCode.US_XX.value,
            condition=StateSupervisionViolatedConditionType.SPECIAL_CONDITIONS,
            condition_raw_text="DRG",
        )
    ],
    state_entities.StateSupervisionViolation: [
        state_entities.StateSupervisionViolation(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            violation_date=datetime.date(2021, 1, 1),
        ),
    ],
    state_entities.StateSupervisionViolationResponse: [
        state_entities.StateSupervisionViolationResponse(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            response_date=datetime.date(2021, 1, 1),
        ),
    ],
    state_entities.StateSupervisionViolationResponseDecisionEntry: [
        state_entities.StateSupervisionViolationResponseDecisionEntry(
            state_code=StateCode.US_XX.value,
            decision=StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
        )
    ],
    state_entities.StateSupervisionViolationTypeEntry: [
        state_entities.StateSupervisionViolationTypeEntry(
            state_code=StateCode.US_XX.value,
            violation_type=StateSupervisionViolationType.FELONY,
        )
    ],
    state_entities.StateTaskDeadline: [
        state_entities.StateTaskDeadline(
            state_code=StateCode.US_XX.value,
            task_type=StateTaskType.DRUG_SCREEN,
            eligible_date=None,
            due_date=datetime.date(2021, 1, 1),
            update_datetime=datetime.datetime.now(tz=pytz.UTC),
        ),
        state_entities.StateTaskDeadline(
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

    def test_get_module_for_entity_cls(self) -> None:
        self.assertEqual(
            normalized_entities, get_module_for_entity_class(NormalizedStateSentence)
        )
        self.assertEqual(
            normalized_entities, get_module_for_entity_class(NormalizedStateAssessment)
        )
        self.assertEqual(state_entities, get_module_for_entity_class(StatePerson))

    def test_normalized_and_state_have_same_class_hierarchy(self) -> None:
        """If this test fails _NORMALIZED_STATE_CLASS_HIERARCHY and
        _STATE_CLASS_HIERARCHY are not sorted in the same way for equivalent classes.
        """
        equivalent_state_classes_list = []
        normalized_state_direction_checker = direction_checker_for_module(
            normalized_entities
        )

        def sort_fn(a: Type[Entity], b: Type[Entity]) -> int:
            return (
                -1
                if not normalized_state_direction_checker.is_higher_ranked(a, b)
                else 1
            )

        sorted_normalized_by_rank = sorted(
            get_all_entity_classes_in_module(normalized_state_direction_checker),
            key=cmp_to_key(sort_fn),
        )
        for normalized_entity_cls in sorted_normalized_by_rank:
            equivalent_state_class = get_entity_class_in_module_with_name(
                state_entities,
                assert_subclass(
                    normalized_entity_cls, NormalizedStateEntity
                ).base_class_name(),
            )
            if not equivalent_state_class:
                continue
            equivalent_state_classes_list.append(equivalent_state_class)
        state_direction_checker = direction_checker_for_module(state_entities)
        state_direction_checker.assert_sorted(equivalent_state_classes_list)

    def test_is_reference_only_entity(self) -> None:
        for entity_cls in get_all_entity_classes_in_module(state_entities):
            if entity_cls not in REFERENCE_ENTITY_EXAMPLES:
                self.fail(
                    f"Expected to find [{entity_cls}] in REFERENCE_ENTITY_EXAMPLES"
                )
            for entity in REFERENCE_ENTITY_EXAMPLES[entity_cls]:
                self.assertIsInstance(entity, entity_cls)
                self.assertTrue(is_reference_only_entity(entity))

            if entity_cls not in HAS_MEANINGFUL_DATA_ENTITIES:
                self.fail(
                    f"Expected to find [{entity_cls}] in NON_REFERENCE_ENTITY_EXAMPLES"
                )

            for entity in HAS_MEANINGFUL_DATA_ENTITIES[entity_cls]:
                self.assertIsInstance(entity, entity_cls)
                self.assertFalse(is_reference_only_entity(entity), f"{entity}")

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
        self.assertEqual(
            "state_charge_v2_state_sentence_association",
            get_association_table_id(StateSentence, StateChargeV2),
        )
        self.assertEqual(
            "state_charge_v2_state_sentence_association",
            get_association_table_id(StateChargeV2, StateSentence),
        )
        self.assertEqual(
            "state_charge_supervision_sentence_association",
            get_association_table_id(StateSupervisionSentence, StateCharge),
        )
        self.assertEqual(
            "state_charge_incarceration_sentence_association",
            get_association_table_id(StateCharge, StateIncarcerationSentence),
        )

    def test_get_association_table_id_normalized(self) -> None:
        self.assertEqual(
            "state_charge_v2_state_sentence_association",
            get_association_table_id(NormalizedStateSentence, NormalizedStateChargeV2),
        )
        self.assertEqual(
            "state_charge_v2_state_sentence_association",
            get_association_table_id(NormalizedStateChargeV2, NormalizedStateSentence),
        )
        self.assertEqual(
            "state_charge_supervision_sentence_association",
            get_association_table_id(
                NormalizedStateSupervisionSentence, NormalizedStateCharge
            ),
        )
        self.assertEqual(
            "state_charge_incarceration_sentence_association",
            get_association_table_id(
                NormalizedStateCharge, NormalizedStateIncarcerationSentence
            ),
        )

    def test_get_association_table_id_agrees_with_schema(self) -> None:
        state_tables = get_all_table_classes_in_schema(SchemaType.STATE)
        schema_association_tables = {
            table.name for table in state_tables if is_association_table(table.name)
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
                    get_association_table_id(entity_cls_a, entity_cls_b)
                )

        # Check that the set of association tables defined in state_entities.py matches the set
        #  of association tables defined in state_entities.py
        self.assertEqual(schema_association_tables, association_tables)

    def test_get_association_table_id_agrees_with_schema_normalized(self) -> None:
        state_tables = get_all_table_classes_in_schema(SchemaType.STATE)
        schema_association_tables = {
            table.name for table in state_tables if is_association_table(table.name)
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
                    get_association_table_id(entity_cls_a, entity_cls_b)
                )

        # Check that the set of association tables defined in state_entities.py matches the set
        #  of association tables defined in state_entities.py
        self.assertEqual(schema_association_tables, association_tables)

    def test_get_entities_by_association_table_id(self) -> None:
        self.assertEqual(
            (StateChargeV2, StateSentence),
            get_entities_by_association_table_id(
                state_entities, "state_charge_v2_state_sentence_association"
            ),
        )
        self.assertEqual(
            (StateCharge, StateSupervisionSentence),
            get_entities_by_association_table_id(
                state_entities, "state_charge_supervision_sentence_association"
            ),
        )

        self.assertEqual(
            (StateCharge, StateIncarcerationSentence),
            get_entities_by_association_table_id(
                state_entities, "state_charge_incarceration_sentence_association"
            ),
        )

    def test_get_entities_by_association_table_id_normalized(self) -> None:

        self.assertEqual(
            (NormalizedStateChargeV2, NormalizedStateSentence),
            get_entities_by_association_table_id(
                normalized_entities, "state_charge_v2_state_sentence_association"
            ),
        )
        self.assertEqual(
            (NormalizedStateCharge, NormalizedStateSupervisionSentence),
            get_entities_by_association_table_id(
                normalized_entities, "state_charge_supervision_sentence_association"
            ),
        )

        self.assertEqual(
            (NormalizedStateCharge, NormalizedStateIncarcerationSentence),
            get_entities_by_association_table_id(
                normalized_entities, "state_charge_incarceration_sentence_association"
            ),
        )

    def test_get_entities_by_association_table_id_works_for_all_schema_tables(
        self,
    ) -> None:
        state_tables = get_all_table_classes_in_schema(SchemaType.STATE)
        schema_association_tables = {
            table.name for table in state_tables if is_association_table(table.name)
        }

        for table_id in schema_association_tables:
            # These shouldn't crash
            _ = get_entities_by_association_table_id(state_entities, table_id)
            _ = get_entities_by_association_table_id(normalized_entities, table_id)


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
        _ = set_backedges(person)
        all_entities = get_all_entities_from_tree(person)
        for entity in all_entities:
            field_index = EntityFieldIndex.for_entity(entity)
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
        _ = set_backedges(staff)
        all_entities = get_all_entities_from_tree(staff)
        for entity in all_entities:
            field_index = EntityFieldIndex.for_entity(entity)
            if isinstance(entity, StateStaff):
                continue
            related_entities: List[Entity] = []
            for field in field_index.get_all_entity_fields(
                entity.__class__, EntityFieldType.BACK_EDGE
            ):
                related_entities += entity.get_field_as_list(field)
            self.assertGreater(len(related_entities), 0)

    def test_get_many_to_many_relationships_person(self) -> None:
        person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=True, set_ids=True
        )
        all_entities = get_all_entities_from_tree(person)
        for entity in all_entities:
            many_to_many_relationships = get_many_to_many_relationships(
                entity.__class__
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
        all_entities = get_all_entities_from_tree(staff)
        for entity in all_entities:
            many_to_many_relationships = get_many_to_many_relationships(
                entity.__class__
            )
            self.assertEqual(len(many_to_many_relationships), 0)
