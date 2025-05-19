# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for the RootEntityUpdateMerger class."""
import datetime
import unittest
from typing import Optional

import attr
import pytz
from parameterized import parameterized

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
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
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
    StateResidencyStatus,
)
from recidiviz.common.constants.state.state_person_address_period import (
    StatePersonAddressType,
)
from recidiviz.common.constants.state.state_person_housing_status_period import (
    StatePersonHousingStatusType,
)
from recidiviz.common.constants.state.state_person_staff_relationship_period import (
    StatePersonStaffRelationshipType,
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
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
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
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
)
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity_matching.root_entity_update_merger import (
    RootEntityUpdateMerger,
    is_reference_only_state_entity,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
)
from recidiviz.tests.persistence.entity_matching.us_xx_entity_builders import (
    make_assessment,
    make_incarceration_sentence,
    make_person,
    make_person_external_id,
    make_person_race,
    make_staff,
    make_staff_external_id,
    make_state_charge,
    make_supervision_sentence,
    make_task_deadline,
)
from recidiviz.tests.test_debug_helpers import launch_entity_tree_html_diff_comparison
from recidiviz.utils.environment import in_ci
from recidiviz.utils.types import assert_type

_FULL_NAME_1 = '{"given_names": "FIRST1", "middle_names": "", "name_suffix": "", "surname": "LAST1"}'

_ASSESSMENT_1 = make_assessment(
    external_id="A",
    assessment_class=StateAssessmentClass.RISK,
    assessment_type=StateAssessmentType.LSIR,
    assessment_date=datetime.date(2022, 1, 1),
    assessment_score=10,
)
_ASSESSMENT_2 = make_assessment(
    external_id="B",
    assessment_type=StateAssessmentType.LSIR,
    assessment_date=datetime.date(2022, 2, 2),
    assessment_score=20,
)

_EXTERNAL_ID_ENTITY_1 = make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
_EXTERNAL_ID_ENTITY_2 = make_person_external_id(external_id="ID_2", id_type="ID_TYPE_2")
_EXTERNAL_ID_ENTITY_3 = make_person_external_id(external_id="ID_3", id_type="ID_TYPE_3")


_UPDATE_DATETIME_1 = datetime.datetime(2000, 1, 2, 3, 4, 5, 6)
_UPDATE_DATETIME_2 = datetime.datetime(2000, 2, 3, 4, 5, 6, 7)

_TASK_DEADLINE_1 = make_task_deadline(
    task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
    update_datetime=_UPDATE_DATETIME_1,
    task_metadata="metadata1",
)
_TASK_DEADLINE_2 = make_task_deadline(
    task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
    update_datetime=_UPDATE_DATETIME_2,
    task_metadata="metadata2",
)
_EXTERNAL_ID = "EXTERNAL_ID-1"
_ID_TYPE = "ID_TYPE"


# Entities in this dict should not contain any meaningful information.
# Instead, they only identify the entity for referencing.
# Concretely, this means the object has an external_id but no other set fields.
_REFERENCE_ENTITY_EXAMPLES: dict[type[Entity], list[Entity]] = {
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
    state_entities.StateIncarcerationPeriod: [],
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
    state_entities.StatePersonStaffRelationshipPeriod: [],
}

_HAS_MEANINGFUL_DATA_ENTITIES: dict[type[Entity], list[Entity]] = {
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
            date_charged=datetime.date(2022, 1, 1),
        )
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
            admission_date=datetime.date(2021, 1, 1),
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
            address_line_1="Person Address Line 1",
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
    state_entities.StatePersonStaffRelationshipPeriod: [
        state_entities.StatePersonStaffRelationshipPeriod(
            state_code=StateCode.US_XX.value,
            relationship_start_date=datetime.date(2021, 1, 1),
            relationship_end_date_exclusive=None,
            system_type=StateSystemType.INCARCERATION,
            relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
            associated_staff_external_id="ABC123",
            associated_staff_external_id_type="US_XX_STAFF_ID",
            relationship_type_raw_text=None,
            relationship_priority=1,
        ),
        state_entities.StatePersonStaffRelationshipPeriod(
            state_code=StateCode.US_XX.value,
            relationship_start_date=datetime.date(2021, 1, 1),
            relationship_end_date_exclusive=datetime.date(2023, 1, 1),
            system_type=StateSystemType.INCARCERATION,
            relationship_type=StatePersonStaffRelationshipType.CASE_MANAGER,
            associated_staff_external_id="ABC123",
            associated_staff_external_id_type="US_XX_STAFF_ID",
            relationship_type_raw_text=None,
            relationship_priority=1,
        ),
    ],
}


class TestRootEntityUpdateMerger(unittest.TestCase):
    """Tests for the RootEntityUpdateMerger class."""

    def setUp(self) -> None:
        self.merger = RootEntityUpdateMerger()

    def assert_expected_matches_result(
        self, *, expected_result: RootEntity, result: RootEntity, debug: bool = False
    ) -> None:
        if debug:
            if in_ci():
                self.fail("The |debug| flag should only be used for local debugging.")
            entities_module_context = entities_module_context_for_entity(
                assert_type(expected_result, Entity)
            )
            launch_entity_tree_html_diff_comparison(
                found_root_entities=[result],
                expected_root_entities=[expected_result],
                entities_module_context=entities_module_context,
                region_code="US_XX",
            )

        self.assertEqual(expected_result, result)

    def test_throws_primary_keys_set_root_entity(self) -> None:
        root_entity_with_primary_key = make_person(
            # Primary key should not be set when we get to root entity merging.
            person_id=123,
            external_ids=[_EXTERNAL_ID_ENTITY_1],
        )

        # Test primary key set on new root entity
        with self.assertRaisesRegex(
            ValueError,
            r"Found set primary key on \[StatePerson\] entity: 123. Primary key fields "
            r"should not be set at this point.",
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=None,
                root_entity_updates=root_entity_with_primary_key,
            )

        root_entity_without_primary_key = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
        )

        # Test primary key set on old root entity
        with self.assertRaisesRegex(
            ValueError,
            r"Found set primary key on \[StatePerson\] entity: 123. Primary key fields "
            r"should not be set at this point.",
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=root_entity_with_primary_key,
                root_entity_updates=root_entity_without_primary_key,
            )

    def test_throws_primary_keys_set_child_entity(self) -> None:
        charge = make_state_charge(
            # Primary key should not be set when we get to root entity merging.
            charge_id=123,
            external_id="CHARGE_A",
            ncic_code="3599",
        )
        incarceration_sentence = make_incarceration_sentence(
            external_id="A",
            status=StateSentenceStatus.SERVING,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            min_length_days=365,
            max_length_days=365 * 2,
            charges=[attr.evolve(charge)],
        )
        root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[attr.evolve(incarceration_sentence)],
        )

        # Test primary key set on new child entity
        with self.assertRaisesRegex(
            ValueError,
            r"Found set primary key on \[StateCharge\] entity: 123. Primary key fields "
            r"should not be set at this point.",
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=None,
                root_entity_updates=root_entity,
            )

        root_entity_without_primary_key = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[
                attr.evolve(
                    incarceration_sentence,
                    charges=[attr.evolve(charge, charge_id=None)],
                )
            ],
        )

        # Test primary key set on old root entity
        with self.assertRaisesRegex(
            ValueError,
            r"Found set primary key on \[StateCharge\] entity: 123. Primary key fields "
            r"should not be set at this point.",
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=root_entity,
                root_entity_updates=root_entity_without_primary_key,
            )

    def test_throws_backedges_to_root_set(self) -> None:
        incarceration_sentence = make_incarceration_sentence(
            external_id="A",
            status=StateSentenceStatus.SERVING,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            min_length_days=365,
            max_length_days=365 * 2,
        )
        root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[incarceration_sentence],
        )
        # Set person backedge
        incarceration_sentence.person = root_entity

        # Test primary key set on new child entity
        with self.assertRaisesRegex(
            ValueError,
            r"Found set back edges on \[StateIncarcerationSentence\] entity: "
            r"\{'person'\}. Back edge fields should not be set at this point.",
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=None,
                root_entity_updates=root_entity,
            )

        root_entity_without_backedge = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[attr.evolve(incarceration_sentence, person=None)],
        )

        # Test primary key set on old root entity
        with self.assertRaisesRegex(
            ValueError,
            r"Found set back edges on \[StateIncarcerationSentence\] entity: "
            r"\{'person'\}. Back edge fields should not be set at this point.",
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=root_entity,
                root_entity_updates=root_entity_without_backedge,
            )

    def test_throws_non_root_backedges_set(self) -> None:
        charge = make_state_charge(
            external_id="CHARGE_A",
            ncic_code="3599",
        )
        incarceration_sentence = make_incarceration_sentence(
            external_id="A",
            status=StateSentenceStatus.SERVING,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            min_length_days=365,
            max_length_days=365 * 2,
            charges=[charge],
        )
        # Backedge set on charge
        charge.incarceration_sentences = [incarceration_sentence]
        root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[attr.evolve(incarceration_sentence)],
        )

        # Test primary key set on new child entity
        with self.assertRaisesRegex(
            ValueError,
            r"Found set back edges on \[StateCharge\] entity: "
            r"\{'incarceration_sentences'\}. Back edge fields should not be set at "
            r"this point.",
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=None,
                root_entity_updates=root_entity,
            )

        root_entity_without_backedge = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[
                attr.evolve(
                    incarceration_sentence,
                    charges=[attr.evolve(charge, incarceration_sentences=[])],
                )
            ],
        )

        # Test primary key set on old root entity
        with self.assertRaisesRegex(
            ValueError,
            r"Found set back edges on \[StateCharge\] entity: "
            r"\{'incarceration_sentences'\}. Back edge fields should not be set at "
            r"this point.",
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=root_entity,
                root_entity_updates=root_entity_without_backedge,
            )

    def test_merge_people_exact_match(self) -> None:
        previous_root_entity = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
        )
        entity_updates = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
        )

        # Expect no change since there is no new info in the updates
        expected_result = attr.evolve(previous_root_entity)

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_same_external_id_str_different_type(self) -> None:
        previous_root_entity = make_person(
            external_ids=[
                make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1")
            ],
        )
        entity_updates = make_person(
            external_ids=[
                make_person_external_id(external_id="ID_1", id_type="ID_TYPE_2")
            ],
        )

        expected_result = attr.evolve(
            previous_root_entity,
            external_ids=[
                make_person_external_id(external_id="ID_1", id_type="ID_TYPE_1"),
                make_person_external_id(external_id="ID_1", id_type="ID_TYPE_2"),
            ],
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_exact_match_except_id_active_dates(self) -> None:
        external_id = _EXTERNAL_ID_ENTITY_1
        external_id_with_update_dates = attr.evolve(
            external_id, id_active_from_datetime=datetime.datetime(2020, 1, 1)
        )

        previous_root_entity = make_person(
            external_ids=[external_id_with_update_dates],
        )
        entity_updates = make_person(
            external_ids=[external_id],
        )

        # Expect no change since there is no new info in the updates
        expected_result = attr.evolve(previous_root_entity)

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_exact_match_except_id_active_dates_reverse_order(
        self,
    ) -> None:
        external_id = _EXTERNAL_ID_ENTITY_1
        external_id_with_update_dates = attr.evolve(
            external_id, id_active_from_datetime=datetime.datetime(2020, 1, 1)
        )

        previous_root_entity = make_person(
            external_ids=[external_id],
        )
        entity_updates = make_person(
            external_ids=[external_id_with_update_dates],
        )

        # Expect no change since there is no new info in the updates
        expected_result = attr.evolve(previous_root_entity)

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_conflicting_external_id_date_values(self) -> None:
        previous_root_entity = make_person(
            external_ids=[
                make_person_external_id(
                    external_id="ID_1",
                    id_type="ID_TYPE_1",
                    id_active_from_datetime=datetime.datetime(2020, 1, 1),
                )
            ],
        )
        entity_updates = make_person(
            external_ids=[
                make_person_external_id(
                    external_id="ID_1",
                    id_type="ID_TYPE_1",
                    id_active_from_datetime=datetime.datetime(2020, 2, 2),
                )
            ],
        )

        with self.assertRaisesRegex(
            ValueError, r"Error merging children of type \[StatePersonExternalId\]"
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=previous_root_entity,
                root_entity_updates=entity_updates,
            )

    def test_merge_people_conflicting_is_current_display_values(self) -> None:
        previous_root_entity = make_person(
            external_ids=[
                make_person_external_id(
                    external_id="ID_1",
                    id_type="ID_TYPE_1",
                    is_current_display_id_for_type=True,
                )
            ],
        )
        entity_updates = make_person(
            external_ids=[
                make_person_external_id(
                    external_id="ID_1",
                    id_type="ID_TYPE_1",
                    is_current_display_id_for_type=False,
                )
            ],
        )

        with self.assertRaisesRegex(
            ValueError, r"Error merging children of type \[StatePersonExternalId\]"
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=previous_root_entity,
                root_entity_updates=entity_updates,
            )

    def test_merge_people_complex_exact_match(self) -> None:
        previous_root_entity = generate_full_graph_state_person(
            set_back_edges=False,
            include_person_back_edges=False,
            set_ids=False,
        )
        entity_updates = generate_full_graph_state_person(
            set_back_edges=False,
            include_person_back_edges=False,
            set_ids=False,
        )

        # Expect no change since there is no new info in the updates
        expected_result = attr.evolve(previous_root_entity)

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_complex_multiple_differences(self) -> None:
        # Arrange
        previous_root_entity = generate_full_graph_state_person(
            set_back_edges=False,
            include_person_back_edges=False,
            set_ids=False,
        )

        previous_root_entity.incarceration_periods = []
        previous_root_entity.assessments = []
        # TODO(#20936): Un-comment this line once multi-parent merging is
        #  implemented.
        # previous_root_entity.incarceration_sentences[0].charges = []

        entity_updates = generate_full_graph_state_person(
            set_back_edges=False,
            include_person_back_edges=False,
            set_ids=False,
        )

        entity_updates.supervision_contacts = []
        new_response_date = datetime.date.today()
        entity_updates.supervision_violations[0].supervision_violation_responses[
            0
        ].response_date = new_response_date

        # Act
        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        # Assert
        # Expect the full tree to be hydrated because new and old hydrated different
        # parts.
        expected_result = generate_full_graph_state_person(
            set_back_edges=False,
            include_person_back_edges=False,
            set_ids=False,
        )
        # The only thing that should be updated is the new response date
        expected_result.supervision_violations[0].supervision_violation_responses[
            0
        ].response_date = new_response_date

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_different_enum_entities(self) -> None:
        previous_root_entity = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            races=[
                make_person_race(race=StateRace.BLACK, race_raw_text="B"),
                make_person_race(race=StateRace.WHITE, race_raw_text="W"),
            ],
        )
        entity_updates = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            races=[make_person_race(race=StateRace.ASIAN, race_raw_text="A")],
        )

        # Expect no change since there is no new info in the updates
        expected_result = attr.evolve(
            previous_root_entity,
            races=[
                make_person_race(race=StateRace.BLACK, race_raw_text="B"),
                make_person_race(race=StateRace.WHITE, race_raw_text="W"),
                make_person_race(race=StateRace.ASIAN, race_raw_text="A"),
            ],
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_different_enum_entities_different_raw_text(self) -> None:
        previous_root_entity = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            races=[
                make_person_race(race=StateRace.BLACK, race_raw_text="B"),
            ],
        )
        entity_updates = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            races=[
                make_person_race(race=StateRace.BLACK, race_raw_text="BLA"),
            ],
        )

        # Expect no change since there is no new info in the updates
        expected_result = attr.evolve(
            previous_root_entity,
            races=[
                # Because each enum entity has different raw text, we keep both
                make_person_race(race=StateRace.BLACK, race_raw_text="B"),
                make_person_race(race=StateRace.BLACK, race_raw_text="BLA"),
            ],
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_different_flat_fields(self) -> None:
        previous_root_entity = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            birthdate=datetime.date(1990, 1, 1),
        )
        entity_updates = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            gender=StateGender.MALE,
            gender_raw_text="M",
        )

        # Expect the flat fields are combined because it is a root entity
        expected_result = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            birthdate=datetime.date(1990, 1, 1),
            gender=StateGender.MALE,
            gender_raw_text="M",
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_different_flat_fields_new_enum_and_raw_text_null(
        self,
    ) -> None:
        previous_root_entity = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            residency_status=StateResidencyStatus.PERMANENT,
            residency_status_raw_text="P",
        )
        entity_updates = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            birthdate=datetime.date(1990, 1, 1),
        )

        # Expect the flat fields are combined because it is a root entity. Residency
        # info should not be overwritten.
        expected_result = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            residency_status=StateResidencyStatus.PERMANENT,
            residency_status_raw_text="P",
            birthdate=datetime.date(1990, 1, 1),
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_different_flat_fields_new_enum_null(self) -> None:
        previous_root_entity = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            residency_status=StateResidencyStatus.PERMANENT,
            residency_status_raw_text=None,
        )
        entity_updates = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            residency_status=None,
            residency_status_raw_text="X",
        )

        # Expect residency status and raw text updated as a pair since one is non-null
        expected_result = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            residency_status=None,
            residency_status_raw_text="X",
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_different_flat_fields_new_raw_text_null(self) -> None:
        previous_root_entity = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            residency_status=None,
            residency_status_raw_text="X",
        )
        entity_updates = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            residency_status=StateResidencyStatus.PERMANENT,
            residency_status_raw_text=None,
        )

        # Expect residency status and raw text updated as a pair since one is non-null
        expected_result = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            residency_status=StateResidencyStatus.PERMANENT,
            residency_status_raw_text=None,
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_staff_different_flat_fields(self) -> None:
        previous_root_entity = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
            ],
            email="foo@bar.com",
        )
        entity_updates = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
            ],
            full_name='{"given_names": "DONALD", "middle_names": "", "name_suffix": "", "surname": "DUCK"}',
        )

        # Expect the flat fields are combined because it is a root entity
        expected_result = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
            ],
            email="foo@bar.com",
            full_name='{"given_names": "DONALD", "middle_names": "", "name_suffix": "", "surname": "DUCK"}',
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_different_flat_fields_enums(self) -> None:
        previous_root_entity = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            birthdate=datetime.date(1990, 1, 1),
            gender_raw_text="M",
        )
        entity_updates = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            gender=StateGender.MALE,
        )

        # Expect the flat fields are combined because it is a root entity,
        # but the values of gender/gender_raw_text taken as a pair.
        expected_result = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            birthdate=datetime.date(1990, 1, 1),
            gender=StateGender.MALE,
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    @parameterized.expand(
        [
            (None, "X", StateResidencyStatus.PERMANENT, None),
            (StateResidencyStatus.PERMANENT, None, None, "X"),
        ]
    )
    def test_merge_people_both_enum_and_raw_text(
        self,
        old_residency_status: Optional[StateResidencyStatus],
        old_residency_status_raw_text: Optional[str],
        new_residency_status: Optional[StateResidencyStatus],
        new_residency_status_raw_text: Optional[str],
    ) -> None:
        previous_root_entity = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            residency_status=old_residency_status,
            residency_status_raw_text=old_residency_status_raw_text,
        )
        entity_root_updates = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1],
            residency_status=new_residency_status,
            residency_status_raw_text=new_residency_status_raw_text,
        )
        expected_result = attr.evolve(
            previous_root_entity,
            residency_status=new_residency_status,
            residency_status_raw_text=new_residency_status_raw_text,
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_root_updates,
        )
        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_staff_exact_match(self) -> None:
        previous_root_entity = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
            ],
        )
        entity_updates = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1")
            ],
        )

        # Expect no change since there is no new info in the updates
        expected_result = attr.evolve(previous_root_entity)

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_match_complex_root_entity_merging(self) -> None:
        # Arrange
        staff_1 = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1"),
                make_staff_external_id(external_id="ID_2", id_type="ID_TYPE_1"),
            ],
        )

        staff_2 = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_3", id_type="ID_TYPE_1"),
            ],
        )

        # Act
        result = self.merger.merge_root_entity_trees(
            old_root_entity=staff_1,
            root_entity_updates=staff_2,
        )

        # Assert
        expected_result = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1"),
                make_staff_external_id(external_id="ID_2", id_type="ID_TYPE_1"),
                make_staff_external_id(external_id="ID_3", id_type="ID_TYPE_1"),
            ],
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

        # Arrange 2
        staff_3 = make_staff(
            external_ids=[
                # NOTE: ORDER HERE MATTERS FOR THIS ONE ONLY
                make_staff_external_id(external_id="ID_3", id_type="ID_TYPE_1"),
                make_staff_external_id(external_id="ID_2", id_type="ID_TYPE_1"),
            ],
        )

        # Act 2
        result = self.merger.merge_root_entity_trees(
            old_root_entity=result,
            root_entity_updates=staff_3,
        )

        # Assert 2
        expected_result = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1"),
                make_staff_external_id(external_id="ID_2", id_type="ID_TYPE_1"),
                make_staff_external_id(external_id="ID_3", id_type="ID_TYPE_1"),
            ],
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

        # Arrange 3
        staff_4 = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1"),
                make_staff_external_id(external_id="ID_4", id_type="ID_TYPE_2"),
            ],
        )

        # Act 3
        result = self.merger.merge_root_entity_trees(
            old_root_entity=result,
            root_entity_updates=staff_4,
        )

        # Assert 3
        expected_result = make_staff(
            external_ids=[
                make_staff_external_id(external_id="ID_1", id_type="ID_TYPE_1"),
                make_staff_external_id(external_id="ID_2", id_type="ID_TYPE_1"),
                make_staff_external_id(external_id="ID_3", id_type="ID_TYPE_1"),
                make_staff_external_id(external_id="ID_4", id_type="ID_TYPE_2"),
            ],
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_people_new_external_id(self) -> None:
        previous_root_entity = make_person(
            full_name=_FULL_NAME_1,
            external_ids=[_EXTERNAL_ID_ENTITY_1, _EXTERNAL_ID_ENTITY_2],
        )
        entity_updates = make_person(
            external_ids=[_EXTERNAL_ID_ENTITY_1, _EXTERNAL_ID_ENTITY_3],
        )

        expected_result = make_person(
            full_name=_FULL_NAME_1,
            external_ids=[
                _EXTERNAL_ID_ENTITY_1,
                _EXTERNAL_ID_ENTITY_2,
                _EXTERNAL_ID_ENTITY_3,
            ],
        )

        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_new_child(self) -> None:
        # Arrange 1
        previous_root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
        )

        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            assessments=[attr.evolve(_ASSESSMENT_1)],
        )

        # Act 1 - Merge one new child entity onto person
        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        # Assert 1
        expected_result = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            assessments=[attr.evolve(_ASSESSMENT_1)],
        )
        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

        # Arrange 2
        old_root_entity = expected_result

        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            assessments=[attr.evolve(_ASSESSMENT_2)],
        )

        # Act 2 - Merge a second new child entity onto person
        result = self.merger.merge_root_entity_trees(
            old_root_entity=old_root_entity,
            root_entity_updates=entity_updates,
        )

        # Assert 2
        expected_result = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            assessments=[attr.evolve(_ASSESSMENT_1), attr.evolve(_ASSESSMENT_2)],
        )
        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_updated_child(self) -> None:
        # Arrange
        previous_root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            assessments=[attr.evolve(_ASSESSMENT_1)],
        )

        updated_assessment_1 = make_assessment(
            external_id=_ASSESSMENT_1.external_id,
            assessment_type=StateAssessmentType.COMPAS,
            assessment_type_raw_text="COMP",
            assessment_date=datetime.date(2022, 1, 1),
            assessment_score=10,
            assessment_level=StateAssessmentLevel.MEDIUM,
        )

        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            assessments=[updated_assessment_1],
        )

        # Act
        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        # Assert
        expected_result = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            assessments=[attr.evolve(updated_assessment_1)],
        )
        self.assert_expected_matches_result(
            expected_result=expected_result,
            result=result,
        )

    def test_merge_updated_grandchild(self) -> None:
        # Arrange
        charge = make_state_charge(external_id="CHARGE_A", ncic_code="3599")
        incarceration_sentence = make_incarceration_sentence(
            external_id="A",
            status=StateSentenceStatus.SERVING,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            min_length_days=365,
            max_length_days=365 * 2,
            charges=[attr.evolve(charge)],
        )
        assessment = make_assessment(external_id="a1")
        previous_root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[attr.evolve(incarceration_sentence)],
            assessments=[attr.evolve(assessment)],
        )

        updated_charge = attr.evolve(charge, ncic_code="9999")
        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[
                attr.evolve(incarceration_sentence, charges=[updated_charge])
            ],
        )

        # Act
        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        # Assert
        # The charge should have been updated, assessments remain unchanged
        expected_result = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[
                attr.evolve(
                    incarceration_sentence, charges=[attr.evolve(updated_charge)]
                )
            ],
            assessments=[attr.evolve(assessment)],
        )
        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_multi_parent_charge_on_update(self) -> None:
        # Arrange
        charge = make_state_charge(external_id="CHARGE_A", ncic_code="3599")
        incarceration_sentence = make_incarceration_sentence(
            external_id="A",
            status=StateSentenceStatus.SERVING,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            min_length_days=365,
            max_length_days=365 * 2,
            charges=[attr.evolve(charge)],
        )
        previous_root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[attr.evolve(incarceration_sentence)],
        )

        supervision_sentence = make_supervision_sentence(
            external_id="A",
            status=StateSentenceStatus.SERVING,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            min_length_days=90,
            max_length_days=365,
            # Separate, updated copy of charge also attached to this supervision
            # sentence
            charges=[attr.evolve(charge, description="Charge description")],
        )
        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            supervision_sentences=[attr.evolve(supervision_sentence)],
        )

        # Act
        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        # Assert
        expected_charge = attr.evolve(charge, description="Charge description")

        # Both the incarceration and supervision sentence should reference the same
        # charge.
        expected_result = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[
                attr.evolve(incarceration_sentence, charges=[expected_charge])
            ],
            supervision_sentences=[
                attr.evolve(supervision_sentence, charges=[expected_charge])
            ],
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_multi_parent_charge_both_parents_new(self) -> None:
        # Arrange
        charge = make_state_charge(external_id="CHARGE_A", ncic_code="3599")
        incarceration_sentence = make_incarceration_sentence(
            external_id="A",
            status=StateSentenceStatus.SERVING,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            min_length_days=365,
            max_length_days=365 * 2,
            charges=[attr.evolve(charge)],
        )
        supervision_sentence = make_supervision_sentence(
            external_id="A",
            status=StateSentenceStatus.SERVING,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            min_length_days=90,
            max_length_days=365,
            # Separate copy of charge also attached to this sentence (mimics output of
            # ingest view parser).
            charges=[attr.evolve(charge)],
        )
        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[attr.evolve(incarceration_sentence)],
            supervision_sentences=[attr.evolve(supervision_sentence)],
        )

        # Act
        result = self.merger.merge_root_entity_trees(
            old_root_entity=None,
            root_entity_updates=entity_updates,
        )

        # Assert
        expected_charge = attr.evolve(charge)

        # Both the incarceration and supervision sentence should reference the same
        # charge.
        expected_result = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[
                attr.evolve(incarceration_sentence, charges=[expected_charge])
            ],
            supervision_sentences=[
                attr.evolve(supervision_sentence, charges=[expected_charge])
            ],
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_multi_parent_charge_on_update_original_was_already_multi_parent(
        self,
    ) -> None:
        # Arrange
        charge = make_state_charge(external_id="CHARGE_A", ncic_code="3599")
        original_charge = attr.evolve(charge)
        incarceration_sentence = make_incarceration_sentence(
            external_id="A",
            status=StateSentenceStatus.SERVING,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            min_length_days=365,
            max_length_days=365 * 2,
            charges=[original_charge],
        )
        supervision_sentence = make_supervision_sentence(
            external_id="B",
            status=StateSentenceStatus.SERVING,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            min_length_days=90,
            max_length_days=365,
            charges=[original_charge],
        )
        previous_root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[attr.evolve(incarceration_sentence)],
            supervision_sentences=[attr.evolve(supervision_sentence)],
        )

        updated_charge = attr.evolve(charge, description="Charge description")
        supervision_sentence_2 = make_supervision_sentence(
            external_id="C",
            status=StateSentenceStatus.SERVING,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            min_length_days=180,
            max_length_days=400,
            # Separate, updated copy of charge also attached to this supervision
            # sentence
            charges=[updated_charge],
        )
        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            supervision_sentences=[attr.evolve(supervision_sentence_2)],
        )

        # Act
        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        # Assert
        expected_charge = attr.evolve(charge, description="Charge description")

        # Both the incarceration and supervision sentence should reference the same
        # charge.
        expected_result = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            incarceration_sentences=[
                attr.evolve(incarceration_sentence, charges=[expected_charge])
            ],
            supervision_sentences=[
                attr.evolve(supervision_sentence, charges=[expected_charge]),
                attr.evolve(supervision_sentence_2, charges=[expected_charge]),
            ],
        )

        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_task_deadline(self) -> None:
        # Arrange
        previous_root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            task_deadlines=[attr.evolve(_TASK_DEADLINE_1)],
        )

        slightly_different_task_deadline = attr.evolve(
            _TASK_DEADLINE_1, task_metadata="metadata3"
        )

        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            task_deadlines=[slightly_different_task_deadline],
        )

        # Act
        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        # Assert
        expected_result = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            task_deadlines=[
                attr.evolve(_TASK_DEADLINE_1),
                attr.evolve(slightly_different_task_deadline),
            ],
        )
        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_merge_task_deadline_similar_deadlines_on_previous(self) -> None:
        # Arrange
        task_deadline = attr.evolve(_TASK_DEADLINE_1)
        slightly_different_task_deadline = attr.evolve(
            _TASK_DEADLINE_1, task_metadata="metadata3"
        )
        previous_root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            task_deadlines=[
                # These two deadlines only differ in the task_metadata but still should
                # not end up merged.
                task_deadline,
                slightly_different_task_deadline,
            ],
        )

        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            task_deadlines=[attr.evolve(_TASK_DEADLINE_2)],
        )

        # Act
        result = self.merger.merge_root_entity_trees(
            old_root_entity=previous_root_entity,
            root_entity_updates=entity_updates,
        )

        # Assert
        expected_result = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            task_deadlines=[
                attr.evolve(task_deadline),
                attr.evolve(slightly_different_task_deadline),
                attr.evolve(_TASK_DEADLINE_2),
            ],
        )
        self.assert_expected_matches_result(
            expected_result=expected_result, result=result
        )

    def test_throws_if_old_root_entity_has_improperly_merged_values(self) -> None:
        # Arrange
        previous_root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            task_deadlines=[
                # We would expect these two deadlines to be merged by the time we get
                # to this point, but they are not
                attr.evolve(_TASK_DEADLINE_1),
                attr.evolve(_TASK_DEADLINE_1),
            ],
        )

        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            task_deadlines=[attr.evolve(_TASK_DEADLINE_2)],
        )

        # Act
        with self.assertRaisesRegex(
            ValueError,
            (
                r"Found duplicate item "
                r"\[.*DISCHARGE_FROM_INCARCERATION||2000-01-02T03:04:05.000006|metadata1\]"
            ),
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=previous_root_entity,
                root_entity_updates=entity_updates,
            )

    def test_throws_if_new_root_entity_has_improperly_merged_values(self) -> None:
        # Arrange
        previous_root_entity = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            task_deadlines=[attr.evolve(_TASK_DEADLINE_1)],
        )

        entity_updates = make_person(
            external_ids=[attr.evolve(_EXTERNAL_ID_ENTITY_1)],
            task_deadlines=[
                # We would expect these two deadlines to be merged by the time we get
                # to this point, but they are not
                attr.evolve(_TASK_DEADLINE_2),
                attr.evolve(_TASK_DEADLINE_2),
            ],
        )

        # Act
        with self.assertRaisesRegex(
            ValueError,
            (
                r"Found duplicate item "
                r"\[.*#DISCHARGE_FROM_INCARCERATION||2000-02-03T04:05:06.000007|metadata2\]"
            ),
        ):
            _ = self.merger.merge_root_entity_trees(
                old_root_entity=previous_root_entity,
                root_entity_updates=entity_updates,
            )


class TestIsReferenceOnlyEntity(unittest.TestCase):
    def test_is_reference_only_state_entity(self) -> None:
        for entity_cls in get_all_entity_classes_in_module(state_entities):
            if entity_cls not in _REFERENCE_ENTITY_EXAMPLES:
                self.fail(
                    f"Expected to find [{entity_cls}] in REFERENCE_ENTITY_EXAMPLES"
                )
            for entity in _REFERENCE_ENTITY_EXAMPLES[entity_cls]:
                self.assertIsInstance(entity, entity_cls)
                self.assertTrue(is_reference_only_state_entity(entity))

            if entity_cls not in _HAS_MEANINGFUL_DATA_ENTITIES:
                self.fail(
                    f"Expected to find [{entity_cls}] in NON_REFERENCE_ENTITY_EXAMPLES"
                )

            for entity in _HAS_MEANINGFUL_DATA_ENTITIES[entity_cls]:
                self.assertIsInstance(entity, entity_cls)
                self.assertFalse(is_reference_only_state_entity(entity), f"{entity}")
