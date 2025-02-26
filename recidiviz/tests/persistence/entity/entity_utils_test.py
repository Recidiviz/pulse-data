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
from typing import Dict, List, Type
from unittest import TestCase

import attr
import pytz

from recidiviz.common.constants.state.state_agent import StateAgentType
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
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema_utils import get_state_database_entities
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    SchemaEdgeDirectionChecker,
    deep_entity_update,
    is_placeholder,
    is_reference_only_entity,
    is_standalone_class,
    prune_dangling_placeholders_from_tree,
)
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
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
                    state_code="US_XX", status=StateChargeStatus.PRESENT_WITHOUT_INFO
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


PLACEHOLDER_ENTITY_EXAMPLES: Dict[Type[DatabaseEntity], List[DatabaseEntity]] = {
    schema.StateAgent: [schema.StateAgent(state_code=StateCode.US_XX.value)],
    schema.StateAssessment: [schema.StateAssessment(state_code=StateCode.US_XX.value)],
    schema.StateCharge: [
        schema.StateCharge(
            state_code=StateCode.US_XX.value,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO.value,
        )
    ],
    schema.StateCourtCase: [schema.StateCourtCase(state_code=StateCode.US_XX.value)],
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

REFERENCE_ENTITY_EXAMPLES: Dict[Type[DatabaseEntity], List[DatabaseEntity]] = {
    schema.StateAgent: [
        schema.StateAgent(state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID)
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
    schema.StateCourtCase: [
        schema.StateCourtCase(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
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
    schema.StateSupervisionCaseTypeEntry: [
        schema.StateSupervisionCaseTypeEntry(
            state_code=StateCode.US_XX.value, external_id=_EXTERNAL_ID
        )
    ],
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
    schema.StateAgent: [
        schema.StateAgent(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
        ),
        # If meaningful/non-default data is filled out, we do not consider it to be a placeholder.
        schema.StateAgent(
            state_code=StateCode.US_XX.value,
            agent_type=StateAgentType.SUPERVISION_OFFICER.value,
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
    schema.StateCourtCase: [
        schema.StateCourtCase(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
            county_code="my county",
        ),
        schema.StateCourtCase(
            state_code=StateCode.US_XX.value, county_code="my county"
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
        schema.StatePersonEthnicity(
            state_code=StateCode.US_XX.value, ethnicity_raw_text="X"
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
        schema.StatePersonRace(state_code=StateCode.US_XX.value, race_raw_text="X"),
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
    schema.StateSupervisionCaseTypeEntry: [
        schema.StateSupervisionCaseTypeEntry(
            state_code=StateCode.US_XX.value,
            external_id=_EXTERNAL_ID,
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
            state_code=StateCode.US_XX.value, condition="DRG"
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

    @staticmethod
    def to_entity(schema_obj):
        return converter.convert_schema_object_to_entity(
            schema_obj, populate_back_edges=False
        )

    def test_isStandaloneClass(self) -> None:
        for cls in schema_utils.get_state_database_entities():
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

    def test_is_placeholder(self) -> None:
        field_index = CoreEntityFieldIndex()
        for db_entity_cls in get_state_database_entities():
            if db_entity_cls not in PLACEHOLDER_ENTITY_EXAMPLES:
                self.fail(
                    f"Expected to find [{db_entity_cls}] in PLACEHOLDER_ENTITY_EXAMPLES"
                )
            for entity in PLACEHOLDER_ENTITY_EXAMPLES[db_entity_cls]:
                self.assertIsInstance(entity, db_entity_cls)
                self.assertTrue(
                    is_placeholder(entity, field_index),
                    f"Found entity that should be a placeholder but does not [{entity}]",
                )

            if db_entity_cls not in REFERENCE_ENTITY_EXAMPLES:
                self.fail(
                    f"Expected to find [{db_entity_cls}] in REFERENCE_ENTITY_EXAMPLES"
                )
            for entity in REFERENCE_ENTITY_EXAMPLES[db_entity_cls]:
                self.assertIsInstance(entity, db_entity_cls)
                self.assertFalse(is_placeholder(entity, field_index))

            if db_entity_cls not in HAS_MEANINGFUL_DATA_ENTITIES:
                self.fail(
                    f"Expected to find [{db_entity_cls}] in NON_REFERENCE_ENTITY_EXAMPLES"
                )
            for entity in HAS_MEANINGFUL_DATA_ENTITIES[db_entity_cls]:
                self.assertIsInstance(entity, db_entity_cls)
                self.assertFalse(is_placeholder(entity, field_index))

    def test_is_reference_only_entity(self) -> None:
        field_index = CoreEntityFieldIndex()
        for db_entity_cls in get_state_database_entities():
            if db_entity_cls not in PLACEHOLDER_ENTITY_EXAMPLES:
                self.fail(
                    f"Expected to find [{db_entity_cls}] in PLACEHOLDER_ENTITY_EXAMPLES"
                )
            for entity in PLACEHOLDER_ENTITY_EXAMPLES[db_entity_cls]:
                self.assertIsInstance(entity, db_entity_cls)
                self.assertFalse(is_reference_only_entity(entity, field_index))

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

    def test_build_new_entity_with_bidirectionally_updated_attributes(self):
        sp = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=123,
            external_id="123",
            state_code=_STATE_CODE,
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
            case_type_entries=updated_case_type_entries,
        )

        for c in updated_case_type_entries:
            c.supervision_period = expected_sp

        self.assertEqual(expected_sp, updated_entity)

    def test_build_new_entity_with_bidirectionally_updated_attributes_flat_fields_only(
        self,
    ):
        sp = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=123,
            external_id="123",
            state_code=_STATE_CODE,
        )

        updated_entity = deep_entity_update(
            original_entity=sp,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        expected_sp = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=123,
            external_id="123",
            state_code=_STATE_CODE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        self.assertEqual(expected_sp, updated_entity)

    def test_build_new_entity_with_bidirectionally_updated_attributes_flat_and_refs(
        self,
    ):
        """Tests when there are related entities on the entity, but the only attribute
        being updated is a flat field."""
        sp = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=123,
            external_id="123",
            state_code=_STATE_CODE,
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

    def test_build_new_entity_with_bidirectionally_updated_attributes_add_to_list(self):
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
    ):
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
