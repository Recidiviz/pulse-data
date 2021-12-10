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
"""Helper methods to generate schema objects with required fields
prepopulated.
"""
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_court_case import StateCourtCaseStatus
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.schema.state import schema

_ID_TYPE = "ID_TYPE"
_STATE_CODE = "US_XX"


def generate_person(**kwargs) -> schema.StatePerson:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StatePerson(**args)


def generate_external_id(**kwargs) -> schema.StatePersonExternalId:
    args = {
        "state_code": _STATE_CODE,
        "id_type": _ID_TYPE,
    }
    args.update(kwargs)
    return schema.StatePersonExternalId(**args)


def generate_sentence_group(**kwargs) -> schema.StateSentenceGroup:
    args = {
        "status": StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateSentenceGroup(**args)


def generate_race(**kwargs) -> schema.StatePersonRace:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StatePersonRace(**args)


def generate_ethnicity(**kwargs) -> schema.StatePersonEthnicity:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StatePersonEthnicity(**args)


def generate_alias(**kwargs) -> schema.StatePersonAlias:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StatePersonAlias(**args)


def generate_incarceration_sentence(
    person, **kwargs
) -> schema.StateIncarcerationSentence:
    args = {
        "status": StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateIncarcerationSentence(person=person, **args)


def generate_incarceration_period(person, **kwargs) -> schema.StateIncarcerationPeriod:
    args = {
        "state_code": _STATE_CODE,
        "incarceration_type": StateIncarcerationType.STATE_PRISON.value,
    }
    args.update(kwargs)
    return schema.StateIncarcerationPeriod(person=person, **args)


def generate_incarceration_incident(
    person, **kwargs
) -> schema.StateIncarcerationIncident:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateIncarcerationIncident(person=person, **args)


def generate_supervision_violation_response(
    person, **kwargs
) -> schema.StateSupervisionViolationResponse:

    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateSupervisionViolationResponse(person=person, **args)


def generate_supervision_violation_response_decision_entry(
    person, **kwargs
) -> schema.StateSupervisionViolationResponseDecisionEntry:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateSupervisionViolationResponseDecisionEntry(person=person, **args)


def generate_supervision_violation(
    person, **kwargs
) -> schema.StateSupervisionViolation:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateSupervisionViolation(person=person, **args)


def generate_supervision_violation_type_entry(
    person, **kwargs
) -> schema.StateSupervisionViolationTypeEntry:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateSupervisionViolationTypeEntry(person=person, **args)


def generate_supervision_violated_condition_entry(
    person, **kwargs
) -> schema.StateSupervisionViolatedConditionEntry:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateSupervisionViolatedConditionEntry(person=person, **args)


def generate_supervision_case_type_entry(
    person, **kwargs
) -> schema.StateSupervisionCaseTypeEntry:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateSupervisionCaseTypeEntry(person=person, **args)


def generate_supervision_period(person, **kwargs) -> schema.StateSupervisionPeriod:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateSupervisionPeriod(person=person, **args)


def generate_supervision_sentence(person, **kwargs) -> schema.StateSupervisionSentence:
    args = {
        "state_code": _STATE_CODE,
        "status": StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
    }
    args.update(kwargs)
    return schema.StateSupervisionSentence(person=person, **args)


def generate_charge(person, **kwargs) -> schema.StateCharge:
    args = {
        "status": ChargeStatus.PRESENT_WITHOUT_INFO.value,
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateCharge(person=person, **args)


def generate_court_case(person, **kwargs) -> schema.StateCourtCase:
    args = {
        "status": StateCourtCaseStatus.PRESENT_WITHOUT_INFO.value,
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateCourtCase(person=person, **args)


def generate_assessment(person, **kwargs) -> schema.StateAssessment:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateAssessment(person=person, **args)


def generate_agent(**kwargs) -> schema.StateAgent:
    args = {
        "state_code": _STATE_CODE,
        "agent_type": StateAgentType.JUDGE.value,
    }
    args.update(kwargs)
    return schema.StateAgent(**args)


def generate_parole_decision(person, **kwargs) -> schema.StateParoleDecision:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateParoleDecision(person=person, **args)


def generate_early_discharge(person, **kwargs) -> schema.StateEarlyDischarge:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateEarlyDischarge(person=person, **args)


def generate_program_assignment(person, **kwargs) -> schema.StateProgramAssignment:
    args = {
        "state_code": _STATE_CODE,
        "participation_status": StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO.value,
    }
    args.update(kwargs)
    return schema.StateProgramAssignment(person=person, **args)


def generate_supervision_contact(person, **kwargs) -> schema.StateSupervisionContact:
    args = {
        "state_code": _STATE_CODE,
    }
    args.update(kwargs)
    return schema.StateSupervisionContact(person=person, **args)
