"""Utils for testing the database."""

import datetime
from typing import List, Optional

from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
)
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
)
from recidiviz.common.constants.state.state_person import StateEthnicity, StateRace
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)
from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
    StateStaffRoleType,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactStatus,
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
from recidiviz.persistence.database.schema.state import schema as state_schema


def generate_test_supervision_violation_response_decision_entry(
    person_id: int,
) -> state_schema.StateSupervisionViolationResponseDecisionEntry:
    return state_schema.StateSupervisionViolationResponseDecisionEntry(
        supervision_violation_response_decision_entry_id=123,
        state_code="US_XX",
        decision=StateSupervisionViolationResponseDecision.REVOCATION.value,
        decision_raw_text="REV",
        person_id=person_id,
    )


def generate_test_supervision_violation_response(
    person_id: int,
    decisions: Optional[
        List[state_schema.StateSupervisionViolationResponseDecisionEntry]
    ] = None,
) -> state_schema.StateSupervisionViolationResponse:
    decisions = decisions or [
        generate_test_supervision_violation_response_decision_entry(person_id)
    ]

    instance = state_schema.StateSupervisionViolationResponse(
        supervision_violation_response_id=456,
        external_id="external_id",
        state_code="US_XX",
        person_id=person_id,
        supervision_violation_response_decisions=decisions,
        response_date=datetime.date(2000, 1, 1),
    )

    for decision in decisions:
        decision.supervision_violation_response_id = (
            instance.supervision_violation_response_id
        )

    return instance


def generate_test_assessment(person_id: int) -> state_schema.StateAssessment:
    instance = state_schema.StateAssessment(
        assessment_id=345,
        external_id="external_id",
        state_code="US_XX",
        assessment_type=StateAssessmentType.LSIR.value,
        assessment_score=10,
        person_id=person_id,
    )
    return instance


def generate_test_supervision_case_type(
    person_id: int,
) -> state_schema.StateSupervisionCaseTypeEntry:
    instance = state_schema.StateSupervisionCaseTypeEntry(
        person_id=person_id,
        supervision_case_type_entry_id=12345,
        external_id="external_id",
        state_code="US_XX",
        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE.value,
        case_type_raw_text="DV",
    )
    return instance


def generate_test_supervision_contact(
    person_id: int,
) -> state_schema.StateSupervisionContact:
    instance = state_schema.StateSupervisionContact(
        person_id=person_id,
        supervision_contact_id=12345,
        external_id="external_id",
        state_code="US_XX",
        status=StateSupervisionContactStatus.COMPLETED.value,
        status_raw_text="COMPLETED",
    )
    return instance


def generate_test_supervision_violation(
    person_id: int,
    supervision_violation_responses: List[
        state_schema.StateSupervisionViolationResponse
    ],
) -> state_schema.StateSupervisionViolation:
    """Generates a test StateSupervisionViolation."""
    supervision_violation_id = 321

    instance = state_schema.StateSupervisionViolation(
        supervision_violation_id=supervision_violation_id,
        external_id="external_id",
        state_code="US_XX",
        person_id=person_id,
        supervision_violated_conditions=[
            state_schema.StateSupervisionViolatedConditionEntry(
                supervision_violated_condition_entry_id=765,
                state_code="US_XX",
                condition=StateSupervisionViolatedConditionType.SPECIAL_CONDITIONS,
                condition_raw_text="CURFEW",
                person_id=person_id,
                supervision_violation_id=supervision_violation_id,
            )
        ],
        supervision_violation_types=[
            state_schema.StateSupervisionViolationTypeEntry(
                supervision_violation_type_entry_id=987,
                state_code="US_XX",
                violation_type=StateSupervisionViolationType.TECHNICAL.value,
                violation_type_raw_text="T",
                person_id=person_id,
                supervision_violation_id=supervision_violation_id,
            )
        ],
        supervision_violation_responses=supervision_violation_responses,
    )

    return instance


def generate_test_supervision_period(
    person_id: int, case_types: List[state_schema.StateSupervisionCaseTypeEntry]
) -> state_schema.StateSupervisionPeriod:
    instance = state_schema.StateSupervisionPeriod(
        supervision_period_id=4444,
        external_id="external_id",
        state_code="US_XX",
        person_id=person_id,
        case_type_entries=case_types,
    )

    return instance


def generate_test_incarceration_incident_outcome(
    person_id: int,
) -> state_schema.StateIncarcerationIncidentOutcome:
    instance = state_schema.StateIncarcerationIncidentOutcome(
        incarceration_incident_outcome_id=3211,
        external_id="external_id",
        outcome_type=StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR.value,
        state_code="US_XX",
        person_id=person_id,
    )
    return instance


def generate_test_incarceration_incident(
    person_id: int,
    incarceration_incident_outcomes: List[
        state_schema.StateIncarcerationIncidentOutcome
    ],
) -> state_schema.StateIncarcerationIncident:
    instance = state_schema.StateIncarcerationIncident(
        incarceration_incident_id=321,
        external_id="external_id",
        state_code="US_XX",
        person_id=person_id,
        incarceration_incident_outcomes=incarceration_incident_outcomes,
    )

    return instance


def generate_test_incarceration_period(
    person_id: int,
) -> state_schema.StateIncarcerationPeriod:
    instance = state_schema.StateIncarcerationPeriod(
        incarceration_period_id=5555,
        external_id="external_id",
        state_code="US_XX",
        person_id=person_id,
    )
    return instance


def generate_test_charge(
    person_id: int,
    charge_id: int,
    state_code: str = "US_XX",
) -> state_schema.StateCharge:
    instance = state_schema.StateCharge(
        external_id=f"{charge_id}",
        charge_id=charge_id,
        person_id=person_id,
        state_code=state_code,
        status=StateChargeStatus.PRESENT_WITHOUT_INFO.value,
    )

    return instance


def generate_test_supervision_sentence(
    person_id: int,
    charges: List[state_schema.StateCharge],
    early_discharges: Optional[List[state_schema.StateEarlyDischarge]] = None,
) -> state_schema.StateSupervisionSentence:
    instance = state_schema.StateSupervisionSentence(
        supervision_sentence_id=1111,
        external_id="external_id",
        status=StateSentenceStatus.SERVING.value,
        state_code="US_XX",
        person_id=person_id,
        charges=charges,
        early_discharges=(early_discharges if early_discharges else []),
    )

    return instance


def generate_test_incarceration_sentence(
    person_id: int,
    charges: Optional[List[state_schema.StateCharge]] = None,
    early_discharges: Optional[List[state_schema.StateEarlyDischarge]] = None,
) -> state_schema.StateIncarcerationSentence:
    instance = state_schema.StateIncarcerationSentence(
        incarceration_sentence_id=2222,
        external_id="external_id",
        status=StateSentenceStatus.SUSPENDED.value,
        state_code="US_XX",
        person_id=person_id,
        is_capital_punishment=False,
        charges=(charges if charges else []),
        early_discharges=(early_discharges if early_discharges else []),
    )

    return instance


def generate_test_sentence_status_snapshot(
    person_id: int,
) -> state_schema.StateSentenceStatusSnapshot:
    instance = state_schema.StateSentenceStatusSnapshot(
        sentence_status_snapshot_id=2222,
        status=StateSentenceStatus.SERVING.value,
        state_code="US_XX",
        status_update_datetime=datetime.datetime(2020, 1, 1, 0, 0, 0),
        person_id=person_id,
    )

    return instance


def generate_test_sentence(
    person_id: int,
    status_snapshots: List[state_schema.StateSentenceStatusSnapshot],
) -> state_schema.StateSentence:
    instance = state_schema.StateSentence(
        sentence_id=2222,
        external_id="external_id",
        state_code="US_XX",
        person_id=person_id,
        is_capital_punishment=False,
        sentence_status_snapshots=status_snapshots,
    )

    return instance


def generate_test_early_discharge(person_id: int) -> state_schema.StateEarlyDischarge:
    instance = state_schema.StateEarlyDischarge(
        early_discharge_id=1,
        external_id="external_id",
        state_code="US_XX",
        person_id=person_id,
        decision=StateEarlyDischargeDecision.REQUEST_DENIED.value,
    )
    return instance


def generate_test_program_assignment(
    person_id: int,
) -> state_schema.StateProgramAssignment:
    instance = state_schema.StateProgramAssignment(
        program_assignment_id=1,
        external_id="external_id",
        state_code="US_XX",
        person_id=person_id,
        participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO.value,
        referral_date=datetime.date(2000, 1, 1),
    )
    return instance


def generate_test_person(
    *,
    person_id: int,
    state_code: str,
    incarceration_incidents: List[state_schema.StateIncarcerationIncident],
    supervision_violations: List[state_schema.StateSupervisionViolation],
    supervision_contacts: List[state_schema.StateSupervisionContact],
    incarceration_sentences: List[state_schema.StateIncarcerationSentence],
    supervision_sentences: List[state_schema.StateSupervisionSentence],
    incarceration_periods: List[state_schema.StateIncarcerationPeriod],
    supervision_periods: List[state_schema.StateSupervisionPeriod],
) -> state_schema.StatePerson:
    """Returns a StatePerson to be used for testing."""
    instance = state_schema.StatePerson(
        person_id=person_id,
        state_code=state_code,
        full_name="name",
        birthdate=datetime.date(1980, 1, 5),
        external_ids=[
            state_schema.StatePersonExternalId(
                person_external_id_id=234,
                external_id="person_external_id",
                id_type="STATE",
                state_code="US_XX",
                person_id=person_id,
            )
        ],
        aliases=[
            state_schema.StatePersonAlias(
                person_alias_id=1456,
                state_code="US_XX",
                full_name="name",
                person_id=person_id,
            )
        ],
        races=[
            state_schema.StatePersonRace(
                person_race_id=345,
                state_code="US_XX",
                race=StateRace.BLACK.value,
                race_raw_text="BLK",
                person_id=person_id,
            )
        ],
        ethnicity=StateEthnicity.NOT_HISPANIC.value,
        ethnicity_raw_text="HISP",
        incarceration_incidents=incarceration_incidents,
        supervision_violations=supervision_violations,
        supervision_contacts=supervision_contacts,
        incarceration_sentences=incarceration_sentences,
        supervision_sentences=supervision_sentences,
        incarceration_periods=incarceration_periods,
        supervision_periods=supervision_periods,
        assessments=[
            state_schema.StateAssessment(
                assessment_id=456,
                person_id=person_id,
                state_code="US_XX",
            ),
            state_schema.StateAssessment(
                assessment_id=4567,
                person_id=person_id,
                state_code="US_XX",
            ),
        ],
        program_assignments=[
            state_schema.StateProgramAssignment(
                program_assignment_id=567,
                participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO.value,
                state_code="US_XX",
            )
        ],
    )
    return instance


def generate_schema_state_person_obj_tree() -> state_schema.StatePerson:
    """Test util for generating a StatePerson schema object that has at
     least one child of each possible schema object type defined on
     state/schema.py.

    Returns:
        A test instance of a StatePerson schema object.
    """
    test_person_id = 143

    test_supervision_case_type = generate_test_supervision_case_type(test_person_id)
    test_supervision_period = generate_test_supervision_period(
        test_person_id,
        [test_supervision_case_type],
    )

    test_incarceration_period = generate_test_incarceration_period(
        test_person_id,
    )

    test_charge_1 = generate_test_charge(
        test_person_id,
        6666,
    )
    test_charge_2 = generate_test_charge(
        test_person_id,
        7777,
    )

    test_early_discharge = generate_test_early_discharge(test_person_id)

    test_supervision_sentence = generate_test_supervision_sentence(
        test_person_id,
        [test_charge_1, test_charge_2],
        [test_early_discharge],
    )

    test_incarceration_sentence = generate_test_incarceration_sentence(
        test_person_id,
        [test_charge_1, test_charge_2],
    )

    test_state_code = "US_XX"

    test_incarceration_incident_outcome = generate_test_incarceration_incident_outcome(
        test_person_id
    )

    test_incarceration_incident = generate_test_incarceration_incident(
        test_person_id, [test_incarceration_incident_outcome]
    )

    test_supervision_violation_response = generate_test_supervision_violation_response(
        test_person_id
    )

    test_supervision_violation = generate_test_supervision_violation(
        test_person_id, [test_supervision_violation_response]
    )

    test_contact = generate_test_supervision_contact(test_person_id)

    test_person = generate_test_person(
        person_id=test_person_id,
        state_code=test_state_code,
        incarceration_incidents=[test_incarceration_incident],
        supervision_violations=[test_supervision_violation],
        supervision_contacts=[test_contact],
        incarceration_sentences=[test_incarceration_sentence],
        supervision_sentences=[test_supervision_sentence],
        incarceration_periods=[test_incarceration_period],
        supervision_periods=[test_supervision_period],
    )

    return test_person


def generate_test_staff_external_id(staff_id: int) -> state_schema.StateStaffExternalId:
    return state_schema.StateStaffExternalId(
        staff_external_id_id=123,
        external_id="123",
        state_code="US_XX",
        id_type="TYPE",
        staff_id=staff_id,
    )


def generate_test_staff_location_period(
    staff_id: int,
) -> state_schema.StateStaffLocationPeriod:
    return state_schema.StateStaffLocationPeriod(
        staff_location_period_id=123,
        external_id="123",
        state_code="US_XX",
        start_date=datetime.date(2023, 1, 1),
        end_date=datetime.date(2023, 6, 1),
        location_external_id="LOC",
        staff_id=staff_id,
    )


def generate_test_staff_caseload_type_period(
    staff_id: int,
) -> state_schema.StateStaffCaseloadTypePeriod:
    return state_schema.StateStaffCaseloadTypePeriod(
        staff_caseload_type_period_id=123,
        external_id="123",
        state_code="US_XX",
        caseload_type=StateStaffCaseloadType.ADMINISTRATIVE_SUPERVISION,
        caseload_type_raw_text="AS",
        start_date=datetime.date(2023, 1, 1),
        end_date=datetime.date(2023, 6, 1),
        staff_id=staff_id,
    )


def generate_test_staff_supervisor_period(
    staff_id: int,
) -> state_schema.StateStaffSupervisorPeriod:
    return state_schema.StateStaffSupervisorPeriod(
        staff_supervisor_period_id=123,
        external_id="123",
        state_code="US_XX",
        start_date=datetime.date(2023, 1, 1),
        end_date=datetime.date(2023, 6, 1),
        supervisor_staff_external_id="234",
        supervisor_staff_external_id_type="SUPERVISOR",
        staff_id=staff_id,
    )


def generate_test_staff_role_period(staff_id: int) -> state_schema.StateStaffRolePeriod:
    return state_schema.StateStaffRolePeriod(
        staff_role_period_id=123,
        external_id="123",
        state_code="US_XX",
        start_date=datetime.date(2023, 1, 1),
        end_date=datetime.date(2023, 6, 1),
        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
        role_type_raw_text="SUPERVISION_OFFICER",
        role_subtype=StateStaffRoleSubtype.SUPERVISION_OFFICER,
        role_subtype_raw_text="SUPERVISION_OFFICER",
        staff_id=staff_id,
    )


def generate_test_staff(
    staff_id: int,
    external_ids: Optional[List[state_schema.StateStaffExternalId]] = None,
    location_periods: Optional[List[state_schema.StateStaffLocationPeriod]] = None,
    role_periods: Optional[List[state_schema.StateStaffRolePeriod]] = None,
    supervisor_periods: Optional[List[state_schema.StateStaffSupervisorPeriod]] = None,
    caseload_type_periods: Optional[
        List[state_schema.StateStaffCaseloadTypePeriod]
    ] = None,
) -> state_schema.StateStaff:
    return state_schema.StateStaff(
        staff_id=staff_id,
        full_name="FIRST LAST",
        email="first@agency.com",
        state_code="US_XX",
        external_ids=external_ids or [],
        location_periods=location_periods or [],
        role_periods=role_periods or [],
        supervisor_periods=supervisor_periods or [],
        caseload_type_periods=caseload_type_periods or [],
    )
