"""Utils for testing the database."""

import datetime
from typing import List, Optional

import attr

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.person_characteristics import Ethnicity, Race
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_court_case import StateCourtType
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
)
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
)
from recidiviz.common.constants.state.state_parole_decision import (
    StateParoleDecisionOutcome,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactStatus,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.common.ingest_metadata import (
    IngestMetadata,
    LegacyStateAndJailsIngestMetadata,
    SystemLevel,
)
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


@attr.s
class FakeLegacyStateAndJailsIngestMetadata(IngestMetadata):
    @classmethod
    def for_state(
        cls,
        region: str,
        enum_overrides: Optional[EnumOverrides] = None,
    ) -> LegacyStateAndJailsIngestMetadata:
        return LegacyStateAndJailsIngestMetadata(
            region=region,
            jurisdiction_id="",
            ingest_time=datetime.datetime(2020, 4, 14, 12, 31, 00),
            enum_overrides=enum_overrides or EnumOverrides.empty(),
            system_level=SystemLevel.STATE,
            database_key=SQLAlchemyDatabaseKey.canonical_for_schema(SchemaType.STATE),
        )

    @classmethod
    def for_county(
        cls,
        region: str,
        jurisdiction_id: Optional[str] = None,
        ingest_time: Optional[datetime.datetime] = None,
        enum_overrides: Optional[EnumOverrides] = None,
        facility_id: Optional[str] = None,
    ) -> LegacyStateAndJailsIngestMetadata:
        return LegacyStateAndJailsIngestMetadata(
            region=region,
            jurisdiction_id=jurisdiction_id or "jurisdiction_id",
            ingest_time=ingest_time or datetime.datetime(2020, 4, 14, 12, 31, 00),
            enum_overrides=enum_overrides or EnumOverrides.empty(),
            facility_id=facility_id,
            system_level=SystemLevel.COUNTY,
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS),
        )


def generate_test_supervision_violation_response_decision_entry(
    person_id,
) -> state_schema.StateSupervisionViolationResponseDecisionEntry:
    return state_schema.StateSupervisionViolationResponseDecisionEntry(
        supervision_violation_response_decision_entry_id=123,
        state_code="US_XX",
        decision=StateSupervisionViolationResponseDecision.REVOCATION.value,
        decision_raw_text="REV",
        person_id=person_id,
    )


def generate_test_supervision_violation_response(
    person_id,
    decisions: Optional[
        List[state_schema.StateSupervisionViolationResponseDecisionEntry]
    ] = None,
) -> state_schema.StateSupervisionViolationResponse:
    decisions = decisions or [
        generate_test_supervision_violation_response_decision_entry(person_id)
    ]

    instance = state_schema.StateSupervisionViolationResponse(
        supervision_violation_response_id=456,
        state_code="US_XX",
        person_id=person_id,
        supervision_violation_response_decisions=decisions,
    )

    return instance


def generate_test_assessment(person_id) -> state_schema.StateAssessment:
    instance = state_schema.StateAssessment(
        assessment_id=345,
        state_code="US_XX",
        assessment_type=StateAssessmentType.ASI.value,
        person_id=person_id,
    )
    return instance


def generate_test_supervision_case_type(
    person_id,
) -> state_schema.StateSupervisionCaseTypeEntry:
    instance = state_schema.StateSupervisionCaseTypeEntry(
        person_id=person_id,
        supervision_case_type_entry_id=12345,
        state_code="US_XX",
        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE.value,
        case_type_raw_text="DV",
    )
    return instance


def generate_test_supervision_contact(
    person_id, contacted_agent=None
) -> state_schema.StateSupervisionContact:
    instance = state_schema.StateSupervisionContact(
        person_id=person_id,
        supervision_contact_id=12345,
        state_code="US_XX",
        contacted_agent=contacted_agent,
        status=StateSupervisionContactStatus.COMPLETED.value,
        status_raw_text="COMPLETED",
    )
    return instance


def generate_test_supervision_violation(
    person_id, supervision_violation_responses
) -> state_schema.StateSupervisionViolation:
    """Generates a test StateSupervisionViolation."""
    supervision_violation_id = 321

    instance = state_schema.StateSupervisionViolation(
        supervision_violation_id=supervision_violation_id,
        state_code="US_XX",
        person_id=person_id,
        supervision_violated_conditions=[
            state_schema.StateSupervisionViolatedConditionEntry(
                supervision_violated_condition_entry_id=765,
                state_code="US_XX",
                condition="CURFEW",
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
    person_id, case_types
) -> state_schema.StateSupervisionPeriod:
    instance = state_schema.StateSupervisionPeriod(
        supervision_period_id=4444,
        state_code="US_XX",
        person_id=person_id,
        case_type_entries=case_types,
    )

    return instance


def generate_test_incarceration_incident_outcome(
    person_id,
) -> state_schema.StateIncarcerationIncidentOutcome:
    instance = state_schema.StateIncarcerationIncidentOutcome(
        incarceration_incident_outcome_id=3211,
        outcome_type=StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR.value,
        state_code="US_XX",
        person_id=person_id,
    )
    return instance


def generate_test_incarceration_incident(
    person_id, incarceration_incident_outcomes
) -> state_schema.StateIncarcerationIncident:
    instance = state_schema.StateIncarcerationIncident(
        incarceration_incident_id=321,
        state_code="US_XX",
        person_id=person_id,
        incarceration_incident_outcomes=incarceration_incident_outcomes,
    )

    return instance


def generate_test_parole_decision(person_id) -> state_schema.StateParoleDecision:
    instance = state_schema.StateParoleDecision(
        parole_decision_id=789,
        state_code="US_XX",
        decision_outcome=StateParoleDecisionOutcome.PAROLE_DENIED.value,
        person_id=person_id,
    )
    return instance


def generate_test_incarceration_period(
    person_id, parole_decisions
) -> state_schema.StateIncarcerationPeriod:
    instance = state_schema.StateIncarcerationPeriod(
        incarceration_period_id=5555,
        state_code="US_XX",
        person_id=person_id,
        parole_decisions=parole_decisions,
    )
    return instance


def generate_test_court_case(person_id) -> state_schema.StateCourtCase:
    instance = state_schema.StateCourtCase(
        court_case_id=8888,
        state_code="US_XX",
        court_type=StateCourtType.PRESENT_WITHOUT_INFO.value,
        court_type_raw_text=None,
        person_id=person_id,
    )

    return instance


def generate_test_charge(
    person_id: int,
    charge_id: int,
    court_case: Optional[state_schema.StateCourtCase] = None,
    state_code: str = "US_XX",
) -> state_schema.StateCharge:
    instance = state_schema.StateCharge(
        charge_id=charge_id,
        person_id=person_id,
        state_code=state_code,
        status=ChargeStatus.PRESENT_WITHOUT_INFO.value,
        court_case=court_case,
        court_case_id=(court_case.court_case_id if court_case else None),
    )

    return instance


def generate_test_supervision_sentence(
    person_id, charges, early_discharges=None
) -> state_schema.StateSupervisionSentence:
    instance = state_schema.StateSupervisionSentence(
        supervision_sentence_id=1111,
        status=StateSentenceStatus.SERVING.value,
        state_code="US_XX",
        person_id=person_id,
        charges=charges,
        early_discharges=(early_discharges if early_discharges else []),
    )

    return instance


def generate_test_incarceration_sentence(
    person_id, charges=None, early_discharges=None
) -> state_schema.StateIncarcerationSentence:
    instance = state_schema.StateIncarcerationSentence(
        incarceration_sentence_id=2222,
        status=StateSentenceStatus.SUSPENDED.value,
        state_code="US_XX",
        person_id=person_id,
        is_capital_punishment=False,
        charges=(charges if charges else []),
        early_discharges=(early_discharges if early_discharges else []),
    )

    return instance


def generate_test_early_discharge(person_id) -> state_schema.StateEarlyDischarge:
    instance = state_schema.StateEarlyDischarge(
        early_discharge_id=1,
        external_id="external_id",
        state_code="US_XX",
        person_id=person_id,
        decision=StateEarlyDischargeDecision.REQUEST_DENIED.value,
    )
    return instance


def generate_test_sentence_group(
    person_id,
) -> state_schema.StateSentenceGroup:
    instance = state_schema.StateSentenceGroup(
        sentence_group_id=567,
        status=StateSentenceStatus.SUSPENDED.value,
        state_code="US_XX",
        person_id=person_id,
    )
    return instance


def generate_test_assessment_agent() -> state_schema.StateAgent:
    instance = state_schema.StateAgent(
        agent_id=1010,
        external_id="ASSAGENT1234",
        agent_type=StateAgentType.SUPERVISION_OFFICER.value,
        state_code="US_XX",
        full_name="JOHN SMITH",
    )
    return instance


def generate_test_person(
    person_id: int,
    state_code: str,
    sentence_groups: List[state_schema.StateSentenceGroup],
    agent: Optional[state_schema.StateAgent],
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
                race=Race.BLACK.value,
                race_raw_text="BLK",
                person_id=person_id,
            )
        ],
        ethnicities=[
            state_schema.StatePersonEthnicity(
                person_ethnicity_id=345,
                state_code="US_XX",
                ethnicity=Ethnicity.NOT_HISPANIC.value,
                ethnicity_raw_text="HISP",
                person_id=person_id,
            )
        ],
        sentence_groups=sentence_groups,
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
                conducting_agent=agent,
            ),
            state_schema.StateAssessment(
                assessment_id=4567,
                person_id=person_id,
                state_code="US_XX",
                conducting_agent=agent,
            ),
        ],
        program_assignments=[
            state_schema.StateProgramAssignment(
                program_assignment_id=567,
                participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO.value,
                state_code="US_XX",
                referring_agent=agent,
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

    test_parole_decision = generate_test_parole_decision(test_person_id)

    test_incarceration_period = generate_test_incarceration_period(
        test_person_id, [test_parole_decision]
    )

    test_court_case = generate_test_court_case(test_person_id)

    test_charge_1 = generate_test_charge(
        test_person_id,
        6666,
        test_court_case,
    )
    test_charge_2 = generate_test_charge(
        test_person_id,
        7777,
        test_court_case,
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

    test_sentence_group = generate_test_sentence_group(
        test_person_id,
    )

    test_agent = generate_test_assessment_agent()

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
        test_person_id,
        test_state_code,
        [test_sentence_group],
        test_agent,
        [test_incarceration_incident],
        [test_supervision_violation],
        [test_contact],
        [test_incarceration_sentence],
        [test_supervision_sentence],
        [test_incarceration_period],
        [test_supervision_period],
    )

    test_sentence_group.person = test_person

    return test_person
