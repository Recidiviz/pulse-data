"""Utils for testing the database."""

import datetime

from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import Ethnicity, Race
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_court_case import StateCourtType
from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_parole_decision import \
    StateParoleDecisionOutcome
from recidiviz.common.constants.state.state_program_assignment import \
    StateProgramAssignmentParticipationStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus
from recidiviz.common.constants.state.state_incarceration_incident import \
    StateIncarcerationIncidentOutcomeType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseDecision, \
    StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.database.schema.state import schema as state_schema


def generate_test_supervision_violation_response(person_id) -> \
        state_schema.StateSupervisionViolationResponse:
    instance = state_schema.StateSupervisionViolationResponse(
        supervision_violation_response_id=456,
        state_code='us_ca',
        person_id=person_id,
        supervision_violation_response_decisions=[
            state_schema.StateSupervisionViolationResponseDecisionEntry(
                supervision_violation_response_decision_entry_id=123,
                state_code='us_ca',
                decision=
                StateSupervisionViolationResponseDecision.
                REVOCATION.value,
                decision_raw_text='REV',
                revocation_type=
                StateSupervisionViolationResponseRevocationType.
                REINCARCERATION.value,
                revocation_type_raw_text='REINC',
                person_id=person_id,
            )
        ]
    )

    return instance


def generate_test_supervision_violation(person_id,
                                        supervision_violation_responses) -> \
        state_schema.StateSupervisionViolation:

    instance = state_schema.StateSupervisionViolation(
        supervision_violation_id=321,
        violation_type=StateSupervisionViolationType.TECHNICAL.value,
        state_code='us_ca',
        person_id=person_id,
        supervision_violated_conditions=[
            state_schema.StateSupervisionViolatedConditionEntry(
                supervision_violated_condition_entry_id=765,
                state_code='us_ca',
                condition='CURFEW',
                person_id=person_id,
            )
        ],
        supervision_violation_types=[
            state_schema.StateSupervisionViolationTypeEntry(
                supervision_violation_type_entry_id=987,
                state_code='us_ca',
                violation_type=StateSupervisionViolationType.TECHNICAL.value,
                violation_type_raw_text='T',
                person_id=person_id,
            )
        ],
        supervision_violation_responses=supervision_violation_responses
    )

    return instance


def generate_test_supervision_period(person_id, supervision_violations) -> \
        state_schema.StateSupervisionPeriod:
    instance = state_schema.StateSupervisionPeriod(
        supervision_period_id=4444,
        status=StateSupervisionPeriodStatus.EXTERNAL_UNKNOWN.value,
        state_code='us_ca',
        person_id=person_id,
        supervision_violations=supervision_violations,
    )

    return instance


def generate_test_incarceration_incident_outcome(person_id) \
        -> state_schema.StateIncarcerationIncidentOutcome:
    instance = state_schema.StateIncarcerationIncidentOutcome(
        incarceration_incident_outcome_id=3211,
        outcome_type=StateIncarcerationIncidentOutcomeType.
        DISCIPLINARY_LABOR.value,
        state_code='us_ca',
        person_id=person_id)
    return instance


def generate_test_incarceration_incident(person_id,
                                         incarceration_incident_outcomes) -> \
        state_schema.StateIncarcerationIncident:
    instance = state_schema.StateIncarcerationIncident(
        incarceration_incident_id=321,
        state_code='us_ca',
        person_id=person_id,
        incarceration_incident_outcomes=incarceration_incident_outcomes
    )

    return instance


def generate_test_parole_decision(person_id) -> \
        state_schema.StateParoleDecision:
    instance = state_schema.StateParoleDecision(
        parole_decision_id=789,
        state_code='us_ca',
        decision_outcome=StateParoleDecisionOutcome.PAROLE_DENIED.value,
        person_id=person_id,
    )
    return instance


def generate_test_incarceration_period(person_id, incarceration_incidents,
                                       parole_decisions) -> \
        state_schema.StateIncarcerationPeriod:
    instance = state_schema.StateIncarcerationPeriod(
        incarceration_period_id=5555,
        status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value,
        state_code='us_ca',
        person_id=person_id,
        incarceration_incidents=incarceration_incidents,
        parole_decisions=parole_decisions,
    )
    return instance


def generate_test_court_case(person_id) -> state_schema.StateCourtCase:
    instance = state_schema.StateCourtCase(
        court_case_id=8888,
        state_code='us_ca',
        court_type=
        StateCourtType.PRESENT_WITHOUT_INFO.value,
        court_type_raw_text=None,
        person_id=person_id,
    )

    return instance


def generate_test_bond(person_id) -> state_schema.StateBond:
    instance = state_schema.StateBond(
        bond_id=9999,
        person_id=person_id,
        status=BondStatus.PENDING.value,
        status_raw_text='PENDING',
        bond_type=BondType.CASH.value,
        bond_type_raw_text='CASH',
        date_paid=None,
        state_code='us_ca',
        county_code='us_ca_san_francisco',
        amount_dollars=1000,
        bond_agent=None,
    )

    return instance


def generate_test_charge(person_id, charge_id, court_case, bond) -> \
        state_schema.StateCharge:
    instance = state_schema.StateCharge(
        charge_id=charge_id,
        person_id=person_id,
        status=ChargeStatus.PENDING.value,
        state_code='us_ca',
        court_case=court_case,
        bond=bond,
    )

    return instance


def generate_test_supervision_sentence(
        person_id, charges, supervision_periods) \
        -> state_schema.StateSupervisionSentence:
    instance = state_schema.StateSupervisionSentence(
        supervision_sentence_id=1111,
        status=StateSentenceStatus.SERVING.value,
        state_code='us_ca',
        person_id=person_id,
        charges=charges,
        supervision_periods=supervision_periods,
    )

    return instance


def generate_test_incarceration_sentence(
        person_id, charges, incarceration_periods) \
        -> state_schema.StateIncarcerationSentence:
    instance = state_schema.StateIncarcerationSentence(
        incarceration_sentence_id=2222,
        status=StateSentenceStatus.SUSPENDED.value,
        state_code='us_ca',
        person_id=person_id,
        is_capital_punishment=False,
        charges=charges,
        incarceration_periods=incarceration_periods,
    )

    return instance


def generate_test_fine(person_id) -> state_schema.StateFine:
    instance = state_schema.StateFine(
        fine_id=3333,
        status=StateFineStatus.PAID.value,
        state_code='us_ca',
        person_id=person_id,
    )

    return instance


def generate_test_sentence_group(person_id, supervision_sentences,
                                 incarceration_sentences,
                                 fines) -> state_schema.StateSentenceGroup:
    instance = state_schema.StateSentenceGroup(
        sentence_group_id=567,
        status=StateSentenceStatus.SUSPENDED.value,
        state_code='us_ca',
        supervision_sentences=supervision_sentences,
        incarceration_sentences=incarceration_sentences,
        fines=fines,
        person_id=person_id
    )
    return instance


def generate_test_assessment_agent() -> state_schema.StateAgent:
    instance = state_schema.StateAgent(
        agent_id=1010,
        external_id='ASSAGENT1234',
        agent_type=StateAgentType.SUPERVISION_OFFICER.value,
        state_code='us_ca',
        full_name='JOHN SMITH',
    )
    return instance


def generate_test_person(person_id, sentence_groups, incarceration_period,
                         agent, supervision_period) -> state_schema.StatePerson:
    """Returns a StatePerson to be used for testing."""
    instance = state_schema.StatePerson(
        person_id=person_id,
        full_name='name',
        birthdate=datetime.date(1980, 1, 5),
        birthdate_inferred_from_age=False,
        external_ids=[
            state_schema.StatePersonExternalId(
                person_external_id_id=234,
                external_id='person_external_id',
                id_type='STATE',
                state_code='us_ny',
                person_id=person_id,
            )
        ],
        aliases=[
            state_schema.StatePersonAlias(
                person_alias_id=1456,
                state_code='us_ca',
                full_name='name',
                person_id=person_id,
            )
        ],
        races=[
            state_schema.StatePersonRace(
                person_race_id=345,
                state_code='us_ca',
                race=Race.BLACK.value,
                race_raw_text='BLK',
                person_id=person_id,
            )
        ],
        ethnicities=[
            state_schema.StatePersonEthnicity(
                person_ethnicity_id=345,
                state_code='us_ca',
                ethnicity=Ethnicity.NOT_HISPANIC.value,
                ethnicity_raw_text='HISP',
                person_id=person_id,
            )
        ],
        sentence_groups=sentence_groups,
        assessments=[
            state_schema.StateAssessment(
                assessment_id=456,
                person_id=person_id,
                state_code='us_ca',
                incarceration_period=incarceration_period,
                conducting_agent=agent,
            ),
            state_schema.StateAssessment(
                assessment_id=4567,
                person_id=person_id,
                state_code='us_ca',
                supervision_period=supervision_period,
                conducting_agent=agent,
            )
        ],
        program_assignments=[
            state_schema.StateProgramAssignment(
                program_assignment_id=567,
                participation_status=
                StateProgramAssignmentParticipationStatus.
                PRESENT_WITHOUT_INFO.value,
                state_code='us_ca',
                referring_agent=agent,
            )
        ]
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

    test_supervision_violation_response = \
        generate_test_supervision_violation_response(test_person_id)

    test_supervision_violation = \
        generate_test_supervision_violation(
            test_person_id,
            [test_supervision_violation_response])

    test_supervision_period = generate_test_supervision_period(
        test_person_id, [test_supervision_violation])

    test_incarceration_incident_outcome = \
        generate_test_incarceration_incident_outcome(test_person_id)

    test_incarceration_incident = \
        generate_test_incarceration_incident(
            test_person_id,
            [test_incarceration_incident_outcome])

    test_parole_decision = generate_test_parole_decision(test_person_id)

    test_incarceration_period = generate_test_incarceration_period(
        test_person_id, [test_incarceration_incident], [test_parole_decision])

    test_court_case = generate_test_court_case(test_person_id)

    test_bond = generate_test_bond(test_person_id)

    test_charge_1 = generate_test_charge(test_person_id, 6666, test_court_case,
                                         test_bond)
    test_charge_2 = generate_test_charge(test_person_id, 7777, test_court_case,
                                         test_bond)

    test_supervision_sentence = generate_test_supervision_sentence(
        test_person_id, [test_charge_1, test_charge_2],
        [test_supervision_period])

    test_incarceration_sentence = generate_test_incarceration_sentence(
        test_person_id, [test_charge_1, test_charge_2],
        [test_incarceration_period])

    test_fine = generate_test_fine(test_person_id)

    test_sentence_group = generate_test_sentence_group(
        test_person_id, [test_supervision_sentence],
        [test_incarceration_sentence], [test_fine])

    test_agent = generate_test_assessment_agent()

    test_person = generate_test_person(test_person_id,
                                       [test_sentence_group],
                                       test_incarceration_period,
                                       test_agent,
                                       test_supervision_period)

    test_sentence_group.person = test_person

    return test_person
