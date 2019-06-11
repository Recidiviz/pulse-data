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
"""Test utils for generating state Entity classes."""

import datetime

from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.charge import ChargeStatus, ChargeDegree
from recidiviz.common.constants.person_characteristics import Race, Ethnicity
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentClass, StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_charge import \
    StateChargeClassification
from recidiviz.common.constants.state.state_court_case import \
    StateCourtCaseStatus
from recidiviz.common.constants.state.state_fine import StateFineStatus
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import \
    StateIncarcerationIncidentOffense, StateIncarcerationIncidentOutcome
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationFacilitySecurityLevel, \
    StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus, StateSupervisionPeriodAdmissionReason, \
    StateSupervisionLevel
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.\
    state_supervision_violation_response import (
        StateSupervisionViolationResponseType,
        StateSupervisionViolationResponseDecision,
        StateSupervisionViolationResponseDecidingBodyType,
    )

from recidiviz.persistence.entity.state import entities


def generate_full_graph_state_person(
        set_back_edges: bool) -> entities.StatePerson:
    """Test util for generating a StatePerson that has at least one child of
    each possible Entity type, with all possible edge types defined between
    objects.

    Args:
        set_back_edges: explicitly sets all the back edges on the graph that
            will get automatically filled in when this entity graph is written
            to the DB.

    Returns:
        A test instance of a StatePerson.
    """
    person = entities.StatePerson.new_with_defaults()

    person.external_ids = [
        entities.StatePersonExternalId.new_with_defaults(
            state_code='us_ca',
            external_id='ELITE_ID_123',
        )
    ]
    person.aliases = [
        entities.StatePersonAlias.new_with_defaults(
            state_code='us_ca',
            full_name='Beyoncé Giselle Knowles',
            given_names='Beyoncé Giselle',
            surname='Knowles',
        ),
        entities.StatePersonAlias.new_with_defaults(
            state_code='us_ca',
            full_name='Beyoncé Giselle Knowles-Carter',
            given_names='Beyoncé Giselle',
            surname='Knowles-Carter',
        )
    ]

    person.races = [
        entities.StatePersonRace.new_with_defaults(
            state_code='us_ca',
            race=Race.ASIAN,
            race_raw_text='ASIAN'
        ),
        entities.StatePersonRace.new_with_defaults(
            state_code='us_ca',
            race=Race.BLACK,
            race_raw_text='BLACK'
        )
    ]

    person.ethnicities = [
        entities.StatePersonEthnicity.new_with_defaults(
            state_code='us_ca',
            ethnicity=Ethnicity.NOT_HISPANIC,
            ethnicity_raw_text='NOT HISPANIC'
        )
    ]

    sentence_group = entities.StateSentenceGroup.new_with_defaults(
        external_id='BOOK_ID1234',
        status=StateSentenceStatus.SERVING,
        status_raw_text='SERVING',
        date_imposed=datetime.date(year=2016, month=10, day=14),
        state_code='us_ca',
        county_code='us_ca_san_francisco',
        min_length_days=90,
        max_length_days=120,
    )

    person.sentence_groups = [sentence_group]

    incarceration_sentence = \
        entities.StateIncarcerationSentence.new_with_defaults(
            external_id='BOOK_ID1234-1',
            status=StateSentenceStatus.COMPLETED,
            status_raw_text='COMPLETED',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='PRISON',
            date_imposed=datetime.date(year=2018, month=7, day=3),
            projected_min_release_date=
            datetime.date(year=2017, month=5, day=14),
            projected_max_release_date=None,
            parole_eligibility_date=
            datetime.date(year=2018, month=5, day=14),
            state_code='us_ca',
            county_code='us_ca_san_francisco',

            #   - What
            # These will be None if is_life is true
            min_length_days=90,
            max_length_days=900,
            is_life=False,
            parole_possible=True,
            initial_time_served_days=None,
            good_time_days=10,
            earned_time_days=None,
            person=person,
        )

    supervision_sentence = \
        entities.StateSupervisionSentence.new_with_defaults(
            external_id='BOOK_ID1234-2',
            status=StateSentenceStatus.SERVING,
            status_raw_text='SERVING',
            supervision_type=StateSupervisionType.PAROLE,
            supervision_type_raw_text='PAROLE',
            projected_completion_date=
            datetime.date(year=2020, month=5, day=14),
            completion_date=None,
            state_code='us_ca',
            min_length_days=None,
            max_length_days=200,
            person=person,
        )

    fine = entities.StateFine.new_with_defaults(
        external_id='BOOK_ID1234-3',
        status=StateFineStatus.UNPAID,
        status_raw_text='UNPAID',
        date_paid=None,
        state_code='us_ca',
        fine_dollars=15000,
        person=person,
    )

    sentence_group.incarceration_sentences = [incarceration_sentence]
    sentence_group.supervision_sentences = [supervision_sentence]
    sentence_group.fines = [fine]

    court_case = entities.StateCourtCase.new_with_defaults(
        external_id='CASEID456',
        status=StateCourtCaseStatus.EXTERNAL_UNKNOWN,
        date_convicted=datetime.date(year=2018, month=7, day=1),
        next_court_date=datetime.date(year=2019, month=7, day=1),
        state_code='us_ca',
        court_fee_dollars=150,
        judge_name='JUDGE JUDY',
        person=person,
    )

    bond = entities.StateBond.new_with_defaults(
        external_id='BONDID1456',
        status=BondStatus.POSTED,
        status_raw_text='POSTED',
        bond_type=BondType.CASH,
        bond_type_raw_text='CASH',
        date_paid=datetime.date(year=2015, month=7, day=1),
        state_code='us_ca',
        amount_dollars=45,
        bond_agent='CA BAILBONDSMEN',
        person=person,
    )

    charge = entities.StateCharge.new_with_defaults(
        external_id='CHARGE1_EXTERNAL_ID',
        status=ChargeStatus.CONVICTED,
        status_raw_text='CONVICTED',
        offense_date=datetime.date(year=2003, month=7, day=1),
        date_charged=datetime.date(year=2003, month=8, day=1),
        state_code='us_ca',
        statute='A102.3',
        description='DRUG POSSESSION',
        attempted=True,
        charge_classification=StateChargeClassification.FELONY,
        charge_classification_raw_text='F',
        degree=ChargeDegree.FIRST,
        degree_raw_text='1st',
        counts=1,
        charge_notes=None,
        person=person,
        court_case=court_case,
        bond=None,
    )

    charge2 = entities.StateCharge.new_with_defaults(
        external_id='CHARGE2_EXTERNAL_ID',
        status=ChargeStatus.CONVICTED,
        status_raw_text='CONVICTED',
        offense_date=datetime.date(year=2003, month=7, day=1),
        date_charged=datetime.date(year=2003, month=8, day=1),
        state_code='us_ca',
        statute='A102.3',
        description='DRUG POSSESSION',
        attempted=True,
        charge_classification=StateChargeClassification.FELONY,
        charge_classification_raw_text='F',
        degree=ChargeDegree.FIRST,
        degree_raw_text='1st',
        counts=1,
        charge_notes=None,
        person=person,
        court_case=court_case,
        bond=None,
    )

    charge3 = entities.StateCharge.new_with_defaults(
        external_id='CHARGE3_EXTERNAL_ID',
        status=ChargeStatus.DROPPED,
        status_raw_text='DROPPED',
        offense_date=datetime.date(year=2003, month=7, day=1),
        date_charged=datetime.date(year=2003, month=8, day=1),
        state_code='us_ca',
        statute='A102.3',
        description='DRUG POSSESSION',
        attempted=True,
        charge_classification=StateChargeClassification.FELONY,
        charge_classification_raw_text='F',
        degree=ChargeDegree.FIRST,
        degree_raw_text='1st',
        counts=1,
        charge_notes=None,
        person=person,
        court_case=court_case,
        bond=None,
    )

    supervision_sentence.charges = [charge, charge2, charge3]
    incarceration_sentence.charges = [charge, charge2, charge3]

    incarceration_period = \
        entities.StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            status_raw_text='IN CUSTODY',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text=None,
            admission_date=datetime.date(year=2003, month=8, day=1),
            release_date=datetime.date(year=2004, month=8, day=1),
            state_code='us_ca',
            county_code='us_ca_sf',
            facility='ALCATRAZ',
            housing_unit='BLOCK A',
            facility_security_level=
            StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            facility_security_level_raw_text='MAX',
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text='NEW ADMISSION',
            projected_release_reason=
            StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            projected_release_reason_raw_text='CONDITIONAL RELEASE',
            release_reason=
            StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text='CONDITIONAL RELEASE',
            person=person,
        )

    incarceration_sentence.incarceration_periods = [incarceration_period]
    supervision_sentence.incarceration_periods = [incarceration_period]

    incident_responding_officer = entities.StateAgent.new_with_defaults(
        agent_type=StateAgentType.CORRECTIONAL_OFFICER,
        state_code='us_ca',
        full_name='MR SIR',
    )

    incarceration_incident = \
        entities.StateIncarcerationIncident.new_with_defaults(
            incident_date=datetime.date(year=2003, month=8, day=10),
            state_code='us_ca',
            location_within_facility='CELL',
            offense=StateIncarcerationIncidentOffense.CONTRABAND,
            offense_raw_text='CONTRABAND',
            outcome=StateIncarcerationIncidentOutcome.WRITE_UP,
            outcome_raw_text='WRITE UP',
            person=person,
            # incarceration_period=incarceration_period,
            responding_officer=incident_responding_officer,
        )

    incarceration_period.incarceration_incidents = [incarceration_incident]

    parole_decision = entities.StateParoleDecision.new_with_defaults(
        received_parole=True,
        decision_date=datetime.date(year=2004, month=7, day=1),
        corrective_action_deadline=None,
        state_code='us_ca',
        decision_outcome='GRANTED PAROLE',
        decision_reasoning='GOOD BEHAVIOR',
        corrective_action=None,
        person=person,
        # incarceration_period=incarceration_period,
    )

    incarceration_period.parole_decisions = [parole_decision]

    assessment_agent = entities.StateAgent.new_with_defaults(
        agent_type=StateAgentType.SUPERVISION_OFFICER,
        state_code='us_ca',
        full_name='MR SIR',
    )

    assessment1 = entities.StateAssessment.new_with_defaults(
        assessment_class=StateAssessmentClass.RISK,
        assessment_class_raw_text=None,
        assessment_type=StateAssessmentType.LSIR,
        assessment_type_raw_text='LSIR',
        assessment_date=datetime.date(2003, month=8, day=10),
        state_code='us_ca',
        assessment_score=55,
        assessment_level=StateAssessmentLevel.MEDIUM,
        assessment_level_raw_text='MED',
        assessment_metadata='assessment metadata',
        person=person,
        incarceration_period=incarceration_period,
        supervision_period=None,
        conducting_agent=assessment_agent,
    )

    incarceration_period.assessments = [assessment1]

    supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
        status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
        status_raw_text='UNDER SUPERVISION',
        supervision_type=StateSupervisionType.PAROLE,
        supervision_type_raw_text='PAROLE',
        start_date=datetime.date(year=2004, month=8, day=1),
        termination_date=None,
        state_code='us_ca',
        admission_reason=
        StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
        admission_reason_raw_text='RELEASE',
        termination_reason=None,
        termination_reason_raw_text=None,
        supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
        supervision_level_raw_text='UNKNOWN',
        conditions='10PM CURFEW',
        person=person,
    )

    incarceration_sentence.supervision_periods = [supervision_period]
    supervision_sentence.supervision_periods = [supervision_period]

    assessment2 = entities.StateAssessment.new_with_defaults(
        assessment_class=StateAssessmentClass.RISK,
        assessment_class_raw_text=None,
        assessment_type=StateAssessmentType.LSIR,
        assessment_type_raw_text='LSIR',
        assessment_date=datetime.date(2004, month=9, day=10),
        state_code='us_ca',
        assessment_score=10,
        assessment_level=StateAssessmentLevel.LOW,
        assessment_level_raw_text='LOW',
        assessment_metadata='more assessment metadata',
        person=person,
        incarceration_period=None,
        supervision_period=supervision_period,
        conducting_agent=assessment_agent,
    )
    supervision_period.assessments = [assessment2]

    supervision_violation = \
        entities.StateSupervisionViolation.new_with_defaults(
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text='TECHNICAL',
            violation_date=datetime.date(year=2004, month=9, day=1),
            state_code='us_ca',
            is_violent=False,
            violated_conditions='MISSED CURFEW',
            person=person,
        )

    supervision_period.supervision_violations = [supervision_violation]

    supervision_officer_agent = entities.StateAgent.new_with_defaults(
        agent_type=StateAgentType.SUPERVISION_OFFICER,
        state_code='us_ca',
        full_name='JOHN SMITH',
    )

    supervision_violation_response = \
        entities.StateSupervisionViolationResponse.new_with_defaults(
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=datetime.date(year=2004, month=9, day=2),
            state_code='us_ca',
            decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
            decision_raw_text='CONTINUE',
            revocation_type=None,
            revocation_type_raw_text=None,
            deciding_body_type=
            StateSupervisionViolationResponseDecidingBodyType.
            SUPERVISION_OFFICER,
            person=person,
            decision_agents=[supervision_officer_agent]
        )

    supervision_violation.supervision_violation_responses = \
        [supervision_violation_response]

    if set_back_edges:
        person_children = \
            person.external_ids + person.races + \
            person.aliases + person.ethnicities + \
            person.sentence_groups
        for child in person_children:
            child.person = person

        person.assessments.extend(incarceration_period.assessments)
        person.assessments.extend(supervision_period.assessments)

        sentence_group_children = \
            sentence_group.incarceration_sentences + \
            sentence_group.supervision_sentences + \
            sentence_group.fines
        for child in sentence_group_children:
            child.sentence_group = sentence_group

        incarceration_sentence_children = \
            incarceration_sentence.charges + \
            incarceration_sentence.incarceration_periods + \
            incarceration_sentence.supervision_periods

        for child in incarceration_sentence_children:
            child.incarceration_sentences = [incarceration_sentence]

        supervision_sentence_children = \
            supervision_sentence.charges + \
            supervision_sentence.incarceration_periods + \
            supervision_sentence.supervision_periods

        for child in supervision_sentence_children:
            child.supervision_sentences = [supervision_sentence]

        court_case.charges = [charge, charge2, charge3]
        bond.charges = [charge, charge2, charge3]

        incarceration_period_children = \
            incarceration_period.parole_decisions + \
            incarceration_period.assessments + \
            incarceration_period.incarceration_incidents

        for child in incarceration_period_children:
            child.incarceration_period = incarceration_period

        supervision_period_children = \
            supervision_period.supervision_violations + \
            supervision_period.assessments

        for child in supervision_period_children:
            child.supervision_period = supervision_period

        supervision_violation_response.supervision_violation = \
            supervision_violation

    return person
