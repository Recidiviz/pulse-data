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

"""Tests for ingest_info"""

import unittest

from recidiviz.ingest.models import ingest_info
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.models.ingest_info_pb2 import Person, \
    Booking, Charge, Hold, Arrest, Sentence, Bond, StatePerson, \
    StateAssessment, StateSentenceGroup, StateSupervisionSentence, \
    StateIncarcerationSentence, StateFine, StateCharge, StateCourtCase, \
    StateBond, StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateIncarcerationIncident, StateParoleDecision, \
    StateSupervisionViolation, StateSupervisionViolationResponse, \
    StateIncarcerationIncidentOutcome, StateProgramAssignment


class FieldsDontMatchError(Exception):
    pass


class TestIngestInfo(unittest.TestCase):
    """Tests for ingest_info"""

    def test_proto_fields_match(self):
        def _verify_fields(proto, ingest_info_source, ignore=None):
            ignore = ignore or []
            proto_fields = [field.name for field in proto.DESCRIPTOR.fields]
            source_fields = vars(ingest_info_source)
            for field in proto_fields:
                if field not in source_fields and field not in ignore:
                    raise FieldsDontMatchError(
                        "Field '%s' exists in '%s' proto"
                        " but not in the IngestInfo object" % (
                            field, proto.__name__))

            for field in source_fields:
                if field not in proto_fields and field not in ignore:
                    raise FieldsDontMatchError(
                        "Field '%s' exists in '%s'"
                        " IngestInfo object but not in the proto object" % (
                            field, proto.__name__))

        # These should only contain fields that are listed in ingest_info.proto
        # as ids but listed in ingest_info.py as full objects. Think carefully
        # before adding anything else.
        person_fields_ignore = ['booking_ids', 'bookings']
        booking_fields_ignore = ['arrest_id', 'charge_ids', 'hold_ids',
                                 'arrest', 'charges', 'holds']
        charge_fields_ignore = ['bond_id', 'sentence_id', 'bond', 'sentence']
        sentence_fields_ignore = ['sentence_relationships']

        state_person_fields_ignore = [
            'state_person_race_ids', 'state_person_races',
            'state_person_ethnicity_ids', 'state_person_ethnicities',
            'state_alias_ids', 'state_aliases',
            'state_person_external_ids_ids', 'state_person_external_ids',
            'state_assessment_ids', 'state_assessments',
            'state_program_assignment_ids', 'state_program_assignments',
            'supervising_officer_id', 'supervising_officer',
            'state_sentence_group_ids', 'state_sentence_groups']
        assessment_fields_ignore = ['conducting_agent_id', 'conducting_agent']
        sentence_group_fields_ignore = \
            ['state_supervision_sentence_ids', 'state_supervision_sentences',
             'state_incarceration_sentence_ids',
             'state_incarceration_sentences',
             'state_fine_ids', 'state_fines']
        supervision_sentence_fields_ignore = \
            ['state_charge_ids', 'state_charges',
             'state_incarceration_period_ids', 'state_incarceration_periods',
             'state_supervision_period_ids', 'state_supervision_periods']
        incarceration_sentence_fields_ignore = \
            ['state_charge_ids', 'state_charges',
             'state_incarceration_period_ids', 'state_incarceration_periods',
             'state_supervision_period_ids', 'state_supervision_periods']
        fine_fields_ignore = ['state_charge_ids', 'state_charges']
        state_charge_fields_ignore = ['state_court_case_id', 'state_court_case',
                                      'state_bond_id', 'state_bond']
        state_court_case_fields_ignore = ['judge_id', 'judge']
        incarceration_period_fields_ignore = \
            ['state_incarceration_incident_ids',
             'state_incarceration_incidents',
             'state_parole_decision_ids', 'state_parole_decisions',
             'state_assessment_ids', 'state_assessments',
             'state_program_assignment_ids', 'state_program_assignments']
        incarceration_incident_fields_ignore = \
            ['responding_officer_id', 'responding_officer',
             'state_incarceration_incident_outcomes',
             'state_incarceration_incident_outcome_ids']
        parole_decision_fields_ignore = \
            ['decision_agent_ids', 'decision_agents']
        supervision_period_fields_ignore = \
            ['supervising_officer_id', 'supervising_officer',
             'state_supervision_violation_ids', 'state_supervision_violations',
             'state_assessment_ids', 'state_assessments',
             'state_program_assignment_ids', 'state_program_assignments']
        supervision_violation_fields_ignore = \
            ['state_supervision_violation_response_ids',
             'state_supervision_violation_responses']
        supervision_violation_response_fields_ignore = \
            ['decision_agent_ids', 'decision_agents']
        program_assignment_fields_ignore = \
            ['referring_agent', 'referring_agent_id']

        _verify_fields(Person, ingest_info.Person(), person_fields_ignore)
        _verify_fields(Booking, ingest_info.Booking(), booking_fields_ignore)
        _verify_fields(Charge, ingest_info.Charge(), charge_fields_ignore)
        _verify_fields(Hold, ingest_info.Hold())
        _verify_fields(Arrest, ingest_info.Arrest())
        _verify_fields(Sentence, ingest_info.Sentence(), sentence_fields_ignore)
        _verify_fields(Bond, ingest_info.Bond())

        _verify_fields(StatePerson, ingest_info.StatePerson(),
                       state_person_fields_ignore)
        _verify_fields(StateAssessment, ingest_info.StateAssessment(),
                       assessment_fields_ignore)
        _verify_fields(StateSentenceGroup, ingest_info.StateSentenceGroup(),
                       sentence_group_fields_ignore)
        _verify_fields(StateSupervisionSentence,
                       ingest_info.StateSupervisionSentence(),
                       supervision_sentence_fields_ignore)
        _verify_fields(StateIncarcerationSentence,
                       ingest_info.StateIncarcerationSentence(),
                       incarceration_sentence_fields_ignore)
        _verify_fields(StateFine, ingest_info.StateFine(), fine_fields_ignore)
        _verify_fields(StateCharge, ingest_info.StateCharge(),
                       state_charge_fields_ignore)
        _verify_fields(StateCourtCase, ingest_info.StateCourtCase(),
                       state_court_case_fields_ignore)
        _verify_fields(StateBond, ingest_info.StateBond())
        _verify_fields(StateIncarcerationPeriod,
                       ingest_info.StateIncarcerationPeriod(),
                       incarceration_period_fields_ignore)
        _verify_fields(StateSupervisionPeriod,
                       ingest_info.StateSupervisionPeriod(),
                       supervision_period_fields_ignore)
        _verify_fields(StateIncarcerationIncident,
                       ingest_info.StateIncarcerationIncident(),
                       incarceration_incident_fields_ignore)
        _verify_fields(StateIncarcerationIncidentOutcome,
                       ingest_info.StateIncarcerationIncidentOutcome(),
                       incarceration_incident_fields_ignore)
        _verify_fields(StateParoleDecision, ingest_info.StateParoleDecision(),
                       parole_decision_fields_ignore)
        _verify_fields(StateSupervisionViolation,
                       ingest_info.StateSupervisionViolation(),
                       supervision_violation_fields_ignore)
        _verify_fields(StateSupervisionViolationResponse,
                       ingest_info.StateSupervisionViolationResponse(),
                       supervision_violation_response_fields_ignore)
        _verify_fields(StateProgramAssignment,
                       ingest_info.StateProgramAssignment(),
                       program_assignment_fields_ignore)

    def test_bool_falsy(self):
        ii = IngestInfo()
        person = ii.create_person()
        person.create_booking().create_arrest()
        person.create_booking()
        self.assertFalse(ii)

    def test_bool_truthy(self):
        ii = IngestInfo()
        person = ii.create_person()
        person.create_booking().create_arrest(arrest_date='1/2/3')
        person.create_booking()
        self.assertTrue(ii)

    def test_prune(self):
        ii = IngestInfo(people=[
            ingest_info.Person(),
            ingest_info.Person(bookings=[
                ingest_info.Booking(),
                ingest_info.Booking(arrest=ingest_info.Arrest(), charges=[
                    ingest_info.Charge(),
                    ingest_info.Charge(bond=ingest_info.Bond(),
                                       sentence=ingest_info.Sentence()),
                    ingest_info.Charge(bond=ingest_info.Bond(),
                                       sentence=ingest_info.Sentence(
                                           is_life='False'))
                ], holds=[
                    ingest_info.Hold(), ingest_info.Hold(hold_id=1)
                ])
            ])
        ])

        expected = IngestInfo(people=[
            ingest_info.Person(bookings=[
                ingest_info.Booking(
                    charges=[ingest_info.Charge(
                        sentence=ingest_info.Sentence(is_life='False'))],
                    holds=[
                        ingest_info.Hold(jurisdiction_name='UNSPECIFIED'),
                        ingest_info.Hold(hold_id=1,
                                         jurisdiction_name='UNSPECIFIED')
                    ])])])
        self.assertEqual(ii.prune(), expected)

    def test_sort(self):
        b1 = ingest_info.Booking(admission_date='1')
        b2 = ingest_info.Booking(admission_date='2')

        ii = IngestInfo(people=[ingest_info.Person(bookings=[b1, b2])])
        ii_reversed = IngestInfo(people=[ingest_info.Person(bookings=[b2, b1])])

        self.assertNotEqual(ii, ii_reversed)
        ii.sort()
        ii_reversed.sort()
        self.assertEqual(ii, ii_reversed)
