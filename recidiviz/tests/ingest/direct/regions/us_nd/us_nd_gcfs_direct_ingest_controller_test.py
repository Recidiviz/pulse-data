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

"""Scraper tests for us_nd."""
import copy
import datetime
import json
import unittest

from recidiviz.common.constants.state.external_id_types import \
    US_ND_ELITE, US_ND_SID
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsIngestArgs
from recidiviz.ingest.models.ingest_info import IngestInfo, \
    StateSentenceGroup, StatePerson, StateIncarcerationSentence, StateAlias, \
    StateSupervisionSentence, \
    StateCharge, StateCourtCase, StateIncarcerationPeriod, StatePersonRace, \
    StatePersonExternalId, StateAssessment, StatePersonEthnicity, \
    StateSupervisionPeriod, StateSupervisionViolation, \
    StateSupervisionViolationResponse, StateAgent, StateIncarcerationIncident, \
    StateIncarcerationIncidentOutcome
from recidiviz.ingest.direct.regions.us_nd.\
    us_nd_gcsfs_direct_ingest_controller import UsNdGcsfsDirectIngestController
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.direct.regions.direct_ingest_util import \
    create_mock_gcsfs


class TestUsNdGcsfsIngestController(unittest.TestCase):
    """Unit tests for each North Dakota file to be ingested by the
    UsNdGcsfsDirectIngestController.
    """

    FIXTURE_PATH_PREFIX = 'direct/regions/us_nd'

    def test_populate_data_elite_offenders(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='39768', surname='HOPKINS',
                            given_names='JON', birthdate='8/15/1979',
                            gender='M',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='39768',
                                    id_type=US_ND_ELITE)
                            ],
                            state_person_races=[
                                StatePersonRace(race='CAUCASIAN')
                            ]),
                StatePerson(state_person_id='52163', surname='KNOWLES',
                            given_names='SOLANGE', birthdate='6/24/1986',
                            gender='F',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='52163',
                                    id_type=US_ND_ELITE)
                            ],
                            state_person_races=[
                                StatePersonRace(race='BLACK')
                            ])
            ])

        self.run_file_test(expected, 'elite_offenders')

    def test_populate_data_elite_offender_identifiers(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='39768',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='92237',
                                    id_type=US_ND_SID),
                                StatePersonExternalId(
                                    state_person_external_id_id='39768',
                                    id_type=US_ND_ELITE)
                            ]),
                StatePerson(state_person_id='52163',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='241896',
                                    id_type=US_ND_SID),
                                StatePersonExternalId(
                                    state_person_external_id_id='52163',
                                    id_type=US_ND_ELITE)
                            ])
            ])

        self.run_file_test(expected, 'elite_offender_identifiers')

    def test_populate_data_elite_aliases(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='39768',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='39768',
                                    id_type=US_ND_ELITE)
                            ],
                            state_aliases=[
                                StateAlias(surname='HOPKINS',
                                           given_names='TODD',
                                           name_suffix='III'),
                                StateAlias(surname='HODGSON',
                                           given_names='JON',
                                           middle_names='R')
                            ]),
                StatePerson(state_person_id='52163',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='52163',
                                    id_type=US_ND_ELITE)
                            ],
                            state_aliases=[
                                StateAlias(given_names='SOLANGE')
                            ])
            ])

        self.run_file_test(expected, 'elite_aliases')

    def test_populate_data_elite_bookings(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='52163',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='52163',
                                    id_type=US_ND_ELITE)
                            ],
                            state_sentence_groups=[
                                StateSentenceGroup(state_sentence_group_id=
                                                   '113377',
                                                   status='OUT'),
                                StateSentenceGroup(state_sentence_group_id=
                                                   '114909',
                                                   status='OUT')
                            ]),
                StatePerson(state_person_id='39768',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='39768',
                                    id_type=US_ND_ELITE)
                            ],
                            state_sentence_groups=[
                                StateSentenceGroup(state_sentence_group_id=
                                                   '105640',
                                                   status='OUT')
                            ])
            ])

        self.run_file_test(expected, 'elite_bookings')

    def test_populate_data_elite_sentence_aggs(self):
        incarceration_sentence_105640 = StateIncarcerationSentence(
            parole_eligibility_date='11/24/2005')

        incarceration_sentence_113377 = StateIncarcerationSentence(
            parole_eligibility_date='7/26/2018')

        expected = IngestInfo(
            state_people=[
                StatePerson(state_sentence_groups=[
                    StateSentenceGroup(state_sentence_group_id='105640',
                                       date_imposed='2/12/2004',
                                       max_length='2Y 28D',
                                       state_incarceration_sentences=[
                                           incarceration_sentence_105640
                                       ]),
                    StateSentenceGroup(state_sentence_group_id='114909',
                                       date_imposed='2/27/2018',
                                       max_length='10M 12D'),
                    StateSentenceGroup(state_sentence_group_id='113377',
                                       date_imposed='3/27/2018',
                                       max_length='9M 10D',
                                       state_incarceration_sentences=[
                                           incarceration_sentence_113377
                                       ]),
                    StateSentenceGroup(state_sentence_group_id='115077',
                                       date_imposed='10/29/2018',
                                       max_length='3Y 7M 8D')
                ]),
            ])

        self.run_file_test(expected, 'elite_sentence_aggs')

    def test_populate_data_elite_sentences(self):
        sentences_114909 = [
            StateIncarcerationSentence(
                state_incarceration_sentence_id='114909-1',
                status='A',
                date_imposed='2/27/18  12:00:00 AM',
                projected_min_release_date='12/16/18  12:00:00 AM',
                projected_max_release_date='12/26/18  12:00:00 AM',
                is_life='False'),
            StateIncarcerationSentence(
                state_incarceration_sentence_id='114909-2',
                status='A',
                date_imposed='2/27/18  12:00:00 AM',
                projected_min_release_date='1/8/19  12:00:00 AM',
                projected_max_release_date='1/8/19  12:00:00 AM',
                is_life='False')
        ]

        sentences_105640 = [
            StateIncarcerationSentence(
                state_incarceration_sentence_id='105640-1',
                date_imposed='1/8/90  12:00:00 AM',
                projected_min_release_date='10/21/96  12:00:00 AM',
                projected_max_release_date='6/18/99  12:00:00 AM',
                is_life='False'),
            StateIncarcerationSentence(
                state_incarceration_sentence_id='105640-2',
                date_imposed='1/8/90  12:00:00 AM',
                projected_min_release_date='10/21/96  12:00:00 AM',
                projected_max_release_date='6/18/99  12:00:00 AM',
                is_life='False'),
            StateIncarcerationSentence(
                state_incarceration_sentence_id='105640-5',
                date_imposed='10/21/96  12:00:00 AM',
                projected_min_release_date='2/15/12  12:00:00 AM',
                projected_max_release_date='10/21/16  12:00:00 AM',
                is_life='False'),
            StateIncarcerationSentence(
                state_incarceration_sentence_id='105640-6',
                date_imposed='2/15/12  12:00:00 AM',
                projected_min_release_date='6/26/15  12:00:00 AM',
                projected_max_release_date='2/15/17  12:00:00 AM',
                is_life='False'),
            StateIncarcerationSentence(
                state_incarceration_sentence_id='105640-7',
                status='A',
                date_imposed='2/15/12  12:00:00 AM',
                projected_min_release_date='10/18/13  12:00:00 AM',
                projected_max_release_date='2/15/14  12:00:00 AM',
                is_life='False')
        ]

        sentences_113377 = [
            StateIncarcerationSentence(
                state_incarceration_sentence_id='113377-4',
                status='A',
                date_imposed='2/27/18  12:00:00 AM',
                projected_min_release_date='9/30/18  12:00:00 AM',
                projected_max_release_date='11/24/18  12:00:00 AM',
                is_life='False')
        ]

        expected = IngestInfo(state_people=[
            StatePerson(state_sentence_groups=[
                StateSentenceGroup(state_sentence_group_id='114909',
                                   state_incarceration_sentences=
                                   sentences_114909),
                StateSentenceGroup(state_sentence_group_id='105640',
                                   state_incarceration_sentences=
                                   sentences_105640),
                StateSentenceGroup(state_sentence_group_id='113377',
                                   state_incarceration_sentences=
                                   sentences_113377),
            ]),
        ])

        self.run_file_test(expected, 'elite_sentences')

    def test_populate_data_elite_sentence_terms(self):
        supervision_sentence_105640_2 = StateSupervisionSentence(
            state_supervision_sentence_id='105640-2',
            supervision_type='PROBATION',
            max_length='10Y')

        supervision_sentence_105640_6 = StateSupervisionSentence(
            state_supervision_sentence_id='105640-6',
            supervision_type='PROBATION',
            max_length='5Y')

        incarceration_sentence_105640_1 = StateIncarcerationSentence(
            state_incarceration_sentence_id='105640-1', max_length='10Y')

        incarceration_sentence_105640_2 = StateIncarcerationSentence(
            state_incarceration_sentence_id='105640-2', max_length='10Y')

        incarceration_sentence_105640_5 = StateIncarcerationSentence(
            state_incarceration_sentence_id='105640-5', max_length='20Y')

        incarceration_sentence_105640_6 = StateIncarcerationSentence(
            state_incarceration_sentence_id='105640-6', max_length='5Y')

        incarceration_sentence_105640_7 = StateIncarcerationSentence(
            state_incarceration_sentence_id='105640-7', max_length='2Y')

        incarceration_sentence_114909_1 = StateIncarcerationSentence(
            state_incarceration_sentence_id='114909-1', max_length='1Y 1D')

        incarceration_sentence_114909_2 = StateIncarcerationSentence(
            state_incarceration_sentence_id='114909-2', max_length='360D')

        incarceration_sentence_113377_1 = StateIncarcerationSentence(
            state_incarceration_sentence_id='113377-1', max_length='1Y 1D')

        incarceration_sentence_113377_4 = StateIncarcerationSentence(
            state_incarceration_sentence_id='113377-4', max_length='360D')

        incarceration_sentence_113377_5 = StateIncarcerationSentence(
            state_incarceration_sentence_id='113377-5', max_length='1000D')

        expected = IngestInfo(
            state_people=[
                StatePerson(state_sentence_groups=[
                    StateSentenceGroup(state_sentence_group_id='105640',
                                       state_supervision_sentences=[
                                           supervision_sentence_105640_2,
                                           supervision_sentence_105640_6
                                       ],
                                       state_incarceration_sentences=[
                                           incarceration_sentence_105640_1,
                                           incarceration_sentence_105640_2,
                                           incarceration_sentence_105640_5,
                                           incarceration_sentence_105640_6,
                                           incarceration_sentence_105640_7
                                       ]),
                    StateSentenceGroup(state_sentence_group_id='114909',
                                       state_incarceration_sentences=[
                                           incarceration_sentence_114909_1,
                                           incarceration_sentence_114909_2
                                       ]),
                    StateSentenceGroup(state_sentence_group_id='113377',
                                       state_incarceration_sentences=[
                                           incarceration_sentence_113377_1,
                                           incarceration_sentence_113377_4,
                                           incarceration_sentence_113377_5
                                       ])
                ]),
            ])

        self.run_file_test(expected, 'elite_sentence_terms')

    def test_populate_data_elite_charges(self):
        state_charge_105640_1 = StateCharge(
            state_charge_id='105640-1',
            status='SENTENCED',
            offense_date='6/19/89  12:00:00 AM',
            statute='1801',
            description='KIDNAPPING',
            classification_type='F',
            classification_subtype='B',
            counts='1',
            state_court_case=StateCourtCase(state_court_case_id='5190'))

        state_charge_105640_2 = StateCharge(
            state_charge_id='105640-2',
            status='SENTENCED',
            offense_date='6/19/89  12:00:00 AM',
            statute='A2003',
            description='GROSS SEXUAL IMPOSITION',
            classification_type='F',
            classification_subtype='A',
            counts='1',
            charge_notes='TO REGISTER AS A SEX OFFENDER (PLEA OF GUILTY)',
            state_court_case=StateCourtCase(state_court_case_id='5190'))

        state_charge_105640_5 = StateCharge(
            state_charge_id='105640-5',
            status='SENTENCED',
            offense_date='6/19/89  12:00:00 AM',
            statute='1601',
            classification_type='F',
            classification_subtype='A',
            counts='1',
            charge_notes='ATTEMPTED',
            state_court_case=StateCourtCase(state_court_case_id='5192'))

        state_charge_105640_6 = StateCharge(
            state_charge_id='105640-6',
            status='SENTENCED',
            offense_date='4/29/91  12:00:00 AM',
            statute='A2003',
            classification_type='F',
            classification_subtype='B',
            counts='1',
            state_court_case=StateCourtCase(state_court_case_id='5193'))

        state_charge_105640_7 = StateCharge(
            state_charge_id='105640-7',
            status='SENTENCED',
            offense_date='6/28/10  12:00:00 AM',
            statute='17011',
            classification_type='F',
            classification_subtype='C',
            counts='1',
            charge_notes='On a CO',
            state_court_case=StateCourtCase(state_court_case_id='154576'))

        state_charge_113377_2 = StateCharge(
            state_charge_id='113377-2',
            status='SENTENCED',
            offense_date='11/13/16  12:00:00 AM',
            statute='2203',
            classification_type='M',
            classification_subtype='A',
            counts='1',
            state_court_case=StateCourtCase(state_court_case_id='178986'))

        state_charge_113377_4 = StateCharge(
            state_charge_id='113377-4',
            status='SENTENCED',
            offense_date='7/30/16  12:00:00 AM',
            statute='2203',
            classification_type='M',
            classification_subtype='A',
            counts='1',
            state_court_case=StateCourtCase(state_court_case_id='178987'))

        state_charge_113377_1 = StateCharge(
            state_charge_id='113377-1',
            status='SENTENCED',
            offense_date='8/23/17  12:00:00 AM',
            statute='362102',
            classification_type='F',
            classification_subtype='C',
            counts='1',
            state_court_case=StateCourtCase(state_court_case_id='178768'))

        state_charge_114909_1 = StateCharge(
            state_charge_id='114909-1',
            status='SENTENCED',
            offense_date='8/23/17  12:00:00 AM',
            statute='362102',
            classification_type='F',
            classification_subtype='C',
            counts='1',
            state_court_case=StateCourtCase(state_court_case_id='181820'))

        state_charge_114909_2 = StateCharge(
            state_charge_id='114909-2',
            status='SENTENCED',
            offense_date='11/13/16  12:00:00 AM',
            statute='2203',
            classification_type='M',
            classification_subtype='A',
            counts='1',
            state_court_case=StateCourtCase(state_court_case_id='181821'))

        expected = IngestInfo(state_people=[
            StatePerson(state_sentence_groups=[
                StateSentenceGroup(state_sentence_group_id='105640',
                                   state_incarceration_sentences=[
                                       StateIncarcerationSentence(
                                           state_incarceration_sentence_id=
                                           '105640-1',
                                           state_charges=[
                                               state_charge_105640_1]),
                                       StateIncarcerationSentence(
                                           state_incarceration_sentence_id=
                                           '105640-2',
                                           state_charges=[
                                               state_charge_105640_2]),
                                       StateIncarcerationSentence(
                                           state_incarceration_sentence_id=
                                           '105640-5',
                                           state_charges=[
                                               state_charge_105640_5]),
                                       StateIncarcerationSentence(
                                           state_incarceration_sentence_id=
                                           '105640-6',
                                           state_charges=[
                                               state_charge_105640_6]),
                                       StateIncarcerationSentence(
                                           state_incarceration_sentence_id=
                                           '105640-7',
                                           state_charges=[
                                               state_charge_105640_7])
                                   ]),
                StateSentenceGroup(state_sentence_group_id='113377',
                                   state_incarceration_sentences=[
                                       StateIncarcerationSentence(
                                           state_incarceration_sentence_id=
                                           '113377-2',
                                           state_charges=[
                                               state_charge_113377_2]),
                                       StateIncarcerationSentence(
                                           state_incarceration_sentence_id=
                                           '113377-4',
                                           state_charges=[
                                               state_charge_113377_4]),
                                       StateIncarcerationSentence(
                                           state_incarceration_sentence_id=
                                           '113377-1',
                                           state_charges=[
                                               state_charge_113377_1])
                                   ]),
                StateSentenceGroup(state_sentence_group_id='114909',
                                   state_incarceration_sentences=[
                                       StateIncarcerationSentence(
                                           state_incarceration_sentence_id=
                                           '114909-1',
                                           state_charges=[
                                               state_charge_114909_1]),
                                       StateIncarcerationSentence(
                                           state_incarceration_sentence_id=
                                           '114909-2',
                                           state_charges=[
                                               state_charge_114909_2])
                                   ])
            ]),
        ])

        self.run_file_test(expected, 'elite_charges')

    def test_populate_data_elite_orders(self):
        court_case_5190 = StateCourtCase(state_court_case_id='5190',
                                         status='A',
                                         date_convicted='6/19/89  12:00:00 AM',
                                         next_court_date='6/19/89  12:00:00 AM',
                                         county_code='CA',
                                         judge=StateAgent(
                                             agent_type='JUDGE',
                                             full_name='Sheindlin, Judy',

                                         ))

        court_case_5192 = StateCourtCase(state_court_case_id='5192',
                                         status='A',
                                         date_convicted='6/19/89  12:00:00 AM',
                                         next_court_date='6/19/89  12:00:00 AM',
                                         county_code='CA',
                                         judge=StateAgent(
                                             agent_type='JUDGE',
                                             full_name='Sheindlin, Judy',

                                         ))

        court_case_5193 = StateCourtCase(state_court_case_id='5193',
                                         status='A',
                                         date_convicted='4/29/91  12:00:00 AM',
                                         next_court_date='4/29/91  12:00:00 AM',
                                         county_code='BR',
                                         judge=StateAgent(
                                             agent_type='JUDGE',
                                             full_name='BIRDMAN, HARVEY',

                                         ))

        court_case_154576 = StateCourtCase(
            state_court_case_id='154576',
            status='A',
            date_convicted='8/10/10  12:00:00 AM',
            next_court_date='8/10/10  12:00:00 AM',
            county_code='BR',
            judge=StateAgent(
                agent_type='JUDGE',
                full_name='Hollywood, Paul',

            ))

        expected = IngestInfo(
            state_people=[
                StatePerson(state_sentence_groups=[
                    StateSentenceGroup(state_sentence_group_id='105640',
                                       state_incarceration_sentences=[
                                           StateIncarcerationSentence(
                                               state_charges=[
                                                   StateCharge(
                                                       state_court_case=
                                                       court_case_5190),
                                                   StateCharge(
                                                       state_court_case=
                                                       court_case_5192),
                                                   StateCharge(
                                                       state_court_case=
                                                       court_case_5193),
                                                   StateCharge(
                                                       state_court_case=
                                                       court_case_154576)
                                               ]),
                                       ]),
                ]),
            ])

        self.run_file_test(expected, 'elite_orders')

    def test_populate_data_elite_external_movements(self):
        incarceration_periods_113377 = [
            StateIncarcerationPeriod(state_incarceration_period_id='113377-2',
                                     status='REL',
                                     release_date='7/26/18  12:00:00 AM',
                                     facility='NDSP',
                                     release_reason='RPAR'),
            StateIncarcerationPeriod(state_incarceration_period_id='113377-1',
                                     status='ADM',
                                     admission_date='2/28/18  12:00:00 AM',
                                     facility='NDSP',
                                     admission_reason='ADMN')
        ]

        incarceration_periods_105640 = [
            StateIncarcerationPeriod(
                state_incarceration_period_id='105640-1',
                status='ADM',
                facility='NDSP',
                admission_date='1/1/19  12:00:00 AM',
                admission_reason='ADMN'),
            StateIncarcerationPeriod(
                state_incarceration_period_id='105640-3',
                status='ADM',
                facility='JRCC',
                admission_date='2/1/19  12:00:00 AM',
                admission_reason='INT'),
            StateIncarcerationPeriod(
                state_incarceration_period_id='105640-5',
                status='TAP',
                facility='JRCC',
                admission_date='4/1/19  12:00:00 AM',
                admission_reason='HOSPS'),
            StateIncarcerationPeriod(
                state_incarceration_period_id='105640-2',
                status='TRN',
                facility='NDSP',
                release_date='2/1/19  12:00:00 AM',
                release_reason='INT'),
            StateIncarcerationPeriod(
                state_incarceration_period_id='105640-4',
                status='TAP',
                facility='JRCC',
                release_date='3/1/19  12:00:00 AM',
                release_reason='HOSPS')
        ]

        incarceration_periods_114909 = [
            StateIncarcerationPeriod(
                state_incarceration_period_id='114909-2',
                status='REL',
                release_date='1/8/19  12:00:00 AM',
                facility='NDSP',
                release_reason='RPRB'),
            StateIncarcerationPeriod(
                state_incarceration_period_id='114909-1',
                status='ADM',
                admission_date='11/9/18  12:00:00 AM',
                facility='NDSP',
                admission_reason='PV')
        ]

        expected = IngestInfo(
            state_people=[
                StatePerson(state_sentence_groups=[
                    StateSentenceGroup(state_sentence_group_id='113377',
                                       state_incarceration_sentences=[
                                           StateIncarcerationSentence(
                                               state_incarceration_periods=
                                               incarceration_periods_113377)
                                       ]),
                    StateSentenceGroup(state_sentence_group_id='105640',
                                       state_incarceration_sentences=[
                                           StateIncarcerationSentence(
                                               state_incarceration_periods=
                                               incarceration_periods_105640)
                                       ]),
                    StateSentenceGroup(state_sentence_group_id='114909',
                                       state_incarceration_sentences=[
                                           StateIncarcerationSentence(
                                               state_incarceration_periods=
                                               incarceration_periods_114909)
                                       ]),
                ]),
            ])

        self.run_file_test(expected, 'elite_external_movements')

    def test_populate_data_elite_elite_offense_in_custody_and_pos_report_data(
            self):
        state_incarceration_incident_353844 = StateIncarcerationIncident(
            state_incarceration_incident_id='353844',
            incident_type=None,  # TODO(1948): Make MISC types more specific
            incident_date='2/26/2017',
            facility='NDSP',
            location_within_facility='TRAF',
            incident_details=
            '230Inmate Jon Hopkins would not follow directives to stop and be '
            'pat searched when coming from the IDR past traffic.',
            state_incarceration_incident_outcomes=[
                StateIncarcerationIncidentOutcome(
                    state_incarceration_incident_outcome_id='353844-41',
                    outcome_type='LCP',
                    date_effective='3/19/2017',
                    outcome_description='Loss of Commissary Privileges',
                    punishment_length_days='7'
                )
            ]
        )

        state_incarceration_incident_354527 = StateIncarcerationIncident(
            state_incarceration_incident_id='354527',
            incident_type='MINOR',
            incident_date='3/10/2017',
            facility='NDSP',
            location_within_facility='IDR',
            incident_details=
            '210R 221RInmate Hopkins was walking through the lunch line in the '
            'IDR when he cut through one of the last rows of tables and when '
            'confronted he threatened Geer.',
        )

        state_incarceration_incident_378515 = StateIncarcerationIncident(
            state_incarceration_incident_id='378515',
            incident_type='INS',
            incident_date='2/17/2018',
            facility='NDSP',
            location_within_facility='IDR',
            incident_details=
            '230EInmate Hopkins was observed putting peanut butter and jelly '
            'in his pockets in the IDR.  He ignored staff multiple times when '
            'they called his name to get his attention.  He looked at staff, '
            'but continued to ignore him and then walked out of the IDR.This '
            'report was reheard and resolved informally on 3/27/18.',
            state_incarceration_incident_outcomes=[
                StateIncarcerationIncidentOutcome(
                    state_incarceration_incident_outcome_id='378515-57',
                    outcome_type='RTQ',
                    date_effective='2/27/2018',
                    outcome_description='RESTRICTED TO QUARTERS',
                    punishment_length_days='28',
                )
            ]
        )

        state_incarceration_incident_363863 = StateIncarcerationIncident(
            state_incarceration_incident_id='363863',
            incident_type='MINOR',
            incident_date='3/22/2018',
            facility='NDSP',
            location_within_facility='SU1',
            incident_details=
            '101Inmate Knowles and another inmate were involved in a possible '
            'fight.',
        )

        state_incarceration_incident_366571 = StateIncarcerationIncident(
            state_incarceration_incident_id='366571',
            incident_type='IIASSAULT',
            incident_date='1/27/2019',
            facility='NDSP',
            location_within_facility='TRAF',
            incident_details=
            '305 215EInmate Knowles was involved in a gang related assault on '
            'another inmate.',
            state_incarceration_incident_outcomes=[
                StateIncarcerationIncidentOutcome(
                    state_incarceration_incident_outcome_id='366571-29',
                    outcome_type='NOT_GUILTY',
                    date_effective='8/18/2017',
                    outcome_description='LOSS OF PRIVILEGES',
                    punishment_length_days='120'
                )
            ]
        )

        state_incarceration_incident_381647 = StateIncarcerationIncident(
            state_incarceration_incident_id='381647',
            incident_type=None, # TODO(1948): Make MISC types more specific
            incident_date='4/17/2018',
            facility='MRCC',
            location_within_facility='HRT1',
            incident_details=
            '227Staff saw Martha Stewart with a cigarette to her mouth in the '
            'bathroom. Stewart admitted to smoking the cigarette.'
        )

        state_incarceration_incident_381647_2 = \
            copy.copy(state_incarceration_incident_381647)

        incident_list_381647 = [
            state_incarceration_incident_381647,
            state_incarceration_incident_381647_2
        ]

        state_incarceration_incident_381647.\
            state_incarceration_incident_outcomes = [
                StateIncarcerationIncidentOutcome(
                    state_incarceration_incident_outcome_id='381647-36',
                    outcome_type='PAY',
                    date_effective='4/20/2018',
                    outcome_description='LOSS OF PAY',
                    punishment_length_days=None)
            ]

        state_incarceration_incident_381647_2. \
            state_incarceration_incident_outcomes = [
                StateIncarcerationIncidentOutcome(
                    state_incarceration_incident_outcome_id='381647-37',
                    outcome_type='EXD',
                    date_effective='4/20/2018',
                    outcome_description='EXTRA DUTY',
                    punishment_length_days=None
                )
            ]

        expected = IngestInfo(state_people=[
            StatePerson(
                state_person_id='39768',
                state_person_external_ids=[
                    StatePersonExternalId(
                        state_person_external_id_id='39768',
                        id_type=US_ND_ELITE,
                    )
                ],
                state_sentence_groups=[
                    StateSentenceGroup(
                        state_sentence_group_id='105640',
                        state_incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_incarceration_periods=[
                                    StateIncarcerationPeriod(
                                        state_incarceration_incidents=[
                                            state_incarceration_incident_353844,
                                            state_incarceration_incident_354527,
                                            state_incarceration_incident_378515,
                                        ]
                                    )
                                ]
                            )
                        ]
                    )
                ]
            ),
            StatePerson(
                state_person_id='52163',
                state_person_external_ids=[
                    StatePersonExternalId(
                        state_person_external_id_id='52163',
                        id_type=US_ND_ELITE,
                    )
                ],
                state_sentence_groups=[
                    StateSentenceGroup(
                        state_sentence_group_id='113377',
                        state_incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_incarceration_periods=[
                                    StateIncarcerationPeriod(
                                        state_incarceration_incidents=[
                                            state_incarceration_incident_363863,
                                        ]
                                    )
                                ]
                            )
                        ]
                    ),
                    StateSentenceGroup(
                        state_sentence_group_id='110651',
                        state_incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_incarceration_periods=[
                                    StateIncarcerationPeriod(
                                        state_incarceration_incidents=[
                                            state_incarceration_incident_366571,
                                        ]
                                    )
                                ]
                            )
                        ]
                    )
                ]
            ),
            StatePerson(
                state_person_id='21109',
                state_person_external_ids=[
                    StatePersonExternalId(
                        state_person_external_id_id='21109',
                        id_type=US_ND_ELITE,
                    )
                ],
                state_sentence_groups=[
                    StateSentenceGroup(
                        state_sentence_group_id='5129',
                        state_incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_incarceration_periods=[
                                    StateIncarcerationPeriod(
                                        state_incarceration_incidents=
                                        incident_list_381647
                                    )
                                ]
                            )
                        ]
                    )
                ]
            )
        ])
        self.run_file_test(expected,
                           'elite_offense_in_custody_and_pos_report_data')

    def test_populate_data_docstars_offenders(self):
        assessment_metadata_241896 = json.dumps({
            'highest_scoring_domains': ['LR', 'AD', 'EE', 'CP', 'AO', 'FM']})

        assessment_metadata_92237 = json.dumps(
            {'highest_scoring_domains': ['LR', 'AD', 'CP', 'EE', 'AO', 'FM']}
        )

        expected = IngestInfo(state_people=[
            StatePerson(state_person_id='241896',
                        surname='Knowles',
                        given_names='Solange',
                        middle_names='P',
                        birthdate='24-Jun-86',
                        gender='2',
                        current_address='000 1st AVE APT 1, '
                                        'WEST FARGO, ND, 58078',
                        residency_status='N',
                        state_person_races=[StatePersonRace(race='2')],
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='52163',
                                id_type=US_ND_ELITE),
                            StatePersonExternalId(
                                state_person_external_id_id='241896',
                                id_type=US_ND_SID)
                        ],
                        state_assessments=[
                            StateAssessment(assessment_score='18',
                                            assessment_class='RISK',
                                            assessment_type='LSIR',
                                            assessment_metadata=
                                            assessment_metadata_241896),
                            StateAssessment(assessment_level='MODERATE',
                                            assessment_class='RISK',
                                            assessment_type='SORAC')
                        ]),
            StatePerson(state_person_id='92237',
                        surname='Hopkins',
                        given_names='Jon',
                        birthdate='15-Aug-79',
                        gender='1',
                        current_address='123 2nd ST N, FARGO, ND, 58102',
                        residency_status='N',
                        state_person_races=[StatePersonRace(race='1')],
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='39768',
                                id_type=US_ND_ELITE),
                            StatePersonExternalId(
                                state_person_external_id_id='92237',
                                id_type=US_ND_SID)
                        ],
                        state_assessments=[
                            StateAssessment(assessment_score='36',
                                            assessment_class='RISK',
                                            assessment_type='LSIR',
                                            assessment_metadata=
                                            assessment_metadata_92237),
                            StateAssessment(assessment_level='HIGH',
                                            assessment_class='RISK',
                                            assessment_type='SORAC'),
                        ]),
            StatePerson(state_person_id='92307',
                        surname='Sandison',
                        given_names='Mike',
                        birthdate='1-Jun-70',
                        gender='1',
                        current_address='111 3rd ST S #6, FARGO, ND, 58103',
                        state_person_races=[StatePersonRace(race='5')],
                        state_person_ethnicities=[
                            StatePersonEthnicity(ethnicity='HISPANIC')
                        ],
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='92307',
                                id_type=US_ND_SID)
                        ])
        ])

        self.run_file_test(expected, 'docstars_offenders')

    def test_populate_data_docstars_cases(self):
        violation_for_17111 = StateSupervisionViolation(
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    response_type='PERMANENT_DECISION',
                    response_date='12/8/2014',
                    decision='REVOCATION',
                    revocation_type='DOCR Inmate Sentence')
            ])

        violation_for_140408 = StateSupervisionViolation(
            violation_type='TECHNICAL',
            state_supervision_violation_responses=[
                StateSupervisionViolationResponse(
                    response_type='PERMANENT_DECISION',
                    response_date='2/27/2018',
                    decision='REVOCATION',
                    revocation_type='DOCR Inmate Sentence')
            ])

        expected = IngestInfo(state_people=[
            StatePerson(state_person_id='92237',
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='92237',
                                id_type=US_ND_SID)
                        ],
                        state_sentence_groups=[
                            StateSentenceGroup(
                                state_supervision_sentences=[
                                    StateSupervisionSentence(
                                        state_supervision_sentence_id='117110',
                                        supervision_type='Parole',
                                        projected_completion_date='10/6/2014',
                                        max_length='0Y 3M',
                                        state_supervision_periods=[
                                            StateSupervisionPeriod(
                                                start_date='7/17/2014',
                                                termination_date='10/6/2014')
                                        ],
                                        state_charges=[
                                            StateCharge(
                                                state_court_case=
                                                StateCourtCase(
                                                    judge=StateAgent(
                                                        agent_type='JUDGE',
                                                        full_name='The Judge',

                                                    ),
                                                )
                                            )
                                        ]
                                    ),
                                    StateSupervisionSentence(
                                        state_supervision_sentence_id='117111',
                                        supervision_type='Parole',
                                        projected_completion_date='8/7/2015',
                                        max_length='1Y 1M',
                                        state_supervision_periods=[
                                            StateSupervisionPeriod(
                                                start_date='7/17/2014',
                                                termination_date='12/8/2014',
                                                state_supervision_violations=[
                                                    violation_for_17111
                                                ]
                                            )
                                        ],
                                        state_charges=[
                                            StateCharge(
                                                state_court_case=
                                                StateCourtCase(
                                                    judge=StateAgent(
                                                        agent_type='JUDGE',
                                                        full_name='The Judge',

                                                    ),
                                                )
                                            )
                                        ]
                                    )
                                ]
                            )]
                        ),
            StatePerson(state_person_id='241896',
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='241896',
                                id_type=US_ND_SID)
                        ],
                        state_sentence_groups=[
                            StateSentenceGroup(
                                state_supervision_sentences=[
                                    StateSupervisionSentence(
                                        state_supervision_sentence_id='140408',
                                        supervision_type='Suspended',
                                        projected_completion_date='3/23/2019',
                                        state_supervision_periods=[
                                            StateSupervisionPeriod(
                                                start_date='3/24/2017',
                                                termination_date='2/27/2018',
                                                state_supervision_violations=[
                                                    violation_for_140408
                                                ]
                                            )
                                        ],
                                        state_charges=[
                                            StateCharge(
                                                state_court_case=
                                                StateCourtCase(
                                                    judge=StateAgent(
                                                        agent_type='JUDGE',
                                                        full_name=
                                                        'Judge Person',
                                                    ),
                                                )
                                            )
                                        ]
                                    )]
                            )]
                        )
        ])

        self.run_file_test(expected, 'docstars_cases')

    def test_populate_data_docstars_offenses(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id='92237',
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id='92237',
                            id_type=US_ND_SID)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id='117111',
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id='122553',
                                            county_code='008',
                                            statute='3522',
                                            classification_type='F',
                                            counts='1'
                                        ),
                                        StateCharge(
                                            state_charge_id='122554',
                                            county_code='008',
                                            statute='3550',
                                            classification_type='IF',
                                            counts='1'
                                        )]
                                ),
                                StateSupervisionSentence(
                                    state_supervision_sentence_id='117110',
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id='122552',
                                            county_code='008',
                                            statute='3562',
                                            classification_type='F',
                                            counts='1'
                                        )]
                                )]
                        )]
                ),
                StatePerson(
                    state_person_id='241896',
                    state_person_external_ids=[
                        StatePersonExternalId(
                            state_person_external_id_id='241896',
                            id_type=US_ND_SID)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id='140408',
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id='149349',
                                            offense_date='7/30/2016',
                                            county_code='020',
                                            statute='2204',
                                            classification_type='M',
                                            classification_subtype='A',
                                            counts='1'
                                        )]
                                )]
                        )]
                )]
        )

        self.run_file_test(expected, 'docstars_offenses')

    def run_file_test(self, expected: IngestInfo, file_tag: str):
        controller = UsNdGcsfsDirectIngestController(create_mock_gcsfs())
        fixture_contents = fixtures.as_string(
            self.FIXTURE_PATH_PREFIX, f'{file_tag}.csv')
        args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=f'{self.FIXTURE_PATH_PREFIX}/{file_tag}.csv'
        )

        # pylint:disable=protected-access
        final_info = controller._parse(args, fixture_contents)

        print(final_info)
        print(expected)
        assert final_info == expected

    def test_run_full_ingest_all_files(self):
        controller = UsNdGcsfsDirectIngestController(create_mock_gcsfs())
        file_tags = sorted(controller.file_mappings_by_file.keys())
        for file_tag in file_tags:
            controller.run_ingest(GcsfsIngestArgs(
                ingest_time=datetime.datetime.now(),
                file_path=f'{self.FIXTURE_PATH_PREFIX}/{file_tag}.csv'
            ))

        # TODO(2057): For now we just check that we don't crash, but we should
        #  test more comprehensively for state.

    def test_run_full_ingest_all_files_reverse(self):
        controller = UsNdGcsfsDirectIngestController(create_mock_gcsfs())
        file_tags = reversed(sorted(controller.file_mappings_by_file.keys()))
        for file_tag in file_tags:
            controller.run_ingest(GcsfsIngestArgs(
                ingest_time=datetime.datetime.now(),
                file_path=f'{self.FIXTURE_PATH_PREFIX}/{file_tag}.csv'
            ))

        # TODO(2057): For now we just check that we don't crash, but we should
        #  test more comprehensively for state.
