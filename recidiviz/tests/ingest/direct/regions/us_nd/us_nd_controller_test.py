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
from typing import List

import attr
from mock import patch, Mock

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    ResidencyStatus, Ethnicity
from recidiviz.common.constants.state.external_id_types import \
    US_ND_ELITE, US_ND_SID
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentClass, StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_charge import \
    StateChargeClassificationType
from recidiviz.common.constants.state.state_court_case import \
    StateCourtCaseStatus, StateCourtType
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import \
    StateIncarcerationIncidentOutcomeType, StateIncarcerationIncidentType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodReleaseReason, \
    StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus, StateSupervisionPeriodTerminationReason
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseType, \
    StateSupervisionViolationResponseRevocationType, \
    StateSupervisionViolationResponseDecision
from recidiviz.ingest.direct.regions.us_nd. \
    us_nd_controller import UsNdController
from recidiviz.ingest.models.ingest_info import IngestInfo, \
    StateSentenceGroup, StatePerson, StateIncarcerationSentence, StateAlias, \
    StateSupervisionSentence, \
    StateCharge, StateCourtCase, StateIncarcerationPeriod, StatePersonRace, \
    StatePersonExternalId, StateAssessment, StatePersonEthnicity, \
    StateSupervisionPeriod, StateSupervisionViolation, \
    StateSupervisionViolationResponse, StateAgent, StateIncarcerationIncident, \
    StateIncarcerationIncidentOutcome
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import \
    get_set_entity_field_names, EntityFieldType
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.direct_ingest_util import \
    build_controller_for_tests, ingest_args_for_fixture_file, \
    add_paths_with_tags_and_process
from recidiviz.tests.utils import fakes

_INCIDENT_DETAILS_1 = \
    '230Inmate Jon Hopkins would not follow directives to stop and be pat ' \
    'searched when coming from the IDR past traffic.'
_INCIDENT_DETAILS_2 = \
    '210R 221RInmate Hopkins was walking through the lunch line in the IDR ' \
    'when he cut through one of the last rows of tables and when confronted ' \
    'he threatened Geer.'
_INCIDENT_DETAILS_3 = \
    '230EInmate Hopkins was observed putting peanut butter and jelly in his ' \
    'pockets in the IDR.  He ignored staff multiple times when they called ' \
    'his name to get his attention.  He looked at staff, but continued to ' \
    'ignore him and then walked out of the IDR.This report was reheard and ' \
    'resolved informally on 3/27/18.'
_INCIDENT_DETAILS_4 = \
    '101Inmate Knowles and another inmate were involved in a possible fight.'
_INCIDENT_DETAILS_5 = \
    '305 215EInmate Knowles was involved in a gang related assault on ' \
    'another inmate.'
_INCIDENT_DETAILS_6 = \
    '227Staff saw Martha Stewart with a cigarette to her mouth in the ' \
    'bathroom. Stewart admitted to smoking the cigarette.'
_STATE_CODE = 'US_ND'


@patch('recidiviz.utils.metadata.project_id',
       Mock(return_value='recidiviz-staging'))
class TestUsNdController(unittest.TestCase):
    """Unit tests for each North Dakota file to be ingested by the
    UsNdController.
    """

    FIXTURE_PATH_PREFIX = 'direct/regions/us_nd'

    def setup_method(self, _test_method):
        self.controller = build_controller_for_tests(
            UsNdController,
            self.FIXTURE_PATH_PREFIX,
            run_async=False)
        fakes.use_in_memory_sqlite_database(StateBase)

        self.maxDiff = 250000

    def _run_ingest_job_for_filename(self, filename: str) -> None:
        args = ingest_args_for_fixture_file(self.controller, filename)
        self.controller.fs.test_add_path(args.file_path)

        # pylint:disable=protected-access
        self.controller._run_ingest_job(args)

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

        self.run_parse_file_test(expected, 'elite_offenders')

    def test_populate_data_elite_offenderidentifier(self):
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
                            ]),
                StatePerson(state_person_id='92237',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='12345',
                                    id_type=US_ND_SID),
                                StatePersonExternalId(
                                    state_person_external_id_id='92237',
                                    id_type=US_ND_ELITE)
                            ])
            ])

        self.run_parse_file_test(expected, 'elite_offenderidentifier')

    def test_populate_data_elite_alias(self):
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

        self.run_parse_file_test(expected, 'elite_alias')

    def test_populate_data_elite_offenderbookingstable(self):
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
                                                   status='C'),
                                StateSentenceGroup(state_sentence_group_id=
                                                   '114909',
                                                   status='C')
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
                                                   status='C')
                            ])
            ])

        self.run_parse_file_test(expected, 'elite_offenderbookingstable')

    def test_populate_data_elite_offendersentenceaggs(self):
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

        self.run_parse_file_test(expected, 'elite_offendersentenceaggs')

    def test_populate_data_elite_offendersentences(self):
        sentences_114909 = [
            StateIncarcerationSentence(
                state_incarceration_sentence_id='114909-1',
                date_imposed='2/27/18  12:00:00 AM',
                projected_min_release_date='12/16/18  12:00:00 AM',
                projected_max_release_date='12/26/18  12:00:00 AM',
                is_life='False'),
            StateIncarcerationSentence(
                state_incarceration_sentence_id='114909-2',
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
                date_imposed='2/15/12  12:00:00 AM',
                projected_min_release_date='10/18/13  12:00:00 AM',
                projected_max_release_date='2/15/14  12:00:00 AM',
                is_life='False')
        ]

        sentences_113377 = [
            StateIncarcerationSentence(
                state_incarceration_sentence_id='113377-4',
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

        self.run_parse_file_test(expected, 'elite_offendersentences')

    def test_populate_data_elite_offendersentenceterms(self):
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

        self.run_parse_file_test(expected, 'elite_offendersentenceterms')

    def test_populate_data_elite_offenderchargestable(self):
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

        self.run_parse_file_test(expected, 'elite_offenderchargestable')

    def test_populate_data_elite_orderstable(self):
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

        self.run_parse_file_test(expected, 'elite_orderstable')

    def test_populate_data_elite_externalmovements(self):
        incarceration_periods_113377 = [
            StateIncarcerationPeriod(state_incarceration_period_id='113377-2',
                                     status='OUT',
                                     release_date='7/26/18  12:00:00 AM',
                                     facility='NDSP',
                                     release_reason='RPAR'),
            StateIncarcerationPeriod(state_incarceration_period_id='113377-1',
                                     status='IN',
                                     admission_date='2/28/18  12:00:00 AM',
                                     facility='NDSP',
                                     admission_reason='ADMN')
        ]

        incarceration_periods_105640 = [
            StateIncarcerationPeriod(
                state_incarceration_period_id='105640-1',
                status='IN',
                facility='NDSP',
                admission_date='1/1/19  12:00:00 AM',
                admission_reason='ADMN'),
            StateIncarcerationPeriod(
                state_incarceration_period_id='105640-3',
                status='IN',
                facility='JRCC',
                admission_date='2/1/19  12:00:00 AM',
                admission_reason='INT'),
            StateIncarcerationPeriod(
                state_incarceration_period_id='105640-5',
                status='IN',
                facility='JRCC',
                admission_date='4/1/19  12:00:00 AM',
                admission_reason='HOSPS'),
            StateIncarcerationPeriod(
                state_incarceration_period_id='105640-2',
                status='OUT',
                facility='NDSP',
                release_date='2/1/19  12:00:00 AM',
                release_reason='INT'),
            StateIncarcerationPeriod(
                state_incarceration_period_id='105640-4',
                status='OUT',
                facility='JRCC',
                release_date='3/1/19  12:00:00 AM',
                release_reason='HOSPS')
        ]

        incarceration_periods_114909 = [
            StateIncarcerationPeriod(
                state_incarceration_period_id='114909-2',
                status='OUT',
                release_date='1/8/19  12:00:00 AM',
                facility='NDSP',
                release_reason='RPRB'),
            StateIncarcerationPeriod(
                state_incarceration_period_id='114909-1',
                status='IN',
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

        self.run_parse_file_test(expected, 'elite_externalmovements')

    def test_populate_data_elite_elite_offense_in_custody_and_pos_report_data(
            self):
        state_incarceration_incident_353844 = StateIncarcerationIncident(
            state_incarceration_incident_id='353844',
            incident_type=None,  # TODO(1948): Make MISC types more specific
            incident_date='1/27/2019',
            facility='NDSP',
            location_within_facility='TRAF',
            incident_details=_INCIDENT_DETAILS_1,
            state_incarceration_incident_outcomes=[
                StateIncarcerationIncidentOutcome(
                    state_incarceration_incident_outcome_id='353844-41',
                    outcome_type='LCP',
                    date_effective='3/19/2019',
                    outcome_description='Loss of Commissary Privileges',
                    punishment_length_days='7'
                )
            ]
        )

        state_incarceration_incident_354527 = StateIncarcerationIncident(
            state_incarceration_incident_id='354527',
            incident_type='MINOR',
            incident_date='4/10/2019',
            facility='JRCC',
            location_within_facility='IDR',
            incident_details=_INCIDENT_DETAILS_2,
        )

        state_incarceration_incident_378515 = StateIncarcerationIncident(
            state_incarceration_incident_id='378515',
            incident_type='INS',
            incident_date='1/17/2019',
            facility='NDSP',
            location_within_facility='IDR',
            incident_details=_INCIDENT_DETAILS_3,
            state_incarceration_incident_outcomes=[
                StateIncarcerationIncidentOutcome(
                    state_incarceration_incident_outcome_id='378515-57',
                    outcome_type='RTQ',
                    date_effective='1/27/2019',
                    outcome_description='RESTRICTED TO QUARTERS',
                    punishment_length_days='31',
                )
            ]
        )

        state_incarceration_incident_363863 = StateIncarcerationIncident(
            state_incarceration_incident_id='363863',
            incident_type='MINOR',
            incident_date='3/22/2018',
            facility='NDSP',
            location_within_facility='SU1',
            incident_details=_INCIDENT_DETAILS_4,
        )

        state_incarceration_incident_366571 = StateIncarcerationIncident(
            state_incarceration_incident_id='366571',
            incident_type='IIASSAULT',
            incident_date='1/27/2019',
            facility='NDSP',
            location_within_facility='TRAF',
            incident_details=_INCIDENT_DETAILS_5,
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
            incident_type=None,  # TODO(1948): Make MISC types more specific
            incident_date='4/17/2018',
            facility='MRCC',
            location_within_facility='HRT1',
            incident_details=_INCIDENT_DETAILS_6,
        )

        state_incarceration_incident_381647_2 = \
            copy.copy(state_incarceration_incident_381647)

        incident_list_381647 = [
            state_incarceration_incident_381647,
            state_incarceration_incident_381647_2
        ]

        state_incarceration_incident_381647. \
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
                    punishment_length_days=None)
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
        self.run_parse_file_test(expected,
                                 'elite_offense_in_custody_and_pos_report_data')

    def test_populate_data_docstars_offenders(self):
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

        self.run_parse_file_test(expected, 'docstars_offenders')

    def test_populate_data_docstars_offendercasestable(self):
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
                                        max_length='92d',
                                        state_supervision_periods=[
                                            StateSupervisionPeriod(
                                                start_date='7/17/2014',
                                                termination_date='10/6/2014',
                                                termination_reason='7')
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
                                        max_length='580d',
                                        state_supervision_periods=[
                                            StateSupervisionPeriod(
                                                start_date='7/17/2014',
                                                termination_date='12/8/2014',
                                                termination_reason='9',
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
                                                termination_reason='9',
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

        self.run_parse_file_test(expected, 'docstars_offendercasestable')

    def test_populate_data_docstars_offensestable(self):
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
                                            classification_type='F',
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

        self.run_parse_file_test(expected, 'docstars_offensestable')

    def test_populate_data_docstars_lsichronology(self):
        metadata_12345 = json.dumps({
            "domain_criminal_history": 7, "domain_education_employment": 7,
            "domain_financial": 1, "domain_family_marital": 2,
            "domain_accommodation": 0, "domain_leisure_recreation": 2,
            "domain_companions": 3, "domain_alcohol_drug_problems": 0,
            "domain_emotional_personal": 2, "domain_attitudes_orientation": 1,
            "question_18": 0, "question_19": 0, "question_20": 0,
            "question_21": 1, "question_23": 3, "question_24": 1,
            "question_25": 1, "question_27": 3, "question_31": 1,
            "question_39": 2, "question_40": 1, "question_51": 2,
            "question_52": 3
        })

        metadata_12346 = json.dumps({
            "domain_criminal_history": 6, "domain_education_employment": 8,
            "domain_financial": 1, "domain_family_marital": 2,
            "domain_accommodation": 0, "domain_leisure_recreation": 2,
            "domain_companions": 2, "domain_alcohol_drug_problems": 0,
            "domain_emotional_personal": 0, "domain_attitudes_orientation": 2,
            "question_18": 0, "question_19": 0, "question_20": 0,
            "question_21": 0, "question_23": 2, "question_24": 2,
            "question_25": 0, "question_27": 2, "question_31": 0,
            "question_39": 2, "question_40": 3, "question_51": 2,
            "question_52": 2
        })

        metadata_55555 = json.dumps({
            "domain_criminal_history": 6, "domain_education_employment": 10,
            "domain_financial": 2, "domain_family_marital": 3,
            "domain_accommodation": 0, "domain_leisure_recreation": 2,
            "domain_companions": 2, "domain_alcohol_drug_problems": 0,
            "domain_emotional_personal": 0, "domain_attitudes_orientation": 0,
            "question_18": 0, "question_19": 0, "question_20": 0,
            "question_21": 0, "question_23": 2, "question_24": 0,
            "question_25": 0, "question_27": 2, "question_31": 1,
            "question_39": 1, "question_40": 0, "question_51": 3,
            "question_52": 3
        })

        expected = IngestInfo(state_people=[
            StatePerson(state_person_id='92237',
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='92237',
                                id_type=US_ND_SID)
                        ],
                        state_assessments=[
                            StateAssessment(state_assessment_id='12345',
                                            assessment_class='RISK',
                                            assessment_type='LSIR',
                                            assessment_date='7/14/16',
                                            assessment_score='25',
                                            assessment_metadata=metadata_12345),
                            StateAssessment(state_assessment_id='12346',
                                            assessment_class='RISK',
                                            assessment_type='LSIR',
                                            assessment_date='1/13/17',
                                            assessment_score='23',
                                            assessment_metadata=metadata_12346),
                        ]),
            StatePerson(state_person_id='241896',
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='241896',
                                id_type=US_ND_SID)
                        ],
                        state_assessments=[
                            StateAssessment(state_assessment_id='55555',
                                            assessment_class='RISK',
                                            assessment_type='LSIR',
                                            assessment_date='12/10/18',
                                            assessment_score='25',
                                            assessment_metadata=metadata_55555),
                        ])
        ])

        self.run_parse_file_test(expected, 'docstars_lsichronology')

    def run_parse_file_test(self, expected: IngestInfo, file_tag: str):
        args = ingest_args_for_fixture_file(self.controller, f'{file_tag}.csv')
        self.controller.fs.test_add_path(args.file_path)

        # pylint:disable=protected-access
        fixture_contents = self.controller._read_contents(args)
        final_info = self.controller._parse(args, fixture_contents)

        print(final_info)
        print("\n\n\n")
        print(expected)
        assert final_info == expected

    # TODO(2157): Move into integration specific file
    @patch.dict('os.environ', {'PERSIST_LOCALLY': 'true'})
    def test_run_full_ingest_all_files_specific_order(self):

        ######################################
        # ELITE OFFENDERS
        ######################################
        # Arrange
        person_1 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "JON", "surname": "HOPKINS"}',
            birthdate=datetime.date(year=1979, month=8, day=15),
            birthdate_inferred_from_age=False, gender=Gender.MALE,
            gender_raw_text='M')
        person_1_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id='39768', id_type=US_ND_ELITE, state_code=_STATE_CODE,
            person=person_1)
        person_1_race = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.WHITE, race_raw_text='CAUCASIAN',
            person=person_1)
        person_1.external_ids = [person_1_external_id]
        person_1.races = [person_1_race]

        person_2 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "SOLANGE", "surname": "KNOWLES"}',
            birthdate=datetime.date(year=1986, month=6, day=24),
            birthdate_inferred_from_age=False, gender=Gender.FEMALE,
            gender_raw_text='F')
        person_2_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id='52163', id_type=US_ND_ELITE, state_code=_STATE_CODE,
            person=person_2)
        person_2_race = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.BLACK, race_raw_text='BLACK',
            person=person_2)
        person_2.external_ids = [person_2_external_id]
        person_2.races = [person_2_race]

        expected_people = [person_2, person_1]

        # Act
        self._run_ingest_job_for_filename('elite_offenders.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # ELITE OFFENDER IDENTIFIERS
        ######################################
        # Arrange
        person_1_external_id_2 = \
            entities.StatePersonExternalId.new_with_defaults(
                external_id='92237', id_type=US_ND_SID, state_code=_STATE_CODE,
                person=person_1)
        person_2_external_id_2 = \
            entities.StatePersonExternalId.new_with_defaults(
                external_id='241896', id_type=US_ND_SID, state_code=_STATE_CODE,
                person=person_2)
        person_1.external_ids.append(person_1_external_id_2)
        person_2.external_ids.append(person_2_external_id_2)

        person_3 = entities.StatePerson.new_with_defaults()
        person_3_external_id_1 = \
            entities.StatePersonExternalId.new_with_defaults(
                external_id='12345', id_type=US_ND_SID,
                state_code=_STATE_CODE,
                person=person_3)
        person_3_external_id_2 = \
            entities.StatePersonExternalId.new_with_defaults(
                external_id='92237', id_type=US_ND_ELITE,
                state_code=_STATE_CODE,
                person=person_3)
        person_3.external_ids.append(person_3_external_id_1)
        person_3.external_ids.append(person_3_external_id_2)
        expected_people.append(person_3)

        # Act
        self._run_ingest_job_for_filename('elite_offenderidentifier.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # ELITE ALIASES
        ######################################
        # Arrange
        # TODO(2158): Why do we fill out full_name and keep the distinct
        # parts (in comparison to Person).
        person_1_alias_1 = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "TODD", "name_suffix": "III", '
                      '"surname": "HOPKINS"}',
            state_code=_STATE_CODE, person=person_1)
        person_1_alias_2 = entities.StatePersonAlias.new_with_defaults(
            full_name='{"given_names": "JON", "middle_names": "R", '
                      '"surname": "HODGSON"}',
            state_code=_STATE_CODE, person=person_1)
        person_2_alias = entities.StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE,
            full_name='{"given_names": "SOLANGE"}', person=person_2)
        person_1.aliases.append(person_1_alias_1)
        person_1.aliases.append(person_1_alias_2)
        person_2.aliases.append(person_2_alias)

        # Act
        self._run_ingest_job_for_filename('elite_alias.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # ELITE BOOKINGS
        ######################################
        # Arrange
        sentence_group_105640 = entities.StateSentenceGroup.new_with_defaults(
            external_id='105640', status=StateSentenceStatus.COMPLETED,
            status_raw_text='C', state_code=_STATE_CODE, person=person_1)
        sentence_group_113377 = entities.StateSentenceGroup.new_with_defaults(
            external_id='113377', status=StateSentenceStatus.COMPLETED,
            status_raw_text='C', state_code=_STATE_CODE, person=person_2)
        sentence_group_114909 = entities.StateSentenceGroup.new_with_defaults(
            external_id='114909', status=StateSentenceStatus.COMPLETED,
            status_raw_text='C', state_code=_STATE_CODE, person=person_2)
        person_1.sentence_groups.append(sentence_group_105640)
        person_2.sentence_groups.append(sentence_group_113377)
        person_2.sentence_groups.append(sentence_group_114909)

        # Act
        self._run_ingest_job_for_filename('elite_offenderbookingstable.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # ELITE SENTENCE AGGS
        ######################################
        # Arrange
        # TODO(2155): Should these have an external_id?
        incarceration_sentence_1 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                parole_eligibility_date=
                datetime.date(year=2005, month=11, day=24),
                sentence_group=sentence_group_105640, person=person_1)
        incarceration_sentence_2 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                parole_eligibility_date=
                datetime.date(year=2018, month=7, day=26),
                sentence_group=sentence_group_113377, person=person_2)
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_1)
        sentence_group_105640.date_imposed = datetime.date(
            year=2004, month=2, day=12)
        sentence_group_105640.max_length_days = 758

        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_2)
        sentence_group_113377.date_imposed = datetime.date(
            year=2018, month=3, day=27)
        sentence_group_113377.max_length_days = 284

        sentence_group_114909.date_imposed = datetime.date(
            year=2018, month=2, day=27)
        sentence_group_114909.max_length_days = 316

        person_4 = entities.StatePerson.new_with_defaults()
        sentence_group_115077 = entities.StateSentenceGroup.new_with_defaults(
            external_id='115077',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE, max_length_days=1316,
            date_imposed=datetime.date(year=2018, month=10, day=29),
            person=person_4)
        person_4.sentence_groups.append(sentence_group_115077)
        expected_people.append(person_4)

        # Act
        self._run_ingest_job_for_filename('elite_offendersentenceaggs.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # ELITE SENTENCES
        ######################################
        # Arrange
        incarceration_sentence_114909_1 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='114909-1',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=2018, month=2, day=27),
                is_life=False,
                projected_min_release_date=datetime.date(
                    year=2018, month=12, day=16),
                projected_max_release_date=datetime.date(
                    year=2018, month=12, day=26),
                state_code=_STATE_CODE, sentence_group=sentence_group_114909,
                person=sentence_group_114909.person)
        incarceration_sentence_114909_2 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='114909-2',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=2018, month=2, day=27),
                is_life=False,
                projected_min_release_date=datetime.date(
                    year=2019, month=1, day=8),
                projected_max_release_date=datetime.date(
                    year=2019, month=1, day=8),
                state_code=_STATE_CODE, sentence_group=sentence_group_114909,
                person=sentence_group_114909.person)
        incarceration_sentence_105640_1 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='105640-1',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=1990, month=1, day=8),
                is_life=False,
                projected_min_release_date=datetime.date(
                    year=1996, month=10, day=21),
                projected_max_release_date=datetime.date(
                    year=1999, month=6, day=18),
                state_code=_STATE_CODE, sentence_group=sentence_group_105640,
                person=sentence_group_105640.person)
        incarceration_sentence_105640_2 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='105640-2',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=1990, month=1, day=8),
                is_life=False,
                projected_min_release_date=datetime.date(
                    year=1996, month=10, day=21),
                projected_max_release_date=datetime.date(
                    year=1999, month=6, day=18),
                state_code=_STATE_CODE, sentence_group=sentence_group_105640,
                person=sentence_group_105640.person)
        incarceration_sentence_105640_5 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='105640-5',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=1996, month=10, day=21),
                is_life=False,
                projected_min_release_date=datetime.date(
                    year=2012, month=2, day=15),
                projected_max_release_date=datetime.date(
                    year=2016, month=10, day=21),
                state_code=_STATE_CODE, sentence_group=sentence_group_105640,
                person=sentence_group_105640.person)
        incarceration_sentence_105640_6 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='105640-6',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=2012, month=2, day=15),
                is_life=False,
                projected_min_release_date=datetime.date(
                    year=2015, month=6, day=26),
                projected_max_release_date=datetime.date(
                    year=2017, month=2, day=15),
                state_code=_STATE_CODE, sentence_group=sentence_group_105640,
                person=sentence_group_105640.person)
        incarceration_sentence_105640_7 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='105640-7',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=2012, month=2, day=15),
                is_life=False,
                projected_min_release_date=datetime.date(
                    year=2013, month=10, day=18),
                projected_max_release_date=datetime.date(
                    year=2014, month=2, day=15),
                state_code=_STATE_CODE, sentence_group=sentence_group_105640,
                person=sentence_group_105640.person)
        incarceration_sentence_113377_4 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='113377-4',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=2018, month=2, day=27),
                is_life=False,
                projected_min_release_date=datetime.date(
                    year=2018, month=9, day=30),
                projected_max_release_date=datetime.date(
                    year=2018, month=11, day=24),
                state_code=_STATE_CODE, sentence_group=sentence_group_113377,
                person=sentence_group_113377.person)
        sentence_group_114909.incarceration_sentences.append(
            incarceration_sentence_114909_1)
        sentence_group_114909.incarceration_sentences.append(
            incarceration_sentence_114909_2)
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_1)
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_2)
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_5)
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_6)
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_7)
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_4)

        # Act
        self._run_ingest_job_for_filename('elite_offendersentences.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # ELITE SENTENCE TERMS
        ######################################
        # Arrange
        supervision_sentence_105640_2 = \
            entities.StateSupervisionSentence.new_with_defaults(
                external_id='105640-2',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_type_raw_text='PROBATION',
                max_length_days=3652, state_code=_STATE_CODE,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person)
        supervision_sentence_105640_6 = \
            entities.StateSupervisionSentence.new_with_defaults(
                external_id='105640-6',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_type_raw_text='PROBATION',
                max_length_days=1826, state_code=_STATE_CODE,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person)
        incarceration_sentence_113377_1 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='113377-1',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                max_length_days=366, state_code=_STATE_CODE,
                sentence_group=sentence_group_113377,
                person=sentence_group_113377.person)
        incarceration_sentence_113377_5 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='113377-5',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                max_length_days=1000, state_code=_STATE_CODE,
                sentence_group=sentence_group_113377,
                person=sentence_group_113377.person)
        incarceration_sentence_105640_1.max_length_days = 3652
        incarceration_sentence_105640_2.max_length_days = 3652
        incarceration_sentence_105640_5.max_length_days = 7305
        incarceration_sentence_105640_6.max_length_days = 1826
        incarceration_sentence_105640_7.max_length_days = 730
        incarceration_sentence_114909_1.max_length_days = 366
        incarceration_sentence_114909_2.max_length_days = 360
        incarceration_sentence_113377_4.max_length_days = 360
        sentence_group_105640.supervision_sentences.append(
            supervision_sentence_105640_2)
        sentence_group_105640.supervision_sentences.append(
            supervision_sentence_105640_6)
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_1)
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_5)

        # Act
        self._run_ingest_job_for_filename('elite_offendersentenceterms.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # ELITE CHARGES
        ######################################
        # Arrange
        incarceration_sentence_113377_2 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                external_id='113377-2',
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE, sentence_group=sentence_group_113377,
                person=sentence_group_113377.person)
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_2)
        charge_105640_1 = entities.StateCharge.new_with_defaults(
            external_id='105640-1', status=ChargeStatus.SENTENCED,
            status_raw_text='SENTENCED',
            offense_date=datetime.date(year=1989, month=6, day=19),
            statute='1801', description='KIDNAPPING',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='F', classification_subtype='B',
            counts=1, state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_105640_1],
            person=incarceration_sentence_105640_1.person)
        charge_105640_2 = entities.StateCharge.new_with_defaults(
            external_id='105640-2', status=ChargeStatus.SENTENCED,
            status_raw_text='SENTENCED',
            offense_date=datetime.date(year=1989, month=6, day=19),
            statute='A2003', description='GROSS SEXUAL IMPOSITION',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='F', classification_subtype='A',
            counts=1,
            charge_notes='TO REGISTER AS A SEX OFFENDER (PLEA OF GUILTY)',
            state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_105640_2],
            person=incarceration_sentence_105640_2.person)
        charge_105640_5 = entities.StateCharge.new_with_defaults(
            external_id='105640-5', status=ChargeStatus.SENTENCED,
            status_raw_text='SENTENCED',
            offense_date=datetime.date(year=1989, month=6, day=19),
            statute='1601',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='F', classification_subtype='A',
            counts=1,
            charge_notes='ATTEMPTED', state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_105640_5],
            person=incarceration_sentence_105640_5.person)
        charge_105640_6 = entities.StateCharge.new_with_defaults(
            external_id='105640-6', status=ChargeStatus.SENTENCED,
            status_raw_text='SENTENCED',
            offense_date=datetime.date(year=1991, month=4, day=29),
            statute='A2003',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='F', classification_subtype='B',
            counts=1, state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_105640_6],
            person=incarceration_sentence_105640_6.person)
        charge_105640_7 = entities.StateCharge.new_with_defaults(
            external_id='105640-7', status=ChargeStatus.SENTENCED,
            status_raw_text='SENTENCED',
            offense_date=datetime.date(year=2010, month=6, day=28),
            statute='17011',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            classification_subtype='C',
            counts=1, charge_notes='ON A CO', state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_105640_7],
            person=incarceration_sentence_105640_7.person)
        charge_113377_1 = entities.StateCharge.new_with_defaults(
            external_id='113377-1',
            status=ChargeStatus.SENTENCED, status_raw_text='SENTENCED',
            offense_date=datetime.date(year=2017, month=8, day=23),
            statute='362102',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            classification_subtype='C', counts=1, state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_113377_1],
            person=incarceration_sentence_113377_1.person)
        charge_113377_2 = entities.StateCharge.new_with_defaults(
            external_id='113377-2',
            status=ChargeStatus.SENTENCED, status_raw_text='SENTENCED',
            offense_date=datetime.date(year=2016, month=11, day=13),
            statute='2203',
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text='M',
            classification_subtype='A', counts=1, state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_113377_2],
            person=incarceration_sentence_113377_2.person)
        charge_113377_4 = entities.StateCharge.new_with_defaults(
            external_id='113377-4',
            status=ChargeStatus.SENTENCED, status_raw_text='SENTENCED',
            offense_date=datetime.date(year=2016, month=7, day=30),
            statute='2203',
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text='M',
            classification_subtype='A', counts=1, state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_113377_4],
            person=incarceration_sentence_113377_4.person)
        charge_114909_1 = entities.StateCharge.new_with_defaults(
            external_id='114909-1',
            status=ChargeStatus.SENTENCED, status_raw_text='SENTENCED',
            offense_date=datetime.date(year=2017, month=8, day=23),
            statute='362102',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            classification_subtype='C', counts=1, state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_114909_1],
            person=incarceration_sentence_114909_1.person)
        charge_114909_2 = entities.StateCharge.new_with_defaults(
            external_id='114909-2',
            status=ChargeStatus.SENTENCED, status_raw_text='SENTENCED',
            offense_date=datetime.date(year=2016, month=11, day=13),
            statute='2203',
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text='M',
            classification_subtype='A', counts=1, state_code=_STATE_CODE,
            incarceration_sentences=[incarceration_sentence_114909_2],
            person=incarceration_sentence_114909_2.person)

        court_case_5190 = entities.StateCourtCase.new_with_defaults(
            external_id='5190',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_105640_1, charge_105640_2], state_code=_STATE_CODE,
            person=sentence_group_105640.person)
        court_case_5192 = entities.StateCourtCase.new_with_defaults(
            external_id='5192',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_105640_5], state_code=_STATE_CODE,
            person=sentence_group_105640.person)
        court_case_5193 = entities.StateCourtCase.new_with_defaults(
            external_id='5193',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_105640_6], state_code=_STATE_CODE,
            person=sentence_group_105640.person)
        court_case_154576 = entities.StateCourtCase.new_with_defaults(
            external_id='154576',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_105640_7], state_code=_STATE_CODE,
            person=sentence_group_105640.person)
        court_case_178768 = entities.StateCourtCase.new_with_defaults(
            external_id='178768',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_113377_1], state_code=_STATE_CODE,
            person=sentence_group_113377.person)
        court_case_178986 = entities.StateCourtCase.new_with_defaults(
            external_id='178986',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_113377_2], state_code=_STATE_CODE,
            person=sentence_group_113377.person)
        court_case_178987 = entities.StateCourtCase.new_with_defaults(
            external_id='178987',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_113377_4], state_code=_STATE_CODE,
            person=sentence_group_113377.person)
        court_case_181820 = entities.StateCourtCase.new_with_defaults(
            external_id='181820',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_114909_1], state_code=_STATE_CODE,
            person=sentence_group_114909.person)
        court_case_181821 = entities.StateCourtCase.new_with_defaults(
            external_id='181821',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            charges=[charge_114909_2], state_code=_STATE_CODE,
            person=sentence_group_114909.person)

        charge_105640_1.court_case = court_case_5190
        charge_105640_2.court_case = court_case_5190
        charge_105640_5.court_case = court_case_5192
        charge_105640_6.court_case = court_case_5193
        charge_105640_7.court_case = court_case_154576
        charge_113377_1.court_case = court_case_178768
        charge_113377_2.court_case = court_case_178986
        charge_113377_4.court_case = court_case_178987
        charge_114909_1.court_case = court_case_181820
        charge_114909_2.court_case = court_case_181821

        incarceration_sentence_105640_1.charges.append(charge_105640_1)
        incarceration_sentence_105640_2.charges.append(charge_105640_2)
        incarceration_sentence_105640_5.charges.append(charge_105640_5)
        incarceration_sentence_105640_6.charges.append(charge_105640_6)
        incarceration_sentence_105640_7.charges.append(charge_105640_7)
        incarceration_sentence_113377_1.charges.append(charge_113377_1)
        incarceration_sentence_113377_2.charges.append(charge_113377_2)
        incarceration_sentence_113377_4.charges.append(charge_113377_4)
        incarceration_sentence_114909_1.charges.append(charge_114909_1)
        incarceration_sentence_114909_2.charges.append(charge_114909_2)

        # Act
        self._run_ingest_job_for_filename('elite_offenderchargestable.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # ELITE ORDERS
        ######################################
        # Arrange
        # TODO(2158): Parse full_name in the same way we do for person
        # TODO(2111): Remove duplicates once we have external_ids.
        agent_judy = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text='JUDGE',
            state_code=_STATE_CODE,
            full_name='{"full_name": "SHEINDLIN, JUDY"}')
        agent_judy_dup = attr.evolve(agent_judy)
        agent_harvey = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text='JUDGE',
            state_code=_STATE_CODE,
            full_name='{"full_name": "BIRDMAN, HARVEY"}')
        agent_paul = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text='JUDGE',
            state_code=_STATE_CODE,
            full_name='{"full_name": "HOLLYWOOD, PAUL"}')

        court_case_5190.date_convicted = datetime.date(
            year=1989, month=6, day=19)
        court_case_5190.next_court_date = datetime.date(
            year=1989, month=6, day=19)
        court_case_5190.county_code = 'CA'
        court_case_5190.status_raw_text = 'A'
        court_case_5190.judge = agent_judy

        court_case_5192.date_convicted = datetime.date(
            year=1989, month=6, day=19)
        court_case_5192.next_court_date = datetime.date(
            year=1989, month=6, day=19)
        court_case_5192.county_code = 'CA'
        court_case_5192.status_raw_text = 'A'
        court_case_5192.judge = agent_judy_dup

        court_case_5193.date_convicted = datetime.date(
            year=1991, month=4, day=29)
        court_case_5193.next_court_date = datetime.date(
            year=1991, month=4, day=29)
        court_case_5193.county_code = 'BR'
        court_case_5193.status_raw_text = 'A'
        court_case_5193.judge = agent_harvey

        court_case_154576.date_convicted = datetime.date(
            year=2010, month=8, day=10)
        court_case_154576.next_court_date = datetime.date(
            year=2010, month=8, day=10)
        court_case_154576.county_code = 'BR'
        court_case_154576.status_raw_text = 'A'
        court_case_154576.judge = agent_paul

        # Act
        self._run_ingest_job_for_filename('elite_orderstable.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)

        self.assertCountEqual(found_people, expected_people)

        ######################################
        # ELITE EXTERNAL MOVEMENTS
        ######################################
        # Arrange
        incarceration_sentence_113377_ips = \
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE, sentence_group=sentence_group_113377,
                person=sentence_group_113377.person)
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_ips)
        incarceration_period_113377_1 = \
            entities.StateIncarcerationPeriod.new_with_defaults(
                external_id='113377-1|113377-2',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                status_raw_text='OUT',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                facility='NDSP',
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text='ADMN',
                admission_date=datetime.date(year=2018, month=2, day=28),
                release_reason=
                StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
                release_reason_raw_text='RPAR',
                release_date=datetime.date(year=2018, month=7, day=26),
                state_code=_STATE_CODE,
                incarceration_sentences=[incarceration_sentence_113377_ips],
                person=incarceration_sentence_113377_ips.person)
        incarceration_sentence_105640_ips = \
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                state_code=_STATE_CODE,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                sentence_group=sentence_group_105640,
                person=sentence_group_105640.person)
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_ips)
        incarceration_period_105640_1 = \
            entities.StateIncarcerationPeriod.new_with_defaults(
                external_id='105640-1|105640-2',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                status_raw_text='OUT',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                facility='NDSP',
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text='ADMN',
                admission_date=datetime.date(year=2019, month=1, day=1),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
                release_reason_raw_text='INT', state_code=_STATE_CODE,
                release_date=datetime.date(year=2019, month=2, day=1),
                incarceration_sentences=[incarceration_sentence_105640_ips],
                person=incarceration_sentence_105640_ips.person)
        incarceration_period_105640_2 = \
            entities.StateIncarcerationPeriod.new_with_defaults(
                external_id='105640-3|105640-4',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                status_raw_text='OUT',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                facility='JRCC',
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
                admission_reason_raw_text='INT',
                admission_date=datetime.date(year=2019, month=2, day=1),
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
                release_reason_raw_text='HOSPS', state_code=_STATE_CODE,
                release_date=datetime.date(year=2019, month=3, day=1),
                incarceration_sentences=[incarceration_sentence_105640_ips],
                person=incarceration_sentence_105640_ips.person)
        incarceration_period_105640_3 = \
            entities.StateIncarcerationPeriod.new_with_defaults(
                external_id='105640-5',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                status_raw_text='IN',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                facility='JRCC',
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
                admission_reason_raw_text='HOSPS',
                admission_date=datetime.date(year=2019, month=4, day=1),
                state_code=_STATE_CODE,
                incarceration_sentences=[incarceration_sentence_105640_ips],
                person=incarceration_sentence_105640_ips.person)

        incarceration_sentence_114909_ips = \
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE, sentence_group=sentence_group_114909,
                person=sentence_group_114909.person)
        sentence_group_114909.incarceration_sentences.append(
            incarceration_sentence_114909_ips)
        incarceration_period_114909_1 = \
            entities.StateIncarcerationPeriod.new_with_defaults(
                external_id='114909-1|114909-2',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                status_raw_text='OUT',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                facility='NDSP',
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                admission_reason_raw_text='PV',
                admission_date=datetime.date(year=2018, month=11, day=9),
                release_reason=
                StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
                release_reason_raw_text='RPRB',
                release_date=datetime.date(year=2019, month=1, day=8),
                state_code=_STATE_CODE,
                incarceration_sentences=[incarceration_sentence_114909_ips],
                person=incarceration_sentence_114909_ips.person)

        incarceration_sentence_113377_ips.incarceration_periods.append(
            incarceration_period_113377_1)
        incarceration_sentence_105640_ips.incarceration_periods.append(
            incarceration_period_105640_1)
        incarceration_sentence_105640_ips.incarceration_periods.append(
            incarceration_period_105640_2)
        incarceration_sentence_105640_ips.incarceration_periods.append(
            incarceration_period_105640_3)
        incarceration_sentence_114909_ips.incarceration_periods.append(
            incarceration_period_114909_1)

        # Act
        self._run_ingest_job_for_filename('elite_externalmovements.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ##############################################
        # ELITE OFFENSE AND IN CUSTODY POS REPORT DATA
        ##############################################
        # Arrange
        incarceration_sentence_105640_placeholder = \
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE, sentence_group=sentence_group_105640,
                person=sentence_group_105640.person)
        incarceration_period_105640_placeholder = \
            entities.StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                incarceration_sentences=[
                    incarceration_sentence_105640_placeholder],
                person=sentence_group_105640.person)
        incarceration_sentence_105640_placeholder.incarceration_periods.append(
            incarceration_period_105640_placeholder)
        sentence_group_105640.incarceration_sentences.append(
            incarceration_sentence_105640_placeholder)
        incident_353844 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id='353844',
            incident_date=datetime.date(year=2019, month=1, day=27),
            facility='NDSP', location_within_facility='TRAF',
            state_code=_STATE_CODE,
            incident_details=_INCIDENT_DETAILS_1.upper(),
            incarceration_period=incarceration_period_105640_1,
            person=incarceration_sentence_105640_1.person)
        outcome_353844_41 = \
            entities.StateIncarcerationIncidentOutcome.new_with_defaults(
                external_id='353844-41',
                outcome_type=
                StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
                outcome_type_raw_text='LCP', state_code=_STATE_CODE,
                outcome_description='LOSS OF COMMISSARY PRIVILEGES',
                date_effective=datetime.date(year=2019, month=3, day=19),
                punishment_length_days=7,
                incarceration_incident=incident_353844,
                person=incident_353844.person)
        incident_353844.incarceration_incident_outcomes.append(
            outcome_353844_41)
        incarceration_period_105640_1.incarceration_incidents.append(
            incident_353844)

        incident_354527 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id='354527',
            incident_date=datetime.date(year=2019, month=4, day=10),
            incident_type=StateIncarcerationIncidentType.MINOR_OFFENSE,
            incident_type_raw_text='MINOR', facility='JRCC',
            state_code=_STATE_CODE, location_within_facility='IDR',
            incident_details=_INCIDENT_DETAILS_2.upper(),
            incarceration_period=incarceration_period_105640_3,
            person=incarceration_period_105640_3.person)
        incarceration_period_105640_3.incarceration_incidents.append(
            incident_354527)

        incident_378515 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id='378515',
            incident_type=StateIncarcerationIncidentType.DISORDERLY_CONDUCT,
            incident_type_raw_text='INS',
            incident_date=datetime.date(year=2019, month=1, day=17),
            facility='NDSP', state_code=_STATE_CODE,
            location_within_facility='IDR',
            incident_details=_INCIDENT_DETAILS_3.upper().replace('  ', ' '),
            incarceration_period=incarceration_period_105640_1,
            person=incarceration_sentence_105640_1.person)
        outcome_378515_57 = \
            entities.StateIncarcerationIncidentOutcome.new_with_defaults(
                external_id='378515-57',
                outcome_type=
                StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
                outcome_type_raw_text='RTQ',
                date_effective=datetime.date(year=2019, month=1, day=27),
                state_code=_STATE_CODE,
                outcome_description='RESTRICTED TO QUARTERS',
                punishment_length_days=31,
                incarceration_incident=incident_378515,
                person=incident_378515.person)
        incident_378515.incarceration_incident_outcomes.append(
            outcome_378515_57)
        incarceration_period_105640_1.incarceration_incidents.append(
            incident_378515)

        # TODO(2131): Remove placeholders automatically
        incarceration_sentence_113377_placeholder = \
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                sentence_group=sentence_group_113377,
                person=sentence_group_113377.person)
        incarceration_period_113377_placeholder = \
            entities.StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                incarceration_sentences=[
                    incarceration_sentence_113377_placeholder],
                person=sentence_group_113377.person)
        incarceration_sentence_113377_placeholder.incarceration_periods.append(
            incarceration_period_113377_placeholder)
        sentence_group_113377.incarceration_sentences.append(
            incarceration_sentence_113377_placeholder)

        incident_363863 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id='363863',
            incident_type=StateIncarcerationIncidentType.MINOR_OFFENSE,
            incident_type_raw_text='MINOR',
            incident_date=datetime.date(year=2018, month=3, day=22),
            facility='NDSP', state_code=_STATE_CODE,
            location_within_facility='SU1',
            incident_details=_INCIDENT_DETAILS_4.upper(),
            incarceration_period=incarceration_period_113377_1,
            person=incarceration_period_113377_1.person)
        incarceration_period_113377_1.incarceration_incidents.append(
            incident_363863)

        # 110651 ---
        sentence_group_110651 = entities.StateSentenceGroup.new_with_defaults(
            external_id='110651',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE, person=person_2)
        incarceration_sentence_110651_placeholder = \
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE, sentence_group=sentence_group_110651,
                person=sentence_group_110651.person)
        incarceration_period_110651_placeholder = \
            entities.StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                incarceration_sentences=[
                    incarceration_sentence_110651_placeholder],
                person=sentence_group_110651.person)
        incident_366571 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id='366571',
            incident_type=StateIncarcerationIncidentType.VIOLENCE,
            incident_type_raw_text='IIASSAULT',
            incident_date=datetime.date(year=2019, month=1, day=27),
            facility='NDSP', state_code=_STATE_CODE,
            location_within_facility='TRAF',
            incident_details=_INCIDENT_DETAILS_5.upper(),
            incarceration_period=incarceration_period_110651_placeholder,
            person=incarceration_period_110651_placeholder.person)
        outcome_366571_29 = \
            entities.StateIncarcerationIncidentOutcome.new_with_defaults(
                external_id='366571-29',
                outcome_type=StateIncarcerationIncidentOutcomeType.NOT_GUILTY,
                outcome_type_raw_text='NOT_GUILTY',
                date_effective=datetime.date(year=2017, month=8, day=18),
                outcome_description='LOSS OF PRIVILEGES',
                state_code=_STATE_CODE, punishment_length_days=120,
                incarceration_incident=incident_366571,
                person=incident_366571.person)
        incident_366571.incarceration_incident_outcomes.append(
            outcome_366571_29)
        incarceration_period_110651_placeholder.incarceration_incidents.append(
            incident_366571)
        incarceration_sentence_110651_placeholder.incarceration_periods.append(
            incarceration_period_110651_placeholder)
        sentence_group_110651.incarceration_sentences.append(
            incarceration_sentence_110651_placeholder)
        person_2.sentence_groups.append(sentence_group_110651)

        person_5 = entities.StatePerson.new_with_defaults()
        person_5_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id='21109', id_type=US_ND_ELITE, state_code=_STATE_CODE,
            person=person_5)
        sentence_group_5129 = entities.StateSentenceGroup.new_with_defaults(
            external_id='5129', status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE, person=person_5)
        incarceration_sentence_5129_placeholder = \
            entities.StateIncarcerationSentence.new_with_defaults(
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE, sentence_group=sentence_group_5129,
                person=sentence_group_5129.person)
        incarceration_period_5129_placeholder = \
            entities.StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code=_STATE_CODE,
                incarceration_sentences=[
                    incarceration_sentence_5129_placeholder],
                person=sentence_group_5129.person)

        person_5.external_ids.append(person_5_external_id)
        person_5.sentence_groups.append(sentence_group_5129)
        sentence_group_5129.incarceration_sentences.append(
            incarceration_sentence_5129_placeholder)
        incarceration_sentence_5129_placeholder.incarceration_periods.append(
            incarceration_period_5129_placeholder)
        expected_people.append(person_5)

        incident_381647 = entities.StateIncarcerationIncident.new_with_defaults(
            external_id='381647',
            incident_date=datetime.date(year=2018, month=4, day=17),
            facility='MRCC',
            location_within_facility='HRT1',
            incident_details=_INCIDENT_DETAILS_6.upper(),
            state_code=_STATE_CODE,
            incarceration_period=incarceration_period_5129_placeholder,
            person=incarceration_sentence_5129_placeholder.person)
        outcome_381647_36 = \
            entities.StateIncarcerationIncidentOutcome.new_with_defaults(
                external_id='381647-36',
                outcome_type=
                StateIncarcerationIncidentOutcomeType.FINANCIAL_PENALTY,
                outcome_type_raw_text='PAY',
                date_effective=datetime.date(year=2018, month=4, day=20),
                outcome_description='LOSS OF PAY', state_code=_STATE_CODE,
                incarceration_incident=incident_381647,
                person=incident_381647.person)
        outcome_381647_37 = \
            entities.StateIncarcerationIncidentOutcome.new_with_defaults(
                external_id='381647-37',
                outcome_type=
                StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR,
                outcome_type_raw_text='EXD',
                date_effective=datetime.date(year=2018, month=4, day=20),
                outcome_description='EXTRA DUTY', state_code=_STATE_CODE,
                incarceration_incident=incident_381647,
                person=incident_381647.person)

        incident_381647.incarceration_incident_outcomes.append(
            outcome_381647_36)
        incident_381647.incarceration_incident_outcomes.append(
            outcome_381647_37)
        incarceration_period_5129_placeholder.incarceration_incidents.append(
            incident_381647)

        # Act
        self._run_ingest_job_for_filename(
            'elite_offense_in_custody_and_pos_report_data.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # DOCSTARS OFFENDERS
        ######################################
        # Arrange
        # TODO(2156): custom matching logic for assessments?
        person_1_assessment_1 = entities.StateAssessment.new_with_defaults(
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_level_raw_text='HIGH',
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text='RISK',
            assessment_type=StateAssessmentType.SORAC,
            assessment_type_raw_text='SORAC',
            state_code=_STATE_CODE,
            person=person_1)
        # TODO(2134): Currently overwriting race entity when the Enum value
        # is unchanged.
        person_1_race_2 = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.WHITE, race_raw_text='1',
            person=person_1)
        person_1.races = [person_1_race_2]
        person_1.gender_raw_text = '1'
        person_1.assessments = [person_1_assessment_1]
        person_1.current_address = '123 2ND ST N, FARGO, ND, 58102'
        person_1.residency_status = ResidencyStatus.PERMANENT

        person_2_assessment_1 = entities.StateAssessment.new_with_defaults(
            assessment_level=StateAssessmentLevel.MODERATE,
            assessment_level_raw_text='MODERATE',
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text='RISK',
            assessment_type=StateAssessmentType.SORAC,
            assessment_type_raw_text='SORAC',
            state_code=_STATE_CODE, person=person_2)
        person_2_race_2 = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.BLACK, race_raw_text='2',
            person=person_2)
        person_2.races = [person_2_race_2]
        person_2.assessments = [person_2_assessment_1]
        person_2.full_name = '{"given_names": "SOLANGE", ' \
                             '"middle_names": "P", ' \
                             '"surname": "KNOWLES"}'
        person_2.current_address = '000 1ST AVE APT 1, WEST FARGO, ND, 58078'
        person_2.gender_raw_text = '2'
        person_2.residency_status = ResidencyStatus.PERMANENT

        person_6 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "MIKE", "surname": "SANDISON"}',
            birthdate=datetime.date(year=1970, month=6, day=1),
            birthdate_inferred_from_age=False,
            current_address='111 3RD ST S #6, FARGO, ND, 58103',
            residency_status=ResidencyStatus.PERMANENT,
            gender=Gender.MALE,
            gender_raw_text='1')
        person_6_external_id = entities.StatePersonExternalId.new_with_defaults(
            external_id='92307',
            id_type=US_ND_SID,
            state_code=_STATE_CODE,
            person=person_6)
        person_6_race = entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE, race=Race.OTHER, race_raw_text='5',
            person=person_6)
        person_6_ethnicity = entities.StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE, ethnicity=Ethnicity.HISPANIC,
            ethnicity_raw_text='HISPANIC',
            person=person_6)
        person_6.external_ids = [person_6_external_id]
        person_6.races = [person_6_race]
        person_6.ethnicities = [person_6_ethnicity]
        expected_people.append(person_6)

        # Act
        self._run_ingest_job_for_filename('docstars_offenders.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # DOCSTARS CASES
        ######################################
        # Arrange
        sentence_group_placeholder_ss = \
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=person_1)
        supervision_sentence_117110 = \
            entities.StateSupervisionSentence.new_with_defaults(
                external_id='117110',
                supervision_type=StateSupervisionType.PAROLE,
                supervision_type_raw_text='PAROLE',
                projected_completion_date=datetime.date(year=2014, month=10,
                                                        day=6),
                max_length_days=92, state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=sentence_group_placeholder_ss.person,
                sentence_group=sentence_group_placeholder_ss)
        # TODO(2155): no external id for these StateSupervisionPeriods
        supervision_period_117110 = \
            entities.StateSupervisionPeriod.new_with_defaults(
                start_date=datetime.date(year=2014, month=7, day=17),
                termination_date=datetime.date(year=2014, month=10, day=6),
                termination_reason=
                StateSupervisionPeriodTerminationReason.EXPIRATION,
                termination_reason_raw_text='7',
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                state_code=_STATE_CODE,
                supervision_sentences=[supervision_sentence_117110],
                person=supervision_sentence_117110.person)
        charge_117110 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE, status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_117110],
            person=supervision_sentence_117110.person)
        agent_judge = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE, agent_type_raw_text='JUDGE',
            full_name='{"full_name": "THE JUDGE"}', state_code=_STATE_CODE)
        court_case_117110 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO, judge=agent_judge,
            charges=[charge_117110], person=charge_117110.person)
        charge_117110.court_case = court_case_117110
        supervision_sentence_117110.charges = [charge_117110]
        supervision_sentence_117110.supervision_periods.append(
            supervision_period_117110)
        sentence_group_placeholder_ss.supervision_sentences.append(
            supervision_sentence_117110)

        supervision_sentence_117111 = \
            entities.StateSupervisionSentence.new_with_defaults(
                external_id='117111',
                supervision_type=StateSupervisionType.PAROLE,
                supervision_type_raw_text='PAROLE',
                projected_completion_date=datetime.date(year=2015, month=8,
                                                        day=7),
                max_length_days=580, state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=sentence_group_placeholder_ss.person,
                sentence_group=sentence_group_placeholder_ss)
        charge_117111 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_117111],
            person=supervision_sentence_117111.person)
        agent_judge_dup = attr.evolve(agent_judge)
        court_case_117111 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=agent_judge_dup,
            charges=[charge_117111], person=charge_117111.person)
        charge_117111.court_case = court_case_117111
        supervision_sentence_117111.charges = [charge_117111]

        supervision_period_117111 = \
            entities.StateSupervisionPeriod.new_with_defaults(
                start_date=datetime.date(year=2014, month=7, day=17),
                termination_date=datetime.date(year=2014, month=12, day=8),
                termination_reason=
                StateSupervisionPeriodTerminationReason.REVOCATION,
                termination_reason_raw_text='9',
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                state_code=_STATE_CODE,
                supervision_sentences=[supervision_sentence_117111],
                person=supervision_sentence_117111.person)
        supervision_violation_117111 = \
            entities.StateSupervisionViolation.new_with_defaults(
                state_code=_STATE_CODE,
                supervision_period=supervision_period_117111,
                person=supervision_period_117111.person)
        supervision_violation_response_117111 = \
            entities.StateSupervisionViolationResponse.new_with_defaults(
                response_type=
                StateSupervisionViolationResponseType.PERMANENT_DECISION,
                response_type_raw_text='PERMANENT_DECISION',
                response_date=datetime.date(year=2014, month=12, day=8),
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
                decision_raw_text='REVOCATION',
                revocation_type=
                StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                revocation_type_raw_text='DOCR INMATE SENTENCE',
                state_code=_STATE_CODE,
                supervision_violation=supervision_violation_117111,
                person=supervision_violation_117111.person)
        supervision_violation_117111.supervision_violation_responses.append(
            supervision_violation_response_117111)
        supervision_period_117111.supervision_violations.append(
            supervision_violation_117111)
        supervision_sentence_117111.supervision_periods.append(
            supervision_period_117111)
        sentence_group_placeholder_ss.supervision_sentences.append(
            supervision_sentence_117111)
        person_1.sentence_groups.append(sentence_group_placeholder_ss)

        sentence_group_person_2_placeholder_ss = \
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=person_2)
        supervision_sentence_140408 = \
            entities.StateSupervisionSentence.new_with_defaults(
                external_id='140408',
                supervision_type=StateSupervisionType.PROBATION,
                supervision_type_raw_text='SUSPENDED',
                projected_completion_date=datetime.date(
                    year=2019, month=3, day=23),
                state_code=_STATE_CODE,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                person=sentence_group_person_2_placeholder_ss.person,
                sentence_group=sentence_group_person_2_placeholder_ss)
        supervision_period_140408 = \
            entities.StateSupervisionPeriod.new_with_defaults(
                start_date=datetime.date(year=2017, month=3, day=24),
                termination_date=datetime.date(year=2018, month=2, day=27),
                termination_reason=
                StateSupervisionPeriodTerminationReason.REVOCATION,
                termination_reason_raw_text='9',
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                state_code=_STATE_CODE,
                supervision_sentences=[supervision_sentence_140408],
                person=supervision_sentence_140408.person)
        supervision_violation_140408 = \
            entities.StateSupervisionViolation.new_with_defaults(
                violation_type=StateSupervisionViolationType.TECHNICAL,
                violation_type_raw_text='TECHNICAL',
                state_code=_STATE_CODE,
                supervision_period=supervision_period_140408,
                person=supervision_period_140408.person)
        supervision_violation_response_140408 = \
            entities.StateSupervisionViolationResponse.new_with_defaults(
                response_type=
                StateSupervisionViolationResponseType.PERMANENT_DECISION,
                response_type_raw_text='PERMANENT_DECISION',
                response_date=datetime.date(year=2018, month=2, day=27),
                decision=StateSupervisionViolationResponseDecision.REVOCATION,
                decision_raw_text='REVOCATION',
                revocation_type=
                StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                revocation_type_raw_text='DOCR INMATE SENTENCE',
                state_code=_STATE_CODE,
                supervision_violation=supervision_violation_140408,
                person=supervision_violation_140408.person)
        charge_140408 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE, status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_140408],
            person=supervision_sentence_140408.person)
        agent_person = entities.StateAgent.new_with_defaults(
            agent_type=StateAgentType.JUDGE, agent_type_raw_text='JUDGE',
            full_name='{"full_name": "JUDGE PERSON"}', state_code=_STATE_CODE)
        court_case_140408 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE,
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=agent_person,
            charges=[charge_140408], person=charge_140408.person)
        incarceration_period_114909_1.source_supervision_violation_response = \
            supervision_violation_response_140408
        charge_140408.court_case = court_case_140408
        supervision_sentence_140408.charges = [charge_140408]
        supervision_violation_140408.supervision_violation_responses.append(
            supervision_violation_response_140408)
        supervision_period_140408.supervision_violations.append(
            supervision_violation_140408)
        supervision_sentence_140408.supervision_periods.append(
            supervision_period_140408)
        sentence_group_person_2_placeholder_ss.supervision_sentences.append(
            supervision_sentence_140408)
        person_2.sentence_groups.append(sentence_group_person_2_placeholder_ss)

        # Act
        self._run_ingest_job_for_filename('docstars_offendercasestable.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # DOCSTARS OFFENSES
        ######################################
        # Arrange
        charge_122553 = entities.StateCharge.new_with_defaults(
            external_id='122553',
            county_code='008',
            statute='3522',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            counts=1,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_117111],
            person=supervision_sentence_117111.person)
        charge_122554 = entities.StateCharge.new_with_defaults(
            external_id='122554',
            county_code='008',
            statute='3550',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            counts=1,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_117111],
            person=supervision_sentence_117111.person)
        supervision_sentence_117111.charges.append(charge_122553)
        supervision_sentence_117111.charges.append(charge_122554)

        charge_122552 = entities.StateCharge.new_with_defaults(
            external_id='122552',
            county_code='008',
            statute='3562',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            counts=1,
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_117110],
            person=supervision_sentence_117110.person)
        supervision_sentence_117110.charges.append(charge_122552)

        charge_149349 = entities.StateCharge.new_with_defaults(
            external_id='149349',
            county_code='020',
            statute='2204',
            offense_date=datetime.date(year=2016, month=7, day=30),
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text='M',
            counts=1,
            classification_subtype='A',
            state_code=_STATE_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence_140408],
            person=supervision_sentence_140408.person)
        supervision_sentence_140408.charges.append(charge_149349)

        # Act
        self._run_ingest_job_for_filename('docstars_offensestable.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        ######################################
        # DOCSTARS LSI CHRONOLOGY
        ######################################
        # Arrange
        metadata_12345 = json.dumps({
            "DOMAIN_CRIMINAL_HISTORY": 7, "DOMAIN_EDUCATION_EMPLOYMENT": 7,
            "DOMAIN_FINANCIAL": 1, "DOMAIN_FAMILY_MARITAL": 2,
            "DOMAIN_ACCOMMODATION": 0, "DOMAIN_LEISURE_RECREATION": 2,
            "DOMAIN_COMPANIONS": 3, "DOMAIN_ALCOHOL_DRUG_PROBLEMS": 0,
            "DOMAIN_EMOTIONAL_PERSONAL": 2, "DOMAIN_ATTITUDES_ORIENTATION": 1,
            "QUESTION_18": 0, "QUESTION_19": 0, "QUESTION_20": 0,
            "QUESTION_21": 1, "QUESTION_23": 3, "QUESTION_24": 1,
            "QUESTION_25": 1, "QUESTION_27": 3, "QUESTION_31": 1,
            "QUESTION_39": 2, "QUESTION_40": 1, "QUESTION_51": 2,
            "QUESTION_52": 3
        })

        metadata_12346 = json.dumps({
            "DOMAIN_CRIMINAL_HISTORY": 6, "DOMAIN_EDUCATION_EMPLOYMENT": 8,
            "DOMAIN_FINANCIAL": 1, "DOMAIN_FAMILY_MARITAL": 2,
            "DOMAIN_ACCOMMODATION": 0, "DOMAIN_LEISURE_RECREATION": 2,
            "DOMAIN_COMPANIONS": 2, "DOMAIN_ALCOHOL_DRUG_PROBLEMS": 0,
            "DOMAIN_EMOTIONAL_PERSONAL": 0, "DOMAIN_ATTITUDES_ORIENTATION": 2,
            "QUESTION_18": 0, "QUESTION_19": 0, "QUESTION_20": 0,
            "QUESTION_21": 0, "QUESTION_23": 2, "QUESTION_24": 2,
            "QUESTION_25": 0, "QUESTION_27": 2, "QUESTION_31": 0,
            "QUESTION_39": 2, "QUESTION_40": 3, "QUESTION_51": 2,
            "QUESTION_52": 2
        })

        metadata_55555 = json.dumps({
            "DOMAIN_CRIMINAL_HISTORY": 6, "DOMAIN_EDUCATION_EMPLOYMENT": 10,
            "DOMAIN_FINANCIAL": 2, "DOMAIN_FAMILY_MARITAL": 3,
            "DOMAIN_ACCOMMODATION": 0, "DOMAIN_LEISURE_RECREATION": 2,
            "DOMAIN_COMPANIONS": 2, "DOMAIN_ALCOHOL_DRUG_PROBLEMS": 0,
            "DOMAIN_EMOTIONAL_PERSONAL": 0, "DOMAIN_ATTITUDES_ORIENTATION": 0,
            "QUESTION_18": 0, "QUESTION_19": 0, "QUESTION_20": 0,
            "QUESTION_21": 0, "QUESTION_23": 2, "QUESTION_24": 0,
            "QUESTION_25": 0, "QUESTION_27": 2, "QUESTION_31": 1,
            "QUESTION_39": 1, "QUESTION_40": 0, "QUESTION_51": 3,
            "QUESTION_52": 3
        })

        assessment_12345 = entities.StateAssessment.new_with_defaults(
            external_id='12345',
            state_code='US_ND',
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text='RISK',
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text='LSIR',
            assessment_date=datetime.date(year=2016, month=7, day=14),
            assessment_score=25,
            assessment_metadata=metadata_12345,
            person=person_1
        )
        assessment_12346 = entities.StateAssessment.new_with_defaults(
            external_id='12346',
            state_code='US_ND',
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text='RISK',
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text='LSIR',
            assessment_date=datetime.date(year=2017, month=1, day=13),
            assessment_score=23,
            assessment_metadata=metadata_12346,
            person=person_1
        )
        person_1.assessments.append(assessment_12345)
        person_1.assessments.append(assessment_12346)

        assessment_55555 = entities.StateAssessment.new_with_defaults(
            external_id='55555',
            state_code='US_ND',
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text='RISK',
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text='LSIR',
            assessment_date=datetime.date(year=2018, month=12, day=10),
            assessment_score=25,
            assessment_metadata=metadata_55555,
            person=person_2
        )
        person_2.assessments.append(assessment_55555)

        # Act
        self._run_ingest_job_for_filename('docstars_lsichronology.csv')

        # Assert
        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

        # pylint:disable=protected-access
        file_tags = self.controller._get_file_tag_rank_list()
        for file_tag in file_tags:
            self._run_ingest_job_for_filename(f'{file_tag}.csv')

        session = SessionFactory.for_schema_base(StateBase)
        found_people = dao.read_people(session)
        self.clear_db_ids(found_people)
        self.assertCountEqual(found_people, expected_people)

    def clear_db_ids(self, db_entities: List[Entity]):
        for entity in db_entities:
            entity.clear_id()
            for field_name in get_set_entity_field_names(
                    entity, EntityFieldType.FORWARD_EDGE):
                self.clear_db_ids(self.get_field_as_list(entity, field_name))

    def get_field_as_list(self, entity, field_name):
        field = getattr(entity, field_name)
        if not isinstance(field, list):
            return [field]
        return field

    @patch.dict('os.environ', {'PERSIST_LOCALLY': 'true'})
    def test_run_full_ingest_all_files(self):
        # pylint:disable=protected-access
        file_tags = sorted(self.controller._get_file_tag_rank_list())
        add_paths_with_tags_and_process(self, self.controller, file_tags)

        # TODO(2057): For now we just check that we don't crash, but we should
        #  test more comprehensively for state.

    @patch.dict('os.environ', {'PERSIST_LOCALLY': 'true'})
    def test_run_full_ingest_all_files_reverse(self):
        # pylint:disable=protected-access
        file_tags = list(
            reversed(sorted(self.controller._get_file_tag_rank_list())))
        add_paths_with_tags_and_process(self, self.controller, file_tags)

        # TODO(2057): For now we just check that we don't crash, but we should
        #  test more comprehensively for state.
