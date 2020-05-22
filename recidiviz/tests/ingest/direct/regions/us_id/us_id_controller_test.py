# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Unit and integration tests for Idaho direct ingest."""
import datetime
from typing import Type

from recidiviz import IngestInfo
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import Gender, Race, Ethnicity
from recidiviz.common.constants.state.external_id_types import US_ID_DOC
from recidiviz.common.constants.state.shared_enums import StateActingBodyType
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_court_case import StateCourtCaseStatus, StateCourtType
from recidiviz.common.constants.state.state_early_discharge import StateEarlyDischargeDecision
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateSpecializedPurposeForIncarceration, StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodStatus, \
    StateSupervisionPeriodAdmissionReason, StateSupervisionPeriodTerminationReason
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response import \
    StateSupervisionViolationResponseType, StateSupervisionViolationResponseDecision
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import GcsfsDirectIngestController
from recidiviz.ingest.direct.regions.us_id.us_id_controller import UsIdController
from recidiviz.ingest.models.ingest_info import StatePerson, StatePersonExternalId, StatePersonRace, StateAlias, \
    StatePersonEthnicity, StateAssessment, StateSentenceGroup, StateIncarcerationSentence, StateCharge, \
    StateCourtCase, StateAgent, StateSupervisionSentence, StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionViolation, StateSupervisionViolationTypeEntry, StateSupervisionViolationResponse, \
    StateSupervisionViolationResponseDecisionEntry, StateEarlyDischarge
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.base_state_direct_ingest_controller_tests import \
    BaseStateDirectIngestControllerTests
from recidiviz.tests.ingest.direct.regions.utils import populate_person_backedges

_STATE_CODE_UPPER = 'US_ID'


class TestUsIdController(BaseStateDirectIngestControllerTests):
    """Unit tests for each Idaho file to be ingested by the UsNdController."""

    @classmethod
    def region_code(cls) -> str:
        return _STATE_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        return UsIdController

    def test_populate_data_offender_ofndr_dob(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='1111',
                            surname='LAST_1',
                            given_names='FIRST_1',
                            middle_names='MIDDLE_1',
                            gender='M',
                            birthdate='01/01/75',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='1111', id_type=US_ID_DOC)
                            ],
                            state_person_races=[StatePersonRace(race='W')],
                            state_aliases=[
                                StateAlias(
                                    surname='LAST_1',
                                    given_names='FIRST_1',
                                    middle_names='MIDDLE_1',
                                    alias_type='GIVEN_NAME')
                                ]),
                StatePerson(state_person_id='2222',
                            surname='LAST_2',
                            given_names='FIRST_2',
                            middle_names='MIDDLE_2',
                            gender='F',
                            birthdate='01/01/85',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='2222', id_type=US_ID_DOC)
                            ],
                            state_person_ethnicities=[StatePersonEthnicity(ethnicity='H')],
                            state_aliases=[
                                StateAlias(
                                    surname='LAST_2',
                                    given_names='FIRST_2',
                                    middle_names='MIDDLE_2',
                                    alias_type='GIVEN_NAME')
                                ]),
                StatePerson(state_person_id='3333',
                            surname='LAST_3',
                            given_names='FIRST_3',
                            middle_names='MIDDLE_3',
                            gender='F',
                            birthdate='01/01/95',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='3333', id_type=US_ID_DOC)
                            ],
                            state_person_races=[StatePersonRace(race='O')],
                            state_aliases=[
                                StateAlias(
                                    surname='LAST_3',
                                    given_names='FIRST_3',
                                    middle_names='MIDDLE_3',
                                    alias_type='GIVEN_NAME')
                                ]),
            ])

        self.run_parse_file_test(expected, 'offender_ofndr_dob')

    def test_populate_data_ofndr_agnt_applc_usr_body_loc_cd_current_pos(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='1111',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='1111', id_type=US_ID_DOC)
                            ],
                            supervising_officer=StateAgent(
                                state_agent_id='po1',
                                full_name='NAME1',
                                agent_type='SUPERVISION_OFFICER',
                            )),
                StatePerson(state_person_id='2222',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='2222', id_type=US_ID_DOC)
                            ],
                            supervising_officer=StateAgent(
                                state_agent_id='po1',
                                full_name='NAME1',
                                agent_type='SUPERVISION_OFFICER',
                            )),
                StatePerson(state_person_id='3333',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='3333', id_type=US_ID_DOC)
                            ],
                            supervising_officer=StateAgent(
                                state_agent_id='po2',
                                full_name='NAME2',
                                agent_type='SUPERVISION_OFFICER',
                            )),
            ])

        self.run_parse_file_test(expected, 'ofndr_agnt_applc_usr_body_loc_cd_current_pos')

    def test_populate_data_ofndr_tst_ofndr_tst_cert(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='1111',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='1111', id_type=US_ID_DOC)
                            ],
                            state_assessments=[
                                StateAssessment(
                                    state_assessment_id='1',
                                    assessment_date='01/01/2002',
                                    assessment_type='LSIR',
                                    assessment_score='6.0',
                                    assessment_level='Minimum'),
                                StateAssessment(
                                    state_assessment_id='2',
                                    assessment_date='01/01/2003',
                                    assessment_type='LSIR',
                                    assessment_score='16.0',
                                    assessment_level='Low-Medium')
                            ]),
                StatePerson(state_person_id='2222',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='2222', id_type=US_ID_DOC)
                            ],
                            state_assessments=[
                                StateAssessment(
                                    state_assessment_id='3',
                                    assessment_date='01/01/2003',
                                    assessment_type='LSIR',
                                    assessment_score='31.0',
                                    assessment_level='High-Medium')
                            ]),
                StatePerson(state_person_id='3333',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='3333', id_type=US_ID_DOC)
                            ],
                            state_assessments=[
                                StateAssessment(
                                    state_assessment_id='4',
                                    assessment_date='01/01/2003',
                                    assessment_type='LSIR',
                                    assessment_score='45.0',
                                    assessment_level='Maximum')
                            ]),
            ])

        self.run_parse_file_test(expected, 'ofndr_tst_ofndr_tst_cert')

    def test_populate_data_mittimus_judge_sentence_offense_sentprob_incarceration_sentences(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id='1111',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='1111', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='1111-1',
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id='1111-1',
                                    status='D',
                                    date_imposed='1/01/08',
                                    start_date='1/01/08',
                                    parole_eligibility_date='01/01/09',
                                    projected_min_release_date='01/01/09',
                                    projected_max_release_date='01/01/10',
                                    completion_date='01/01/09',
                                    county_code='CNTY_1',
                                    min_length='398',
                                    max_length='731',
                                    is_life='False',
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id='1111-1',
                                            counts='1',
                                            statute='1-11',
                                            description='CRIME 1 + DESC',
                                            state_court_case=StateCourtCase(
                                                state_court_case_id='1111-123',
                                                judge=StateAgent(
                                                    state_agent_id='1',
                                                    agent_type='JUDGE',
                                                    full_name='JUDGE 1',
                                                )
                                            )
                                        )
                                    ]
                                )
                            ]
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id='1111-2',
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id='1111-3',
                                    status='I',
                                    date_imposed='1/01/19',
                                    start_date='1/01/19',
                                    county_code='CNTY_1',
                                    is_life='True',
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id='1111-3',
                                            counts='1',
                                            statute='2-22',
                                            description='CRIME 2 + DESC',
                                            state_court_case=StateCourtCase(
                                                state_court_case_id='1111-456',
                                                judge=StateAgent(
                                                    state_agent_id='1',
                                                    agent_type='JUDGE',
                                                    full_name='JUDGE 1',
                                                )
                                            )
                                        )
                                    ]
                                )
                            ]
                        ),
                    ]
                ),
                StatePerson(
                    state_person_id='2222',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='2222', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='2222-1',
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id='2222-2',
                                    status='I',
                                    date_imposed='1/01/10',
                                    start_date='1/01/10',
                                    county_code='CNTY_2',
                                    parole_eligibility_date='01/01/11',
                                    projected_min_release_date='01/01/11',
                                    projected_max_release_date='01/01/25',
                                    min_length='365',
                                    max_length='5479',
                                    is_life='False',
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id='2222-2',
                                            counts='1',
                                            statute='3-33',
                                            description='CRIME 3 + DESC',
                                            state_court_case=StateCourtCase(
                                                state_court_case_id='2222-789',
                                                judge=StateAgent(
                                                    state_agent_id='2',
                                                    agent_type='JUDGE',
                                                    full_name='JUDGE 2',
                                                )
                                            )
                                        )
                                    ]
                                )
                            ]
                        ),
                    ]
                )
            ]
        )

        self.run_parse_file_test(expected, 'mittimus_judge_sentence_offense_sentprob_incarceration_sentences')

    def test_populate_data_mittimus_judge_sentence_offense_sentprob_supervision_sentences(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id='1111',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='1111', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='1111-2',
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id='1111-2',
                                    status='K',
                                    date_imposed='1/01/18',
                                    start_date='01/01/18',
                                    supervision_type='PROBATION',
                                    completion_date='12/31/18',
                                    projected_completion_date='01/01/20',
                                    county_code='CNTY_1',
                                    min_length='365',
                                    max_length='730',
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id='1111-2',
                                            counts='2',
                                            statute='4-44',
                                            description='CRIME 4 + DESC',
                                            state_court_case=StateCourtCase(
                                                state_court_case_id='1111-234',
                                                judge=StateAgent(
                                                    state_agent_id='3',
                                                    agent_type='JUDGE',
                                                    full_name='JUDGE 3',
                                                )
                                            )
                                        )
                                    ]
                                )
                            ]
                        ),
                    ]
                ),
                StatePerson(
                    state_person_id='2222',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='2222', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='2222-1',
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id='2222-1',
                                    status='K',
                                    date_imposed='1/01/09',
                                    start_date='01/01/09',
                                    supervision_type='PROBATION',
                                    completion_date='12/31/09',
                                    projected_completion_date='01/01/13',
                                    county_code='CNTY_2',
                                    min_length='730',
                                    max_length='1461',
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id='2222-1',
                                            counts='2',
                                            statute='5-55',
                                            description='CRIME 5 + DESC',
                                            state_court_case=StateCourtCase(
                                                state_court_case_id='2222-567',
                                                judge=StateAgent(
                                                    state_agent_id='4',
                                                    agent_type='JUDGE',
                                                    full_name='JUDGE 4',
                                                )
                                            )
                                        )
                                    ]
                                )
                            ],
                        ),
                    ]
                ),
                StatePerson(
                    state_person_id='3333',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='3333', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='3333-1',
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id='3333-1',
                                    status='B',
                                    date_imposed='1/01/15',
                                    supervision_type='PROBATION',
                                    start_date='01/01/15',
                                    projected_completion_date='01/01/25',
                                    county_code='CNTY_3',
                                    min_length='365',
                                    max_length='3653',
                                    state_charges=[
                                        StateCharge(
                                            state_charge_id='3333-1',
                                            counts='2',
                                            statute='6-66',
                                            description='CRIME 6 + DESC',
                                            state_court_case=StateCourtCase(
                                                state_court_case_id='3333-890',
                                                judge=StateAgent(
                                                    state_agent_id='5',
                                                    agent_type='JUDGE',
                                                    full_name='JUDGE 5',
                                                )
                                            )
                                        )
                                    ]
                                )
                            ],
                        ),
                    ]
                )
            ]
        )

        self.run_parse_file_test(expected, 'mittimus_judge_sentence_offense_sentprob_supervision_sentences')

    def test_populate_data_early_discharge_incarceration_sentence(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id='1111',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='1111', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='1111-2',
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_sentence_id='1111-3',
                                    state_early_discharges=[
                                        StateEarlyDischarge(
                                            state_early_discharge_id='ed1-1',
                                            request_date='02/01/2020',
                                            requesting_body_type='SPECIAL PROGRESS REPORT OFFENDER INITIATED PAROLE '
                                                                 'DISCHARGE REQUEST',
                                            deciding_body_type='PAROLE',
                                            decision_date='02/05/2020',
                                            decision='Deny - Programming Needed',
                                        ),
                                        StateEarlyDischarge(
                                            state_early_discharge_id='ed2-2',
                                            request_date='03/01/2020',
                                            requesting_body_type='SPECIAL PROGRESS REPORT FOR PAROLE COMMUTATION',
                                            deciding_body_type='PAROLE',
                                        ),
                                    ],
                                )
                            ]
                        ),
                    ]
                ),
            ]
        )

        self.run_parse_file_test(expected, 'early_discharge_incarceration_sentence')

    def test_populate_data_early_discharge_supervision_sentence(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id='3333',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='3333', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='3333-1',
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_sentence_id='3333-1',
                                    state_early_discharges=[
                                        StateEarlyDischarge(
                                            state_early_discharge_id='ed3-3',
                                            request_date='01/01/2015',
                                            requesting_body_type='REQUEST FOR DISCHARGE: PROBATION',
                                            deciding_body_type='PROBATION',
                                            decision_date='06/01/2015',
                                            decision='Deny',
                                        ),
                                    ],
                                )
                            ]
                        ),
                    ]
                ),
            ]
        )

        self.run_parse_file_test(expected, 'early_discharge_supervision_sentence')

    def test_populate_data_movement_facility_location_offstat_incarceration_periods(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id='1111',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='1111', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='1111-1',
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id='1111-1',
                                            incarceration_type='STATE_PRISON',
                                            facility='FACILITY 1',
                                            admission_reason='NEW_ADMISSION',
                                            admission_date='2008-01-01',
                                            release_reason='I',
                                            release_date='2008-10-01',
                                        ),
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id='1111-2',
                                            incarceration_type='COUNTY_JAIL',
                                            facility='SHERIFF DEPT 1',
                                            admission_reason='I',
                                            admission_date='2008-10-01',
                                            release_reason='H',
                                            release_date='2009-01-01',
                                        ),
                                    ]
                                )
                            ]
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id='1111-2',
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id='1111-3',
                                            facility='FACILITY 1',
                                            incarceration_type='STATE_PRISON',
                                            admission_reason='P',
                                            admission_date='2019-01-01',
                                            release_reason='I',
                                            release_date='2019-02-01',
                                        ),
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id='1111-4',
                                            facility='FACILITY 1',
                                            incarceration_type='STATE_PRISON',
                                            admission_reason='I',
                                            admission_date='2019-02-01',
                                            release_reason='P',
                                            release_date='2020-01-01',
                                            specialized_purpose_for_incarceration='TREATMENT_IN_PRISON',
                                        ),
                                    ]
                                )
                            ]
                        ),
                    ]
                ),
                StatePerson(
                    state_person_id='2222',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='2222', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='2222-1',
                            state_incarceration_sentences=[
                                StateIncarcerationSentence(
                                    state_incarceration_periods=[
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id='2222-1',
                                            facility='SHERIFF DEPT 2',
                                            incarceration_type='COUNTY_JAIL',
                                            admission_reason='TEMPORARY_CUSTODY',
                                            admission_date='2010-01-01',
                                            release_reason='RELEASED_FROM_TEMPORARY_CUSTODY',
                                            release_date='2010-06-01',
                                        ),
                                        StateIncarcerationPeriod(
                                            state_incarceration_period_id='2222-2',
                                            facility='FACILITY 3',
                                            incarceration_type='STATE_PRISON',
                                            admission_reason='PROBATION_REVOCATION',
                                            admission_date='2010-06-01',
                                            release_reason='F',
                                            release_date='2011-01-01',
                                        ),
                                    ]
                                )
                            ]
                        ),
                    ]
                ),
            ]
        )

        self.run_parse_file_test(expected, 'movement_facility_location_offstat_incarceration_periods')

    def test_populate_data_movement_facility_location_offstat_supervision_periods(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id='1111',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='1111', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='1111-1',
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_period_id='1111-1',
                                            supervision_site='DISTRICT 1',
                                            admission_reason='INVESTIGATION',
                                            start_date='2007-10-01',
                                            termination_reason='INVESTIGATION',
                                            termination_date='2008-01-01',
                                        ),
                                    ],
                                )
                            ]
                        ),
                        StateSentenceGroup(
                            state_sentence_group_id='1111-2',
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_period_id='1111-2',
                                            supervision_site='DISTRICT 1',
                                            admission_reason='INVESTIGATION',
                                            start_date='2017-10-01',
                                            termination_reason='INVESTIGATION',
                                            termination_date='2018-01-01',
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id='1111-3',
                                            supervision_site='DISTRICT 1',
                                            admission_reason='COURT_SENTENCE',
                                            start_date='2018-01-01',
                                            termination_reason='I',
                                            termination_date='2018-12-31',
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id='1111-4',
                                            supervision_site='DISTRICT 1',
                                            admission_reason='I',
                                            start_date='2020-01-01',
                                        ),
                                    ]
                                )
                            ],
                        ),
                    ]
                ),
                StatePerson(
                    state_person_id='2222',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='2222', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='2222-1',
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_period_id='2222-1',
                                            supervision_site='DISTRICT 2',
                                            admission_reason='COURT_SENTENCE',
                                            start_date='2009-01-01',
                                            termination_reason='F',
                                            termination_date='2009-07-01',
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id='2222-2',
                                            admission_reason='ABSCONSION',
                                            start_date='2009-07-01',
                                            termination_reason='RETURN_FROM_ABSCONSION',
                                            termination_date='2009-12-01',
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id='2222-3',
                                            supervision_site='DISTRICT 2',
                                            admission_reason='F',
                                            start_date='2009-12-01',
                                            termination_reason='I',
                                            termination_date='2009-12-31',
                                        ),
                                    ]
                                )
                            ],
                        ),
                    ]
                ),
                StatePerson(
                    state_person_id='3333',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='3333', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_sentence_group_id='3333-1',
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_period_id='3333-1',
                                            supervision_site='DISTRICT 4',
                                            admission_reason='COURT_SENTENCE',
                                            start_date='2015-01-01',
                                            termination_reason='TRANSFER_OUT_OF_STATE',
                                            termination_date='2018-01-01',
                                        ),
                                        StateSupervisionPeriod(
                                            state_supervision_period_id='3333-2',
                                            admission_reason='TRANSFER_OUT_OF_STATE',
                                            start_date='2018-01-01',
                                        ),
                                    ]
                                )
                            ],
                        ),
                    ]
                ),
            ]
        )

        self.run_parse_file_test(expected, 'movement_facility_location_offstat_supervision_periods')

    # TODO(2999): Associate VRs by date to SPs
    def test_populate_data_ofndr_tst_tst_qstn_rspns_violation_reports(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id='1111',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='1111', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                StateSupervisionViolation(
                                                    is_violent='True',
                                                    is_sex_offense='True',
                                                    state_supervision_violation_id='5',
                                                    state_supervision_violation_types=[
                                                        StateSupervisionViolationTypeEntry(
                                                            violation_type='New Misdemeanor',
                                                        ),
                                                        StateSupervisionViolationTypeEntry(
                                                            violation_type='Technical (enter details below)',
                                                        ),
                                                    ],
                                                    state_supervision_violation_responses=[
                                                        StateSupervisionViolationResponse(
                                                            state_supervision_violation_response_id='5',
                                                            response_type='VIOLATION_REPORT',
                                                            response_date='12/01/2018',
                                                            supervision_violation_response_decisions=[
                                                                StateSupervisionViolationResponseDecisionEntry(
                                                                    decision='Imposition of Sentence',
                                                                )
                                                            ]
                                                        )
                                                    ]
                                                )
                                            ]
                                        ),
                                    ]
                                )
                            ],
                        ),
                    ]
                ),
                StatePerson(
                    state_person_id='2222',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='2222', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                StateSupervisionViolation(
                                                    is_violent='False',
                                                    is_sex_offense='False',
                                                    state_supervision_violation_id='6',
                                                    state_supervision_violation_types=[
                                                        StateSupervisionViolationTypeEntry(
                                                            violation_type='Absconding',
                                                        ),
                                                        StateSupervisionViolationTypeEntry(
                                                            violation_type='Technical (enter details below)',
                                                        ),
                                                    ],
                                                    state_supervision_violation_responses=[
                                                        StateSupervisionViolationResponse(
                                                            state_supervision_violation_response_id='6',
                                                            response_type='VIOLATION_REPORT',
                                                            response_date='06/15/2009',
                                                        )
                                                    ]
                                                )
                                            ]
                                        ),
                                    ]
                                )
                            ],
                        ),
                    ]
                ),
                StatePerson(
                    state_person_id='3333',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='3333', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                StateSupervisionViolation(
                                                    is_violent='False',
                                                    is_sex_offense='False',
                                                    state_supervision_violation_id='7',
                                                    state_supervision_violation_types=[
                                                        StateSupervisionViolationTypeEntry(
                                                            violation_type='Technical (enter details below)',
                                                        ),
                                                    ],
                                                    state_supervision_violation_responses=[
                                                        StateSupervisionViolationResponse(
                                                            state_supervision_violation_response_id='7',
                                                            response_type='VIOLATION_REPORT',
                                                            response_date='01/01/2016',
                                                            supervision_violation_response_decisions=[
                                                                StateSupervisionViolationResponseDecisionEntry(
                                                                    decision='Reinstatement',
                                                                )
                                                            ]
                                                        )
                                                    ]
                                                )
                                            ]
                                        ),
                                    ]
                                )
                            ],
                        ),
                    ]
                ),
            ]
        )

        self.run_parse_file_test(expected, 'ofndr_tst_tst_qstn_rspns_violation_reports')

    def test_populate_data_ofndr_tst_tst_qstn_rspns_violation_reports_old(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(
                    state_person_id='2222',
                    state_person_external_ids=[
                        StatePersonExternalId(state_person_external_id_id='2222', id_type=US_ID_DOC)
                    ],
                    state_sentence_groups=[
                        StateSentenceGroup(
                            state_supervision_sentences=[
                                StateSupervisionSentence(
                                    state_supervision_periods=[
                                        StateSupervisionPeriod(
                                            state_supervision_violation_entries=[
                                                StateSupervisionViolation(
                                                    is_violent='False',
                                                    is_sex_offense='False',
                                                    state_supervision_violation_id='8',
                                                    state_supervision_violation_types=[
                                                        StateSupervisionViolationTypeEntry(
                                                            violation_type='New Misdemeanor',
                                                        ),
                                                        StateSupervisionViolationTypeEntry(
                                                            violation_type='Technical',
                                                        ),
                                                    ],
                                                    state_supervision_violation_responses=[
                                                        StateSupervisionViolationResponse(
                                                            state_supervision_violation_response_id='8',
                                                            response_type='VIOLATION_REPORT',
                                                            response_date='02/01/2009',
                                                            supervision_violation_response_decisions=[
                                                                StateSupervisionViolationResponseDecisionEntry(
                                                                    decision='Referral to Problem Solving Court',
                                                                )
                                                            ]
                                                        )
                                                    ]
                                                )
                                            ]
                                        ),
                                    ]
                                )
                            ],
                        ),
                    ]
                ),
            ]
        )

        self.run_parse_file_test(expected, 'ofndr_tst_tst_qstn_rspns_violation_reports_old')

    def test_run_full_ingest_all_files_specific_order(self) -> None:
        self.maxDiff = None

        ######################################
        # OFFENDER_OFNDR_DOB
        ######################################
        # Arrange
        person_1 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "FIRST_1", "middle_names": "MIDDLE_1", "surname": "LAST_1"}',
            gender=Gender.MALE,
            gender_raw_text='M',
            birthdate=datetime.date(year=1975, month=1, day=1),
            birthdate_inferred_from_age=False,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='1111', id_type=US_ID_DOC),
                ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    full_name='{"given_names": "FIRST_1", "middle_names": "MIDDLE_1", "surname": "LAST_1"}',
                    state_code=_STATE_CODE_UPPER,
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME')
                ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, race=Race.WHITE, race_raw_text='W'),
                ],
        )
        person_2 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "FIRST_2", "middle_names": "MIDDLE_2", "surname": "LAST_2"}',
            gender=Gender.FEMALE,
            gender_raw_text='F',
            birthdate=datetime.date(year=1985, month=1, day=1),
            birthdate_inferred_from_age=False,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='2222', id_type=US_ID_DOC),
                ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    full_name='{"given_names": "FIRST_2", "middle_names": "MIDDLE_2", "surname": "LAST_2"}',
                    state_code=_STATE_CODE_UPPER,
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME')
                ],
            ethnicities=[
                entities.StatePersonEthnicity.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, ethnicity=Ethnicity.HISPANIC, ethnicity_raw_text='H'),
                ],
        )
        person_3 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "FIRST_3", "middle_names": "MIDDLE_3", "surname": "LAST_3"}',
            gender=Gender.FEMALE,
            gender_raw_text='F',
            birthdate=datetime.date(year=1995, month=1, day=1),
            birthdate_inferred_from_age=False,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='3333', id_type=US_ID_DOC),
                ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    full_name='{"given_names": "FIRST_3", "middle_names": "MIDDLE_3", "surname": "LAST_3"}',
                    state_code=_STATE_CODE_UPPER,
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME')
                ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, race=Race.OTHER, race_raw_text='O'),
                ],
        )
        expected_people = [person_1, person_2, person_3]
        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename('offender_ofndr_dob.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # US_ID_OFNDR_AGNT_APPLC_USR_BODY_LOC_CD_CURRENT_POS
        ######################################
        # Arrange
        po_1 = entities.StateAgent.new_with_defaults(
            external_id='PO1',
            state_code=_STATE_CODE_UPPER,
            full_name='{"full_name": "NAME1"}',
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text='SUPERVISION_OFFICER',
        )
        po_2 = entities.StateAgent.new_with_defaults(
            external_id='PO2',
            state_code=_STATE_CODE_UPPER,
            full_name='{"full_name": "NAME2"}',
            agent_type=StateAgentType.SUPERVISION_OFFICER,
            agent_type_raw_text='SUPERVISION_OFFICER',
        )
        person_1.supervising_officer = po_1
        person_2.supervising_officer = po_1
        person_3.supervising_officer = po_2

        # Act
        self._run_ingest_job_for_filename('ofndr_agnt_applc_usr_body_loc_cd_current_pos.csv')

        # Assert
        self.assert_expected_db_people(expected_people)


        ######################################
        # OFNDR_TST_OFNDR_TST_CERT
        ######################################
        # Arrange
        assessment_1 = entities.StateAssessment.new_with_defaults(
            external_id='1',
            assessment_date=datetime.date(year=2002, month=1, day=1),
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text='LSIR',
            assessment_score=6,
            assessment_level=StateAssessmentLevel.LOW,
            assessment_level_raw_text='MINIMUM',
            state_code=_STATE_CODE_UPPER,
            person=person_1)
        assessment_2 = entities.StateAssessment.new_with_defaults(
            external_id='2',
            assessment_date=datetime.date(year=2003, month=1, day=1),
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text='LSIR',
            assessment_score=16,
            assessment_level=StateAssessmentLevel.LOW_MEDIUM,
            assessment_level_raw_text='LOW-MEDIUM',
            state_code=_STATE_CODE_UPPER,
            person=person_1)
        assessment_3 = entities.StateAssessment.new_with_defaults(
            external_id='3',
            assessment_date=datetime.date(year=2003, month=1, day=1),
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text='LSIR',
            assessment_score=31,
            assessment_level=StateAssessmentLevel.MEDIUM_HIGH,
            assessment_level_raw_text='HIGH-MEDIUM',
            state_code=_STATE_CODE_UPPER,
            person=person_2)
        assessment_4 = entities.StateAssessment.new_with_defaults(
            external_id='4',
            assessment_date=datetime.date(year=2003, month=1, day=1),
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text='LSIR',
            assessment_score=45,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_level_raw_text='MAXIMUM',
            state_code=_STATE_CODE_UPPER,
            person=person_3)

        person_1.assessments.append(assessment_1)
        person_1.assessments.append(assessment_2)
        person_2.assessments.append(assessment_3)
        person_3.assessments.append(assessment_4)

        # Act
        self._run_ingest_job_for_filename('ofndr_tst_ofndr_tst_cert.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        ###################################################################
        # MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_INCARCERATION_SENTENCES
        ###################################################################
        # Arrange
        judge_1 = entities.StateAgent.new_with_defaults(
            external_id='1',
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text='JUDGE',
            state_code=_STATE_CODE_UPPER,
            full_name='{"full_name": "JUDGE 1"}')
        judge_2 = entities.StateAgent.new_with_defaults(
            external_id='2',
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text='JUDGE',
            state_code=_STATE_CODE_UPPER,
            full_name='{"full_name": "JUDGE 2"}')
        sg_1111_1 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            external_id='1111-1',
            person=person_1)
        is_1111_1 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-1',
            status=StateSentenceStatus.COMPLETED,
            status_raw_text='D',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            date_imposed=datetime.date(year=2008, month=1, day=1),
            start_date=datetime.date(year=2008, month=1, day=1),
            parole_eligibility_date=datetime.date(year=2009, month=1, day=1),
            projected_min_release_date=datetime.date(year=2009, month=1, day=1),
            projected_max_release_date=datetime.date(year=2010, month=1, day=1),
            completion_date=datetime.date(year=2009, month=1, day=1),
            county_code='CNTY_1',
            min_length_days=398,
            max_length_days=731,
            sentence_group=sg_1111_1,
            is_life=False,
            person=sg_1111_1.person)
        c_1111_1 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-1',
            counts=1,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            statute='1-11',
            description='CRIME 1 + DESC',
            incarceration_sentences=[is_1111_1],
            person=is_1111_1.person)
        cc_1111_123 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-123',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=judge_1,
            charges=[c_1111_1],
            person=c_1111_1.person)
        person_1.sentence_groups.append(sg_1111_1)
        sg_1111_1.incarceration_sentences.append(is_1111_1)
        is_1111_1.charges.append(c_1111_1)
        c_1111_1.court_case = cc_1111_123

        sg_1111_2 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            external_id='1111-2',
            person=person_1)
        is_1111_3 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-3',
            status=StateSentenceStatus.SERVING,
            status_raw_text='I',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            date_imposed=datetime.date(year=2019, month=1, day=1),
            start_date=datetime.date(year=2019, month=1, day=1),
            county_code='CNTY_1',
            sentence_group=sg_1111_2,
            is_life=True,
            person=sg_1111_2.person)
        c_1111_3 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-3',
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            counts=1,
            statute='2-22',
            description='CRIME 2 + DESC',
            incarceration_sentences=[is_1111_3],
            person=is_1111_3.person)
        cc_1111_456 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-456',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=judge_1,
            charges=[c_1111_3],
            person=c_1111_3.person)
        person_1.sentence_groups.append(sg_1111_2)
        sg_1111_2.incarceration_sentences.append(is_1111_3)
        is_1111_3.charges.append(c_1111_3)
        c_1111_3.court_case = cc_1111_456

        sg_2222_1 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            external_id='2222-1',
            person=person_2)
        is_2222_2 = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-2',
            status=StateSentenceStatus.SERVING,
            status_raw_text='I',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            date_imposed=datetime.date(year=2010, month=1, day=1),
            start_date=datetime.date(year=2010, month=1, day=1),
            projected_min_release_date=datetime.date(year=2011, month=1, day=1),
            projected_max_release_date=datetime.date(year=2025, month=1, day=1),
            parole_eligibility_date=datetime.date(year=2011, month=1, day=1),
            county_code='CNTY_2',
            min_length_days=365,
            max_length_days=5479,
            sentence_group=sg_2222_1,
            is_life=False,
            person=sg_2222_1.person)
        c_2222_2 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-2',
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            counts=1,
            statute='3-33',
            description='CRIME 3 + DESC',
            incarceration_sentences=[is_2222_2],
            person=is_2222_2.person)
        cc_2222_789 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-789',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=judge_2,
            charges=[c_2222_2],
            person=c_2222_2.person)
        person_2.sentence_groups.append(sg_2222_1)
        sg_2222_1.incarceration_sentences.append(is_2222_2)
        is_2222_2.charges.append(c_2222_2)
        c_2222_2.court_case = cc_2222_789

        # Act
        self._run_ingest_job_for_filename('mittimus_judge_sentence_offense_sentprob_incarceration_sentences.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        #################################################################
        # MITTIMUS_JUDGE_SENTENCE_OFFENSE_SENTPROB_SUPERVISION_SENTENCES
        #################################################################
        # Arrange
        judge_3 = entities.StateAgent.new_with_defaults(
            external_id='3',
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text='JUDGE',
            state_code=_STATE_CODE_UPPER,
            full_name='{"full_name": "JUDGE 3"}')
        judge_4 = entities.StateAgent.new_with_defaults(
            external_id='4',
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text='JUDGE',
            state_code=_STATE_CODE_UPPER,
            full_name='{"full_name": "JUDGE 4"}')
        judge_5 = entities.StateAgent.new_with_defaults(
            external_id='5',
            agent_type=StateAgentType.JUDGE,
            agent_type_raw_text='JUDGE',
            state_code=_STATE_CODE_UPPER,
            full_name='{"full_name": "JUDGE 5"}')

        ss_1111_2 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-2',
            status=StateSentenceStatus.REVOKED,
            status_raw_text='K',
            supervision_type=StateSupervisionType.PROBATION,
            supervision_type_raw_text='PROBATION',
            date_imposed=datetime.date(year=2018, month=1, day=1),
            start_date=datetime.date(year=2018, month=1, day=1),
            projected_completion_date=datetime.date(year=2020, month=1, day=1),
            completion_date=datetime.date(year=2018, month=12, day=31),
            county_code='CNTY_1',
            min_length_days=365,
            max_length_days=730,
            sentence_group=sg_1111_2,
            person=sg_1111_2.person)
        c_1111_2 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-2',
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            counts=2,
            statute='4-44',
            description='CRIME 4 + DESC',
            supervision_sentences=[ss_1111_2],
            person=ss_1111_2.person)
        cc_1111_234 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-234',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=judge_3,
            charges=[c_1111_2],
            person=c_1111_2.person)
        sg_1111_2.supervision_sentences.append(ss_1111_2)
        ss_1111_2.charges.append(c_1111_2)
        c_1111_2.court_case = cc_1111_234

        ss_2222_1 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-1',
            status=StateSentenceStatus.REVOKED,
            status_raw_text='K',
            supervision_type=StateSupervisionType.PROBATION,
            supervision_type_raw_text='PROBATION',
            date_imposed=datetime.date(year=2009, month=1, day=1),
            start_date=datetime.date(year=2009, month=1, day=1),
            projected_completion_date=datetime.date(year=2013, month=1, day=1),
            completion_date=datetime.date(year=2009, month=12, day=31),
            county_code='CNTY_2',
            min_length_days=730,
            max_length_days=1461,
            sentence_group=sg_2222_1,
            person=sg_2222_1.person)
        c_2222_1 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            counts=2,
            statute='5-55',
            description='CRIME 5 + DESC',
            supervision_sentences=[ss_2222_1],
            person=ss_2222_1.person)
        cc_2222_567 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-567',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=judge_4,
            charges=[c_2222_1],
            person=c_2222_1.person)
        sg_2222_1.supervision_sentences.append(ss_2222_1)
        ss_2222_1.charges.append(c_2222_1)
        c_2222_1.court_case = cc_2222_567

        sg_3333_1 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            external_id='3333-1',
            person=person_3)
        ss_3333_1 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='3333-1',
            status=StateSentenceStatus.SUSPENDED,
            status_raw_text='B',
            supervision_type=StateSupervisionType.PROBATION,
            supervision_type_raw_text='PROBATION',
            date_imposed=datetime.date(year=2015, month=1, day=1),
            start_date=datetime.date(year=2015, month=1, day=1),
            projected_completion_date=datetime.date(year=2025, month=1, day=1),
            county_code='CNTY_3',
            min_length_days=365,
            max_length_days=3653,
            sentence_group=sg_3333_1,
            person=sg_3333_1.person)
        c_3333_1 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='3333-1',
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            counts=2,
            statute='6-66',
            description='CRIME 6 + DESC',
            supervision_sentences=[ss_3333_1],
            person=ss_3333_1.person)
        cc_3333_890 = entities.StateCourtCase.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='3333-890',
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            judge=judge_5,
            charges=[c_3333_1],
            person=c_3333_1.person)
        person_3.sentence_groups.append(sg_3333_1)
        sg_3333_1.supervision_sentences.append(ss_3333_1)
        ss_3333_1.charges.append(c_3333_1)
        c_3333_1.court_case = cc_3333_890

        # Act
        self._run_ingest_job_for_filename('mittimus_judge_sentence_offense_sentprob_supervision_sentences.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        #################################################################
        # EARLY_DISCHARGE_INCARCERATION_SENTENCE
        #################################################################
        # Arrange

        ed_1 = entities.StateEarlyDischarge.new_with_defaults(
            external_id='ED1-1',
            state_code=_STATE_CODE_UPPER,
            request_date=datetime.date(year=2020, month=2, day=1),
            requesting_body_type=StateActingBodyType.SENTENCED_PERSON,
            requesting_body_type_raw_text='SPECIAL PROGRESS REPORT OFFENDER INITIATED PAROLE DISCHARGE REQUEST',
            deciding_body_type=StateActingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text='PAROLE',
            decision_date=datetime.date(year=2020, month=2, day=5),
            decision=StateEarlyDischargeDecision.REQUEST_DENIED,
            decision_raw_text='DENY - PROGRAMMING NEEDED',
            incarceration_sentence=is_1111_3,
            person=is_1111_3.person,
        )
        ed_2 = entities.StateEarlyDischarge.new_with_defaults(
            external_id='ED2-2',
            state_code=_STATE_CODE_UPPER,
            request_date=datetime.date(year=2020, month=3, day=1),
            requesting_body_type=StateActingBodyType.SUPERVISION_OFFICER,
            requesting_body_type_raw_text='SPECIAL PROGRESS REPORT FOR PAROLE COMMUTATION',
            deciding_body_type=StateActingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text='PAROLE',
            incarceration_sentence=is_1111_3,
            person=is_1111_3.person,
        )
        is_1111_3.early_discharges.extend([ed_1, ed_2])

        # Act
        self._run_ingest_job_for_filename('early_discharge_incarceration_sentence.csv')
        # Assert
        self.assert_expected_db_people(expected_people)

        #################################################################
        # EARLY_DISCHARGE_SUPERVISION_SENTENCE
        #################################################################
        # Arrange
        ed_3 = entities.StateEarlyDischarge.new_with_defaults(
            external_id='ED3-3',
            state_code=_STATE_CODE_UPPER,
            request_date=datetime.date(year=2015, month=1, day=1),
            requesting_body_type=StateActingBodyType.SUPERVISION_OFFICER,
            requesting_body_type_raw_text='REQUEST FOR DISCHARGE: PROBATION',
            deciding_body_type=StateActingBodyType.COURT,
            deciding_body_type_raw_text='PROBATION',
            decision_date=datetime.date(year=2015, month=6, day=1),
            decision=StateEarlyDischargeDecision.REQUEST_DENIED,
            decision_raw_text='DENY',
            supervision_sentence=ss_3333_1,
            person=ss_3333_1.person,
        )
        ss_3333_1.early_discharges.append(ed_3)

        # Act
        self._run_ingest_job_for_filename('early_discharge_supervision_sentence.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        #################################################################
        # MOVEMENT_FACILITY_OFFSTAT_INCARCERATION_PERIODS
        #################################################################
        # TODO(2492): Remove dangling placeholders from expected graph once functionality is in entity matching.
        # Arrange
        is_1111_1_placeholder = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            sentence_group=sg_1111_1,
            person=sg_1111_1.person)
        ip_1111_1 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-1',
            facility='FACILITY 1',
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text='NEW_ADMISSION',
            admission_date=datetime.date(year=2008, month=1, day=1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text='I',
            release_date=datetime.date(year=2008, month=10, day=1),
            incarceration_sentences=[is_1111_1],
            person=is_1111_1.person,
        )
        ip_1111_2 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-2',
            facility='SHERIFF DEPT 1',
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text='COUNTY_JAIL',
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text='I',
            admission_date=datetime.date(year=2008, month=10, day=1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text='H',
            release_date=datetime.date(year=2009, month=1, day=1),
            incarceration_sentences=[is_1111_1],
            person=is_1111_1.person,
        )
        sg_1111_1.incarceration_sentences.append(is_1111_1_placeholder)
        is_1111_1.incarceration_periods.extend([ip_1111_1, ip_1111_2])

        is_1111_2_placeholder = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            sentence_group=sg_1111_2,
            person=sg_1111_2.person)
        ip_1111_3 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-3',
            facility='FACILITY 1',
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text='P',
            admission_date=datetime.date(year=2019, month=1, day=1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text='I',
            release_date=datetime.date(year=2019, month=2, day=1),
            incarceration_sentences=[is_1111_3],
            person=is_1111_3.person,
        )
        ip_1111_4 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-4',
            facility='FACILITY 1',
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text='I',
            admission_date=datetime.date(year=2019, month=2, day=1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text='P',
            release_date=datetime.date(year=2020, month=1, day=1),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            specialized_purpose_for_incarceration_raw_text='TREATMENT_IN_PRISON',
            incarceration_sentences=[is_1111_3],
            person=is_1111_3.person,
        )
        sg_1111_2.incarceration_sentences.append(is_1111_2_placeholder)
        is_1111_3.incarceration_periods.extend([ip_1111_3, ip_1111_4])

        is_2222_1_placeholder = entities.StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            sentence_group=sg_2222_1,
            person=sg_2222_1.person)
        ip_2222_1 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-1',
            facility='SHERIFF DEPT 2',
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text='COUNTY_JAIL',
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text='TEMPORARY_CUSTODY',
            admission_date=datetime.date(year=2010, month=1, day=1),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text='RELEASED_FROM_TEMPORARY_CUSTODY',
            release_date=datetime.date(year=2010, month=6, day=1),
            incarceration_sentences=[is_2222_2],
            person=is_2222_2.person,
        )
        ip_2222_2 = entities.StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-2',
            facility='FACILITY 3',
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text='STATE_PRISON',
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text='PROBATION_REVOCATION',
            admission_date=datetime.date(year=2010, month=6, day=1),
            release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
            release_reason_raw_text='F',
            release_date=datetime.date(year=2011, month=1, day=1),
            incarceration_sentences=[is_2222_2],
            person=is_2222_2.person,
        )
        is_2222_2.incarceration_periods.extend([ip_2222_1, ip_2222_2])
        sg_2222_1.incarceration_sentences.append(is_2222_1_placeholder)

        # Act
        self._run_ingest_job_for_filename('movement_facility_location_offstat_incarceration_periods.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        #################################################################
        # MOVEMENT_FACILITY_OFFSTAT_SUPERVISION_PERIODS
        #################################################################
        # TODO(2492): Remove dangling placeholders from expected graph once functionality is in entity matching.
        # Arrange
        ss_1111_1_placeholder = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=sg_1111_1,
            person=sg_1111_1.person)
        sp_1111_1 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-1',
            supervision_site='DISTRICT 1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            admission_reason=StateSupervisionPeriodAdmissionReason.INVESTIGATION,
            admission_reason_raw_text='INVESTIGATION',
            start_date=datetime.date(year=2007, month=10, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.INVESTIGATION,
            termination_reason_raw_text='INVESTIGATION',
            termination_date=datetime.date(year=2008, month=1, day=1),
            supervision_sentences=[ss_1111_1_placeholder],
            person=ss_1111_1_placeholder.person,
        )
        sg_1111_1.supervision_sentences.append(ss_1111_1_placeholder)
        ss_1111_1_placeholder.supervision_periods.append(sp_1111_1)

        ss_1111_2_placeholder = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=sg_1111_2,
            person=sg_1111_2.person)
        sp_1111_2 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-2',
            supervision_site='DISTRICT 1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            admission_reason=StateSupervisionPeriodAdmissionReason.INVESTIGATION,
            admission_reason_raw_text='INVESTIGATION',
            start_date=datetime.date(year=2017, month=10, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.INVESTIGATION,
            termination_reason_raw_text='INVESTIGATION',
            termination_date=datetime.date(year=2018, month=1, day=1),
            supervision_sentences=[ss_1111_2_placeholder],
            person=ss_1111_2_placeholder.person,
        )
        sp_1111_3 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-3',
            supervision_site='DISTRICT 1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            admission_reason_raw_text='COURT_SENTENCE',
            start_date=datetime.date(year=2018, month=1, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text='I',
            termination_date=datetime.date(year=2018, month=12, day=31),
            supervision_sentences=[ss_1111_2],
            person=ss_1111_2.person,
        )
        sp_1111_4 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='1111-4',
            supervision_site='DISTRICT 1',
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            admission_reason_raw_text='I',
            start_date=datetime.date(year=2020, month=1, day=1),
            incarceration_sentences=[is_1111_3],
            supervising_officer=po_1,
            person=is_1111_3.person,
        )
        sg_1111_2.supervision_sentences.append(ss_1111_2_placeholder)
        ss_1111_2.supervision_periods.append(sp_1111_3)
        ss_1111_2_placeholder.supervision_periods.append(sp_1111_2)
        is_1111_3.supervision_periods.append(sp_1111_4)

        ss_2222_1_placeholder = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=sg_2222_1,
            person=sg_2222_1.person)
        sp_2222_1 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-1',
            supervision_site='DISTRICT 2',
            status=StateSupervisionPeriodStatus.TERMINATED,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            admission_reason_raw_text='COURT_SENTENCE',
            start_date=datetime.date(year=2009, month=1, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            termination_reason_raw_text='F',
            termination_date=datetime.date(year=2009, month=7, day=1),
            supervision_sentences=[ss_2222_1],
            person=ss_2222_1.person,
        )
        sp_2222_2 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-2',
            status=StateSupervisionPeriodStatus.TERMINATED,
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            admission_reason_raw_text='ABSCONSION',
            start_date=datetime.date(year=2009, month=7, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
            termination_reason_raw_text='RETURN_FROM_ABSCONSION',
            termination_date=datetime.date(year=2009, month=12, day=1),
            supervision_sentences=[ss_2222_1],
            person=ss_2222_1.person,
        )
        sp_2222_3 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='2222-3',
            supervision_site='DISTRICT 2',
            status=StateSupervisionPeriodStatus.TERMINATED,
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            admission_reason_raw_text='F',
            start_date=datetime.date(year=2009, month=12, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_reason_raw_text='I',
            termination_date=datetime.date(year=2009, month=12, day=31),
            supervision_sentences=[ss_2222_1],
            person=ss_2222_1.person,
        )
        sg_2222_1.supervision_sentences.append(ss_2222_1_placeholder)
        ss_2222_1.supervision_periods.extend([sp_2222_1, sp_2222_2, sp_2222_3])

        ss_3333_1_placeholder = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=sg_3333_1,
            person=sg_3333_1.person)
        sp_3333_1 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='3333-1',
            supervision_site='DISTRICT 4',
            status=StateSupervisionPeriodStatus.TERMINATED,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            admission_reason_raw_text='COURT_SENTENCE',
            start_date=datetime.date(year=2015, month=1, day=1),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_OUT_OF_STATE,
            termination_reason_raw_text='TRANSFER_OUT_OF_STATE',
            termination_date=datetime.date(year=2018, month=1, day=1),
            supervision_sentences=[ss_3333_1],
            person=ss_3333_1.person,
        )
        sp_3333_2 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='3333-2',
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_OUT_OF_STATE,
            admission_reason_raw_text='TRANSFER_OUT_OF_STATE',
            start_date=datetime.date(year=2018, month=1, day=1),
            supervision_sentences=[ss_3333_1],
            person=ss_3333_1.person,
            supervising_officer=po_2,
        )
        sg_3333_1.supervision_sentences.append(ss_3333_1_placeholder)
        ss_3333_1.supervision_periods.extend([sp_3333_1, sp_3333_2])

        # Act
        self._run_ingest_job_for_filename('movement_facility_location_offstat_supervision_periods.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        #################################################################
        # ofndr_tst_tst_qstn_rspns_violation_reports
        #################################################################
        # TODO(2492): Remove dangling placeholders from expected graph once functionality is in entity matching.

        # Arrange
        sg_1111_placeholder = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_1
        )
        ss_1111_placeholder = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=sg_1111_placeholder,
            person=sg_1111_placeholder.person)
        sp_1111_placeholder = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[ss_1111_placeholder],
            person=ss_1111_placeholder.person
        )
        sv_1111_5 = entities.StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='5',
            is_violent=True,
            is_sex_offense=True,
            supervision_periods=[sp_1111_3],
            person=sp_1111_3.person
        )
        vte_1111_5_m = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            violation_type=StateSupervisionViolationType.MISDEMEANOR,
            violation_type_raw_text='NEW MISDEMEANOR',
            supervision_violation=sv_1111_5,
            person=sv_1111_5.person
        )
        vte_1111_5_t = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text='TECHNICAL (ENTER DETAILS BELOW)',
            supervision_violation=sv_1111_5,
            person=sv_1111_5.person
        )
        svr_1111_5 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='5',
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text='VIOLATION_REPORT',
            response_date=datetime.date(year=2018, month=12, day=1),
            supervision_violation=sv_1111_5,
            person=sv_1111_5.person
        )
        svrd_1111_5_r = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text='IMPOSITION OF SENTENCE',
            supervision_violation_response=svr_1111_5,
            person=svr_1111_5.person,
        )
        person_1.sentence_groups.append(sg_1111_placeholder)
        sg_1111_placeholder.supervision_sentences.append(ss_1111_placeholder)
        ss_1111_placeholder.supervision_periods.append(sp_1111_placeholder)
        sp_1111_3.supervision_violation_entries.append(sv_1111_5)
        sv_1111_5.supervision_violation_types.extend([vte_1111_5_m, vte_1111_5_t])
        sv_1111_5.supervision_violation_responses.append(svr_1111_5)
        svr_1111_5.supervision_violation_response_decisions.append(svrd_1111_5_r)

        sg_2222_placeholder = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_2
        )
        ss_2222_placeholder = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=sg_2222_placeholder,
            person=sg_2222_placeholder.person)
        sp_2222_placeholder = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[ss_2222_placeholder],
            person=ss_2222_placeholder.person
        )
        sv_2222_6 = entities.StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='6',
            is_violent=False,
            is_sex_offense=False,
            supervision_periods=[sp_2222_1],
            person=sp_2222_1.person
        )
        vte_2222_6_a = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            violation_type=StateSupervisionViolationType.ABSCONDED,
            violation_type_raw_text='ABSCONDING',
            supervision_violation=sv_2222_6,
            person=sv_2222_6.person
        )
        vte_2222_6_t = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text='TECHNICAL (ENTER DETAILS BELOW)',
            supervision_violation=sv_2222_6,
            person=sv_2222_6.person
        )
        svr_2222_6 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='6',
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text='VIOLATION_REPORT',
            response_date=datetime.date(year=2009, month=6, day=15),
            supervision_violation=sv_2222_6,
            person=sv_2222_6.person
        )
        person_2.sentence_groups.append(sg_2222_placeholder)
        sg_2222_placeholder.supervision_sentences.append(ss_2222_placeholder)
        ss_2222_placeholder.supervision_periods.append(sp_2222_placeholder)
        sp_2222_1.supervision_violation_entries.append(sv_2222_6)
        sv_2222_6.supervision_violation_types.extend([vte_2222_6_a, vte_2222_6_t])
        sv_2222_6.supervision_violation_responses.append(svr_2222_6)

        sg_3333_placeholder = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_3
        )
        ss_3333_placeholder = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=sg_3333_placeholder,
            person=sg_3333_placeholder.person)
        sp_3333_placeholder = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[ss_3333_placeholder],
            person=ss_3333_placeholder.person
        )
        sv_3333_7 = entities.StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='7',
            is_violent=False,
            is_sex_offense=False,
            supervision_periods=[sp_3333_1],
            person=sp_3333_1.person
        )
        vte_3333_7_t = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text='TECHNICAL (ENTER DETAILS BELOW)',
            supervision_violation=sv_3333_7,
            person=sv_3333_7.person
        )
        svr_3333_7 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='7',
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text='VIOLATION_REPORT',
            response_date=datetime.date(year=2016, month=1, day=1),
            supervision_violation=sv_3333_7,
            person=sv_3333_7.person
        )
        svrd_3333_7_r = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
            decision_raw_text='REINSTATEMENT',
            supervision_violation_response=svr_3333_7,
            person=svr_3333_7.person,
        )
        person_3.sentence_groups.append(sg_3333_placeholder)
        sg_3333_placeholder.supervision_sentences.append(ss_3333_placeholder)
        ss_3333_placeholder.supervision_periods.append(sp_3333_placeholder)
        sp_3333_1.supervision_violation_entries.append(sv_3333_7)
        sv_3333_7.supervision_violation_types.append(vte_3333_7_t)
        sv_3333_7.supervision_violation_responses.append(svr_3333_7)
        svr_3333_7.supervision_violation_response_decisions.append(svrd_3333_7_r)

        # Act
        self._run_ingest_job_for_filename('ofndr_tst_tst_qstn_rspns_violation_reports.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        #################################################################
        # ofndr_tst_tst_qstn_rspns_violation_reports_old
        #################################################################
        # TODO(3057): Remove this placeholder tree once we have code to combine placeholder trees within a person tree.
        # TODO(2492): Remove dangling placeholders from expected graph once functionality is in entity matching.

        # Arrange
        sg_2222_placeholder_2 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_2
        )
        ss_2222_placeholder_2 = entities.StateSupervisionSentence.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group=sg_2222_placeholder_2,
            person=sg_2222_placeholder_2.person)
        sp_2222_placeholder_2 = entities.StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[ss_2222_placeholder_2],
            person=ss_2222_placeholder_2.person
        )
        sv_2222_8 = entities.StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='8',
            is_violent=False,
            is_sex_offense=False,
            supervision_periods=[sp_2222_1],
            person=sp_2222_1.person
        )
        vte_2222_8_m = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            violation_type=StateSupervisionViolationType.MISDEMEANOR,
            violation_type_raw_text='NEW MISDEMEANOR',
            supervision_violation=sv_2222_8,
            person=sv_2222_8.person
        )
        vte_2222_8_t = entities.StateSupervisionViolationTypeEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text='TECHNICAL',
            supervision_violation=sv_2222_8,
            person=sv_2222_8.person
        )
        svr_2222_8 = entities.StateSupervisionViolationResponse.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='8',
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text='VIOLATION_REPORT',
            response_date=datetime.date(year=2009, month=2, day=1),
            supervision_violation=sv_2222_8,
            person=sv_2222_8.person
        )
        svrd_2222_8_c = entities.StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            decision=StateSupervisionViolationResponseDecision.SPECIALIZED_COURT,
            decision_raw_text='REFERRAL TO PROBLEM SOLVING COURT',
            supervision_violation_response=svr_2222_8,
            person=svr_2222_8.person,
        )
        person_2.sentence_groups.append(sg_2222_placeholder_2)
        sg_2222_placeholder_2.supervision_sentences.append(ss_2222_placeholder_2)
        ss_2222_placeholder_2.supervision_periods.append(sp_2222_placeholder_2)

        sp_2222_1.supervision_violation_entries.append(sv_2222_8)
        sv_2222_8.supervision_violation_types.extend([vte_2222_8_m, vte_2222_8_t])
        sv_2222_8.supervision_violation_responses.append(svr_2222_8)
        svr_2222_8.supervision_violation_response_decisions.append(svrd_2222_8_c)

        # Act
        self._run_ingest_job_for_filename('ofndr_tst_tst_qstn_rspns_violation_reports_old.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        # Rerun for sanity
        file_tags = self.controller.get_file_tag_rank_list()
        for file_tag in file_tags:
            self._run_ingest_job_for_filename(f'{file_tag}.csv')

        # TODO(2492): Until we implement proper cleanup of dangling placeholders, reruns of certain files will create
        #  new dangling placeholders with each rerun.
        self.assert_expected_db_people(expected_people, ignore_dangling_placeholders=True)
