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
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_court_case import StateCourtCaseStatus, StateCourtType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import GcsfsDirectIngestController
from recidiviz.ingest.direct.regions.us_id.us_id_controller import UsIdController
from recidiviz.ingest.models.ingest_info import StatePerson, StatePersonExternalId, StatePersonRace, StateAlias, \
    StatePersonEthnicity, StateAssessment, StateSentenceGroup, StateIncarcerationSentence, StateCharge, \
    StateCourtCase, StateAgent, StateSupervisionSentence
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.base_state_direct_ingest_controller_tests import \
    BaseStateDirectIngestControllerTests
from recidiviz.tests.ingest.direct.regions.utils import _populate_person_backedges

_STATE_CODE_UPPER = 'US_ID'


class TestUsIdController(BaseStateDirectIngestControllerTests):
    """Unit tests for each Idaho file to be ingested by the UsNdController."""

    @classmethod
    def region_code(cls) -> str:
        return _STATE_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        return UsIdController

    def test_populate_data_offender(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='1111',
                            surname='LAST_1',
                            given_names='FIRST_1',
                            middle_names='MIDDLE_1',
                            gender='M',
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

        self.run_parse_file_test(expected, 'offender')

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

    def test_run_full_ingest_all_files_specific_order(self) -> None:
        self.maxDiff = None

        ######################################
        # OFFENDER
        ######################################
        # Arrange
        person_1 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "FIRST_1", "middle_names": "MIDDLE_1", "surname": "LAST_1"}',
            gender=Gender.MALE,
            gender_raw_text='M',
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
        _populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename('offender.csv')

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

        # Rerun for sanity
        # pylint:disable=protected-access
        file_tags = self.controller._get_file_tag_rank_list()
        for file_tag in file_tags:
            self._run_ingest_job_for_filename(f'{file_tag}.csv')

        self.assert_expected_db_people(expected_people)
