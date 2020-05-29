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

"""Unit and integration tests for Pennsylvania direct ingest."""
import datetime
import json
from typing import Type

from recidiviz import IngestInfo
from recidiviz.common.constants.person_characteristics import Gender, Race, Ethnicity, ResidencyStatus
from recidiviz.common.constants.state.external_id_types import US_PA_CONTROL, US_PA_SID, US_PA_PBPP
from recidiviz.common.constants.state.state_assessment import StateAssessmentClass, StateAssessmentType
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import GcsfsDirectIngestController
from recidiviz.ingest.direct.regions.us_pa.us_pa_controller import UsPaController
from recidiviz.ingest.models.ingest_info import StatePerson, StatePersonExternalId, StatePersonRace, StateAlias, \
    StatePersonEthnicity, StateSentenceGroup, StateAssessment
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.base_state_direct_ingest_controller_tests import \
    BaseStateDirectIngestControllerTests
from recidiviz.tests.ingest.direct.regions.utils import populate_person_backedges

_STATE_CODE_UPPER = 'US_PA'


class TestUsPaController(BaseStateDirectIngestControllerTests):
    """Unit tests for each Idaho file to be ingested by the UsNdController."""

    @classmethod
    def region_code(cls) -> str:
        return _STATE_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        return UsPaController

    def test_populate_data_dbo_IcsDoc(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='12345678',
                            surname='RUSSELL',
                            given_names='BERTRAND',
                            gender='2',
                            birthdate='19760318',
                            current_address='123 Easy Street, PITTSBURGH, PA 16161',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='12345678', id_type=US_PA_SID),
                                StatePersonExternalId(state_person_external_id_id='123456', id_type=US_PA_CONTROL),
                                StatePersonExternalId(state_person_external_id_id='123A', id_type=US_PA_PBPP),
                            ],
                            state_person_races=[StatePersonRace(race='2')],
                            state_aliases=[
                                StateAlias(
                                    surname='RUSSELL',
                                    given_names='BERTRAND',
                                    alias_type='GIVEN_NAME'
                                )
                            ],
                            state_sentence_groups=[
                                StateSentenceGroup(state_sentence_group_id='AB7413')
                            ]),
                StatePerson(state_person_id='55554444',
                            surname='SARTRE',
                            given_names='JEAN-PAUL',
                            gender='2',
                            birthdate='19821002',
                            current_address='555 FLATBUSH DR, NEW YORK, NY 10031',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='55554444', id_type=US_PA_SID),
                                StatePersonExternalId(state_person_external_id_id='654321', id_type=US_PA_CONTROL),
                                StatePersonExternalId(state_person_external_id_id='456B', id_type=US_PA_PBPP),
                            ],
                            state_person_races=[StatePersonRace(race='2')],
                            state_aliases=[
                                StateAlias(
                                    surname='SARTRE',
                                    given_names='JEAN-PAUL',
                                    alias_type='GIVEN_NAME'
                                )
                            ],
                            state_sentence_groups=[
                                StateSentenceGroup(state_sentence_group_id='GF3374')
                            ]),
                StatePerson(state_person_id='99990000',
                            surname='KIERKEGAARD',
                            given_names='SOREN',
                            name_suffix='JR ',
                            gender='1',
                            birthdate='19911120',
                            current_address='5000 SUNNY LANE, APT. 55D, PHILADELPHIA, PA 19129',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='99990000', id_type=US_PA_SID),
                                StatePersonExternalId(state_person_external_id_id='445566', id_type=US_PA_CONTROL),
                                StatePersonExternalId(state_person_external_id_id='012D', id_type=US_PA_PBPP),
                            ],
                            state_person_races=[StatePersonRace(race='6')],
                            state_aliases=[
                                StateAlias(
                                    surname='KIERKEGAARD',
                                    given_names='SOREN',
                                    name_suffix='JR ',
                                    alias_type='GIVEN_NAME'
                                )
                            ],
                            state_sentence_groups=[
                                StateSentenceGroup(state_sentence_group_id='CJ1991')
                            ]),
                StatePerson(state_person_id='09876543',
                            surname='RAWLS',
                            given_names='JOHN',
                            gender='2',
                            birthdate='19890617',
                            current_address='214 HAPPY PLACE, PHILADELPHIA, PA 19129',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='09876543', id_type=US_PA_SID),
                                StatePersonExternalId(state_person_external_id_id='778899', id_type=US_PA_CONTROL),
                                StatePersonExternalId(state_person_external_id_id='345E', id_type=US_PA_PBPP),
                            ],
                            state_person_ethnicities=[StatePersonEthnicity(ethnicity='3')],
                            state_aliases=[
                                StateAlias(
                                    surname='RAWLS',
                                    given_names='JOHN',
                                    alias_type='GIVEN_NAME'
                                )
                            ],
                            state_sentence_groups=[
                                StateSentenceGroup(state_sentence_group_id='JE1989')
                            ]),
            ])

        self.run_parse_file_test(expected, 'dbo_IcsDoc')

    def test_populate_data_dbo_tblInmTestScore(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='123456',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='123456', id_type=US_PA_CONTROL),
                            ],
                            state_assessments=[
                                StateAssessment(state_assessment_id='123456-1-1',
                                                assessment_type='CSS-M                                             ',
                                                assessment_class='SOCIAL',
                                                assessment_date='6/22/2008 13:20:54',
                                                assessment_score='19'),
                                StateAssessment(state_assessment_id='123456-2-1',
                                                assessment_type='HIQ                                               ',
                                                assessment_class='SOCIAL',
                                                assessment_date='7/12/2004 8:23:28',
                                                assessment_score='62'),
                                StateAssessment(state_assessment_id='123456-3-3',
                                                assessment_type='LSI-R                                             ',
                                                assessment_class='RISK',
                                                assessment_date='10/3/2010 12:11:41',
                                                assessment_score='25'),
                            ]),
                StatePerson(state_person_id='654321',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='654321', id_type=US_PA_CONTROL),
                            ],
                            state_assessments=[
                                StateAssessment(state_assessment_id='654321-1-1',
                                                assessment_type='CSS-M                                             ',
                                                assessment_class='SOCIAL',
                                                assessment_date='4/1/2003 11:42:17',
                                                assessment_score='22'),
                                StateAssessment(state_assessment_id='654321-3-1',
                                                assessment_type='LSI-R                                             ',
                                                assessment_class='RISK',
                                                assessment_date='6/8/2004 11:07:48',
                                                assessment_score='19'),
                                StateAssessment(state_assessment_id='654321-4-1',
                                                assessment_type='TCU                                               ',
                                                assessment_class='SUBSTANCE_ABUSE',
                                                assessment_date='1/4/2004 11:09:52',
                                                assessment_score='6'),
                                StateAssessment(state_assessment_id='654321-5-1',
                                                assessment_type='ST99                                              ',
                                                assessment_class='SEX_OFFENSE',
                                                assessment_date='7/5/2004 15:30:59',
                                                assessment_score='4'),
                            ]),
                StatePerson(state_person_id='445566',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='445566', id_type=US_PA_CONTROL),
                            ],
                            state_assessments=[
                                StateAssessment(state_assessment_id='445566-2-1',
                                                assessment_type='HIQ                                               ',
                                                assessment_class='SOCIAL',
                                                assessment_date='7/28/2005 10:33:31',
                                                assessment_score='61'),
                                StateAssessment(state_assessment_id='445566-3-2',
                                                assessment_type='LSI-R                                             ',
                                                assessment_class='RISK',
                                                assessment_date='12/19/2016 15:21:56',
                                                assessment_score='13'),
                            ]),
                StatePerson(state_person_id='778899',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='778899', id_type=US_PA_CONTROL),
                            ],
                            state_assessments=[
                                StateAssessment(state_assessment_id='778899-3-3',
                                                assessment_type='LSI-R                                             ',
                                                assessment_class='RISK',
                                                assessment_date='1/6/2017 18:16:56',
                                                assessment_score='14'),
                                StateAssessment(state_assessment_id='778899-6-1',
                                                assessment_type='RST                                               ',
                                                assessment_class='RISK',
                                                assessment_date='12/8/2012 15:09:08',
                                                assessment_score='9',
                                                assessment_metadata=json.dumps({"latest_version": False})),
                                StateAssessment(state_assessment_id='778899-6-2',
                                                assessment_type='RST                                               ',
                                                assessment_class='RISK',
                                                assessment_date='5/11/2018 15:54:06',
                                                assessment_score='7',
                                                assessment_metadata=json.dumps({"latest_version": True})),
                            ]),
            ])

        self.run_parse_file_test(expected, 'dbo_tblInmTestScore')

    def test_populate_data_dbo_Offender(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id=' 123A ',
                            gender='M       ',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id=' 123A ', id_type=US_PA_PBPP),
                                StatePersonExternalId(state_person_external_id_id='12345678', id_type=US_PA_SID),
                            ],
                            state_person_races=[StatePersonRace(race='B    ')],
                            ),
                StatePerson(state_person_id='456B ',
                            gender='M       ',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='456B ', id_type=US_PA_PBPP),
                                StatePersonExternalId(state_person_external_id_id='55554444', id_type=US_PA_SID),
                            ],
                            state_person_races=[StatePersonRace(race='I    ')],
                            ),
                StatePerson(state_person_id='789C ',
                            gender='  F      ',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='789C ', id_type=US_PA_PBPP),
                            ],
                            state_person_races=[StatePersonRace(race='N    ')],
                            ),
                StatePerson(state_person_id='012D ',
                            gender='  F      ',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='012D ', id_type=US_PA_PBPP),
                                StatePersonExternalId(state_person_external_id_id='99990000', id_type=US_PA_SID),
                            ],
                            state_person_races=[StatePersonRace(race='W    ')],
                            ),
                StatePerson(state_person_id='345E ',
                            gender='  M     ',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='345E ', id_type=US_PA_PBPP),
                                StatePersonExternalId(state_person_external_id_id='09876543', id_type=US_PA_SID),
                            ],
                            state_person_races=[StatePersonRace(race='W    ')],
                            )
            ])

        self.run_parse_file_test(expected, 'dbo_Offender')

    def test_populate_data_dbo_LSIR(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='789C',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='789C', id_type=US_PA_PBPP),
                            ],
                            state_assessments=[
                                StateAssessment(state_assessment_id='789C-0-1',
                                                assessment_type='LSIR',
                                                assessment_class='RISK',
                                                assessment_date='01312001',
                                                assessment_score='14'),
                            ]),
                StatePerson(state_person_id='456B',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='456B', id_type=US_PA_PBPP),
                            ],
                            state_assessments=[
                                StateAssessment(state_assessment_id='456B-1-1',
                                                assessment_type='LSIR',
                                                assessment_class='RISK',
                                                assessment_date='12222005',
                                                assessment_score='23'),
                            ]),
                StatePerson(state_person_id='345E',
                            state_person_external_ids=[
                                StatePersonExternalId(state_person_external_id_id='345E', id_type=US_PA_PBPP),
                            ],
                            state_assessments=[
                                StateAssessment(state_assessment_id='345E-3-1',
                                                assessment_type='LSIR',
                                                assessment_class='RISK',
                                                assessment_date='01192006',
                                                assessment_score='30'),
                                StateAssessment(state_assessment_id='345E-3-2',
                                                assessment_type='LSIR',
                                                assessment_class='RISK',
                                                assessment_date='08032006',
                                                assessment_score='30'),
                                StateAssessment(state_assessment_id='345E-3-3',
                                                assessment_type='LSIR',
                                                assessment_class='RISK',
                                                assessment_date='01152007',
                                                assessment_score='31'),
                                StateAssessment(state_assessment_id='345E-4-1',
                                                assessment_type='LSIR',
                                                assessment_class='RISK',
                                                assessment_date='07142007',
                                                assessment_score='33'),
                            ]),
            ])

        self.run_parse_file_test(expected, 'dbo_LSIR')

    def test_run_full_ingest_all_files_specific_order(self) -> None:
        self.maxDiff = None

        ######################################
        # dbo_IcsDoc
        ######################################
        # Arrange
        person_1 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "BERTRAND", "surname": "RUSSELL"}',
            gender=Gender.MALE,
            gender_raw_text='2',
            birthdate=datetime.date(year=1976, month=3, day=18),
            birthdate_inferred_from_age=False,
            current_address='123 EASY STREET, PITTSBURGH, PA 16161',
            residency_status=ResidencyStatus.PERMANENT,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='123456', id_type=US_PA_CONTROL),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='123A', id_type=US_PA_PBPP),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='12345678', id_type=US_PA_SID),
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    full_name='{"given_names": "BERTRAND", "surname": "RUSSELL"}',
                    state_code=_STATE_CODE_UPPER,
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME')
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, race=Race.BLACK, race_raw_text='2'),
            ],
        )
        person_1_sentence_groups = [
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE_UPPER, external_id='AB7413', person=person_1,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
        ]
        person_1.sentence_groups = person_1_sentence_groups

        person_2 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "JEAN-PAUL", "surname": "SARTRE"}',
            gender=Gender.MALE,
            gender_raw_text='2',
            birthdate=datetime.date(year=1982, month=10, day=2),
            birthdate_inferred_from_age=False,
            current_address='555 FLATBUSH DR, NEW YORK, NY 10031',
            residency_status=ResidencyStatus.PERMANENT,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='654321', id_type=US_PA_CONTROL),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='456B', id_type=US_PA_PBPP),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='55554444', id_type=US_PA_SID),
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    full_name='{"given_names": "JEAN-PAUL", "surname": "SARTRE"}',
                    state_code=_STATE_CODE_UPPER,
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME')
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, race=Race.BLACK, race_raw_text='2'),
            ],
        )
        person_2_sentence_groups = [
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE_UPPER, external_id='GF3374', person=person_2,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
        ]
        person_2.sentence_groups = person_2_sentence_groups

        person_3 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "SOREN", "name_suffix": "JR", "surname": "KIERKEGAARD"}',
            gender=Gender.FEMALE,
            gender_raw_text='1',
            birthdate=datetime.date(year=1991, month=11, day=20),
            birthdate_inferred_from_age=False,
            current_address='5000 SUNNY LANE, APT. 55D, PHILADELPHIA, PA 19129',
            residency_status=ResidencyStatus.PERMANENT,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='445566', id_type=US_PA_CONTROL),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='012D', id_type=US_PA_PBPP),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='99990000', id_type=US_PA_SID),
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    full_name='{"given_names": "SOREN", "name_suffix": "JR", "surname": "KIERKEGAARD"}',
                    state_code=_STATE_CODE_UPPER,
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME')
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, race=Race.WHITE, race_raw_text='6'),
            ],
        )
        person_3_sentence_groups = [
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE_UPPER, external_id='CJ1991', person=person_3,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
        ]
        person_3.sentence_groups = person_3_sentence_groups

        person_4 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "JOHN", "surname": "RAWLS"}',
            gender=Gender.MALE,
            gender_raw_text='2',
            birthdate=datetime.date(year=1989, month=6, day=17),
            birthdate_inferred_from_age=False,
            current_address='214 HAPPY PLACE, PHILADELPHIA, PA 19129',
            residency_status=ResidencyStatus.PERMANENT,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='778899', id_type=US_PA_CONTROL),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='345E', id_type=US_PA_PBPP),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='09876543', id_type=US_PA_SID),
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    full_name='{"given_names": "JOHN", "surname": "RAWLS"}',
                    state_code=_STATE_CODE_UPPER,
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME')
            ],
            ethnicities=[
                entities.StatePersonEthnicity.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, ethnicity=Ethnicity.HISPANIC, ethnicity_raw_text='3'),
            ],
        )
        person_4_sentence_groups = [
            entities.StateSentenceGroup.new_with_defaults(
                state_code=_STATE_CODE_UPPER, external_id='JE1989', person=person_4,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
        ]
        person_4.sentence_groups = person_4_sentence_groups

        expected_people = [person_1, person_2, person_3, person_4]
        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename('dbo_IcsDoc.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_tblInmTestScore
        ######################################

        person_1_doc_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SOCIAL, assessment_class_raw_text='SOCIAL',
                assessment_type=StateAssessmentType.CSSM, assessment_type_raw_text='CSS-M',
                assessment_score=19, assessment_date=datetime.date(year=2008, month=6, day=22),
                external_id='123456-1-1', state_code=_STATE_CODE_UPPER, person=person_1),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SOCIAL, assessment_class_raw_text='SOCIAL',
                assessment_type=StateAssessmentType.HIQ, assessment_type_raw_text='HIQ',
                assessment_score=62, assessment_date=datetime.date(year=2004, month=7, day=12),
                external_id='123456-2-1', state_code=_STATE_CODE_UPPER, person=person_1),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.LSIR, assessment_type_raw_text='LSI-R',
                assessment_score=25, assessment_date=datetime.date(year=2010, month=10, day=3),
                external_id='123456-3-3', state_code=_STATE_CODE_UPPER, person=person_1),
        ]
        person_1.assessments = person_1_doc_assessments

        person_2_doc_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SOCIAL, assessment_class_raw_text='SOCIAL',
                assessment_type=StateAssessmentType.CSSM, assessment_type_raw_text='CSS-M',
                assessment_score=22, assessment_date=datetime.date(year=2003, month=4, day=1),
                external_id='654321-1-1', state_code=_STATE_CODE_UPPER, person=person_2),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.LSIR, assessment_type_raw_text='LSI-R',
                assessment_score=19, assessment_date=datetime.date(year=2004, month=6, day=8),
                external_id='654321-3-1', state_code=_STATE_CODE_UPPER, person=person_2),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SUBSTANCE_ABUSE, assessment_class_raw_text='SUBSTANCE_ABUSE',
                assessment_type=StateAssessmentType.TCU_DRUG_SCREEN, assessment_type_raw_text='TCU',
                assessment_score=6, assessment_date=datetime.date(year=2004, month=1, day=4),
                external_id='654321-4-1', state_code=_STATE_CODE_UPPER, person=person_2),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SEX_OFFENSE, assessment_class_raw_text='SEX_OFFENSE',
                assessment_type=StateAssessmentType.STATIC_99, assessment_type_raw_text='ST99',
                assessment_score=4, assessment_date=datetime.date(year=2004, month=7, day=5),
                external_id='654321-5-1', state_code=_STATE_CODE_UPPER, person=person_2),
        ]
        person_2.assessments = person_2_doc_assessments

        person_3_doc_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.SOCIAL, assessment_class_raw_text='SOCIAL',
                assessment_type=StateAssessmentType.HIQ, assessment_type_raw_text='HIQ',
                assessment_score=61, assessment_date=datetime.date(year=2005, month=7, day=28),
                external_id='445566-2-1', state_code=_STATE_CODE_UPPER, person=person_3),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.LSIR, assessment_type_raw_text='LSI-R',
                assessment_score=13, assessment_date=datetime.date(year=2016, month=12, day=19),
                external_id='445566-3-2', state_code=_STATE_CODE_UPPER, person=person_3),
        ]
        person_3.assessments = person_3_doc_assessments

        person_4_doc_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.LSIR, assessment_type_raw_text='LSI-R',
                assessment_score=14, assessment_date=datetime.date(year=2017, month=1, day=6),
                external_id='778899-3-3', state_code=_STATE_CODE_UPPER, person=person_4),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.PA_RST, assessment_type_raw_text='RST',
                assessment_score=9, assessment_date=datetime.date(year=2012, month=12, day=8),
                assessment_metadata='{"LATEST_VERSION": FALSE}',
                external_id='778899-6-1', state_code=_STATE_CODE_UPPER, person=person_4),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.PA_RST, assessment_type_raw_text='RST',
                assessment_score=7, assessment_date=datetime.date(year=2018, month=5, day=11),
                assessment_metadata='{"LATEST_VERSION": TRUE}',
                external_id='778899-6-2', state_code=_STATE_CODE_UPPER, person=person_4),
        ]
        person_4.assessments = person_4_doc_assessments

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename('dbo_tblInmTestScore.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_Offender
        ######################################
        # Arrange
        person_1.gender_raw_text = 'M'
        person_1.races[0].race_raw_text = 'B'

        person_2.gender_raw_text = 'M'
        person_2.races.append(entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE_UPPER, race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE, race_raw_text='I'
        ))

        person_3.gender_raw_text = 'F'
        person_3.races[0].race_raw_text = 'W'

        person_4.gender_raw_text = 'M'
        person_4.races = [entities.StatePersonRace.new_with_defaults(
            state_code=_STATE_CODE_UPPER, race=Race.WHITE, race_raw_text='W'
        )]

        person_5 = entities.StatePerson.new_with_defaults(
            gender=Gender.FEMALE,
            gender_raw_text='F',
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, external_id='789C', id_type=US_PA_PBPP),
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER, race=Race.OTHER, race_raw_text='N'),
            ],
        )
        expected_people.append(person_5)
        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename('dbo_Offender.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # dbo_LSIR
        ######################################
        # Arrange
        person_2_pbpp_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.LSIR, assessment_type_raw_text='LSIR',
                assessment_score=23, assessment_date=datetime.date(year=2005, month=12, day=22),
                external_id='456B-1-1', state_code=_STATE_CODE_UPPER, person=person_2),
        ]
        person_2.assessments.extend(person_2_pbpp_assessments)

        person_4_pbpp_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.LSIR, assessment_type_raw_text='LSIR',
                assessment_score=30, assessment_date=datetime.date(year=2006, month=1, day=19),
                external_id='345E-3-1', state_code=_STATE_CODE_UPPER, person=person_4),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.LSIR, assessment_type_raw_text='LSIR',
                assessment_score=30, assessment_date=datetime.date(year=2006, month=8, day=3),
                external_id='345E-3-2', state_code=_STATE_CODE_UPPER, person=person_4),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.LSIR, assessment_type_raw_text='LSIR',
                assessment_score=31, assessment_date=datetime.date(year=2007, month=1, day=15),
                external_id='345E-3-3', state_code=_STATE_CODE_UPPER, person=person_4),
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.LSIR, assessment_type_raw_text='LSIR',
                assessment_score=33, assessment_date=datetime.date(year=2007, month=7, day=14),
                external_id='345E-4-1', state_code=_STATE_CODE_UPPER, person=person_4),
        ]
        person_4.assessments.extend(person_4_pbpp_assessments)

        person_5_pbpp_assessments = [
            entities.StateAssessment.new_with_defaults(
                assessment_class=StateAssessmentClass.RISK, assessment_class_raw_text='RISK',
                assessment_type=StateAssessmentType.LSIR, assessment_type_raw_text='LSIR',
                assessment_score=14, assessment_date=datetime.date(year=2001, month=1, day=31),
                external_id='789C-0-1', state_code=_STATE_CODE_UPPER, person=person_5),
        ]
        person_5.assessments.extend(person_5_pbpp_assessments)

        populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename('dbo_LSIR.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # Full Rerun for Idempotence
        ######################################

        # Rerun for sanity
        self._do_ingest_job_rerun_for_tags(self.controller.get_file_tag_rank_list())

        self.assert_expected_db_people(expected_people)
