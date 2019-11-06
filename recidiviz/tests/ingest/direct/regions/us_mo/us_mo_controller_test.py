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
"""Tests for the UsMoController."""
import datetime
from typing import Type, List

from recidiviz import IngestInfo
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.external_id_types import US_MO_DOC, \
    US_MO_OLN, US_MO_FBI, US_MO_SID
from recidiviz.common.constants.state.state_charge import \
    StateChargeClassificationType
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_person_alias import \
    StatePersonAliasType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.regions.us_mo.us_mo_controller import \
    UsMoController
from recidiviz.ingest.models.ingest_info import StatePerson, \
    StatePersonExternalId, StatePersonRace, StateAlias, StatePersonEthnicity, \
    StateSentenceGroup, StateIncarcerationSentence, StateCharge
from recidiviz.persistence.entity.entity_utils import get_all_entities_from_tree
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.\
    base_state_direct_ingest_controller_tests import \
    BaseStateDirectIngestControllerTests

_STATE_CODE_UPPER = 'US_MO'


class TestUsMoController(BaseStateDirectIngestControllerTests):
    """Tests for the UsMoController."""

    @classmethod
    def state_code(cls) -> str:
        return _STATE_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[GcsfsDirectIngestController]:
        return UsMoController

    def test_populate_data_tak001_offender_identification(self):
        expected = IngestInfo(
            state_people=[
                StatePerson(state_person_id='110035',
                            surname='ABAGNALE',
                            given_names='FRANK',
                            name_suffix='JR',
                            gender='M',
                            birthdate='19711120',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='110035',
                                    id_type=US_MO_DOC),
                                StatePersonExternalId(
                                    state_person_external_id_id='SI00110035',
                                    id_type=US_MO_SID),
                                StatePersonExternalId(
                                    state_person_external_id_id='F00110035',
                                    id_type=US_MO_FBI),
                                StatePersonExternalId(
                                    state_person_external_id_id=
                                    'OLN0000000110035',
                                    id_type=US_MO_OLN)
                            ],
                            state_person_races=[
                                StatePersonRace(race='I')
                            ],
                            state_person_ethnicities=[
                                StatePersonEthnicity(ethnicity='H')
                            ],
                            state_aliases=[
                                StateAlias(surname='ABAGNALE',
                                           given_names='FRANK',
                                           name_suffix='JR',
                                           alias_type='GIVEN_NAME')
                            ],
                            state_sentence_groups=[
                                StateSentenceGroup(
                                    state_sentence_group_id='110035-19890901'
                                )
                            ]),
                StatePerson(state_person_id='310261',
                            surname='STEWART',
                            given_names='MARTHA',
                            middle_names='HELEN',
                            gender='F',
                            birthdate='19690617',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='310261',
                                    id_type=US_MO_DOC),
                                StatePersonExternalId(
                                    state_person_external_id_id='SI00310261',
                                    id_type=US_MO_SID),
                                StatePersonExternalId(
                                    state_person_external_id_id='F00310261',
                                    id_type=US_MO_FBI),
                                StatePersonExternalId(
                                    state_person_external_id_id=
                                    'OLN0000000310261',
                                    id_type=US_MO_OLN)
                            ],
                            state_person_races=[
                                StatePersonRace(race='W')
                            ],
                            state_person_ethnicities=[
                                StatePersonEthnicity(ethnicity='U')
                            ],
                            state_aliases=[
                                StateAlias(surname='STEWART',
                                           given_names='MARTHA',
                                           middle_names='HELEN',
                                           alias_type='GIVEN_NAME')
                            ],
                            state_sentence_groups=[
                                StateSentenceGroup(
                                    state_sentence_group_id='310261-19890821'
                                )
                            ]),
                StatePerson(state_person_id='710448',
                            surname='WINNIFIELD',
                            given_names='JULES',
                            birthdate='19640831',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='710448',
                                    id_type=US_MO_DOC),
                                StatePersonExternalId(
                                    state_person_external_id_id='SI00710448',
                                    id_type=US_MO_SID),
                                StatePersonExternalId(
                                    state_person_external_id_id='F00710448',
                                    id_type=US_MO_FBI),
                                StatePersonExternalId(
                                    state_person_external_id_id=
                                    'OLN0000000710448',
                                    id_type=US_MO_OLN)
                            ],
                            state_person_races=[
                                StatePersonRace(race='B')
                            ],
                            state_aliases=[
                                StateAlias(surname='WINNIFIELD',
                                           given_names='JULES',
                                           alias_type='GIVEN_NAME')
                            ],
                            state_sentence_groups=[
                                StateSentenceGroup(
                                    state_sentence_group_id='710448-19890901'
                                )
                            ]),
                StatePerson(state_person_id='910324',
                            given_names='KAONASHI',
                            gender='U',
                            birthdate='19580213',
                            state_person_external_ids=[
                                StatePersonExternalId(
                                    state_person_external_id_id='910324',
                                    id_type=US_MO_DOC),
                                StatePersonExternalId(
                                    state_person_external_id_id='SI00910324',
                                    id_type=US_MO_SID),
                                StatePersonExternalId(
                                    state_person_external_id_id='F00910324',
                                    id_type=US_MO_FBI),
                                StatePersonExternalId(
                                    state_person_external_id_id=
                                    'OLN0000000910324',
                                    id_type=US_MO_OLN)
                            ],
                            state_person_races=[
                                StatePersonRace(race='A')
                            ],
                            state_person_ethnicities=[
                                StatePersonEthnicity(ethnicity='N')
                            ],
                            state_aliases=[
                                StateAlias(given_names='KAONASHI',
                                           alias_type='GIVEN_NAME')
                            ],
                            state_sentence_groups=[
                                StateSentenceGroup(
                                    state_sentence_group_id='910324-19890825'
                                )
                            ]),
            ])

        self.run_parse_file_test(expected, 'tak001_offender_identification')

    def test_populate_data_tak040_offender_identification(self):
        expected = IngestInfo(state_people=[
            StatePerson(state_person_id='110035',
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='110035',
                                id_type=US_MO_DOC),
                        ],
                        state_sentence_groups=[
                            StateSentenceGroup(
                                state_sentence_group_id='110035-19890901'
                            ),
                            StateSentenceGroup(
                                state_sentence_group_id='110035-20010414'
                            )
                        ]),
            StatePerson(state_person_id='310261',
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='310261',
                                id_type=US_MO_DOC),
                        ],
                        state_sentence_groups=[
                            StateSentenceGroup(
                                state_sentence_group_id='310261-19890821'
                            )
                        ]),
            StatePerson(state_person_id='710448',
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='710448',
                                id_type=US_MO_DOC),
                        ],
                        state_sentence_groups=[
                            StateSentenceGroup(
                                state_sentence_group_id='710448-19890901'
                            ),
                            StateSentenceGroup(
                                state_sentence_group_id='710448-20010414'
                            )
                        ]),
            StatePerson(state_person_id='910324',
                        state_person_external_ids=[
                            StatePersonExternalId(
                                state_person_external_id_id='910324',
                                id_type=US_MO_DOC),
                        ],
                        state_sentence_groups=[
                            StateSentenceGroup(
                                state_sentence_group_id='910324-19890825'
                            )
                        ]),
        ])

        self.run_parse_file_test(expected, 'tak040_offender_cycles')

    def test_populate_data_tak022_tak023_offender_sentence_institutional(self):
        expected = IngestInfo(state_people=[
            StatePerson(state_person_id='110035',
                        state_sentence_groups=[
                            StateSentenceGroup(
                                state_sentence_group_id='110035-19890901',
                                state_incarceration_sentences=[
                                    StateIncarcerationSentence(
                                        state_incarceration_sentence_id=
                                        '110035-19890901-1',
                                        status='COMPLETED',
                                        date_imposed='19560316',
                                        projected_min_release_date='99999999',
                                        projected_max_release_date='99999999',
                                        parole_eligibility_date='1956-04-25',
                                        county_code='US_MO_ST_LOUIS_COUNTY',
                                        max_length='3655253',
                                        is_life='True',
                                        state_charges=[
                                            StateCharge(
                                                offense_date=None,
                                                county_code='US_MO_JACKSON',
                                                ncic_code='0904',
                                                statute='10021040',
                                                description=
                                                'TC: MURDER 1ST - FIST',
                                                classification_type='F',
                                                classification_subtype='O')
                                        ])
                                ]),
                            StateSentenceGroup(
                                state_sentence_group_id='110035-20010414',
                                state_incarceration_sentences=[
                                    StateIncarcerationSentence(
                                        state_incarceration_sentence_id=
                                        '110035-20010414-1',
                                        status='COMPLETED',
                                        date_imposed='20030110',
                                        projected_min_release_date='20070102',
                                        projected_max_release_date='20070102',
                                        parole_eligibility_date='2003-01-03',
                                        county_code='US_MO_ST_LOUIS_COUNTY',
                                        max_length='4Y 0M 0D',
                                        is_life='False',
                                        state_charges=[
                                            StateCharge(
                                                offense_date='20000604',
                                                county_code=
                                                'US_MO_ST_LOUIS_COUNTY',
                                                ncic_code='5299',
                                                statute='31020990',
                                                description=
                                                'UNLAWFUL USE OF WEAPON',
                                                classification_type='F',
                                                classification_subtype='D')
                                        ])
                                ])
                        ]),
            StatePerson(state_person_id='310261',
                        state_sentence_groups=[
                            StateSentenceGroup(
                                state_sentence_group_id='310261-19890821',
                                state_incarceration_sentences=[
                                    StateIncarcerationSentence(
                                        state_incarceration_sentence_id=
                                        '310261-19890821-3',
                                        status='SERVING',
                                        date_imposed='20150428',
                                        projected_min_release_date='20211205',
                                        projected_max_release_date='20211205',
                                        parole_eligibility_date='2016-12-06',
                                        county_code='US_MO_ST_LOUIS_CITY',
                                        max_length='5Y 0M 0D',
                                        is_life='False',
                                        state_charges=[
                                            StateCharge(
                                                offense_date='20141004',
                                                county_code=
                                                'US_MO_ST_LOUIS_CITY',
                                                ncic_code='3599',
                                                statute='91335990',
                                                description=
                                                'POSSESSION OF CONTROLLED '
                                                'SUBSTANCE',
                                                classification_type='L')
                                        ])
                                ])
                        ]),
            StatePerson(state_person_id='710448',
                        state_sentence_groups=[
                            StateSentenceGroup(
                                state_sentence_group_id='710448-20010414',
                                state_incarceration_sentences=[
                                    StateIncarcerationSentence(
                                        state_incarceration_sentence_id=
                                        '710448-20010414-1',
                                        status='COMPLETED',
                                        date_imposed='20050513',
                                        projected_min_release_date='20091022',
                                        projected_max_release_date='20091022',
                                        parole_eligibility_date='2005-10-23',
                                        county_code='US_MO_ST_LOUIS_COUNTY',
                                        max_length='4Y 0M 0D',
                                        is_life='False',
                                        state_charges=[
                                            StateCharge(
                                                offense_date='20000731',
                                                county_code=
                                                'US_MO_ST_LOUIS_COUNTY',
                                                ncic_code='3599',
                                                statute='32450990',
                                                description='POSSESSION OF C/S',
                                                classification_type='F',
                                                classification_subtype='C')
                                        ]),
                                    StateIncarcerationSentence(
                                        state_incarceration_sentence_id=
                                        '710448-20010414-2',
                                        status='COMPLETED',
                                        date_imposed='20061214',
                                        projected_min_release_date='20111206',
                                        projected_max_release_date='20111206',
                                        parole_eligibility_date='2006-12-07',
                                        county_code='US_MO_ST_LOUIS_CITY',
                                        max_length='5Y 0M 0D',
                                        is_life='False',
                                        state_charges=[StateCharge(
                                            offense_date='20050301',
                                            ncic_code='3599',
                                            statute='32500990',
                                            description=
                                            'TRAFFICKING 2ND DEGREE',
                                            classification_type='F',
                                            classification_subtype='B')]),
                                    StateIncarcerationSentence(
                                        state_incarceration_sentence_id=
                                        '710448-20010414-3',
                                        status='COMPLETED',
                                        date_imposed='20061214',
                                        projected_min_release_date='20101206',
                                        projected_max_release_date='20101206',
                                        parole_eligibility_date='2006-12-07',
                                        county_code='US_MO_ST_LOUIS_CITY',
                                        max_length='4Y 0M 0D',
                                        is_life='False',
                                        state_charges=[StateCharge(
                                            offense_date='20050301',
                                            ncic_code='4899',
                                            statute='27020990',
                                            description='RESISTING ARREST',
                                            classification_type='F',
                                            classification_subtype='D')]),
                                ])
                        ]),
            StatePerson(state_person_id='910324',
                        state_sentence_groups=[
                            StateSentenceGroup(
                                state_sentence_group_id='910324-19890825',
                                state_incarceration_sentences=[
                                    StateIncarcerationSentence(
                                        state_incarceration_sentence_id=
                                        '910324-19890825-1',
                                        status='COMPLETED',
                                        date_imposed='19890829',
                                        projected_min_release_date='19911229',
                                        projected_max_release_date='19930429',
                                        parole_eligibility_date='1989-04-30',
                                        county_code='US_MO_*',
                                        max_length='4Y 0M 0D',
                                        is_life='False',
                                        state_charges=[
                                            StateCharge(
                                                offense_date=None,
                                                ncic_code='3572',
                                                statute='32040720',
                                                description=
                                                'POSSESSION OF METHAMPHETAMINE',
                                                classification_type='F',
                                                classification_subtype='N')
                                        ])
                                ])
                        ])
        ])

        self.run_parse_file_test(
            expected,
            'tak022_tak023_tak025_tak026_offender_sentence_institution')

    @staticmethod
    def _populate_person_backedges(
            persons: List[entities.StatePerson]) -> None:
        for person in persons:
            children = get_all_entities_from_tree(person)
            for child in children:
                if child is not person and hasattr(child, 'person') \
                        and getattr(child, 'person', None) is None:
                    child.set_field('person', person)

    def test_run_full_ingest_all_files_specific_order(self) -> None:
        ######################################
        # TAK001 OFFENDER IDENTIFICATION
        ######################################
        # Arrange
        sg_110035_19890901 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='110035-19890901',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        person_110035 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "FRANK", "name_suffix": "JR", '
                      '"surname": "ABAGNALE"}',
            gender=Gender.MALE,
            gender_raw_text='M',
            birthdate=datetime.date(year=1971, month=11, day=20),
            birthdate_inferred_from_age=False,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='110035',
                    id_type=US_MO_DOC),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='SI00110035',
                    id_type=US_MO_SID),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='F00110035',
                    id_type=US_MO_FBI),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='OLN0000000110035',
                    id_type=US_MO_OLN),
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    full_name='{"given_names": "FRANK", "name_suffix": "JR", '
                              '"surname": "ABAGNALE"}',
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME',
                )
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
                    race_raw_text='I',
                ),
            ],
            ethnicities=[
                entities.StatePersonEthnicity.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    ethnicity=Ethnicity.HISPANIC,
                    ethnicity_raw_text='H',
                )
            ],
            sentence_groups=[
                sg_110035_19890901,
            ]
        )

        sg_310261_19890821 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='310261-19890821',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        person_310261 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "MARTHA", "middle_names": "HELEN", '
                      '"surname": "STEWART"}',
            gender=Gender.FEMALE,
            gender_raw_text='F',
            birthdate=datetime.date(year=1969, month=6, day=17),
            birthdate_inferred_from_age=False,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='310261',
                    id_type=US_MO_DOC),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='SI00310261',
                    id_type=US_MO_SID),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='F00310261',
                    id_type=US_MO_FBI),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='OLN0000000310261',
                    id_type=US_MO_OLN),
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    full_name='{"given_names": "MARTHA", '
                              '"middle_names": "HELEN", "surname": "STEWART"}',
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME',
                )
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    race=Race.WHITE,
                    race_raw_text='W',
                ),
            ],
            ethnicities=[
                entities.StatePersonEthnicity.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    ethnicity=Ethnicity.EXTERNAL_UNKNOWN,
                    ethnicity_raw_text='U',
                )
            ],
            sentence_groups=[
                sg_310261_19890821,
            ]
        )
        person_710448 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "JULES", "surname": "WINNIFIELD"}',
            birthdate=datetime.date(year=1964, month=8, day=31),
            birthdate_inferred_from_age=False,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='710448',
                    id_type=US_MO_DOC),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='SI00710448',
                    id_type=US_MO_SID),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='F00710448',
                    id_type=US_MO_FBI),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='OLN0000000710448',
                    id_type=US_MO_OLN),
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    full_name=
                    '{"given_names": "JULES", "surname": "WINNIFIELD"}',
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME',
                )
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    race=Race.BLACK,
                    race_raw_text='B',
                ),
            ],
            sentence_groups=[
                entities.StateSentenceGroup.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='710448-19890901',
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                )
            ]
        )

        sg_910324_19890825 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='910324-19890825',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        person_910324 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "KAONASHI"}',
            gender=Gender.EXTERNAL_UNKNOWN,
            gender_raw_text='U',
            birthdate=datetime.date(year=1958, month=2, day=13),
            birthdate_inferred_from_age=False,
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='910324',
                    id_type=US_MO_DOC),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='SI00910324',
                    id_type=US_MO_SID),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='F00910324',
                    id_type=US_MO_FBI),
                entities.StatePersonExternalId.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='OLN0000000910324',
                    id_type=US_MO_OLN),
            ],
            aliases=[
                entities.StatePersonAlias.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    full_name='{"given_names": "KAONASHI"}',
                    alias_type=StatePersonAliasType.GIVEN_NAME,
                    alias_type_raw_text='GIVEN_NAME',
                )
            ],
            races=[
                entities.StatePersonRace.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    race=Race.ASIAN,
                    race_raw_text='A',
                ),
            ],
            ethnicities=[
                entities.StatePersonEthnicity.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    ethnicity=Ethnicity.NOT_HISPANIC,
                    ethnicity_raw_text='N',
                )
            ],
            sentence_groups=[
                sg_910324_19890825,
            ]
        )

        expected_people = [person_910324,
                           person_710448,
                           person_310261,
                           person_110035]

        self._populate_person_backedges(expected_people)

        # Act
        self._run_ingest_job_for_filename('tak001_offender_identification.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # TAK040 OFFENDER CYCLES
        ######################################
        # Arrange
        sg_110035_19890901.person = person_110035

        sg_110035_20010414 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='110035-20010414',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_110035,
        )
        person_110035.sentence_groups.append(sg_110035_20010414)

        sg_710448_20010414 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id='710448-20010414',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_710448,
        )
        person_710448.sentence_groups.append(sg_710448_20010414)

        # Act
        self._run_ingest_job_for_filename('tak040_offender_cycles.csv')

        # Assert
        self.assert_expected_db_people(expected_people)

        ##############################################################
        # TAK020_TAK023_TAK025_TAK026 OFFENDER SENTENCE INSTITUTION
        ##############################################################
        # Arrange
        sis_110035_19890901_1 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                external_id='110035-19890901-1',
                status=StateSentenceStatus.COMPLETED,
                status_raw_text='COMPLETED',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=1956, month=3, day=16),
                projected_min_release_date=
                datetime.date(year=9999, month=9, day=9),
                projected_max_release_date=
                datetime.date(year=9999, month=9, day=9),
                parole_eligibility_date=
                datetime.date(year=1956, month=4, day=25),
                county_code='US_MO_ST_LOUIS_COUNTY',
                max_length_days=3655253,
                is_life=True,
                person=person_110035,
                sentence_group=sg_110035_19890901,
            )
        sg_110035_19890901.incarceration_sentences.append(sis_110035_19890901_1)

        charge_110035_19890901 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            county_code='US_MO_JACKSON',
            ncic_code='0904',
            statute='10021040',
            description='TC: MURDER 1ST - FIST',
            classification_type=
            StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            classification_subtype='O',
            incarceration_sentences=[sis_110035_19890901_1],
            person=person_110035,
        )
        sis_110035_19890901_1.charges = [charge_110035_19890901]

        sis_110035_20010414_1 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                external_id='110035-20010414-1',
                status=StateSentenceStatus.COMPLETED,
                status_raw_text='COMPLETED',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=2003, month=1, day=10),
                projected_min_release_date=
                datetime.date(year=2007, month=1, day=2),
                projected_max_release_date=
                datetime.date(year=2007, month=1, day=2),
                parole_eligibility_date=
                datetime.date(year=2003, month=1, day=3),
                county_code='US_MO_ST_LOUIS_COUNTY',
                max_length_days=1461,
                is_life=False,
                person=person_110035,
                sentence_group=sg_110035_20010414,
            )
        sg_110035_20010414.incarceration_sentences.append(sis_110035_20010414_1)

        charge_110035_20010414 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            offense_date=datetime.date(year=2000, month=6, day=4),
            county_code='US_MO_ST_LOUIS_COUNTY',
            ncic_code='5299',
            statute='31020990',
            description='UNLAWFUL USE OF WEAPON',
            classification_type=
            StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            classification_subtype='D',
            incarceration_sentences=[sis_110035_20010414_1],
            person=person_110035,
        )
        sis_110035_20010414_1.charges = [charge_110035_20010414]

        sg_310261_19890821.person = person_310261

        sis_310261_19890821_3 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                external_id='310261-19890821-3',
                status=StateSentenceStatus.SERVING,
                status_raw_text='SERVING',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=2015, month=4, day=28),
                projected_min_release_date=
                datetime.date(year=2021, month=12, day=5),
                projected_max_release_date=
                datetime.date(year=2021, month=12, day=5),
                parole_eligibility_date=
                datetime.date(year=2016, month=12, day=6),
                county_code='US_MO_ST_LOUIS_CITY',
                max_length_days=1826,
                is_life=False,
                person=person_310261,
                sentence_group=sg_310261_19890821,
            )
        sg_310261_19890821.incarceration_sentences.append(sis_310261_19890821_3)

        charge_310261 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            offense_date=datetime.date(year=2014, month=10, day=4),
            county_code='US_MO_ST_LOUIS_CITY',
            ncic_code='3599',
            statute='91335990',
            description='POSSESSION OF CONTROLLED SUBSTANCE',
            classification_type=
            StateChargeClassificationType.INFRACTION,
            classification_type_raw_text='L',
            incarceration_sentences=[sis_310261_19890821_3],
            person=person_310261,
        )
        sis_310261_19890821_3.charges = [charge_310261]

        sis_710448_20010414_1 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                external_id='710448-20010414-1',
                status=StateSentenceStatus.COMPLETED,
                status_raw_text='COMPLETED',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=2005, month=5, day=13),
                projected_min_release_date=
                datetime.date(year=2009, month=10, day=22),
                projected_max_release_date=
                datetime.date(year=2009, month=10, day=22),
                parole_eligibility_date=
                datetime.date(year=2005, month=10, day=23),
                county_code='US_MO_ST_LOUIS_COUNTY',
                max_length_days=1461,
                is_life=False,
                person=person_710448,
                sentence_group=sg_710448_20010414,
            )
        charge_710448_1 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            offense_date=datetime.date(year=2000, month=7, day=31),
            county_code='US_MO_ST_LOUIS_COUNTY',
            ncic_code='3599',
            statute='32450990',
            description='POSSESSION OF C/S',
            classification_type=
            StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            classification_subtype='C',
            incarceration_sentences=[sis_710448_20010414_1],
            person=person_710448,
        )
        sis_710448_20010414_1.charges = [charge_710448_1]

        sis_710448_20010414_2 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                external_id='710448-20010414-2',
                status=StateSentenceStatus.COMPLETED,
                status_raw_text='COMPLETED',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=2006, month=12, day=14),
                projected_min_release_date=
                datetime.date(year=2011, month=12, day=6),
                projected_max_release_date=
                datetime.date(year=2011, month=12, day=6),
                parole_eligibility_date=
                datetime.date(year=2006, month=12, day=7),
                county_code='US_MO_ST_LOUIS_CITY',
                max_length_days=1826,
                is_life=False,
                person=person_710448,
                sentence_group=sg_710448_20010414,
            )
        charge_710448_2 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            offense_date=datetime.date(year=2005, month=3, day=1),
            ncic_code='3599',
            statute='32500990',
            description='TRAFFICKING 2ND DEGREE',
            classification_type=
            StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            classification_subtype='B',
            incarceration_sentences=[sis_710448_20010414_2],
            person=person_710448,
        )
        sis_710448_20010414_2.charges = [charge_710448_2]

        sis_710448_20010414_3 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                external_id='710448-20010414-3',
                status=StateSentenceStatus.COMPLETED,
                status_raw_text='COMPLETED',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=2006, month=12, day=14),
                projected_min_release_date=
                datetime.date(year=2010, month=12, day=6),
                projected_max_release_date=
                datetime.date(year=2010, month=12, day=6),
                parole_eligibility_date=
                datetime.date(year=2006, month=12, day=7),
                county_code='US_MO_ST_LOUIS_CITY',
                max_length_days=1461,
                is_life=False,
                person=person_710448,
                sentence_group=sg_710448_20010414,
            )
        charge_710448_3 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            offense_date=datetime.date(year=2005, month=3, day=1),
            ncic_code='4899',
            statute='27020990',
            description='RESISTING ARREST',
            classification_type=
            StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            classification_subtype='D',
            incarceration_sentences=[sis_710448_20010414_3],
            person=person_710448,
        )
        sis_710448_20010414_3.charges = [charge_710448_3]

        sg_710448_20010414.incarceration_sentences.append(sis_710448_20010414_1)
        sg_710448_20010414.incarceration_sentences.append(sis_710448_20010414_2)
        sg_710448_20010414.incarceration_sentences.append(sis_710448_20010414_3)

        sg_910324_19890825.person = person_910324

        sis_910324_19890825_1 = \
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                external_id='910324-19890825-1',
                status=StateSentenceStatus.COMPLETED,
                status_raw_text='COMPLETED',
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                date_imposed=datetime.date(year=1989, month=8, day=29),
                projected_min_release_date=
                datetime.date(year=1991, month=12, day=29),
                projected_max_release_date=
                datetime.date(year=1993, month=4, day=29),
                parole_eligibility_date=
                datetime.date(year=1989, month=4, day=30),
                county_code='US_MO_*',
                max_length_days=1461,
                is_life=False,
                person=person_910324,
                sentence_group=sg_910324_19890825,
            )
        sg_910324_19890825.incarceration_sentences.append(sis_910324_19890825_1)

        charge_910324 = entities.StateCharge.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            ncic_code='3572',
            statute='32040720',
            description='POSSESSION OF METHAMPHETAMINE',
            classification_type=
            StateChargeClassificationType.FELONY,
            classification_type_raw_text='F',
            classification_subtype='N',
            incarceration_sentences=[sis_910324_19890825_1],
            person=person_910324,
        )
        sis_910324_19890825_1.charges = [charge_910324]


        # Act
        self._run_ingest_job_for_filename(
            'tak022_tak023_tak025_tak026_offender_sentence_institution.csv')

        # Assert
        self.assert_expected_db_people(expected_people)
