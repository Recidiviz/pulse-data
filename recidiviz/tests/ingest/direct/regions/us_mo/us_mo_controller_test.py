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

from typing import Type, List

from recidiviz import IngestInfo
from recidiviz.common.constants.person_characteristics import Gender, Race, \
    Ethnicity
from recidiviz.common.constants.state.external_id_types import US_MO_DOC, \
    US_MO_OLN, US_MO_FBI, US_MO_SID
from recidiviz.common.constants.state.state_person_alias import \
    StatePersonAliasType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.regions.us_mo.us_mo_controller import \
    UsMoController
from recidiviz.ingest.models.ingest_info import StatePerson, \
    StatePersonExternalId, StatePersonRace, StateAlias, StatePersonEthnicity, \
    StateSentenceGroup
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
                                    state_sentence_group_id='19890901'
                                )
                            ]),
                StatePerson(state_person_id='310261',
                            surname='STEWART',
                            given_names='MARTHA',
                            middle_names='HELEN',
                            gender='F',
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
                                    state_sentence_group_id='19890821'
                                )
                            ]),
                StatePerson(state_person_id='710448',
                            surname='WINNIFIELD',
                            given_names='JULES',
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
                                    state_sentence_group_id='19890901'
                                )
                            ]),
                StatePerson(state_person_id='910324',
                            given_names='KAONASHI',
                            gender='U',
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
                                    state_sentence_group_id='19890825'
                                )
                            ]),
            ])

        self.run_parse_file_test(expected, 'tak001_offender_identification')


    def _populate_person_backedges(
            self, persons: List[entities.StatePerson]) -> None:
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
        person_110035 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "FRANK", "name_suffix": "JR", '
                      '"surname": "ABAGNALE"}',
            gender=Gender.MALE,
            gender_raw_text='M',
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
                entities.StateSentenceGroup.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='19890901',
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                )
            ]
        )

        person_310261 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "MARTHA", "middle_names": "HELEN", '
                      '"surname": "STEWART"}',
            gender=Gender.FEMALE,
            gender_raw_text='F',
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
                entities.StateSentenceGroup.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='19890821',
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                )
            ]
        )
        person_710448 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "JULES", "surname": "WINNIFIELD"}',
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
                    external_id='19890901',
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                )
            ]
        )
        person_910324 = entities.StatePerson.new_with_defaults(
            full_name='{"given_names": "KAONASHI"}',
            gender=Gender.EXTERNAL_UNKNOWN,
            gender_raw_text='U',
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
                entities.StateSentenceGroup.new_with_defaults(
                    state_code=_STATE_CODE_UPPER,
                    external_id='19890825',
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                )
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
