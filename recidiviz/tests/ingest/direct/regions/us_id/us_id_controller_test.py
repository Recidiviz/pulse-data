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

from typing import Type

from recidiviz import IngestInfo
from recidiviz.common.constants.person_characteristics import Gender, Race, Ethnicity
from recidiviz.common.constants.state.external_id_types import US_ID_DOC
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import GcsfsDirectIngestController
from recidiviz.ingest.direct.regions.us_id.us_id_controller import UsIdController
from recidiviz.ingest.models.ingest_info import StatePerson, StatePersonExternalId, StatePersonRace, StateAlias, \
    StatePersonEthnicity
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

        # Rerun for sanity
        # pylint:disable=protected-access
        file_tags = self.controller._get_file_tag_rank_list()
        for file_tag in file_tags:
            self._run_ingest_job_for_filename(f'{file_tag}.csv')

        self.assert_expected_db_people(expected_people)
