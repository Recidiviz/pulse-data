# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Ingest view parser tests for US_MO direct ingest."""
import unittest
from datetime import date

from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonAlias,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    StateIngestViewParserTestBase,
)


class UsMoIngestViewParserTest(StateIngestViewParserTestBase, unittest.TestCase):
    """Parser unit tests for each US_MO ingest view file to be ingested."""

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_MO.value.upper()

    @property
    def test(self) -> unittest.TestCase:
        return self

    # ~~ Add parsing tests for new ingest view files here ~~
    # Parser tests must call self._run_parse_ingest_view_test() and follow the naming
    # convention `test_parse_<file_tag>`.
    def test_parse_tak001_offender_identification(self) -> None:
        expected = [
            StatePerson(
                state_code="US_MO",
                full_name='{"given_names": "FRANK", "middle_names": "", "name_suffix": "JR", "surname": "ABAGNALE"}',
                gender=Gender.MALE,
                gender_raw_text="M",
                birthdate=date(1971, 11, 20),
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_MO", external_id="110035", id_type="US_MO_DOC"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_MO",
                        race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
                        race_raw_text="I",
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_MO",
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="H",
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_MO",
                        full_name='{"given_names": "FRANK", "middle_names": "", "name_suffix": "JR", "surname": "ABAGNALE"}',
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                    )
                ],
            ),
            StatePerson(
                state_code="US_MO",
                full_name='{"given_names": "MARTHA", "middle_names": "HELEN", "name_suffix": "", "surname": "STEWART"}',
                gender=Gender.FEMALE,
                gender_raw_text="F",
                birthdate=date(1969, 6, 17),
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_MO", external_id="310261", id_type="US_MO_DOC"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_MO", race=Race.WHITE, race_raw_text="W"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_MO",
                        ethnicity=Ethnicity.EXTERNAL_UNKNOWN,
                        ethnicity_raw_text="U",
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_MO",
                        full_name='{"given_names": "MARTHA", "middle_names": "HELEN", "name_suffix": "", "surname": "STEWART"}',
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                    )
                ],
            ),
            StatePerson(
                state_code="US_MO",
                full_name='{"given_names": "JULES", "middle_names": "", "name_suffix": "", "surname": "WINNIFIELD"}',
                birthdate=date(1964, 8, 31),
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_MO", external_id="710448", id_type="US_MO_DOC"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_MO", race=Race.BLACK, race_raw_text="B"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_MO",
                        full_name='{"given_names": "JULES", "middle_names": "", "name_suffix": "", "surname": "WINNIFIELD"}',
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                    )
                ],
            ),
            StatePerson(
                state_code="US_MO",
                full_name='{"given_names": "KAONASHI", "middle_names": "", "name_suffix": "", "surname": ""}',
                gender=Gender.EXTERNAL_UNKNOWN,
                gender_raw_text="U",
                birthdate=date(1958, 2, 13),
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_MO", external_id="910324", id_type="US_MO_DOC"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_MO", race=Race.ASIAN, race_raw_text="A"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_MO",
                        ethnicity=Ethnicity.NOT_HISPANIC,
                        ethnicity_raw_text="N",
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_MO",
                        full_name='{"given_names": "KAONASHI", "middle_names": "", "name_suffix": "", "surname": ""}',
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test("tak001_offender_identification", expected)
