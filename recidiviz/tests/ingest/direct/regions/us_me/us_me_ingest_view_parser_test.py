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
"""Ingest view parser tests for US_ME direct ingest."""
import unittest
from datetime import date

from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    StateIngestViewParserTestBase,
)


class UsMeIngestViewParserTest(StateIngestViewParserTestBase, unittest.TestCase):
    """Parser unit tests for each US_ME ingest view file to be ingested."""

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_ME.value.upper()

    @property
    def test(self) -> unittest.TestCase:
        return self

    def test_parse_CLIENT(self) -> None:
        expected_output = [
            StatePerson(
                state_code=self.region_code(),
                full_name='{"given_names": "FIRST1", "middle_names": "MIDDLE1", "name_suffix": "", "surname": "LAST1"}',
                birthdate=date(1990, 3, 1),
                gender=Gender.MALE,
                gender_raw_text="1",
                external_ids=[
                    StatePersonExternalId(
                        state_code=self.region_code(),
                        external_id="00000001",
                        id_type="US_ME_DOC",
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code=self.region_code(),
                        race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
                        race_raw_text="1",
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code=self.region_code(),
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="186",
                    )
                ],
            ),
            StatePerson(
                state_code="US_ME",
                full_name='{"given_names": "FIRST2", "middle_names": "MIDDLE2", "name_suffix": "", "surname": "LAST2"}',
                birthdate=date(1990, 3, 2),
                gender=Gender.MALE,
                gender_raw_text="1",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000002",
                        id_type="US_ME_DOC",
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_ME",
                        race=Race.ASIAN,
                        race_raw_text="2",
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_ME",
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="186",
                    )
                ],
            ),
            StatePerson(
                state_code="US_ME",
                full_name='{"given_names": "FIRST3", "middle_names": "MIDDLE3", "name_suffix": "", "surname": "LAST3"}',
                birthdate=date(1990, 3, 3),
                gender=Gender.MALE,
                gender_raw_text="1",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000003",
                        id_type="US_ME_DOC",
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_ME",
                        race=Race.BLACK,
                        race_raw_text="3",
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_ME",
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="186",
                    )
                ],
            ),
            StatePerson(
                state_code="US_ME",
                full_name='{"given_names": "FIRST4", "middle_names": "MIDDLE4", "name_suffix": "", "surname": "LAST4"}',
                birthdate=date(1990, 3, 4),
                gender=Gender.FEMALE,
                gender_raw_text="2",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000004",
                        id_type="US_ME_DOC",
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_ME",
                        race=Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
                        race_raw_text="4",
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_ME",
                        ethnicity=Ethnicity.NOT_HISPANIC,
                        ethnicity_raw_text="187",
                    )
                ],
            ),
            StatePerson(
                state_code="US_ME",
                full_name='{"given_names": "FIRST5", "middle_names": "MIDDLE5", "name_suffix": "", "surname": "LAST5"}',
                birthdate=date(1990, 3, 5),
                gender=Gender.FEMALE,
                gender_raw_text="2",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000005",
                        id_type="US_ME_DOC",
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_ME",
                        race=Race.WHITE,
                        race_raw_text="5",
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_ME",
                        ethnicity=Ethnicity.NOT_HISPANIC,
                        ethnicity_raw_text="187",
                    )
                ],
            ),
            StatePerson(
                state_code="US_ME",
                full_name='{"given_names": "FIRST6", "middle_names": "MIDDLE6", "name_suffix": "", "surname": "LAST6"}',
                birthdate=date(1990, 3, 6),
                gender=Gender.FEMALE,
                gender_raw_text="2",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000006",
                        id_type="US_ME_DOC",
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_ME",
                        race=Race.EXTERNAL_UNKNOWN,
                        race_raw_text="6",
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_ME",
                        ethnicity=Ethnicity.NOT_HISPANIC,
                        ethnicity_raw_text="187",
                    )
                ],
            ),
            StatePerson(
                state_code="US_ME",
                full_name='{"given_names": "FIRST7", "middle_names": "MIDDLE7", "name_suffix": "", "surname": "LAST7"}',
                birthdate=date(1990, 3, 7),
                gender=Gender.EXTERNAL_UNKNOWN,
                gender_raw_text="3",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000007",
                        id_type="US_ME_DOC",
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_ME",
                        race=Race.OTHER,
                        race_raw_text="8",
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_ME",
                        ethnicity=Ethnicity.EXTERNAL_UNKNOWN,
                        ethnicity_raw_text="188",
                    )
                ],
            ),
            StatePerson(
                state_code="US_ME",
                full_name='{"given_names": "FIRST8", "middle_names": "MIDDLE8", "name_suffix": "", "surname": "LAST8"}',
                birthdate=date(1990, 3, 8),
                gender=Gender.EXTERNAL_UNKNOWN,
                gender_raw_text="3",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000008",
                        id_type="US_ME_DOC",
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_ME",
                        race=Race.OTHER,
                        race_raw_text="9",
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_ME",
                        ethnicity=Ethnicity.EXTERNAL_UNKNOWN,
                        ethnicity_raw_text="188",
                    )
                ],
            ),
        ]
        self._run_parse_ingest_view_test("CLIENT", expected_output)
