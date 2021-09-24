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
"""Ingest view parser tests for US_PA direct ingest."""
import datetime
import unittest

from recidiviz.common.constants.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
    ResidencyStatus,
)
from recidiviz.common.constants.state.external_id_types import (
    US_PA_CONTROL,
    US_PA_INMATE,
    US_PA_PBPP,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonAlias,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
    StateSentenceGroup,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    StateIngestViewParserTestBase,
)


class UsPaIngestViewParserTest(StateIngestViewParserTestBase, unittest.TestCase):
    """Parser unit tests for each US_PA ingest view file to be ingested."""

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    @classmethod
    def region_code(cls) -> str:
        return "US_PA"

    @property
    def test(self) -> unittest.TestCase:
        return self

    def test_parse_person_external_ids(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="123456", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="AB7413", id_type=US_PA_INMATE
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="123A", id_type=US_PA_PBPP
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="654321", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="GF3374", id_type=US_PA_INMATE
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="456B", id_type=US_PA_PBPP
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="445566", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="CJ1991", id_type=US_PA_INMATE
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="778899", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="889900", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="JE1989", id_type=US_PA_INMATE
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="345E", id_type=US_PA_PBPP
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="999Z", id_type=US_PA_PBPP
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="789C", id_type=US_PA_PBPP
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA",
                        external_id="888888888",
                        id_type=US_PA_INMATE,
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="888P", id_type=US_PA_PBPP
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="090909", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="9999999", id_type=US_PA_INMATE
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="060606", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="060607", id_type=US_PA_CONTROL
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="66666666", id_type=US_PA_INMATE
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="66666667", id_type=US_PA_INMATE
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="666P", id_type=US_PA_PBPP
                    ),
                    StatePersonExternalId(
                        state_code="US_PA", external_id="777M", id_type=US_PA_PBPP
                    ),
                ],
            ),
        ]

        self._run_parse_ingest_view_test("person_external_ids", expected_output)

    def test_parse_doc_person_info(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                current_address="123 EASY STREET, PITTSBURGH, PA 16161",
                full_name='{"given_names": "BERTRAND", "middle_names": "", "name_suffix": "", "surname": "RUSSELL"}',
                birthdate=datetime.date(1976, 3, 18),
                gender=Gender.MALE,
                gender_raw_text="MALE",
                residency_status=ResidencyStatus.PERMANENT,
                residency_status_raw_text="123 EASY STREET",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="123456", id_type="US_PA_CONT"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_PA",
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                        alias_type_raw_text="GIVEN_NAME",
                        full_name='{"given_names": "BERTRAND", "middle_names": "", "name_suffix": "", "surname": "RUSSELL"}',
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.BLACK, race_raw_text="BLACK"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="AB7413",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    ),
                    StateSentenceGroup(
                        external_id="BC8524",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                current_address="555 FLATBUSH DR, NEW YORK, NY 10031",
                full_name='{"given_names": "JEAN-PAUL", "middle_names": "", "name_suffix": "", "surname": "SARTRE"}',
                birthdate=datetime.date(1982, 10, 2),
                gender=Gender.MALE,
                gender_raw_text="MALE",
                residency_status=ResidencyStatus.PERMANENT,
                residency_status_raw_text="555 FLATBUSH DR",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="654321", id_type="US_PA_CONT"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_PA",
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                        alias_type_raw_text="GIVEN_NAME",
                        full_name='{"given_names": "JEAN-PAUL", "middle_names": "", "name_suffix": "", "surname": "SARTRE"}',
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.BLACK, race_raw_text="BLACK"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="GF3374",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                current_address="5000 SUNNY LANE, APT. 55D, PHILADELPHIA, PA 19129",
                full_name='{"given_names": "SOREN", "middle_names": "", "name_suffix": "JR", "surname": "KIERKEGAARD"}',
                birthdate=datetime.date(1991, 11, 20),
                gender=Gender.FEMALE,
                gender_raw_text="FEMALE",
                residency_status=ResidencyStatus.PERMANENT,
                residency_status_raw_text="5000 SUNNY LANE-APT. 55D",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="445566", id_type="US_PA_CONT"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_PA",
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                        alias_type_raw_text="GIVEN_NAME",
                        full_name='{"given_names": "SOREN", "middle_names": "", "name_suffix": "JR", "surname": "KIERKEGAARD"}',
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.WHITE, race_raw_text="WHITE"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="CJ1991",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                current_address="214 HAPPY PLACE, PHILADELPHIA, PA 19129",
                full_name='{"given_names": "JOHN", "middle_names": "", "name_suffix": "", "surname": "RAWLS"}',
                birthdate=datetime.date(1989, 6, 17),
                gender=Gender.MALE,
                gender_raw_text="MALE",
                residency_status=ResidencyStatus.PERMANENT,
                residency_status_raw_text="214 HAPPY PLACE",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="778899", id_type="US_PA_CONT"
                    )
                ],
                aliases=[
                    StatePersonAlias(
                        state_code="US_PA",
                        alias_type=StatePersonAliasType.GIVEN_NAME,
                        alias_type_raw_text="GIVEN_NAME",
                        full_name='{"given_names": "JOHN", "middle_names": "", "name_suffix": "", "surname": "RAWLS"}',
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_PA",
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="HISPANIC",
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        external_id="JE1989",
                        state_code="US_PA",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    )
                ],
            ),
        ]
        self._run_parse_ingest_view_test("doc_person_info", expected_output)

    def test_parse_dbo_Offender(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_PA",
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="123A", id_type="US_PA_PBPP"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.ASIAN, race_raw_text="A"
                    ),
                    StatePersonRace(
                        state_code="US_PA", race=Race.BLACK, race_raw_text="B"
                    ),
                ],
            ),
            StatePerson(
                state_code="US_PA",
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="456B", id_type="US_PA_PBPP"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA",
                        race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
                        race_raw_text="I",
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_PA",
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="H",
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                gender=Gender.FEMALE,
                gender_raw_text="F",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="789C", id_type="US_PA_PBPP"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.OTHER, race_raw_text="N"
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="345E", id_type="US_PA_PBPP"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.WHITE, race_raw_text="W"
                    )
                ],
            ),
            StatePerson(
                state_code="US_PA",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_PA", external_id="111A", id_type="US_PA_PBPP"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_PA", race=Race.WHITE, race_raw_text="W"
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test("dbo_Offender", expected_output)
