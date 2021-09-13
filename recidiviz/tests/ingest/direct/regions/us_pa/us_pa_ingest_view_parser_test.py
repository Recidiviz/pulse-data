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
import unittest

from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.external_id_types import (
    US_PA_CONTROL,
    US_PA_INMATE,
    US_PA_PBPP,
)
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
