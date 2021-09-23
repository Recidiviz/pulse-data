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
"""Ingest view parser tests for US_TN direct ingest."""
import datetime
import unittest

from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
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


class UsTnIngestViewParserTest(StateIngestViewParserTestBase, unittest.TestCase):
    """Parser unit tests for each US_TN ingest view file to be ingested."""

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    @classmethod
    def region_code(cls) -> str:
        return "US_TN"

    @property
    def test(self) -> unittest.TestCase:
        return self

    def test_parse_OffenderName(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_TN",
                full_name='{"given_names": "FIRST1", "middle_names": "MIDDLE1", "name_suffix": "", "surname": "LAST1"}',
                birthdate=datetime.date(1985, 3, 7),
                gender=Gender.FEMALE,
                gender_raw_text="F",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000001", id_type="US_TN_DOC"
                    )
                ],
                races=[
                    StatePersonRace(
                        state_code="US_TN", race=Race.WHITE, race_raw_text="W"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_TN",
                        ethnicity=Ethnicity.NOT_HISPANIC,
                        ethnicity_raw_text="NOT_HISPANIC",
                    )
                ],
            ),
            StatePerson(
                state_code="US_TN",
                full_name='{"given_names": "FIRST2", "middle_names": "MIDDLE2", "name_suffix": "", "surname": "LAST2"}',
                birthdate=datetime.date(1969, 2, 1),
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000002", id_type="US_TN_DOC"
                    )
                ],
                aliases=[],
                races=[
                    StatePersonRace(
                        state_code="US_TN", race=Race.BLACK, race_raw_text="B"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_TN",
                        ethnicity=Ethnicity.NOT_HISPANIC,
                        ethnicity_raw_text="NOT_HISPANIC",
                    )
                ],
            ),
            StatePerson(
                state_code="US_TN",
                full_name='{"given_names": "FIRST3", "middle_names": "MIDDLE3", "name_suffix": "", "surname": "LAST3"}',
                birthdate=datetime.date(1947, 1, 11),
                gender=Gender.FEMALE,
                gender_raw_text="F",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000003", id_type="US_TN_DOC"
                    )
                ],
                aliases=[],
                races=[
                    StatePersonRace(
                        state_code="US_TN", race=Race.ASIAN, race_raw_text="A"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_TN",
                        ethnicity=Ethnicity.NOT_HISPANIC,
                        ethnicity_raw_text="NOT_HISPANIC",
                    )
                ],
            ),
            StatePerson(
                state_code="US_TN",
                full_name='{"given_names": "FIRST4", "middle_names": "MIDDLE4", "name_suffix": "", "surname": "LAST4"}',
                birthdate=datetime.date(1994, 3, 12),
                gender=Gender.MALE,
                gender_raw_text="M",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_TN", external_id="00000004", id_type="US_TN_DOC"
                    )
                ],
                ethnicities=[
                    StatePersonEthnicity(
                        state_code="US_TN",
                        ethnicity=Ethnicity.HISPANIC,
                        ethnicity_raw_text="HISPANIC",
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test("OffenderName", expected_output)

    # Add parsing tests for new ingest view files here #
