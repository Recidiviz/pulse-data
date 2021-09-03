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
"""Unit and integration tests for US_TN direct ingest."""
import datetime
from typing import Type

from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.external_id_types import US_TN_DOC
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_tn.us_tn_controller import UsTnController
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)

_STATE_CODE_UPPER = "US_TN"


class TestUsTnController(BaseDirectIngestControllerTests):
    """Unit tests for each US_TN file to be ingested."""

    @classmethod
    def region_code(cls) -> str:
        return _STATE_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[BaseDirectIngestController]:
        return UsTnController

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    def test_run_full_ingest_all_files_specific_order(self) -> None:
        self.maxDiff = None
        ######################################
        # OffenderName
        ######################################
        # Arrange
        person_1 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST1", "middle_names": "MIDDLE1", "surname": "LAST1"}',
            gender=Gender.FEMALE,
            gender_raw_text="F",
            birthdate=datetime.date(year=1985, month=3, day=7),
        )
        _add_external_id_to_person(person_1, "00000001")
        _add_race_to_person(person_1, race_raw_text="W", race=Race.WHITE)
        _add_ethnicity_to_person(
            person_1,
            ethnicity_raw_text="NOT_HISPANIC",
            ethnicity=Ethnicity.NOT_HISPANIC,
        )

        person_2 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST2", "middle_names": "MIDDLE2", "surname": "LAST2"}',
            gender=Gender.MALE,
            gender_raw_text="M",
            birthdate=datetime.date(year=1969, month=2, day=1),
        )
        _add_external_id_to_person(person_2, "00000002")
        _add_race_to_person(person_2, race_raw_text="B", race=Race.BLACK)
        _add_ethnicity_to_person(
            person_2,
            ethnicity_raw_text="NOT_HISPANIC",
            ethnicity=Ethnicity.NOT_HISPANIC,
        )

        person_3 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST3", "middle_names": "MIDDLE3", "surname": "LAST3"}',
            gender=Gender.FEMALE,
            gender_raw_text="F",
            birthdate=datetime.date(year=1947, month=1, day=11),
        )
        _add_external_id_to_person(person_3, "00000003")
        _add_race_to_person(person_3, race_raw_text="A", race=Race.ASIAN)
        _add_ethnicity_to_person(
            person_3,
            ethnicity_raw_text="NOT_HISPANIC",
            ethnicity=Ethnicity.NOT_HISPANIC,
        )

        person_4 = entities.StatePerson.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST4", "middle_names": "MIDDLE4", "surname": "LAST4"}',
            gender=Gender.MALE,
            gender_raw_text="M",
            birthdate=datetime.date(year=1994, month=3, day=12),
        )
        _add_external_id_to_person(person_4, "00000004")
        _add_ethnicity_to_person(
            person_4,
            ethnicity_raw_text="HISPANIC",
            ethnicity=Ethnicity.HISPANIC,
        )

        expected_people = [person_1, person_2, person_3, person_4]

        # Act
        self._run_ingest_job_for_filename("OffenderName")

        # Assert
        self.assert_expected_db_people(expected_people)


def _add_race_to_person(
    person: entities.StatePerson, race_raw_text: str, race: entities.Race
) -> None:
    """Append race to the person (updates the person entity in place)."""
    race_to_add: entities.StatePersonRace = entities.StatePersonRace.new_with_defaults(
        state_code=_STATE_CODE_UPPER,
        race=race,
        race_raw_text=race_raw_text,
        person=person,
    )
    person.races.append(race_to_add)


def _add_ethnicity_to_person(
    person: entities.StatePerson,
    ethnicity_raw_text: str,
    ethnicity: entities.Ethnicity,
) -> None:
    """Append ethnicity to the person (updates the person entity in place)."""
    ethnicity_to_add: entities.StatePersonEthnicity = (
        entities.StatePersonEthnicity.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            ethnicity=ethnicity,
            ethnicity_raw_text=ethnicity_raw_text,
            person=person,
        )
    )
    person.ethnicities.append(ethnicity_to_add)


def _add_external_id_to_person(person: entities.StatePerson, external_id: str) -> None:
    """Append external id to the person (updates the person entity in place)."""
    external_id_to_add: entities.StatePersonExternalId = (
        entities.StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            external_id=external_id,
            id_type=US_TN_DOC,
            person=person,
        )
    )
    person.external_ids.append(external_id_to_add)
