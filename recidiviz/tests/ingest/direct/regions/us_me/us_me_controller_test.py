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
"""Unit and integration tests for US_ME direct ingest."""
import datetime
from typing import Type

from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.external_id_types import US_ME_DOC
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_me.us_me_controller import UsMeController
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)
from recidiviz.tests.ingest.direct.regions.utils import build_state_person_entity

_REGION_CODE_UPPER = "US_ME"


class TestUsMeController(BaseDirectIngestControllerTests):
    """Unit tests for each US_ME file to be ingested."""

    @classmethod
    def region_code(cls) -> str:
        return _REGION_CODE_UPPER.lower()

    @classmethod
    def controller_cls(cls) -> Type[BaseDirectIngestController]:
        return UsMeController

    @classmethod
    def schema_type(cls) -> SchemaType:
        return SchemaType.STATE

    def test_run_full_ingest_all_files_specific_order(self) -> None:
        # Arrange
        person_1 = build_state_person_entity(
            state_code=_REGION_CODE_UPPER,
            full_name='{"given_names": "FIRST1", "middle_names": "MIDDLE1", "name_suffix": "", "surname": "LAST1"}',
            gender=Gender.MALE,
            gender_raw_text="1",
            birthdate=datetime.date(year=1990, month=3, day=1),
            external_id="00000001",
            race_raw_text="1",
            race=Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
            ethnicity_raw_text="186",
            ethnicity=Ethnicity.HISPANIC,
            id_type=US_ME_DOC,
        )

        person_2 = build_state_person_entity(
            state_code=_REGION_CODE_UPPER,
            full_name='{"given_names": "FIRST2", "middle_names": "MIDDLE2", "name_suffix": "", "surname": "LAST2"}',
            gender=Gender.MALE,
            gender_raw_text="1",
            birthdate=datetime.date(year=1990, month=3, day=2),
            external_id="00000002",
            race_raw_text="2",
            race=Race.ASIAN,
            ethnicity_raw_text="186",
            ethnicity=Ethnicity.HISPANIC,
            id_type=US_ME_DOC,
        )

        person_3 = build_state_person_entity(
            state_code=_REGION_CODE_UPPER,
            full_name='{"given_names": "FIRST3", "middle_names": "MIDDLE3", "name_suffix": "", "surname": "LAST3"}',
            gender=Gender.MALE,
            gender_raw_text="1",
            birthdate=datetime.date(year=1990, month=3, day=3),
            external_id="00000003",
            race_raw_text="3",
            race=Race.BLACK,
            ethnicity_raw_text="186",
            ethnicity=Ethnicity.HISPANIC,
            id_type=US_ME_DOC,
        )

        person_4 = build_state_person_entity(
            state_code=_REGION_CODE_UPPER,
            full_name='{"given_names": "FIRST4", "middle_names": "MIDDLE4", "name_suffix": "", "surname": "LAST4"}',
            gender=Gender.FEMALE,
            gender_raw_text="2",
            birthdate=datetime.date(year=1990, month=3, day=4),
            external_id="00000004",
            race_raw_text="4",
            race=Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
            ethnicity_raw_text="187",
            ethnicity=Ethnicity.NOT_HISPANIC,
            id_type=US_ME_DOC,
        )

        person_5 = build_state_person_entity(
            state_code=_REGION_CODE_UPPER,
            full_name='{"given_names": "FIRST5", "middle_names": "MIDDLE5", "name_suffix": "", "surname": "LAST5"}',
            gender=Gender.FEMALE,
            gender_raw_text="2",
            birthdate=datetime.date(year=1990, month=3, day=5),
            external_id="00000005",
            race_raw_text="5",
            race=Race.WHITE,
            ethnicity_raw_text="187",
            ethnicity=Ethnicity.NOT_HISPANIC,
            id_type=US_ME_DOC,
        )

        person_6 = build_state_person_entity(
            state_code=_REGION_CODE_UPPER,
            full_name='{"given_names": "FIRST6", "middle_names": "MIDDLE6", "name_suffix": "", "surname": "LAST6"}',
            gender=Gender.FEMALE,
            gender_raw_text="2",
            birthdate=datetime.date(year=1990, month=3, day=6),
            external_id="00000006",
            race_raw_text="6",
            race=Race.EXTERNAL_UNKNOWN,
            ethnicity_raw_text="187",
            ethnicity=Ethnicity.NOT_HISPANIC,
            id_type=US_ME_DOC,
        )

        person_7 = build_state_person_entity(
            state_code=_REGION_CODE_UPPER,
            full_name='{"given_names": "FIRST7", "middle_names": "MIDDLE7", "name_suffix": "", "surname": "LAST7"}',
            gender=Gender.EXTERNAL_UNKNOWN,
            gender_raw_text="3",
            birthdate=datetime.date(year=1990, month=3, day=7),
            external_id="00000007",
            race_raw_text="8",
            race=Race.OTHER,
            ethnicity_raw_text="188",
            ethnicity=Ethnicity.EXTERNAL_UNKNOWN,
            id_type=US_ME_DOC,
        )

        person_8 = build_state_person_entity(
            state_code=_REGION_CODE_UPPER,
            full_name='{"given_names": "FIRST8", "middle_names": "MIDDLE8", "name_suffix": "", "surname": "LAST8"}',
            gender=Gender.EXTERNAL_UNKNOWN,
            gender_raw_text="3",
            birthdate=datetime.date(year=1990, month=3, day=8),
            external_id="00000008",
            race_raw_text="9",
            race=Race.OTHER,
            ethnicity_raw_text="188",
            ethnicity=Ethnicity.EXTERNAL_UNKNOWN,
            id_type=US_ME_DOC,
        )

        expected_people = [
            person_1,
            person_2,
            person_3,
            person_4,
            person_5,
            person_6,
            person_7,
            person_8,
        ]
        # Act
        self._run_ingest_job_for_filename("CLIENT")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # FULL RERUN FOR IDEMPOTENCE
        ######################################

        self._do_ingest_job_rerun_for_tags(self.controller.get_file_tag_rank_list())

        self.assert_expected_db_people(expected_people)
