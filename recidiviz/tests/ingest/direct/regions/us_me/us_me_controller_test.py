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
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_me.us_me_controller import UsMeController
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)
from recidiviz.tests.ingest.direct.regions.utils import (
    add_incarceration_period_to_person,
    add_sentence_group_to_person_and_build_incarceration_sentence,
    build_state_person_entity,
)

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
        ######################################
        # CLIENT
        ######################################
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
        # CURRENT_STATUS_incarceration_period
        ######################################

        incarceration_sentence = (
            add_sentence_group_to_person_and_build_incarceration_sentence(
                _REGION_CODE_UPPER, person_1
            )
        )

        # Person 1 starts new period and is released to SCCP
        add_incarceration_period_to_person(
            person=person_1,
            state_code=_REGION_CODE_UPPER,
            incarceration_sentence=incarceration_sentence,
            external_id="00000001-1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2014, month=10, day=12),
            release_date=datetime.date(year=2015, month=8, day=20),
            facility="MAINE STATE PRISON",
            housing_unit="UNIT 1",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="2",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NULL@@INCARCERATED@@SENTENCE/DISPOSITION@@SOCIETY IN@@SENTENCE/DISPOSITION@@2",
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
            release_reason_raw_text="INCARCERATED@@SCCP@@TRANSFER@@SENTENCE/DISPOSITION@@2@@4",
            specialized_purpose_for_incarceration_raw_text="INCARCERATED@@SENTENCE/DISPOSITION@@2",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            incarceration_type_raw_text="2",
        )
        # Person 1 re-enters from SCCP.
        add_incarceration_period_to_person(
            person=person_1,
            state_code=_REGION_CODE_UPPER,
            incarceration_sentence=incarceration_sentence,
            external_id="00000001-2",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2015, month=9, day=20),
            release_date=datetime.date(year=2016, month=4, day=1),
            facility="MAINE CORRECTIONAL CENTER",
            housing_unit="UNIT 2",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="4",
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            admission_reason_raw_text="SCCP@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@VIOLATION OF SCCP@@2",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@VIOLATION OF SCCP@@2@@2",
            specialized_purpose_for_incarceration_raw_text="INCARCERATED@@VIOLATION OF SCCP@@2",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            incarceration_type_raw_text="2",
        )
        # Person 1 transfers from different facility and next status is Escape
        add_incarceration_period_to_person(
            person=person_1,
            state_code=_REGION_CODE_UPPER,
            incarceration_sentence=incarceration_sentence,
            external_id="00000001-3",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2016, month=9, day=20),
            release_date=datetime.date(year=2017, month=12, day=1),
            facility="SOUTHERN MAINE WOMEN'S REENTRY CENTER",
            housing_unit="SMWRC",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="8",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@POPULATION DISTRIBUTION@@2",
            release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
            release_reason_raw_text="INCARCERATED@@ESCAPE@@ESCAPE@@POPULATION DISTRIBUTION@@2@@2",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="INCARCERATED@@POPULATION DISTRIBUTION@@2",
            incarceration_type_raw_text="2",
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
        self._run_ingest_job_for_filename("CURRENT_STATUS_incarceration_periods")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # FULL RERUN FOR IDEMPOTENCE
        ######################################

        self._do_ingest_job_rerun_for_tags(self.controller.get_file_tag_rank_list())

        self.assert_expected_db_people(expected_people)
