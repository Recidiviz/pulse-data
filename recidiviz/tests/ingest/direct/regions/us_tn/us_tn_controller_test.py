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
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_tn.us_tn_controller import UsTnController
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state import entities
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)
from recidiviz.tests.ingest.direct.regions.utils import (
    add_incarceration_period_to_person,
    build_state_person_entity,
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
        ######################################
        # OffenderName
        ######################################
        # Arrange
        person_1 = build_state_person_entity(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST1", "middle_names": "MIDDLE1", "name_suffix": "", "surname": "LAST1"}',
            gender=Gender.FEMALE,
            gender_raw_text="F",
            birthdate=datetime.date(year=1985, month=3, day=7),
            id_type=US_TN_DOC,
            external_id="00000001",
            race_raw_text="W",
            race=Race.WHITE,
            ethnicity_raw_text="NOT_HISPANIC",
            ethnicity=Ethnicity.NOT_HISPANIC,
        )

        person_2 = build_state_person_entity(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST2", "middle_names": "MIDDLE2", "name_suffix": "", "surname": "LAST2"}',
            gender=Gender.MALE,
            gender_raw_text="M",
            birthdate=datetime.date(year=1969, month=2, day=1),
            external_id="00000002",
            id_type=US_TN_DOC,
            race_raw_text="B",
            race=Race.BLACK,
            ethnicity_raw_text="NOT_HISPANIC",
            ethnicity=Ethnicity.NOT_HISPANIC,
        )

        person_3 = build_state_person_entity(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST3", "middle_names": "MIDDLE3", "name_suffix": "", "surname": "LAST3"}',
            gender=Gender.FEMALE,
            gender_raw_text="F",
            birthdate=datetime.date(year=1947, month=1, day=11),
            external_id="00000003",
            id_type=US_TN_DOC,
            race_raw_text="A",
            race=Race.ASIAN,
            ethnicity_raw_text="NOT_HISPANIC",
            ethnicity=Ethnicity.NOT_HISPANIC,
        )

        person_4 = build_state_person_entity(
            state_code=_STATE_CODE_UPPER,
            full_name='{"given_names": "FIRST4", "middle_names": "MIDDLE4", "name_suffix": "", "surname": "LAST4"}',
            gender=Gender.MALE,
            gender_raw_text="M",
            birthdate=datetime.date(year=1994, month=3, day=12),
            ethnicity_raw_text="HISPANIC",
            ethnicity=Ethnicity.HISPANIC,
            external_id="00000004",
            id_type=US_TN_DOC,
        )

        expected_people = [person_1, person_2, person_3, person_4]

        # Act
        self._run_ingest_job_for_filename("OffenderName")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # OffenderMovementIncarcerationPeriod
        ######################################

        placheolder_sentence_group_2 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_2,
        )

        placeholder_incarceration_sentence_2 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                person=person_2,
                sentence_group=placheolder_sentence_group_2,
            )
        )
        placheolder_sentence_group_2.incarceration_sentences.append(
            placeholder_incarceration_sentence_2
        )
        person_2.sentence_groups.append(placheolder_sentence_group_2)

        add_incarceration_period_to_person(
            person=person_2,
            state_code=_STATE_CODE_UPPER,
            incarceration_sentence=placeholder_incarceration_sentence_2,
            external_id="00000002-2",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            admission_date=datetime.date(year=2021, month=6, day=20),
            release_date=None,
            facility="088",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="CTFA-NEWAD",
            release_reason=None,
            release_reason_raw_text="NONE-NONE",
        )

        placheolder_sentence_group_3 = entities.StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE_UPPER,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person=person_3,
        )

        placeholder_incarceration_sentence_3 = (
            entities.StateIncarcerationSentence.new_with_defaults(
                state_code=_STATE_CODE_UPPER,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                person=person_3,
                sentence_group=placheolder_sentence_group_3,
            )
        )
        placheolder_sentence_group_3.incarceration_sentences.append(
            placeholder_incarceration_sentence_3
        )
        person_3.sentence_groups.append(placheolder_sentence_group_3)

        # Person 3 moves from parole to facility.
        add_incarceration_period_to_person(
            person=person_3,
            state_code=_STATE_CODE_UPPER,
            incarceration_sentence=placeholder_incarceration_sentence_3,
            external_id="00000003-1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2010, month=2, day=5),
            release_date=datetime.date(year=2010, month=2, day=26),
            facility="79A",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="PAFA-VIOLW",
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            release_reason_raw_text="FAFA-JAILT",
        )
        # Person 3 transfers facilities.
        add_incarceration_period_to_person(
            person=person_3,
            state_code=_STATE_CODE_UPPER,
            incarceration_sentence=placeholder_incarceration_sentence_3,
            external_id="00000003-2",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2010, month=2, day=26),
            release_date=datetime.date(year=2010, month=4, day=6),
            facility="WTSP",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="FAFA-JAILT",
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            release_reason_raw_text="PAFA-PAVOK",
        )
        # Person 3 is released to supervision.
        add_incarceration_period_to_person(
            person=person_3,
            state_code=_STATE_CODE_UPPER,
            incarceration_sentence=placeholder_incarceration_sentence_3,
            external_id="00000003-3",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=datetime.date(year=2010, month=4, day=6),
            release_date=datetime.date(year=2010, month=11, day=4),
            facility="WTSP",
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="PAFA-PAVOK",
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
            release_reason_raw_text="FAPA-RELEL",
        )

        expected_people = [person_1, person_2, person_3, person_4]

        # Act
        self._run_ingest_job_for_filename("OffenderMovementIncarcerationPeriod")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # FULL RERUN FOR IDEMPOTENCE
        ######################################

        self._do_ingest_job_rerun_for_tags(self.controller.get_file_tag_rank_list())

        self.assert_expected_db_people(expected_people)
