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
from typing import Optional, Type

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
            full_name='{"given_names": "FIRST1", "middle_names": "MIDDLE1", "name_suffix": "", "surname": "LAST1"}',
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
            full_name='{"given_names": "FIRST2", "middle_names": "MIDDLE2", "name_suffix": "", "surname": "LAST2"}',
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
            full_name='{"given_names": "FIRST3", "middle_names": "MIDDLE3", "name_suffix": "", "surname": "LAST3"}',
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
            full_name='{"given_names": "FIRST4", "middle_names": "MIDDLE4", "name_suffix": "", "surname": "LAST4"}',
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

        ######################################
        # OffenderMovementIncarcerationPeriod
        ######################################
        _add_incarceration_period_to_person(
            person=person_2,
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

        # Person 3 moves from parole to facility.
        _add_incarceration_period_to_person(
            person=person_3,
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
        _add_incarceration_period_to_person(
            person=person_3,
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
        _add_incarceration_period_to_person(
            person=person_3,
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


def _add_incarceration_period_to_person(
    person: entities.StatePerson,
    external_id: str,
    status: StateIncarcerationPeriodStatus,
    admission_date: datetime.date,
    release_date: Optional[datetime.date],
    facility: str,
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason],
    admission_reason_raw_text: str,
    release_reason: Optional[StateIncarcerationPeriodReleaseReason],
    release_reason_raw_text: str,
) -> None:
    """Append an incarceration period to the person (updates the person entity in place)."""
    sentence_group = entities.StateSentenceGroup.new_with_defaults(
        state_code=_STATE_CODE_UPPER,
        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        person=person,
    )

    incarceration_sentence = entities.StateIncarcerationSentence.new_with_defaults(
        state_code=_STATE_CODE_UPPER,
        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        incarceration_type=StateIncarcerationType.STATE_PRISON,
        person=person,
        sentence_group=sentence_group,
    )

    incarceration_period = entities.StateIncarcerationPeriod.new_with_defaults(
        external_id=external_id,
        state_code=_STATE_CODE_UPPER,
        status=status,
        incarceration_type=StateIncarcerationType.STATE_PRISON,
        admission_date=admission_date,
        release_date=release_date,
        county_code=None,
        facility=facility,
        admission_reason=admission_reason,
        admission_reason_raw_text=admission_reason_raw_text,
        release_reason=release_reason,
        release_reason_raw_text=release_reason_raw_text,
        person=person,
        incarceration_sentences=[incarceration_sentence],
    )

    incarceration_sentence.incarceration_periods.append(incarceration_period)
    sentence_group.incarceration_sentences.append(incarceration_sentence)
    person.sentence_groups.append(sentence_group)
