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
from typing import Optional

from recidiviz.common.constants.person_characteristics import Ethnicity, Gender, Race
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.us_me.us_me_custom_enum_parsers import (
    DOC_FACILITY_LOCATION_TYPES,
    parse_admission_reason,
    parse_release_reason,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
    StateSentenceGroup,
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

    def test_parse_CURRENT_STATUS_incarceration_periods(self) -> None:
        expected_output = [
            # # Person 1 is released to supervision
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME", external_id="00000001", id_type="US_ME_DOC"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code="US_ME",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_ME",
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[
                                    StateIncarcerationPeriod(
                                        external_id="00000001-1",
                                        state_code="US_ME",
                                        status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                                        status_raw_text=None,
                                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                                        incarceration_type_raw_text="2",
                                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                                        specialized_purpose_for_incarceration_raw_text="INCARCERATED@@SENTENCE/DISPOSITION@@2",
                                        admission_date=date(2014, 10, 12),
                                        release_date=date(2015, 8, 20),
                                        county_code=None,
                                        facility="MAINE STATE PRISON",
                                        housing_unit="UNIT 1",
                                        custodial_authority_raw_text="2",
                                        custodial_authority=StateCustodialAuthority.STATE_PRISON,
                                        admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                                        admission_reason_raw_text="NULL@@INCARCERATED@@SENTENCE/DISPOSITION@@SOCIETY IN@@SENTENCE/DISPOSITION@@2",
                                        release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
                                        release_reason_raw_text="INCARCERATED@@SCCP@@TRANSFER@@SENTENCE/DISPOSITION@@2@@4",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            # Person 1 returns from supervision and transfers out to different facility
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME", external_id="00000001", id_type="US_ME_DOC"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code="US_ME",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_ME",
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[
                                    StateIncarcerationPeriod(
                                        external_id="00000001-2",
                                        state_code="US_ME",
                                        status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                                        status_raw_text=None,
                                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                                        specialized_purpose_for_incarceration_raw_text="INCARCERATED@@VIOLATION OF SCCP@@2",
                                        incarceration_type_raw_text="2",
                                        admission_date=date(2015, 9, 20),
                                        release_date=date(2016, 4, 1),
                                        county_code=None,
                                        facility="MAINE CORRECTIONAL CENTER",
                                        housing_unit="UNIT 2",
                                        custodial_authority_raw_text="4",
                                        custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                                        admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
                                        admission_reason_raw_text="SCCP@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@VIOLATION OF SCCP@@2",
                                        release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
                                        release_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@VIOLATION OF SCCP@@2@@2",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            # Person 1 release reason is Escape
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME", external_id="00000001", id_type="US_ME_DOC"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code="US_ME",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_ME",
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[
                                    StateIncarcerationPeriod(
                                        external_id="00000001-3",
                                        state_code="US_ME",
                                        status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                                        status_raw_text=None,
                                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                                        specialized_purpose_for_incarceration_raw_text="INCARCERATED@@POPULATION DISTRIBUTION@@2",
                                        incarceration_type_raw_text="2",
                                        admission_date=date(2016, 9, 20),
                                        release_date=date(2017, 12, 1),
                                        county_code=None,
                                        facility="SOUTHERN MAINE WOMEN'S REENTRY CENTER",
                                        housing_unit="SMWRC",
                                        custodial_authority_raw_text="8",
                                        custodial_authority=StateCustodialAuthority.STATE_PRISON,
                                        admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                                        admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@POPULATION DISTRIBUTION@@2",
                                        release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
                                        release_reason_raw_text="INCARCERATED@@ESCAPE@@ESCAPE@@POPULATION DISTRIBUTION@@2@@2",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME", external_id="00000001", id_type="US_ME_DOC"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code="US_ME",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_ME",
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[
                                    StateIncarcerationPeriod(
                                        external_id="00000001-4",
                                        state_code="US_ME",
                                        status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                                        status_raw_text=None,
                                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                                        specialized_purpose_for_incarceration_raw_text="INCARCERATED@@POPULATION DISTRIBUTION@@2",
                                        incarceration_type_raw_text="2",
                                        admission_date=date(2016, 9, 20),
                                        release_date=date(2017, 12, 1),
                                        county_code=None,
                                        facility="SOUTHERN MAINE WOMEN'S REENTRY CENTER",
                                        housing_unit="SMWRC",
                                        custodial_authority_raw_text="7",
                                        custodial_authority=StateCustodialAuthority.STATE_PRISON,
                                        admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                                        admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@POPULATION DISTRIBUTION@@2",
                                        release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
                                        release_reason_raw_text="INCARCERATED@@ESCAPE@@ESCAPE@@POPULATION DISTRIBUTION@@2@@2",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME", external_id="00000001", id_type="US_ME_DOC"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code="US_ME",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_ME",
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[
                                    StateIncarcerationPeriod(
                                        external_id="00000001-5",
                                        state_code="US_ME",
                                        status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                                        status_raw_text=None,
                                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                                        specialized_purpose_for_incarceration_raw_text="INCARCERATED@@POPULATION DISTRIBUTION@@2",
                                        incarceration_type_raw_text="2",
                                        admission_date=date(2016, 9, 20),
                                        release_date=date(2017, 12, 1),
                                        county_code=None,
                                        facility="SOUTHERN MAINE WOMEN'S REENTRY CENTER",
                                        housing_unit="SMWRC",
                                        custodial_authority_raw_text="9",
                                        custodial_authority=StateCustodialAuthority.STATE_PRISON,
                                        admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                                        admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@POPULATION DISTRIBUTION@@2",
                                        release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
                                        release_reason_raw_text="INCARCERATED@@ESCAPE@@ESCAPE@@POPULATION DISTRIBUTION@@2@@2",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME", external_id="00000001", id_type="US_ME_DOC"
                    )
                ],
                sentence_groups=[
                    StateSentenceGroup(
                        state_code="US_ME",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_sentences=[
                            StateIncarcerationSentence(
                                state_code="US_ME",
                                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                                incarceration_type=StateIncarcerationType.STATE_PRISON,
                                incarceration_periods=[
                                    StateIncarcerationPeriod(
                                        external_id="00000001-6",
                                        state_code="US_ME",
                                        status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                                        status_raw_text=None,
                                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                                        specialized_purpose_for_incarceration_raw_text="INCARCERATED@@POPULATION DISTRIBUTION@@2",
                                        incarceration_type_raw_text="2",
                                        admission_date=date(2016, 9, 20),
                                        release_date=date(2017, 12, 1),
                                        county_code=None,
                                        facility="SOUTHERN MAINE WOMEN'S REENTRY CENTER",
                                        housing_unit="SMWRC",
                                        custodial_authority_raw_text="13",
                                        custodial_authority=StateCustodialAuthority.STATE_PRISON,
                                        admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                                        admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@POPULATION DISTRIBUTION@@2",
                                        release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
                                        release_reason_raw_text="INCARCERATED@@ESCAPE@@ESCAPE@@POPULATION DISTRIBUTION@@2@@2",
                                    )
                                ],
                            )
                        ],
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test(
            "CURRENT_STATUS_incarceration_periods", expected_output
        )

    ######################################
    # Release Reasons Custom Parser
    ######################################
    @staticmethod
    def _build_release_reason_raw_text(
        current_status: Optional[str] = "NULL",
        next_status: Optional[str] = "NULL",
        next_movement_type: Optional[str] = "NULL",
        transfer_reason: Optional[str] = "NULL",
        location_type: Optional[str] = "NULL",
        next_location_type: Optional[str] = "NULL",
    ) -> str:
        return (
            f"{current_status}@@{next_status}@@{next_movement_type}"
            f"@@{transfer_reason}@@{location_type}@@{next_location_type}"
        )

    def test_parse_release_reason_sentence_served(self) -> None:
        # Next movement type is Discharge
        release_reason_raw_text = self._build_release_reason_raw_text(
            next_movement_type="Discharge"
        )
        self.assertEqual(
            parse_release_reason(release_reason_raw_text),
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

    def test_parse_release_reason_escape(self) -> None:
        # Next status is Escape
        release_reason_raw_text = self._build_release_reason_raw_text(
            next_status="Escape"
        )
        self.assertEqual(
            parse_release_reason(release_reason_raw_text),
            StateIncarcerationPeriodReleaseReason.ESCAPE,
        )

        # Next movement type is Escape
        release_reason_raw_text = self._build_release_reason_raw_text(
            next_movement_type="Escape"
        )
        self.assertEqual(
            parse_release_reason(release_reason_raw_text),
            StateIncarcerationPeriodReleaseReason.ESCAPE,
        )

    def test_parse_release_reason_temporary_custody(self) -> None:
        # Current status is County Jail and location type is a DOC Facility
        for location_type in DOC_FACILITY_LOCATION_TYPES:
            release_reason_raw_text = self._build_release_reason_raw_text(
                current_status="County Jail", location_type=location_type
            )
            self.assertEqual(
                parse_release_reason(release_reason_raw_text),
                StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
            )

        # Current status is County Jail and location type is not a DOC Facility
        release_reason_raw_text = self._build_release_reason_raw_text(
            current_status="County Jail", location_type="9"
        )
        self.assertEqual(
            parse_release_reason(release_reason_raw_text),
            StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
        )

        # Transfer reason is temporary custody
        release_reason_raw_text = self._build_release_reason_raw_text(
            transfer_reason="Safe Keepers",
        )
        self.assertEqual(
            parse_release_reason(release_reason_raw_text),
            StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

    def test_parse_release_reason_transfer_jurisdiction(self) -> None:
        # Next status is incarceration and next location is not a DOC Facility
        release_reason_raw_text = self._build_release_reason_raw_text(
            next_status="County Jail", location_type="9"
        )
        self.assertEqual(
            parse_release_reason(release_reason_raw_text),
            StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION,
        )

    def test_parse_release_reason_temporary_release(self) -> None:
        # Next movement type is Furlough or Furlough Hospital
        for next_movement_type in ["Furlough", "Furlough Hospital"]:
            release_reason_raw_text = self._build_release_reason_raw_text(
                next_movement_type=next_movement_type
            )
            self.assertEqual(
                parse_release_reason(release_reason_raw_text),
                StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            )

    def test_parse_release_reason_transfer(self) -> None:
        # Next movement type is Transfer
        release_reason_raw_text = self._build_release_reason_raw_text(
            next_movement_type="Transfer"
        )
        self.assertEqual(
            parse_release_reason(release_reason_raw_text),
            StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

    ######################################
    # Admission Reasons Custom Parser
    ######################################
    @staticmethod
    def _build_admission_reason_raw_text(
        previous_status: Optional[str] = "NULL",
        current_status: Optional[str] = "NULL",
        movement_type: Optional[str] = "NULL",
        transfer_type: Optional[str] = "NULL",
        transfer_reason: Optional[str] = "NULL",
        location_type: Optional[str] = "NULL",
    ) -> str:
        return (
            f"{previous_status}@@{current_status}@@{movement_type}@@{transfer_type}"
            f"@@{transfer_reason}@@{location_type}"
        )

    def test_parse_admission_reason_new_admission(self) -> None:
        # Transfer reason is Sentence/Disposition
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_reason="Sentence/Disposition"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

    def test_parse_admission_reason_admitted_from_supervision(self) -> None:
        # Transfer reason is Sentence/Disposition
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_reason="Sentence/Disposition", previous_status="Probation"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
        )

        # Next status is supervision
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_reason="DOC Transfer", previous_status="Probation"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
        )

    def test_parse_admission_reason_temporary_custody(self) -> None:
        # Current status is County Jail and location type is a DOC Facility
        for location_type in DOC_FACILITY_LOCATION_TYPES:
            admission_reason_raw_text = self._build_admission_reason_raw_text(
                current_status="County Jail", location_type=location_type
            )
            self.assertEqual(
                parse_admission_reason(admission_reason_raw_text),
                StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            )

        # Transfer reason is temporary custody
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_reason="Safe Keepers",
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
        )

    def test_parse_admission_reason_transfer_jurisdiction(self) -> None:
        # Transfer type is out of other jurisdiction transfer type
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_type="Non-DOC In"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
        )

        # Transfer reason is Other Jurisdiction
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_reason="Other Jurisdiction"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION,
        )

    def test_parse_admission_reason_temporary_release(self) -> None:
        # Movement type is Furlough or Furlough Hospital
        for movement_type in ["Furlough", "Furlough Hospital"]:
            admission_reason_raw_text = self._build_admission_reason_raw_text(
                movement_type=movement_type
            )
            self.assertEqual(
                parse_admission_reason(admission_reason_raw_text),
                StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE,
            )

    def test_parse_admission_reason_escape(self) -> None:
        # Previous status or movement type is Escape
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            movement_type="Escape"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        )

        admission_reason_raw_text = self._build_admission_reason_raw_text(
            previous_status="Escape"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        )

    def test_parse_admission_reason_probation_revocation(self) -> None:
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_reason="Violation of Probation"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        admission_reason_raw_text = self._build_admission_reason_raw_text(
            current_status="Partial Revocation - incarcerated"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

    def test_parse_admission_reason_parole_revocation(self) -> None:
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            transfer_reason="Violation of Parole"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        )

    def test_parse_admission_reason_transfer(self) -> None:
        # Next movement type is Transfer
        admission_reason_raw_text = self._build_admission_reason_raw_text(
            movement_type="Transfer"
        )
        self.assertEqual(
            parse_admission_reason(admission_reason_raw_text),
            StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )
