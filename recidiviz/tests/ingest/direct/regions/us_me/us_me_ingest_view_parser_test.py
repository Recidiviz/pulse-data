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

from recidiviz.common.constants.shared_enums.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateAssessment,
    StateIncarcerationPeriod,
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
    StatePersonRace,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
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
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000001-1",
                        state_code="US_ME",
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
                        admission_reason_raw_text="NONE@@INCARCERATED@@SENTENCE/DISPOSITION@@SOCIETY IN@@SENTENCE/DISPOSITION@@2",
                        release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION,
                        release_reason_raw_text="INCARCERATED@@SCCP@@TRANSFER@@SENTENCE/DISPOSITION@@2@@4",
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
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000001-2",
                        state_code="US_ME",
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
                        admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
                        admission_reason_raw_text="SCCP@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@VIOLATION OF SCCP@@2",
                        release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
                        release_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@VIOLATION OF SCCP@@2@@2",
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
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000001-3",
                        state_code="US_ME",
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
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME", external_id="00000001", id_type="US_ME_DOC"
                    )
                ],
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000001-4",
                        state_code="US_ME",
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
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME", external_id="00000001", id_type="US_ME_DOC"
                    )
                ],
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000001-5",
                        state_code="US_ME",
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
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME", external_id="00000001", id_type="US_ME_DOC"
                    )
                ],
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000001-6",
                        state_code="US_ME",
                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                        specialized_purpose_for_incarceration_raw_text="INCARCERATED@@POPULATION DISTRIBUTION@@2",
                        incarceration_type_raw_text="2",
                        admission_date=date(2016, 9, 20),
                        release_date=date(2017, 12, 1),
                        county_code=None,
                        facility="SOUTHERN MAINE WOMEN'S REENTRY CENTER",
                        housing_unit="SMWRC",
                        custodial_authority_raw_text="4",
                        custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                        admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                        admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@POPULATION DISTRIBUTION@@2",
                        release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
                        release_reason_raw_text="INCARCERATED@@ESCAPE@@ESCAPE@@POPULATION DISTRIBUTION@@2@@2",
                    )
                ],
            ),
            # Open period
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME", external_id="00000001", id_type="US_ME_DOC"
                    )
                ],
                incarceration_periods=[
                    StateIncarcerationPeriod(
                        external_id="00000001-7",
                        state_code="US_ME",
                        incarceration_type=StateIncarcerationType.STATE_PRISON,
                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                        specialized_purpose_for_incarceration_raw_text="INCARCERATED@@CASE MANAGEMENT PLAN@@2",
                        incarceration_type_raw_text="2",
                        admission_date=date(2018, 1, 1),
                        facility="MAINE CORRECTIONAL CENTER",
                        custodial_authority_raw_text="4",
                        custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                        admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
                        admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@CASE MANAGEMENT PLAN@@2",
                        release_reason_raw_text=None,
                    )
                ],
            ),
        ]

        self._run_parse_ingest_view_test(
            "CURRENT_STATUS_incarceration_periods", expected_output
        )

    def test_parse_assessments(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000001",
                        id_type="US_ME_DOC",
                    )
                ],
                assessments=[
                    StateAssessment(
                        state_code="US_ME",
                        external_id="00000001-18435801161",
                        assessment_class=StateAssessmentClass.RISK,
                        assessment_class_raw_text="JUVENILE, MALE, COMMUNITY",
                        assessment_type=StateAssessmentType.LSIR,
                        assessment_type_raw_text="JUVENILE, MALE, COMMUNITY",
                        assessment_date=date(2009, 8, 12),
                        assessment_score=3,
                        assessment_level=StateAssessmentLevel.LOW,
                        assessment_level_raw_text="LOW",
                        assessment_metadata='{"LSI_RATING": "LOW", "LSI_RATING_APPROVED": "", "LSI_RATING_OVERRIDE": ""}',
                        conducting_agent=StateAgent(
                            state_code="US_ME",
                            external_id="7777",
                            agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                            full_name='{"given_names": "DAN", "middle_names": "L", "name_suffix": "", "surname": "WHITFORD"}',
                        ),
                    ),
                ],
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000002",
                        id_type="US_ME_DOC",
                    )
                ],
                assessments=[
                    StateAssessment(
                        state_code="US_ME",
                        external_id="00000002-3576501161",
                        assessment_class=StateAssessmentClass.RISK,
                        assessment_class_raw_text="ADULT, MALE, FACILITY",
                        assessment_type=StateAssessmentType.LSIR,
                        assessment_type_raw_text="ADULT, MALE, FACILITY",
                        assessment_date=date(2013, 2, 7),
                        assessment_score=14,
                        assessment_level=StateAssessmentLevel.MODERATE,
                        assessment_level_raw_text="MODERATE",
                        assessment_metadata='{"LSI_RATING": "LOW", "LSI_RATING_APPROVED": "", "LSI_RATING_OVERRIDE": "MODERATE"}',
                        conducting_agent=StateAgent(
                            state_code="US_ME",
                            external_id="1234",
                            agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                            full_name='{"given_names": "BEN", "middle_names": "", "name_suffix": "", "surname": "BROWNING"}',
                        ),
                    ),
                ],
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000002",
                        id_type="US_ME_DOC",
                    )
                ],
                assessments=[
                    StateAssessment(
                        state_code="US_ME",
                        external_id="00000002-3576501162",
                        assessment_class=StateAssessmentClass.RISK,
                        assessment_class_raw_text="ADULT, MALE, COMMUNITY",
                        assessment_type=StateAssessmentType.LSIR,
                        assessment_type_raw_text="ADULT, MALE, COMMUNITY",
                        assessment_date=date(2004, 12, 1),
                        assessment_score=10,
                        assessment_level=StateAssessmentLevel.HIGH,
                        assessment_level_raw_text="HIGH",
                        assessment_metadata='{"LSI_RATING": "ADMINISTRATIVE", "LSI_RATING_APPROVED": "HIGH", "LSI_RATING_OVERRIDE": "HIGH"}',
                        conducting_agent=StateAgent(
                            state_code="US_ME",
                            external_id="1234",
                            agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                            full_name='{"given_names": "BEN", "middle_names": "", "name_suffix": "", "surname": "BROWNING"}',
                        ),
                    ),
                ],
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000002",
                        id_type="US_ME_DOC",
                    )
                ],
                assessments=[
                    StateAssessment(
                        state_code="US_ME",
                        external_id="00000002-3576502151",
                        assessment_class=StateAssessmentClass.SEX_OFFENSE,
                        assessment_class_raw_text="STATIC 99",
                        assessment_type=StateAssessmentType.STATIC_99,
                        assessment_type_raw_text="STATIC 99",
                        assessment_date=date(2008, 7, 10),
                        assessment_score=0,
                        assessment_level=StateAssessmentLevel.MINIMUM,
                        assessment_level_raw_text="ADMINISTRATIVE",
                        assessment_metadata='{"LSI_RATING": "ADMINISTRATIVE", "LSI_RATING_APPROVED": "", "LSI_RATING_OVERRIDE": ""}',
                        conducting_agent=StateAgent(
                            state_code="US_ME",
                            external_id="1188",
                            agent_type=StateAgentType.PRESENT_WITHOUT_INFO,
                            full_name='{"given_names": "TIM", "middle_names": "TOM", "name_suffix": "", "surname": "HOEY"}',
                        ),
                    ),
                ],
            ),
        ]

        self._run_parse_ingest_view_test("assessments", expected_output)

    def test_parse_supervision_violations(self) -> None:
        expected_output = [
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000001",
                        id_type="US_ME_DOC",
                    ),
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_ME",
                        external_id="00000001-101",
                        violation_date=date(2018, 1, 2),
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_ME",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="TECHNICAL",
                            ),
                        ],
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code="US_ME",
                                external_id="00000001-101",
                                response_type_raw_text="VIOLATION NOT FOUND@@NONE",
                                response_date=date(2018, 1, 31),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.COURT,
                                deciding_body_type_raw_text="VIOLATION NOT FOUND",
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_ME",
                                        decision=StateSupervisionViolationResponseDecision.VIOLATION_UNFOUNDED,
                                        decision_raw_text="VIOLATION NOT FOUND@@NONE",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000001",
                        id_type="US_ME_DOC",
                    ),
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_ME",
                        external_id="00000001-102",
                        violation_date=date(2018, 12, 28),
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_ME",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="TECHNICAL",
                            ),
                        ],
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code="US_ME",
                                external_id="00000001-102",
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_type_raw_text="GRADUATED SANCTION BY OFFICER@@VIOLATION FOUND - CONDITIONS AMENDED",
                                response_date=date(2019, 2, 18),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
                                deciding_body_type_raw_text="GRADUATED SANCTION BY OFFICER",
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_ME",
                                        decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
                                        decision_raw_text="GRADUATED SANCTION BY OFFICER@@VIOLATION FOUND - CONDITIONS AMENDED",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000002",
                        id_type="US_ME_DOC",
                    ),
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_ME",
                        external_id="00000002-201",
                        violation_date=date(2019, 11, 5),
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_ME",
                                violation_type=StateSupervisionViolationType.MISDEMEANOR,
                                violation_type_raw_text="MISDEMEANOR",
                            ),
                        ],
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code="US_ME",
                                external_id="00000002-201",
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_type_raw_text="VIOLATION FOUND@@FULL REVOCATION",
                                response_subtype="DOC FACILITY",
                                response_date=date(2019, 12, 10),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.COURT,
                                deciding_body_type_raw_text="VIOLATION FOUND",
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_ME",
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="VIOLATION FOUND@@FULL REVOCATION",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000002",
                        id_type="US_ME_DOC",
                    ),
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_ME",
                        external_id="00000002-202",
                        violation_date=date(2009, 3, 27),
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_ME",
                                violation_type=StateSupervisionViolationType.TECHNICAL,
                                violation_type_raw_text="TECHNICAL",
                            ),
                        ],
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code="US_ME",
                                external_id="00000002-202",
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_type_raw_text="VIOLATION FOUND@@VIOLATION FOUND - CONDITIONS AMENDED",
                                response_date=date(2009, 4, 12),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.COURT,
                                deciding_body_type_raw_text="VIOLATION FOUND",
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_ME",
                                        decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
                                        decision_raw_text="VIOLATION FOUND@@VIOLATION FOUND - CONDITIONS AMENDED",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
            StatePerson(
                state_code="US_ME",
                external_ids=[
                    StatePersonExternalId(
                        state_code="US_ME",
                        external_id="00000002",
                        id_type="US_ME_DOC",
                    ),
                ],
                supervision_violations=[
                    StateSupervisionViolation(
                        state_code="US_ME",
                        external_id="00000002-203",
                        violation_date=date(2018, 4, 3),
                        supervision_violation_types=[
                            StateSupervisionViolationTypeEntry(
                                state_code="US_ME",
                                violation_type=StateSupervisionViolationType.FELONY,
                                violation_type_raw_text="FELONY",
                            ),
                        ],
                        supervision_violation_responses=[
                            StateSupervisionViolationResponse(
                                state_code="US_ME",
                                external_id="00000002-203",
                                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                                response_type_raw_text="VIOLATION FOUND@@FULL REVOCATION",
                                response_subtype="DOC FACILITY",
                                response_date=date(2018, 5, 24),
                                deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.COURT,
                                deciding_body_type_raw_text="VIOLATION FOUND",
                                supervision_violation_response_decisions=[
                                    StateSupervisionViolationResponseDecisionEntry(
                                        state_code="US_ME",
                                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                                        decision_raw_text="VIOLATION FOUND@@FULL REVOCATION",
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ]

        self._run_parse_ingest_view_test("supervision_violations", expected_output)
