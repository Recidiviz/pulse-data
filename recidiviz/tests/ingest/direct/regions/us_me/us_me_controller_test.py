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
from typing import Optional, Type

from freezegun import freeze_time

from recidiviz.common.constants.shared_enums.charge import ChargeStatus
from recidiviz.common.constants.shared_enums.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
)
from recidiviz.common.constants.state.external_id_types import US_ME_DOC
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.regions.us_me.us_me_controller import UsMeController
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateCharge,
    StateCourtCase,
    StatePerson,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.tests.ingest.direct.regions.base_direct_ingest_controller_tests import (
    BaseDirectIngestControllerTests,
)
from recidiviz.tests.ingest.direct.regions.utils import (
    add_assessment_to_person,
    add_incarceration_period_to_person,
    add_incarceration_sentence_to_person,
    add_supervision_sentence_to_person,
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

    @freeze_time("2022-04-08")
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

        # Person 1 starts new period and is released to SCCP
        add_incarceration_period_to_person(
            person=person_1,
            state_code=_REGION_CODE_UPPER,
            external_id="00000001-1",
            admission_date=datetime.date(year=2014, month=10, day=12),
            release_date=datetime.date(year=2015, month=8, day=20),
            facility="MAINE STATE PRISON",
            housing_unit="UNIT 1",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="2",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NONE@@INCARCERATED@@SENTENCE/DISPOSITION@@SOCIETY IN@@SENTENCE/DISPOSITION@@2",
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
            external_id="00000001-2",
            admission_date=datetime.date(year=2015, month=9, day=20),
            release_date=datetime.date(year=2016, month=4, day=1),
            facility="MAINE CORRECTIONAL CENTER",
            housing_unit="UNIT 2",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="4",
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
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
            external_id="00000001-3",
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

        # Test all of the custodial authority mapped values
        add_incarceration_period_to_person(
            person=person_1,
            state_code=_REGION_CODE_UPPER,
            external_id="00000001-4",
            admission_date=datetime.date(year=2016, month=9, day=20),
            release_date=datetime.date(year=2017, month=12, day=1),
            facility="SOUTHERN MAINE WOMEN'S REENTRY CENTER",
            housing_unit="SMWRC",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="7",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@POPULATION DISTRIBUTION@@2",
            release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
            release_reason_raw_text="INCARCERATED@@ESCAPE@@ESCAPE@@POPULATION DISTRIBUTION@@2@@2",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="INCARCERATED@@POPULATION DISTRIBUTION@@2",
            incarceration_type_raw_text="2",
        )

        add_incarceration_period_to_person(
            person=person_1,
            state_code=_REGION_CODE_UPPER,
            external_id="00000001-5",
            admission_date=datetime.date(year=2016, month=9, day=20),
            release_date=datetime.date(year=2017, month=12, day=1),
            facility="SOUTHERN MAINE WOMEN'S REENTRY CENTER",
            housing_unit="SMWRC",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            custodial_authority_raw_text="9",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@POPULATION DISTRIBUTION@@2",
            release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
            release_reason_raw_text="INCARCERATED@@ESCAPE@@ESCAPE@@POPULATION DISTRIBUTION@@2@@2",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="INCARCERATED@@POPULATION DISTRIBUTION@@2",
            incarceration_type_raw_text="2",
        )

        add_incarceration_period_to_person(
            person=person_1,
            state_code=_REGION_CODE_UPPER,
            external_id="00000001-6",
            admission_date=datetime.date(year=2016, month=9, day=20),
            release_date=datetime.date(year=2017, month=12, day=1),
            facility="SOUTHERN MAINE WOMEN'S REENTRY CENTER",
            housing_unit="SMWRC",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            custodial_authority_raw_text="4",
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@POPULATION DISTRIBUTION@@2",
            release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
            release_reason_raw_text="INCARCERATED@@ESCAPE@@ESCAPE@@POPULATION DISTRIBUTION@@2@@2",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="INCARCERATED@@POPULATION DISTRIBUTION@@2",
            incarceration_type_raw_text="2",
        )

        add_incarceration_period_to_person(
            person=person_1,
            external_id="00000001-7",
            state_code="US_ME",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="INCARCERATED@@CASE MANAGEMENT PLAN@@2",
            incarceration_type_raw_text="2",
            admission_date=datetime.date(2018, 1, 1),
            release_date=None,
            facility="MAINE CORRECTIONAL CENTER",
            custodial_authority_raw_text="4",
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            admission_reason_raw_text="INCARCERATED@@INCARCERATED@@TRANSFER@@DOC TRANSFER@@CASE MANAGEMENT PLAN@@2",
            release_reason=None,
            release_reason_raw_text=None,
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
        # Assessments
        ######################################

        add_assessment_to_person(
            person=person_1,
            state_code="US_ME",
            external_id="00000001-18435801161",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="JUVENILE, MALE, COMMUNITY",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="JUVENILE, MALE, COMMUNITY",
            assessment_date=datetime.date(year=2009, month=8, day=12),
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
        )

        add_assessment_to_person(
            person=person_2,
            state_code="US_ME",
            external_id="00000002-3576501161",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="ADULT, MALE, FACILITY",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="ADULT, MALE, FACILITY",
            assessment_date=datetime.date(year=2013, month=2, day=7),
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
        )

        add_assessment_to_person(
            person=person_2,
            state_code="US_ME",
            external_id="00000002-3576501162",
            assessment_class=StateAssessmentClass.RISK,
            assessment_class_raw_text="ADULT, MALE, COMMUNITY",
            assessment_type=StateAssessmentType.LSIR,
            assessment_type_raw_text="ADULT, MALE, COMMUNITY",
            assessment_date=datetime.date(year=2004, month=12, day=1),
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
        )

        add_assessment_to_person(
            person=person_2,
            state_code="US_ME",
            external_id="00000002-3576502151",
            assessment_class=StateAssessmentClass.SEX_OFFENSE,
            assessment_class_raw_text="STATIC 99",
            assessment_type=StateAssessmentType.STATIC_99,
            assessment_type_raw_text="STATIC 99",
            assessment_date=datetime.date(year=2008, month=7, day=10),
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
        )

        # Act
        self._run_ingest_job_for_filename("assessments")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # Supervision Violations
        ######################################

        def _assemble_violation_instances(
            person: StatePerson,
            violation_id: str,
            violation_date: datetime.date,
            violation_type: StateSupervisionViolationType,
            violation_type_raw_text: str,
            response_type: Optional[StateSupervisionViolationResponseType],
            response_type_raw_text: str,
            response_subtype: Optional[str],
            response_date: datetime.date,
            deciding_body_type: Optional[
                StateSupervisionViolationResponseDecidingBodyType
            ],
            deciding_body_type_raw_text: Optional[str],
            decision: Optional[StateSupervisionViolationResponseDecision],
            decision_raw_text: str,
        ) -> StateSupervisionViolation:
            """Creates a hydrated graph of violation-related entities, with all cross-references populated,
            and returns the parent violation."""
            violation = StateSupervisionViolation(
                person=person,
                state_code="US_ME",
                external_id=violation_id,
                violation_date=violation_date,
            )

            violation_type_entry = StateSupervisionViolationTypeEntry(
                state_code="US_ME",
                violation_type=violation_type,
                violation_type_raw_text=violation_type_raw_text,
                supervision_violation=violation,
                person=person,
            )
            violation.supervision_violation_types = [violation_type_entry]

            violation_response = StateSupervisionViolationResponse(
                state_code="US_ME",
                external_id=violation_id,
                response_type=response_type,
                response_type_raw_text=response_type_raw_text,
                response_subtype=response_subtype,
                response_date=response_date,
                deciding_body_type=deciding_body_type,
                deciding_body_type_raw_text=deciding_body_type_raw_text,
                supervision_violation=violation,
                person=person,
            )
            violation.supervision_violation_responses = [violation_response]

            violation_response_decision = (
                StateSupervisionViolationResponseDecisionEntry(
                    state_code="US_ME",
                    decision=decision,
                    decision_raw_text=decision_raw_text,
                    supervision_violation_response=violation_response,
                    person=person,
                )
            )
            violation_response.supervision_violation_response_decisions = [
                violation_response_decision
            ]

            return violation

        violation_101 = _assemble_violation_instances(
            person=person_1,
            violation_id="00000001-101",
            violation_date=datetime.date(year=2018, month=1, day=2),
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
            response_type=None,
            response_type_raw_text="VIOLATION NOT FOUND@@NONE",
            response_subtype=None,
            response_date=datetime.date(year=2018, month=1, day=31),
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.COURT,
            deciding_body_type_raw_text="VIOLATION NOT FOUND@@NONE",
            decision=StateSupervisionViolationResponseDecision.VIOLATION_UNFOUNDED,
            decision_raw_text="VIOLATION NOT FOUND@@NONE",
        )

        violation_102 = _assemble_violation_instances(
            person=person_1,
            violation_id="00000001-102",
            violation_date=datetime.date(year=2018, month=12, day=28),
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="GRADUATED SANCTION BY OFFICER@@VIOLATION FOUND - CONDITIONS AMENDED",
            response_subtype=None,
            response_date=datetime.date(year=2019, month=2, day=18),
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
            deciding_body_type_raw_text="GRADUATED SANCTION BY OFFICER@@VIOLATION FOUND - CONDITIONS AMENDED",
            decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
            decision_raw_text="GRADUATED SANCTION BY OFFICER@@VIOLATION FOUND - CONDITIONS AMENDED",
        )

        person_1.supervision_violations = [violation_101, violation_102]

        violation_201 = _assemble_violation_instances(
            person=person_2,
            violation_id="00000002-201",
            violation_date=datetime.date(year=2019, month=11, day=5),
            violation_type=StateSupervisionViolationType.MISDEMEANOR,
            violation_type_raw_text="MISDEMEANOR",
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="VIOLATION FOUND@@FULL REVOCATION",
            response_subtype="DOC FACILITY",
            response_date=datetime.date(year=2019, month=12, day=10),
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.COURT,
            deciding_body_type_raw_text="VIOLATION FOUND@@FULL REVOCATION",
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="VIOLATION FOUND@@FULL REVOCATION",
        )

        violation_202 = _assemble_violation_instances(
            person=person_2,
            violation_id="00000002-202",
            violation_date=datetime.date(year=2009, month=3, day=27),
            violation_type=StateSupervisionViolationType.TECHNICAL,
            violation_type_raw_text="TECHNICAL",
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="VIOLATION FOUND@@VIOLATION FOUND - CONDITIONS AMENDED",
            response_subtype=None,
            response_date=datetime.date(year=2009, month=4, day=12),
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.COURT,
            deciding_body_type_raw_text="VIOLATION FOUND@@VIOLATION FOUND - CONDITIONS AMENDED",
            decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
            decision_raw_text="VIOLATION FOUND@@VIOLATION FOUND - CONDITIONS AMENDED",
        )

        violation_203 = _assemble_violation_instances(
            person=person_2,
            violation_id="00000002-203",
            violation_date=datetime.date(year=2018, month=4, day=3),
            violation_type=StateSupervisionViolationType.FELONY,
            violation_type_raw_text="FELONY",
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="VIOLATION FOUND@@FULL REVOCATION",
            response_subtype="DOC FACILITY",
            response_date=datetime.date(year=2018, month=5, day=24),
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.COURT,
            deciding_body_type_raw_text="VIOLATION FOUND@@FULL REVOCATION",
            decision=StateSupervisionViolationResponseDecision.REVOCATION,
            decision_raw_text="VIOLATION FOUND@@FULL REVOCATION",
        )

        person_2.supervision_violations = [violation_201, violation_202, violation_203]

        violation_204 = _assemble_violation_instances(
            person=person_3,
            violation_id="00000003-204",
            violation_date=datetime.date(year=2018, month=4, day=3),
            violation_type=StateSupervisionViolationType.FELONY,
            violation_type_raw_text="FELONY",
            response_type=None,
            response_type_raw_text="NONE@@NONE",
            response_subtype="DOC FACILITY",
            response_date=datetime.date(year=2018, month=5, day=24),
            deciding_body_type=None,
            deciding_body_type_raw_text=None,
            decision=StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN,
            decision_raw_text="NONE@@NONE",
        )

        violation_205 = _assemble_violation_instances(
            person=person_3,
            violation_id="00000003-205",
            violation_date=datetime.date(year=2018, month=4, day=3),
            violation_type=StateSupervisionViolationType.FELONY,
            violation_type_raw_text="FELONY",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_type_raw_text="NONE@@VIOLATION FOUND - NO SANCTION",
            response_subtype="DOC FACILITY",
            response_date=datetime.date(year=2018, month=5, day=24),
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.COURT,
            deciding_body_type_raw_text="NONE@@VIOLATION FOUND - NO SANCTION",
            decision=StateSupervisionViolationResponseDecision.NO_SANCTION,
            decision_raw_text="NONE@@VIOLATION FOUND - NO SANCTION",
        )

        person_3.supervision_violations = [violation_205, violation_204]

        # Act
        self._run_ingest_job_for_filename("supervision_violations")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # Incarceration Sentences
        ######################################

        person_1_incarceration_sentence_1 = add_incarceration_sentence_to_person(
            person=person_1,
            external_id="00000001-111123-371000",
            state_code="US_ME",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETE",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text=None,
            date_imposed=datetime.date(2019, 6, 8),
            start_date=datetime.date(2019, 6, 8),
            projected_min_release_date=datetime.date(2020, 9, 16),
            projected_max_release_date=datetime.date(2020, 12, 5),
            completion_date=datetime.date(2020, 9, 16),
            county_code="25",
            max_length_days=580,
            is_life=False,
            is_capital_punishment=None,
            earned_time_days=80,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2020-06-04 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2020-09-11 00:00:00", "TERM_INTAKE_DATE": "2019-04-18 08:42:00", "TERM_MAX_CUSTODY_RELEASE_DATE": "2021-12-14 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions=None,
        )

        person_1_charge_1 = StateCharge(
            external_id="00000001-111123-371000-377710",
            state_code="US_ME",
            status=ChargeStatus.CONVICTED,
            status_raw_text="CONVICTED",
            offense_date=datetime.date(2016, 5, 30),
            date_charged=None,
            county_code="25",
            statute="X_29-A_1234",
            description="OPERATING UNDER THE INFLUENCE (X) {1234}",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="C",
            is_sex_offense=False,
            charge_notes=None,
            person=person_1,
            incarceration_sentences=[person_1_incarceration_sentence_1],
        )

        person_1_charge_1_court_case = StateCourtCase(
            external_id="00000001-111123-371000",
            state_code="US_ME",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            court_type_raw_text=None,
            date_convicted=datetime.date(2019, 6, 5),
            county_code="25",
            judge=StateAgent(
                external_id="123",
                state_code="US_ME",
                agent_type=StateAgentType.JUDGE,
                agent_type_raw_text="JUDGE",
                full_name='{"given_names": "MICHAEL", "middle_names": "", "name_suffix": "", "surname": "SANDISON"}',
            ),
            person=person_1,
            charges=[person_1_charge_1],
        )

        person_1_charge_1.court_case = person_1_charge_1_court_case
        person_1_incarceration_sentence_1.charges = [person_1_charge_1]

        person_1_incarceration_sentence_2 = add_incarceration_sentence_to_person(
            person=person_1,
            external_id="00000001-111123-371002",
            state_code="US_ME",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETE",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text=None,
            date_imposed=datetime.date(2019, 6, 8),
            start_date=datetime.date(2019, 6, 8),
            projected_min_release_date=datetime.date(2019, 6, 8),
            projected_max_release_date=datetime.date(2019, 6, 8),
            completion_date=datetime.date(2019, 6, 8),
            county_code="1",
            max_length_days=0,
            is_life=False,
            is_capital_punishment=None,
            earned_time_days=0,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2020-06-04 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2020-09-11 00:00:00", "TERM_INTAKE_DATE": "2019-04-18 08:42:00", "TERM_MAX_CUSTODY_RELEASE_DATE": "2021-12-14 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions=None,
        )

        person_1_charge_2 = StateCharge(
            external_id="00000001-111123-371002-377723",
            state_code="US_ME",
            status=ChargeStatus.CONVICTED,
            status_raw_text="CONVICTED",
            offense_date=datetime.date(2019, 4, 30),
            date_charged=None,
            county_code="1",
            statute="E_29-D_9878",
            description="CRIMINAL TRESPASSING (E) {11000}",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="E",
            is_sex_offense=False,
            charge_notes=None,
            person=person_1,
            incarceration_sentences=[person_1_incarceration_sentence_2],
        )

        person_1_charge_2_court_case = StateCourtCase(
            external_id="00000001-111123-371002",
            state_code="US_ME",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            court_type_raw_text=None,
            date_convicted=datetime.date(2019, 6, 5),
            county_code="1",
            person=person_1,
            charges=[person_1_charge_2],
        )

        person_1_charge_2.court_case = person_1_charge_2_court_case
        person_1_incarceration_sentence_2.charges = [person_1_charge_2]

        person_1_incarceration_sentence_3 = add_incarceration_sentence_to_person(
            person=person_1,
            external_id="00000001-111123-371006",
            state_code="US_ME",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETE",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="COUNTY JAIL",
            date_imposed=datetime.date(2016, 7, 6),
            start_date=datetime.date(2016, 7, 6),
            projected_min_release_date=datetime.date(2016, 10, 19),
            projected_max_release_date=datetime.date(2017, 1, 1),
            completion_date=datetime.date(2016, 10, 19),
            county_code=None,
            max_length_days=0,
            is_life=False,
            is_capital_punishment=None,
            earned_time_days=0,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2020-06-04 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2020-09-11 00:00:00", "TERM_INTAKE_DATE": "2019-04-18 08:42:00", "TERM_MAX_CUSTODY_RELEASE_DATE": "2021-12-14 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions="CANNOT OPERATE A MOTOR VEHICLE\\NNEED STABLE EMPLOYMENT FOR Z MONTHS\\NSOMETHING ABOUT SUBSTANCE USE",
        )

        person_1_charge_3 = StateCharge(
            external_id="00000001-111123-371006-170352",
            state_code="US_ME",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            offense_date=datetime.date(2016, 5, 30),
            date_charged=None,
            county_code=None,
            statute="X_29-A_1234",
            description="OPERATING UNDER THE INFLUENCE (X) {1234}",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="C",
            is_sex_offense=False,
            charge_notes=None,
            person=person_1,
            incarceration_sentences=[person_1_incarceration_sentence_3],
        )

        person_1_charge_3_court_case = StateCourtCase(
            external_id="00000001-111123-371006",
            state_code="US_ME",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            court_type_raw_text=None,
            date_convicted=datetime.date(2016, 6, 24),
            county_code=None,
            judge=StateAgent(
                external_id="123",
                state_code="US_ME",
                agent_type=StateAgentType.JUDGE,
                agent_type_raw_text="JUDGE",
                full_name='{"given_names": "MICHAEL", "middle_names": "", "name_suffix": "", "surname": "SANDISON"}',
            ),
            person=person_1,
            charges=[person_1_charge_3],
        )

        person_1_charge_3.court_case = person_1_charge_3_court_case
        person_1_incarceration_sentence_3.charges = [person_1_charge_3]

        person_2_incarceration_sentence_1 = add_incarceration_sentence_to_person(
            person=person_2,
            external_id="00000002-222123-542890",
            state_code="US_ME",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETE",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="COUNTY JAIL",
            date_imposed=datetime.date(2020, 9, 25),
            start_date=datetime.date(2020, 9, 25),
            projected_min_release_date=datetime.date(2020, 11, 18),
            projected_max_release_date=datetime.date(2020, 12, 23),
            completion_date=datetime.date(2020, 11, 18),
            county_code=None,
            max_length_days=90,
            is_life=False,
            is_capital_punishment=None,
            earned_time_days=0,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2018-08-24 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2018-11-01 00:00:00", "TERM_INTAKE_DATE": "", "TERM_MAX_CUSTODY_RELEASE_DATE": "2020-02-23 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions=None,
        )

        person_2_charge_1 = StateCharge(
            external_id="00000002-222123-542890-858088",
            state_code="US_ME",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            offense_date=datetime.date(2017, 6, 26),
            date_charged=None,
            county_code=None,
            statute="E_29-B_2411",
            description="CARRYING AN UNREGISTERED FIREARM (E) {5555}",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="E",
            is_sex_offense=False,
            charge_notes=None,
            person=person_2,
            incarceration_sentences=[person_2_incarceration_sentence_1],
        )

        person_2_charge_1_court_case = StateCourtCase(
            external_id="00000002-222123-542890",
            state_code="US_ME",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            court_type_raw_text=None,
            date_convicted=datetime.date(2020, 9, 25),
            county_code=None,
            person=person_2,
            charges=[person_2_charge_1],
        )

        person_2_charge_1.court_case = person_2_charge_1_court_case
        person_2_incarceration_sentence_1.charges = [person_2_charge_1]

        person_2_incarceration_sentence_2 = add_incarceration_sentence_to_person(
            person=person_2,
            external_id="00000002-222123-542894",
            state_code="US_ME",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETE",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            incarceration_type_raw_text="COUNTY JAIL",
            date_imposed=datetime.date(2020, 9, 25),
            start_date=datetime.date(2020, 9, 25),
            projected_min_release_date=datetime.date(2020, 11, 18),
            projected_max_release_date=datetime.date(2020, 12, 23),
            completion_date=datetime.date(2020, 11, 18),
            county_code=None,
            max_length_days=0,
            is_life=False,
            is_capital_punishment=None,
            earned_time_days=0,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2018-08-24 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2018-11-01 00:00:00", "TERM_INTAKE_DATE": "", "TERM_MAX_CUSTODY_RELEASE_DATE": "2020-02-23 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions=None,
        )

        person_2_charge_2 = StateCharge(
            external_id="00000002-222123-542894-333662",
            state_code="US_ME",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            offense_date=datetime.date(2019, 12, 10),
            date_charged=None,
            county_code=None,
            statute="X_29-A_1234",
            description="OPERATING UNDER THE INFLUENCE (X) {1234}",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="C",
            is_sex_offense=False,
            charge_notes=None,
            person=person_2,
            incarceration_sentences=[person_2_incarceration_sentence_2],
        )

        person_2_charge_2_court_case = StateCourtCase(
            external_id="00000002-222123-542894",
            state_code="US_ME",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            court_type_raw_text=None,
            date_convicted=datetime.date(2020, 9, 25),
            county_code=None,
            person=person_2,
            charges=[person_2_charge_2],
        )

        person_2_charge_2.court_case = person_2_charge_2_court_case
        person_2_incarceration_sentence_2.charges = [person_2_charge_2]

        person_2_incarceration_sentence_3 = add_incarceration_sentence_to_person(
            person=person_2,
            external_id="00000002-222123-542898",
            state_code="US_ME",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETE",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text=None,
            date_imposed=datetime.date(2020, 1, 4),
            start_date=datetime.date(2020, 1, 4),
            projected_min_release_date=datetime.date(2020, 3, 10),
            projected_max_release_date=datetime.date(2020, 4, 3),
            completion_date=datetime.date(2020, 4, 3),
            county_code=None,
            max_length_days=0,
            is_life=False,
            is_capital_punishment=None,
            earned_time_days=0,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2018-08-24 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2018-11-01 00:00:00", "TERM_INTAKE_DATE": "", "TERM_MAX_CUSTODY_RELEASE_DATE": "2020-02-23 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions=None,
        )

        person_2_charge_3 = StateCharge(
            external_id="00000002-222123-542898-858088",
            state_code="US_ME",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            offense_date=datetime.date(2017, 6, 26),
            date_charged=None,
            county_code=None,
            statute="E_29-B_2411",
            description="CARRYING AN UNREGISTERED FIREARM (E) {5555}",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="E",
            is_sex_offense=False,
            charge_notes=None,
            person=person_2,
            incarceration_sentences=[person_2_incarceration_sentence_3],
        )

        person_2_charge_3_court_case = StateCourtCase(
            external_id="00000002-222123-542898",
            state_code="US_ME",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            court_type_raw_text=None,
            date_convicted=datetime.date(2019, 11, 2),
            county_code=None,
            judge=StateAgent(
                external_id="456",
                state_code="US_ME",
                agent_type=StateAgentType.JUSTICE,
                agent_type_raw_text="JUSTICE",
                full_name='{"given_names": "MARCUS", "middle_names": "", "name_suffix": "", "surname": "EOIN"}',
            ),
            person=person_2,
            charges=[person_2_charge_3],
        )

        person_2_charge_3.court_case = person_2_charge_3_court_case
        person_2_incarceration_sentence_3.charges = [person_2_charge_3]

        person_2_incarceration_sentence_4 = add_incarceration_sentence_to_person(
            person=person_2,
            external_id="00000002-222456-542892",
            state_code="US_ME",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="COMPLETE",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="EARLY TERMINATION",
            date_imposed=datetime.date(2012, 4, 8),
            start_date=datetime.date(2012, 4, 8),
            projected_min_release_date=datetime.date(2012, 5, 26),
            projected_max_release_date=datetime.date(2012, 6, 21),
            completion_date=datetime.date(2012, 5, 26),
            county_code=None,
            max_length_days=0,
            is_life=False,
            is_capital_punishment=None,
            earned_time_days=0,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2012-10-15 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2012-05-26 00:00:00", "TERM_INTAKE_DATE": "", "TERM_MAX_CUSTODY_RELEASE_DATE": "2012-06-24 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions=None,
        )

        person_2_charge_4 = StateCharge(
            external_id="00000002-222456-542892-123456",
            state_code="US_ME",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            offense_date=datetime.date(2011, 8, 2),
            date_charged=None,
            county_code=None,
            statute="D_29-C_3421",
            description="BREAKING AND ENTERING (D) {4858}",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="D",
            is_sex_offense=False,
            charge_notes=None,
            person=person_2,
            incarceration_sentences=[person_2_incarceration_sentence_4],
        )

        person_2_charge_4_court_case = StateCourtCase(
            external_id="00000002-222456-542892",
            state_code="US_ME",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            date_convicted=datetime.date(2012, 4, 8),
            county_code=None,
            person=person_2,
            charges=[person_2_charge_4],
        )

        person_2_charge_4.court_case = person_2_charge_4_court_case
        person_2_incarceration_sentence_4.charges = [person_2_charge_4]

        # Act
        self._run_ingest_job_for_filename("incarceration_sentences")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # Supervision Sentences
        ######################################

        person_1_supervision_sentence_1 = add_supervision_sentence_to_person(
            person=person_1,
            external_id="00000001-111123-371006",
            state_code="US_ME",
            status=StateSentenceStatus.REVOKED,
            status_raw_text="PARTIAL REVOCATION - TERMINATE@@COMPLETE",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            supervision_type_raw_text=None,
            date_imposed=datetime.date(2016, 7, 6),
            start_date=datetime.date(2016, 7, 6),
            projected_completion_date=datetime.date(2019, 6, 5),
            completion_date=datetime.date(2019, 6, 5),
            county_code=None,
            max_length_days=730,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2020-06-04 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2020-09-11 00:00:00", "TERM_INTAKE_DATE": "2019-04-18 08:42:00", "TERM_MAX_CUSTODY_RELEASE_DATE": "2021-12-14 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions="CANNOT OPERATE A MOTOR VEHICLE\\NNEED STABLE EMPLOYMENT FOR Z MONTHS\\NSOMETHING ABOUT SUBSTANCE USE",
        )

        # This is the same charge, entity matched
        person_1_s_charge_1 = person_1_charge_3
        person_1_s_charge_1.supervision_sentences = [person_1_supervision_sentence_1]

        # This is the same court case, entity matched
        person_1_s_charge_1_court_case = person_1_charge_3_court_case

        person_1_s_charge_1.court_case = person_1_s_charge_1_court_case
        person_1_supervision_sentence_1.charges = [person_1_s_charge_1]

        person_2_supervision_sentence_1 = add_supervision_sentence_to_person(
            person=person_2,
            external_id="00000002-222123-542894",
            state_code="US_ME",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="NONE@@COMPLETE",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            supervision_type_raw_text=None,
            date_imposed=datetime.date(2020, 9, 25),
            start_date=datetime.date(2020, 9, 25),
            projected_completion_date=datetime.date(2021, 11, 16),
            completion_date=datetime.date(2021, 11, 16),
            county_code=None,
            max_length_days=365,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2018-08-24 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2018-11-01 00:00:00", "TERM_INTAKE_DATE": "", "TERM_MAX_CUSTODY_RELEASE_DATE": "2020-02-23 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions=None,
        )

        # This is the same charge, entity matched
        person_2_s_charge_1 = person_2_charge_2
        person_2_s_charge_1.supervision_sentences = [person_2_supervision_sentence_1]

        # This is the same court case, entity matched
        person_2_s_charge_1_court_case = person_2_charge_2_court_case

        person_2_s_charge_1.court_case = person_2_s_charge_1_court_case
        person_2_supervision_sentence_1.charges = [person_2_s_charge_1]

        person_2_supervision_sentence_2 = add_supervision_sentence_to_person(
            person=person_2,
            external_id="00000002-222123-542898",
            state_code="US_ME",
            status=StateSentenceStatus.REVOKED,
            status_raw_text="PARTIAL REVOCATION - TERMINATE@@COMPLETE",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            supervision_type_raw_text=None,
            date_imposed=datetime.date(2020, 1, 4),
            start_date=datetime.date(2020, 1, 4),
            projected_completion_date=datetime.date(2020, 9, 25),
            completion_date=datetime.date(2020, 9, 25),
            county_code=None,
            max_length_days=547,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2018-08-24 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2018-11-01 00:00:00", "TERM_INTAKE_DATE": "", "TERM_MAX_CUSTODY_RELEASE_DATE": "2020-02-23 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions=None,
        )

        # This is the same charge, entity matched
        person_2_s_charge_2 = person_2_charge_3
        person_2_s_charge_2.supervision_sentences = [person_2_supervision_sentence_2]

        # This is the same court case, entity matched
        person_2_s_charge_2_court_case = person_2_charge_3_court_case

        person_2_s_charge_2.court_case = person_2_s_charge_2_court_case
        person_2_supervision_sentence_2.charges = [person_2_s_charge_2]

        person_2_supervision_sentence_3 = add_supervision_sentence_to_person(
            person=person_2,
            external_id="00000002-222456-542892",
            state_code="US_ME",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="NONE@@COMPLETE",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            supervision_type_raw_text=None,
            date_imposed=datetime.date(2012, 4, 8),
            start_date=datetime.date(2012, 4, 8),
            projected_completion_date=datetime.date(2013, 5, 25),
            completion_date=datetime.date(2013, 5, 25),
            county_code=None,
            max_length_days=365,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", "TERM_COMMUNITY_RELEASE_DATE": "2012-10-15 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2012-05-26 00:00:00", "TERM_INTAKE_DATE": "", "TERM_MAX_CUSTODY_RELEASE_DATE": "2012-06-24 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions=None,
        )

        # This is the same charge, entity matched
        person_2_s_charge_3 = person_2_charge_4
        person_2_s_charge_3.supervision_sentences = [person_2_supervision_sentence_3]

        # This is the same court case, entity matched
        person_2_s_charge_3_court_case = person_2_charge_4_court_case

        person_2_s_charge_3.court_case = person_2_s_charge_3_court_case
        person_2_supervision_sentence_3.charges = [person_2_s_charge_3]

        person_2_supervision_sentence_4 = add_supervision_sentence_to_person(
            person=person_2,
            external_id="00000002-222456-542896",
            state_code="US_ME",
            status=StateSentenceStatus.COMPLETED,
            status_raw_text="EARLY TERMINATION@@COMPLETE",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            supervision_type_raw_text=None,
            date_imposed=datetime.date(2011, 4, 8),
            start_date=datetime.date(2012, 5, 25),
            projected_completion_date=datetime.date(2012, 12, 5),
            completion_date=datetime.date(2012, 12, 5),
            county_code=None,
            max_length_days=365,
            sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "542892", "TERM_COMMUNITY_RELEASE_DATE": "2012-10-15 00:00:00", "TERM_EARLY_CUSTODY_RELEASE_DATE": "2012-05-26 00:00:00", "TERM_INTAKE_DATE": "", "TERM_MAX_CUSTODY_RELEASE_DATE": "2012-06-24 00:00:00", "TERM_STATUS": "COMPLETE"}',
            conditions="BI-WEEKLY THERAPY\\NBI-WEEKLY THERAPY\\NMUST LIVE WITHIN X MILES OF Y",
        )

        person_2_s_charge_4 = StateCharge(
            external_id="00000002-222456-542896-907131",
            state_code="US_ME",
            status=ChargeStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            offense_date=datetime.date(2011, 8, 2),
            date_charged=None,
            county_code=None,
            statute="D_29-C_3421",
            description="BREAKING AND ENTERING (D) {4858}",
            classification_type=StateChargeClassificationType.MISDEMEANOR,
            classification_type_raw_text="D",
            offense_type=None,
            is_sex_offense=False,
            charge_notes=None,
            person=person_2,
            supervision_sentences=[person_2_supervision_sentence_4],
        )

        person_2_s_charge_4_court_case = StateCourtCase(
            external_id="00000002-222456-542896",
            state_code="US_ME",
            status=StateCourtCaseStatus.PRESENT_WITHOUT_INFO,
            status_raw_text=None,
            court_type=StateCourtType.PRESENT_WITHOUT_INFO,
            court_type_raw_text=None,
            date_convicted=datetime.date(2011, 4, 8),
            county_code=None,
            judge=StateAgent(
                external_id="456",
                state_code="US_ME",
                agent_type=StateAgentType.JUSTICE,
                agent_type_raw_text="JUSTICE",
                full_name='{"given_names": "MARCUS", "middle_names": "", "name_suffix": "", "surname": "EOIN"}',
            ),
            person=person_2,
            charges=[person_2_s_charge_4],
        )

        person_2_s_charge_4.court_case = person_2_s_charge_4_court_case
        person_2_supervision_sentence_4.charges = [person_2_s_charge_4]

        # Act
        self._run_ingest_job_for_filename("supervision_sentences")

        # Assert
        self.assert_expected_db_people(expected_people)

        ######################################
        # FULL RERUN FOR IDEMPOTENCE
        ######################################

        self._do_ingest_job_rerun_for_tags(self.controller.get_ingest_view_rank_list())

        self.assert_expected_db_people(expected_people)
