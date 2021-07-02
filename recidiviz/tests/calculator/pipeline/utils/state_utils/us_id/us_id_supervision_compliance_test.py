# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for the functions in us_id_supervision_compliance.py"""
import unittest
from datetime import date, timedelta
from typing import List

from dateutil.relativedelta import relativedelta
from parameterized import parameterized

from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    DATE_OF_SUPERVISION_LEVEL_SWITCH,
    DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE,
    DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE,
    DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE,
    NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
    SEX_OFFENSE_LSIR_MINIMUM_SCORE,
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS,
    UsIdSupervisionCaseCompliance,
)

# pylint: disable=protected-access
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StatePerson,
    StateSupervisionContact,
    StateSupervisionPeriod,
)

HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = (
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[StateSupervisionCaseType.GENERAL][
        StateSupervisionLevel.HIGH
    ][1]
)
MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = (
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[StateSupervisionCaseType.GENERAL][
        StateSupervisionLevel.MEDIUM
    ][1]
)

HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE = (
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[StateSupervisionCaseType.SEX_OFFENSE][
        StateSupervisionLevel.HIGH
    ][1]
)
MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE = (
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[StateSupervisionCaseType.SEX_OFFENSE][
        StateSupervisionLevel.MEDIUM
    ][1]
)
MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE = (
    SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[StateSupervisionCaseType.SEX_OFFENSE][
        StateSupervisionLevel.MINIMUM
    ][1]
)


class TestAssessmentsInComplianceMonth(unittest.TestCase):
    """Tests for _completed_assessments_on_date."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_XX")

    def test_completed_assessments_in_compliance_month(self) -> None:
        evaluation_date = date(2018, 4, 30)
        assessment_out_of_range = StateAssessment.new_with_defaults(
            state_code="US_ID",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 3, 10),
        )
        assessment_out_of_range_2 = StateAssessment.new_with_defaults(
            state_code="US_ID",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 5, 10),
        )
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_ID",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=1,
            assessment_date=date(2018, 4, 30),
        )
        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_ID",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=100,
            assessment_date=date(2018, 4, 30),
        )
        assessment_no_score = StateAssessment.new_with_defaults(
            state_code="US_ID",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
        )
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        assessments = [
            assessment_out_of_range,
            assessment_out_of_range_2,
            assessment_1,
            assessment_2,
            assessment_no_score,
        ]
        expected_assessments = [assessment_1, assessment_2]

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=assessments,
            supervision_contacts=[],
        )

        self.assertEqual(
            len(expected_assessments),
            us_id_supervision_compliance._completed_assessments_on_date(
                evaluation_date
            ),
        )


class TestFaceToFaceContactsInComplianceMonth(unittest.TestCase):
    """Tests for _face_to_face_contacts_on_dates."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_XX")

    def test_face_to_face_contacts_in_compliance_month(self) -> None:
        evaluation_date = date(2018, 4, 30)
        contact_1 = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 4, 1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_2 = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 4, 15),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_3 = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_out_of_range = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 3, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_incomplete = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.ATTEMPTED,
        )
        contact_wrong_type = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.WRITTEN_MESSAGE,
            status=StateSupervisionContactStatus.COMPLETED,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        contacts = [
            contact_1,
            contact_2,
            contact_3,
            contact_incomplete,
            contact_out_of_range,
            contact_wrong_type,
        ]
        expected_contacts = [contact_3]

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=[],
            supervision_contacts=contacts,
        )
        self.assertEqual(
            len(expected_contacts),
            us_id_supervision_compliance._face_to_face_contacts_on_date(
                evaluation_date
            ),
        )


class TestContactFrequencySufficient(unittest.TestCase):
    """Tests the _contact_frequency_is_sufficient function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_XX")

    def test_face_to_face_frequency_sufficient_start_of_supervision_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS - 1)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_before_supervision_start_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                # Only contact happened before supervision started
                contact_date=start_of_supervision - relativedelta(days=100),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_attempted_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                # Only contact was not completed
                status=StateSupervisionContactStatus.ATTEMPTED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_invalid_contact_type_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                # Only contact is invalid type
                contact_type=StateSupervisionContactType.WRITTEN_MESSAGE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_minimum_level_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_minimum_level_deprecated_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LEVEL 1",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_maximum_level_up_to_date_one_contact_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="LEVEL 4",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(
                DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10
            )
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_maximum_level_up_to_date_two_contacts_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="LEVEL 4",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=40),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(days=50)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 5
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_deprecated_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                    - 10
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                    - 5
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_maximum_level_not_up_to_date_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="LEVEL 4",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                    + 1
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 10
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_up_to_date_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_up_to_date_deprecated_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_not_up_to_date_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="HIGH",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # One contact within the time window, one outside.
        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=15),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 1
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_not_up_to_date_deprecated_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 10)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_up_to_date_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_up_to_date_deprecated_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="LEVEL 2",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(
                DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10
            )
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_not_up_to_date_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # One contact within the time window, one outside.
        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=15),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 1
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_not_up_to_date_deprecated_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="LEVEL 2",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(
                DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 10
            )
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_new_case_opened_on_friday_general_case(
        self,
    ) -> None:
        start_of_supervision = date(1999, 8, 13)  # This was a Friday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts: List[StateSupervisionContact] = []

        evaluation_date = start_of_supervision + relativedelta(
            days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_new_case_opened_on_friday_deprecated_general_case(
        self,
    ) -> None:
        start_of_supervision = date(1999, 8, 13)  # This was a Friday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="LEVEL 2",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts: List[StateSupervisionContact] = []

        evaluation_date = start_of_supervision + relativedelta(
            days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_minimum_level_sex_offense_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="SO LOW",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE - 1)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_minimum_level_sex_offense_case_not_met(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="SO LOW",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE + 1)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_sex_offense_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="SO MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE - 1)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level_sex_offense_case_not_met(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="SO MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision + relativedelta(days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE + 1)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_sex_offense_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="SO HIGH",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE - 10
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE - 20
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level_sex_offense_case_not_met(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="SO HIGH",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # One contact within in time period, and one after.
        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE - 10
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=start_of_supervision
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE + 10
                ),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE)
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_is_new_level_system_case_sensitivity(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="SO HIGH",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )
        self.assertTrue(us_id_supervision_compliance._is_new_level_system("LOW"))
        self.assertFalse(us_id_supervision_compliance._is_new_level_system("Low"))


class TestGuidelinesApplicableForCase(unittest.TestCase):
    """Tests the guidelines_applicable_for_case function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_XX")

    def test_guidelines_applicable_for_case_general(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="LEVEL 2",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_id_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertTrue(applicable)

    def test_guidelines_applicable_for_case_no_supervision_level_general(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=None,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_id_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_invalid_supervision_type_general(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_id_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_invalid_case_type(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )

        self.assertFalse(
            us_id_supervision_compliance._guidelines_applicable_for_case(start_date)
        )

    def test_guidelines_applicable_for_case_sex_offense(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="SO MODERATE",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.SEX_OFFENSE

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_id_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertTrue(applicable)

    def test_guidelines_not_applicable_for_case_invalid_leveL_sex_offense(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="SO MAXIMUM",  # Fake string, not actually possible to have max sex offense.
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.SEX_OFFENSE

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_id_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertFalse(applicable)

    def test_guidelines_not_applicable_for_case_invalid_supervision_type_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="SO MAXIMUM",  # Fake string, not actually possible to have max sex offense.
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        case_type = StateSupervisionCaseType.SEX_OFFENSE

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )

        self.assertFalse(
            us_id_supervision_compliance._guidelines_applicable_for_case(start_date)
        )

    def test_guideline_applicability_around_date_change(self) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="LEVEL 2",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        # Before the change guidelines should apply to someone with MAXIMUM supervision level
        self.assertTrue(
            us_id_supervision_compliance._guidelines_applicable_for_case(
                DATE_OF_SUPERVISION_LEVEL_SWITCH + timedelta(days=-1)
            )
        )
        # After the change guidelines shouldn't apply to someone with MAXIMUM supervision level
        self.assertFalse(
            us_id_supervision_compliance._guidelines_applicable_for_case(
                DATE_OF_SUPERVISION_LEVEL_SWITCH
            )
        )


class TestReassessmentRequirementAreMet(unittest.TestCase):
    """Tests the reassessment_requirements_are_met function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_XX")

    def test_num_days_past_required_reassessment_general_minimum(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment_date = date(2018, 4, 2)
        assessment_score = 25
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 2),
            assessment_score=assessment_score,
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_past_reassessment = (
            us_id_supervision_compliance._num_days_past_required_reassessment(
                start_of_supervision,
                assessment_date,
                assessment_score,
            )
        )

        self.assertEqual(days_past_reassessment, 0)

    def test_num_days_past_required_reassessment_sex_offense_with_score(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment_date = date(2018, 4, 2)
        assessment_score = 34
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_past_reassessment = (
            us_id_supervision_compliance._num_days_past_required_reassessment(
                start_of_supervision,
                assessment_date,
                assessment_score,
            )
        )

        self.assertEqual(days_past_reassessment, 0)

    def test_reassessment_requirements_are_not_met(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment_date = date(2010, 4, 2)
        assessment_score = 25
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_past_reassessment = (
            us_id_supervision_compliance._num_days_past_required_reassessment(
                start_of_supervision,
                assessment_date,
                assessment_score,
            )
        )

        self.assertEqual(days_past_reassessment, 2529)

    @parameterized.expand(
        [
            (Gender.MALE,),
            (Gender.TRANS_MALE,),
            (Gender.FEMALE,),
            (Gender.TRANS_FEMALE,),
        ]
    )
    def test_reassessment_requirements_at_sex_offense_boundaries(
        self, gender: Gender
    ) -> None:
        self.person.gender = gender
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment_date = date(2010, 4, 2)
        assessment_boundary_score = SEX_OFFENSE_LSIR_MINIMUM_SCORE[gender]
        assessment_boundary = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_boundary_score,
        )

        us_id_supervision_compliance_boundary = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment_boundary],
            supervision_contacts=[],
        )

        days_past_reassessment = (
            us_id_supervision_compliance_boundary._num_days_past_required_reassessment(
                start_of_supervision,
                assessment_date,
                assessment_boundary_score,
            )
        )

        assessment_under_boundary = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_boundary_score - 1,
        )

        us_id_supervision_compliance_under_boundary = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[assessment_under_boundary],
            supervision_contacts=[],
        )

        days_past_reassessment = us_id_supervision_compliance_under_boundary._num_days_past_required_reassessment(
            start_of_supervision,
            assessment_date,
            assessment_boundary_score - 1,
        )

        self.assertEqual(days_past_reassessment, 0)


class TestSupervisionDowngrades(unittest.TestCase):
    """Tests the reassessment_requirements_are_met function."""

    def setUp(self) -> None:
        self.start_of_supervision = date(2018, 3, 5)

    def _person_with_gender(self, gender: Gender) -> StatePerson:
        return StatePerson.new_with_defaults(state_code="US_ID", gender=gender)

    def _supervision_period_with_level(
        self, supervision_level: StateSupervisionLevel
    ) -> StateSupervisionPeriod:
        return StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            start_date=self.start_of_supervision,
            termination_date=date(2021, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=supervision_level,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

    def _assessment_with_score(self, score: int) -> StateAssessment:
        return StateAssessment.new_with_defaults(
            state_code=StateCode.US_ID.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=self.start_of_supervision,
            assessment_score=score,
        )

    @parameterized.expand(
        [
            ("male_old", Gender.MALE, date(2019, 12, 31)),
            ("trans_ma_oldle", Gender.TRANS_MALE, date(2019, 12, 31)),
            ("fema_oldle", Gender.FEMALE, date(2019, 12, 31)),
            ("trans_fema_oldle", Gender.TRANS_FEMALE, date(2019, 12, 31)),
            ("male_new", Gender.MALE, date(2020, 12, 31)),
            ("trans_male_new", Gender.TRANS_MALE, date(2020, 12, 31)),
            ("female_new", Gender.FEMALE, date(2020, 12, 31)),
            ("trans_female_new", Gender.TRANS_FEMALE, date(2020, 12, 31)),
        ]
    )
    def test_minimum_no_downgrade(
        self,
        _name: str,
        gender: Gender,
        evaluation_date: date,
    ) -> None:
        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self._person_with_gender(gender),
            supervision_period=self._supervision_period_with_level(
                StateSupervisionLevel.MINIMUM
            ),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[
                self._assessment_with_score(100)
            ],  # No downgrade regardless of score
            supervision_contacts=[],
        )
        self.assertFalse(
            us_id_supervision_compliance._is_eligible_for_supervision_downgrade(
                evaluation_date
            )
        )

    @parameterized.expand(
        [
            ("medium", StateSupervisionLevel.MEDIUM, 16, Gender.EXTERNAL_UNKNOWN),
            ("high", StateSupervisionLevel.HIGH, 24, Gender.EXTERNAL_UNKNOWN),
            ("maximum", StateSupervisionLevel.MAXIMUM, 31, Gender.EXTERNAL_UNKNOWN),
        ]
    )
    def test_old_downgrade_at_border(
        self, _name: str, level: StateSupervisionLevel, score: int, gender: Gender
    ) -> None:
        compliance_no_downgrade = UsIdSupervisionCaseCompliance(
            self._person_with_gender(gender),
            supervision_period=self._supervision_period_with_level(level),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[self._assessment_with_score(score)],
            supervision_contacts=[],
        )
        self.assertFalse(
            compliance_no_downgrade._is_eligible_for_supervision_downgrade(
                date(2019, 12, 31)
            )
        )

        compliance_downgrade = UsIdSupervisionCaseCompliance(
            self._person_with_gender(gender),
            supervision_period=self._supervision_period_with_level(level),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[self._assessment_with_score(score - 1)],
            supervision_contacts=[],
        )
        self.assertTrue(
            compliance_downgrade._is_eligible_for_supervision_downgrade(
                date(2019, 12, 31)
            )
        )

    @parameterized.expand(
        [
            ("medium_female", StateSupervisionLevel.MEDIUM, 23, Gender.FEMALE),
            (
                "medium_trans_female",
                StateSupervisionLevel.MEDIUM,
                23,
                Gender.TRANS_FEMALE,
            ),
            ("medium_male", StateSupervisionLevel.MEDIUM, 21, Gender.MALE),
            ("medium_trans_male", StateSupervisionLevel.MEDIUM, 21, Gender.TRANS_MALE),
            ("high_female", StateSupervisionLevel.HIGH, 31, Gender.FEMALE),
            ("high_trans_female", StateSupervisionLevel.HIGH, 31, Gender.TRANS_FEMALE),
            ("high_male", StateSupervisionLevel.HIGH, 29, Gender.MALE),
            ("high_trans_male", StateSupervisionLevel.HIGH, 29, Gender.TRANS_MALE),
        ]
    )
    def test_new_downgrade_at_border(
        self, _name: str, level: StateSupervisionLevel, score: int, gender: Gender
    ) -> None:
        compliance_no_downgrade = UsIdSupervisionCaseCompliance(
            self._person_with_gender(gender),
            supervision_period=self._supervision_period_with_level(level),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[self._assessment_with_score(score)],
            supervision_contacts=[],
        )
        self.assertFalse(
            compliance_no_downgrade._is_eligible_for_supervision_downgrade(
                date(2020, 12, 31)
            )
        )

        compliance_downgrade = UsIdSupervisionCaseCompliance(
            self._person_with_gender(gender),
            supervision_period=self._supervision_period_with_level(level),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[self._assessment_with_score(score - 1)],
            supervision_contacts=[],
        )
        self.assertTrue(
            compliance_downgrade._is_eligible_for_supervision_downgrade(
                date(2020, 12, 31)
            )
        )
