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
"""Tests for the functions in us_pa_supervision_compliance.py"""
import unittest
from datetime import date
from typing import List

from dateutil.relativedelta import relativedelta
from parameterized import parameterized

from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_compliance import (
    NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
    NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS,
    UsPaSupervisionCaseCompliance,
)
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_assessment import StateAssessmentType

# pylint: disable=protected-access
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StatePerson,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
)


class TestAssessmentsInComplianceMonth(unittest.TestCase):
    """Tests for _completed_assessments_on_date."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_XX")

    def test_completed_assessments_in_compliance_month(self) -> None:
        evaluation_date = date(2018, 4, 30)
        assessment_out_of_range = StateAssessment.new_with_defaults(
            state_code="US_PA",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 3, 10),
        )
        assessment_out_of_range_2 = StateAssessment.new_with_defaults(
            state_code="US_PA",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 5, 10),
        )
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_PA",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=1,
            assessment_date=date(2018, 4, 30),
        )
        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_PA",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=100,
            assessment_date=date(2018, 4, 30),
        )
        assessment_no_score = StateAssessment.new_with_defaults(
            state_code="US_PA",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
        )
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )
        assessments = [
            assessment_out_of_range,
            assessment_out_of_range_2,
            assessment_1,
            assessment_2,
            assessment_no_score,
        ]
        expected_assessments = [assessment_1, assessment_2]

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            start_of_supervision=evaluation_date,
            case_type=StateSupervisionCaseType.GENERAL,
            assessments=assessments,
            supervision_contacts=[],
            violation_responses=[],
        )

        self.assertEqual(
            len(expected_assessments),
            us_pa_supervision_compliance._completed_assessments_on_date(
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
            state_code="US_PA",
            contact_date=date(2018, 4, 1),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_2 = StateSupervisionContact.new_with_defaults(
            state_code="US_PA",
            contact_date=date(2018, 4, 15),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_3 = StateSupervisionContact.new_with_defaults(
            state_code="US_PA",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_out_of_range = StateSupervisionContact.new_with_defaults(
            state_code="US_PA",
            contact_date=date(2018, 3, 30),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_incomplete = StateSupervisionContact.new_with_defaults(
            state_code="US_PA",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.ATTEMPTED,
        )
        contact_wrong_type = StateSupervisionContact.new_with_defaults(
            state_code="US_PA",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.WRITTEN_MESSAGE,
            status=StateSupervisionContactStatus.COMPLETED,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
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

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=[],
            supervision_contacts=contacts,
            violation_responses=[],
        )
        self.assertEqual(
            len(expected_contacts),
            us_pa_supervision_compliance._face_to_face_contacts_on_date(
                evaluation_date
            ),
        )


class TestNextRecommendedFaceToFaceContactDate(unittest.TestCase):
    """Tests the _next_recommended_face_to_face_date function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_PA")

    def test_next_recommended_face_to_face_date_start_of_supervision(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision,
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS - 1)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_face_to_face_date = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        assert next_face_to_face_date is not None
        self.assertTrue(next_face_to_face_date > evaluation_date)

    def test_next_recommended_face_to_face_date_contacts_before_supervision_start(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                # Only contact happened before supervision started
                contact_date=start_of_supervision - relativedelta(days=100),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 7))

    def test_next_recommended_face_to_face_date_contacts_attempted(self) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision,
                contact_type=StateSupervisionContactType.DIRECT,
                # Only contact was not completed
                status=StateSupervisionContactStatus.ATTEMPTED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 7))

    def test_next_recommended_face_to_face_date_contacts_invalid_contact_type(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision,
                # Only contact is invalid type
                contact_type=StateSupervisionContactType.WRITTEN_MESSAGE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 7))

    def test_next_recommended_face_to_face_date_contacts_monitored_level_with_contact(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(months=1, days=5),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(months=12)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertIsNone(next_face_to_face)

    def test_next_recommended_face_to_face_date_contacts_monitored_level_no_contacts(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
        )

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertIsNone(next_face_to_face)

    def test_next_recommended_face_to_face_date_contacts_limited_level(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.LIMITED,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(months=1, days=5),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(months=12)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertIsNone(face_to_face_frequency_sufficient)

    def test_next_recommended_face_to_face_date_contacts_minimum_level(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(months=1, days=1),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(months=3)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_next_recommended_face_to_face_date_contacts_minimum_level_no_contacts(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 7))

    def test_next_recommended_face_to_face_date_contacts_minimum_level_out_of_bounds(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=30),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=(180))

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 7, 3))

    def test_next_recommended_face_to_face_date_contacts_medium_level(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(months=1, days=1),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(months=2)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_next_recommended_face_to_face_date_contacts_medium_level_no_contacts(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 7))

    def test_next_recommended_face_to_face_date_contacts_medium_level_out_of_bounds(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=20),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=(60))

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 4, 24))

    def test_next_recommended_face_to_face_date_contacts_maxiumum_level(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MAXIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=32),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=40),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(days=(60))

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_next_recommended_face_to_face_date_contacts_maximum_level_no_contacts(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MAXIMUM,
        )

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 7))

    def test_next_recommended_face_to_face_date_contacts_maximum_level_out_of_bounds(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MAXIMUM,
        )

        supervision_contacts = [
            # This contact is not in the correct time window, need two within 30 days
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=20),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            # Only this contact is within time window, but need two so insufficent contact
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=40),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(days=(60))

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 4, 24))

    def test_next_recommended_face_to_face_date_contacts_high_level(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.HIGH,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(months=1, days=5),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(months=1, days=10),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(months=1, days=15),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(months=1, days=20),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(months=2)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_next_recommended_face_to_face_date_contacts_high_level_no_contacts(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.HIGH,
        )

        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 7))

    def test_next_recommended_face_to_face_date_contacts_high_level_out_of_bounds(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.HIGH,
        )

        supervision_contacts = [
            # This contact is not in the correct time window, need four within 30 days
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=20),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            # Only this contact is within time window, but need four so insufficent contact
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(months=1, days=10),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        evaluation_date = start_of_supervision + relativedelta(months=2)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 4, 4))

    def test_next_recommended_face_to_face_date_contacts_new_case_opened_on_friday(
        self,
    ) -> None:
        start_of_supervision = date(1999, 8, 13)  # This was a Friday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts: List[StateSupervisionContact] = []

        evaluation_date = start_of_supervision + relativedelta(
            days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)


class TestNextRecommendedHomeVisitDate(unittest.TestCase):
    """Tests the next_recommended_home_visit_date function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_PA")

    @parameterized.expand(
        [
            ("monitoring", StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY),
            ("limited", StateSupervisionLevel.LIMITED),
            ("minimum", StateSupervisionLevel.MINIMUM),
        ]
    )
    def test_next_recommended_home_visit_date_contacts_not_required(
        self, _name: str, supervision_level: StateSupervisionLevel
    ) -> None:
        start_of_supervision = date(2000, 1, 21)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_type=None,
            supervision_level=supervision_level,
        )

        supervision_contacts: List[StateSupervisionContact] = []

        evaluation_date = start_of_supervision + relativedelta(days=20)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_recommended_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertIsNone(next_recommended_home_visit_date)

    @parameterized.expand(
        [
            ("medium", StateSupervisionLevel.MEDIUM),
            ("maximum", StateSupervisionLevel.MAXIMUM),
            ("high", StateSupervisionLevel.HIGH),
        ]
    )
    def test_next_recommended_home_visit_date_no_contacts(
        self, _name: str, supervision_level: StateSupervisionLevel
    ) -> None:
        start_of_supervision = date(2000, 1, 21)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_type=None,
            supervision_level=supervision_level,
        )

        supervision_contacts: List[StateSupervisionContact] = []

        evaluation_date = start_of_supervision + relativedelta(
            days=NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS + 10
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_recommended_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertIsNotNone(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_contacts_medium_level(self) -> None:
        start_of_supervision = date(2000, 1, 21)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts: List[StateSupervisionContact] = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=20),
                contact_type=StateSupervisionContactType.DIRECT,
                location=StateSupervisionContactLocation.RESIDENCE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=30)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_recommended_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertTrue(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_contacts_medium_level_out_of_bounds(
        self,
    ) -> None:
        start_of_supervision = date(2000, 1, 21)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts: List[StateSupervisionContact] = [
            # Out of bounds in that it's not within 60 days of evaluation
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=10),
                contact_type=StateSupervisionContactType.DIRECT,
                location=StateSupervisionContactLocation.RESIDENCE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=70)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_recommended_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertIsNotNone(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_contacts_maximum_level(self) -> None:
        start_of_supervision = date(2000, 1, 21)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MAXIMUM,
        )

        supervision_contacts: List[StateSupervisionContact] = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=20),
                contact_type=StateSupervisionContactType.DIRECT,
                location=StateSupervisionContactLocation.RESIDENCE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=30)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_recommended_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertTrue(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_contacts_maximum_level_out_of_bounds(
        self,
    ) -> None:
        start_of_supervision = date(2000, 1, 21)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MAXIMUM,
        )

        supervision_contacts: List[StateSupervisionContact] = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=10),
                contact_type=StateSupervisionContactType.DIRECT,
                location=StateSupervisionContactLocation.RESIDENCE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=45)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_recommended_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertIsNotNone(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_contacts_high_level(self) -> None:
        start_of_supervision = date(2000, 1, 21)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.HIGH,
        )

        supervision_contacts: List[StateSupervisionContact] = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=20),
                contact_type=StateSupervisionContactType.DIRECT,
                location=StateSupervisionContactLocation.RESIDENCE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=30)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_recommended_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertTrue(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_contacts_high_level_out_of_bounds(
        self,
    ) -> None:
        start_of_supervision = date(2000, 1, 21)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MAXIMUM,
        )

        supervision_contacts: List[StateSupervisionContact] = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=start_of_supervision + relativedelta(days=10),
                contact_type=StateSupervisionContactType.DIRECT,
                location=StateSupervisionContactLocation.RESIDENCE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=45)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
        )

        next_recommended_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertIsNotNone(next_recommended_home_visit_date)


class TestGuidelinesApplicableForCase(unittest.TestCase):
    """Tests the guidelines_applicable_for_case function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_PA")

    def test_guidelines_applicable_for_case(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
        )

        applicable = us_pa_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertTrue(applicable)

    def test_guidelines_applicable_for_case_no_supervision_level(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=None,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
        )

        applicable = us_pa_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_invalid_supervision_level(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
        )

        applicable = us_pa_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertFalse(applicable)


class TestNextRecommendedReassessment(unittest.TestCase):
    """Tests the _next_recommended_reassessment function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_XX")

    def test_next_recommended_reassessment(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        assessment_date = date(2018, 4, 2)
        assessment_score = 25
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_PA.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
            violation_responses=[],
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 5, 30)
        )

        self.assertEqual(reassessment_date, date(2019, 4, 2))

    def test_next_recommended_reassessment_overdue(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
        )

        assessment_date = date(2010, 4, 2)
        assessment_score = 25
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_PA.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
            violation_responses=[],
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2011, 4, 2)
        )

        self.assertEqual(reassessment_date, date(2011, 4, 2))

    def test_next_recommended_reassessment_can_skip_reassessment(self) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        assessment_date = date(2018, 4, 2)
        assessment_score = 18
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_PA.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
            violation_responses=[],
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertEqual(reassessment_date, None)

    def test_next_recommended_reassessment_cannot_skip_reassessment_due_to_violations(
        self,
    ) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2020, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        assessment_date = date(2018, 4, 2)
        assessment_score = 18
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_PA.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
            violation_responses=[
                StateSupervisionViolationResponse.new_with_defaults(
                    supervision_violation_response_id=123,
                    external_id="svr1",
                    state_code=StateCode.US_PA.value,
                    response_date=date(2018, 5, 20),
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                            supervision_violation_response_decision_entry_id=13,
                            state_code=StateCode.US_PA.value,
                            decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
                            decision_raw_text="URIN",
                        )
                    ],
                )
            ],
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertEqual(reassessment_date, date(2019, 4, 2))

    def test_next_recommended_reassessment_cannot_skip_reassessment_due_to_not_enough_time_on_min_supervision(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2020, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        assessment_date = date(2018, 4, 2)
        assessment_score = 18
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_PA.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
            violation_responses=[],
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertEqual(reassessment_date, date(2019, 4, 2))

    def test_next_recommended_reassessment_can_skip_reassessment_due_to_violations_before_most_recent_assessment(
        self,
    ) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2020, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        assessment_date = date(2018, 4, 2)
        assessment_score = 18
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_PA.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
            violation_responses=[
                StateSupervisionViolationResponse.new_with_defaults(
                    supervision_violation_response_id=123,
                    external_id="svr1",
                    state_code=StateCode.US_PA.value,
                    response_date=date(
                        2017, 5, 20
                    ),  # Happened before most recent assessment
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                            supervision_violation_response_decision_entry_id=13,
                            state_code=StateCode.US_PA.value,
                            decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
                            decision_raw_text="URIN",
                        )
                    ],
                )
            ],
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertEqual(reassessment_date, None)

    def test_next_recommended_reassessment_can_skip_reassessment_due_to_non_med_high_violations(
        self,
    ) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2020, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        assessment_date = date(2018, 4, 2)
        assessment_score = 18
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_PA.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
            violation_responses=[
                StateSupervisionViolationResponse.new_with_defaults(
                    supervision_violation_response_id=123,
                    external_id="svr1",
                    state_code=StateCode.US_PA.value,
                    response_date=date(
                        2018, 5, 20
                    ),  # Happened after most recent assessment
                    supervision_violation_response_decisions=[
                        StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                            supervision_violation_response_decision_entry_id=13,
                            state_code=StateCode.US_PA.value,
                            decision=StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
                            decision_raw_text="WTWR",  # Low Sanction
                        )
                    ],
                )
            ],
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertEqual(reassessment_date, None)


class TestSupervisionDowngrades(unittest.TestCase):
    """Tests the _get_recommended_supervision_downgrade_level function."""

    def setUp(self) -> None:
        self.start_of_supervision = date(2018, 3, 5)
        self.evaluation_date = date(2021, 1, 1)

    def _person_with_gender(self, gender: Gender) -> StatePerson:
        return StatePerson.new_with_defaults(state_code="US_PA", gender=gender)

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
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=supervision_level,
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
            ("male", Gender.MALE),
            ("trans_male", Gender.TRANS_MALE),
            ("female", Gender.FEMALE),
            ("trans_female", Gender.TRANS_FEMALE),
        ]
    )
    def test_minimum_no_downgrade(
        self,
        _name: str,
        gender: Gender,
    ) -> None:
        us_id_supervision_compliance = UsPaSupervisionCaseCompliance(
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
            violation_responses=[],
        )
        self.assertIsNone(
            us_id_supervision_compliance._get_recommended_supervision_downgrade_level(
                self.evaluation_date
            )
        )

    @parameterized.expand(
        [
            ("medium", StateSupervisionLevel.MEDIUM, 20),
            ("maximum", StateSupervisionLevel.MAXIMUM, 28),
        ]
    )
    def test_downgrade_at_border(
        self, _name: str, level: StateSupervisionLevel, score: int
    ) -> None:
        compliance_no_downgrade = UsPaSupervisionCaseCompliance(
            self._person_with_gender(Gender.EXTERNAL_UNKNOWN),
            supervision_period=self._supervision_period_with_level(level),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[self._assessment_with_score(score)],
            supervision_contacts=[],
            violation_responses=[],
        )
        self.assertIsNone(
            compliance_no_downgrade._get_recommended_supervision_downgrade_level(
                self.evaluation_date
            )
        )

        compliance_downgrade = UsPaSupervisionCaseCompliance(
            self._person_with_gender(Gender.EXTERNAL_UNKNOWN),
            supervision_period=self._supervision_period_with_level(level),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[self._assessment_with_score(score - 1)],
            supervision_contacts=[],
            violation_responses=[],
        )
        recommended_level = (
            compliance_downgrade._get_recommended_supervision_downgrade_level(
                self.evaluation_date
            )
        )
        assert recommended_level is not None
        self.assertTrue(recommended_level < level)
