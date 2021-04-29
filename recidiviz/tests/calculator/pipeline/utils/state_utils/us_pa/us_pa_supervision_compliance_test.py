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
# pylint: disable=protected-access
import unittest
from datetime import date

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_compliance import (
    NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
    UsPaSupervisionCaseCompliance,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactType,
    StateSupervisionContactStatus,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionLevel,
    StateSupervisionPeriodStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateAssessment,
    StateSupervisionContact,
)


class TestAssessmentsInComplianceMonth(unittest.TestCase):
    """Tests for _completed_assessments_on_date."""

    def test_completed_assessments_in_compliance_month(self):
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

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            start_of_supervision=evaluation_date,
            case_type=None,
            assessments=assessments,
            supervision_contacts=[],
        )

        self.assertEqual(
            len(expected_assessments),
            us_pa_supervision_compliance._completed_assessments_on_date(
                evaluation_date
            ),
        )


class TestFaceToFaceContactsInComplianceMonth(unittest.TestCase):
    """Tests for _face_to_face_contacts_on_dates."""

    def test_face_to_face_contacts_in_compliance_month(self):
        evaluation_date = date(2018, 4, 30)
        contact_1 = StateSupervisionContact.new_with_defaults(
            state_code="US_PA",
            contact_date=date(2018, 4, 1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_2 = StateSupervisionContact.new_with_defaults(
            state_code="US_PA",
            contact_date=date(2018, 4, 15),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_3 = StateSupervisionContact.new_with_defaults(
            state_code="US_PA",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_out_of_range = StateSupervisionContact.new_with_defaults(
            state_code="US_PA",
            contact_date=date(2018, 3, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_incomplete = StateSupervisionContact.new_with_defaults(
            state_code="US_PA",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
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

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=evaluation_date,
            assessments=[],
            supervision_contacts=contacts,
        )
        self.assertEqual(
            len(expected_contacts),
            us_pa_supervision_compliance._face_to_face_contacts_on_date(
                evaluation_date
            ),
        )


class TestContactFrequencySufficient(unittest.TestCase):
    """Tests the _contact_frequency_is_sufficient function."""

    def test_face_to_face_frequency_sufficient_start_of_supervision(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS - 1)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_before_supervision_start(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                # Only contact happened before supervision started
                contact_date=supervision_period.start_date - relativedelta(days=100),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_attempted(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                # Only contact was not completed
                status=StateSupervisionContactStatus.ATTEMPTED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_invalid_contact_type(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date,
                # Only contact is invalid type
                contact_type=StateSupervisionContactType.WRITTEN_MESSAGE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_monitored_level_with_contact(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date
                + relativedelta(months=1, days=5),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(months=12)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_monitored_level_no_contacts(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_limited_level(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.LIMITED,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date
                + relativedelta(months=1, days=5),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(months=12)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_not_sufficient_contacts_limited_level_no_contacts(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.LIMITED,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_not_sufficient_contacts_limited_level_out_of_bounds(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.LIMITED,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date + relativedelta(days=20),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(months=13)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_minimum_level(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date
                + relativedelta(months=1, days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(months=3)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_not_sufficient_contacts_minimum_level_no_contacts(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_not_sufficient_contacts_minimum_level_out_of_bounds(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date + relativedelta(days=30),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=(180))

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date
                + relativedelta(months=1, days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(months=2)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_not_sufficient_contacts_medium_level_no_contacts(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_not_sufficient_contacts_medium_level_out_of_bounds(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date + relativedelta(days=20),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=(60))

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_maxiumum_level(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date + relativedelta(days=32),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date + relativedelta(days=40),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=(60))

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_not_sufficient_contacts_maximum_level_no_contacts(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_not_sufficient_contacts_maximum_level_out_of_bounds(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            # This contact is not in the correct time window, need two within 30 days
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date + relativedelta(days=20),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            # Only this contact is within time window, but need two so insufficent contact
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date + relativedelta(days=40),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=(60))

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_high_level(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.HIGH,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date
                + relativedelta(months=1, days=5),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date
                + relativedelta(months=1, days=10),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date
                + relativedelta(months=1, days=15),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date
                + relativedelta(months=1, days=20),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(months=2)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_not_sufficient_contacts_high_level_no_contacts(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.HIGH,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_not_sufficient_contacts_high_level_out_of_bounds(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.HIGH,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            # This contact is not in the correct time window, need four within 30 days
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date + relativedelta(days=20),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            # Only this contact is within time window, but need four so insufficent contact
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=supervision_period.start_date
                + relativedelta(months=1, days=10),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(months=2)

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_new_case_opened_on_friday(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(1999, 8, 13),  # This was a Friday
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = []

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 1
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_pa_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)


class TestGuidelinesApplicableForCase(unittest.TestCase):
    """Tests the guidelines_applicable_for_case function."""

    def test_guidelines_applicable_for_case(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MEDIUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_date = supervision_period.start_date
        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_pa_supervision_compliance._guidelines_applicable_for_case()

        self.assertTrue(applicable)

    def test_guidelines_applicable_for_case_no_supervision_level(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=None,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_date = supervision_period.start_date
        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_pa_supervision_compliance._guidelines_applicable_for_case()

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_invalid_supervision_level(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_PA",
            custodial_authority_raw_text="US_PA_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        start_date = supervision_period.start_date
        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_pa_supervision_compliance._guidelines_applicable_for_case()

        self.assertFalse(applicable)


class TestReassessmentRequirementAreMet(unittest.TestCase):
    """Tests the reassessment_requirements_are_met function."""

    def test_num_days_past_required_reassessment(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_PA.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 2),
        )

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_past_reassessment = (
            us_pa_supervision_compliance._num_days_past_required_reassessment(
                evaluation_date,
                assessment.assessment_date,
                assessment.assessment_score,
            )
        )

        self.assertEqual(days_past_reassessment, 0)

    def test_reassessment_requirements_are_not_met(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=None,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_PA.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2010, 4, 2),
        )

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=None,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_past_reassessment = (
            us_pa_supervision_compliance._num_days_past_required_reassessment(
                evaluation_date,
                assessment.assessment_date,
                assessment.assessment_score,
            )
        )

        self.assertEqual(days_past_reassessment, 2529)
