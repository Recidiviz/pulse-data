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
"""Unit tests for us_nd_supervision_compliance."""
# pylint: disable=protected-access

import unittest
from datetime import date

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_compliance import (
    UsNdSupervisionCaseCompliance,
    NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactType,
    StateSupervisionContactStatus,
    StateSupervisionContactLocation,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionLevel,
    StateSupervisionPeriodStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateSupervisionPeriod,
    StateSupervisionContact,
)


class TestAssessmentsCompletedInComplianceMonth(unittest.TestCase):
    """Tests for assessments_in_compliance_month."""

    def test_completed_assessments_in_compliance_month(self):
        evaluation_date = date(2018, 4, 30)
        assessment_out_of_range = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 3, 10),
        )
        assessment_out_of_range_2 = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 5, 10),
        )
        assessment_1 = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=1,
            assessment_date=date(2018, 4, 30),
        )
        assessment_2 = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=100,
            assessment_date=date(2018, 4, 30),
        )
        assessment_no_score = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
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

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=assessments,
            supervision_contacts=[],
        )

        self.assertEqual(
            len(expected_assessments),
            us_nd_supervision_compliance._completed_assessments_on_date(
                evaluation_date
            ),
        )


class TestFaceToFaceContactsInComplianceMonth(unittest.TestCase):
    """Tests for face_to_face_contacts_in_compliance_month."""

    def test_face_to_face_contacts_in_compliance_month(self):
        evaluation_date = date(2018, 4, 30)
        contact_1 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 4, 1),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_2 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 4, 15),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_3 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_out_of_range = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 3, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_incomplete = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.ATTEMPTED,
        )
        contact_wrong_type = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.WRITTEN_MESSAGE,
            status=StateSupervisionContactStatus.COMPLETED,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
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

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=[],
            supervision_contacts=contacts,
        )
        self.assertEqual(
            len(expected_contacts),
            us_nd_supervision_compliance._face_to_face_contacts_on_date(
                evaluation_date
            ),
        )


class TestGuidelinesApplicableForCase(unittest.TestCase):
    """Tests the guidelines_applicable_for_case function."""

    def test_guidelines_applicable_for_case_external_unknown(self):
        """The guidelines should not be applicable to people who are not classified."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
            supervision_level_raw_text="5",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=supervision_period.start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case()

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_interstate_compact(self):
        """The guidelines should not be applicable to people who are in interstate compact."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.INTERSTATE_COMPACT,
            supervision_level_raw_text="9",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=supervision_period.start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case()

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_diversion(self):
        """The guidelines should not be applicable to people who are in diversion programs."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.DIVERSION,
            supervision_level_raw_text="7",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=supervision_period.start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case()

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case(self):
        """The guidelines should be applicable to people who are not in interstate compact or not classified."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="2",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=supervision_period.start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case()

        self.assertTrue(applicable)

    def test_guidelines_applicable_for_case_invalid_supervision_level(self):
        """The guidelines should not be applicable to a supervision level other than MINIMUM,
        MEDIUM, MAXIMUM."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="SO HIGH",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=supervision_period.start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case()

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_invalid_supervision_case_type(self):
        """The guidelines should not be applicable to a case type that is not GENERAL or SEX_OFFENSE."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="SO HIGH",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS,
            start_of_supervision=supervision_period.start_date,
            assessments=[],
            supervision_contacts=[],
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case()

        self.assertFalse(applicable)


class TestContactFrequencySufficient(unittest.TestCase):
    """Tests the contact_frequency_is_sufficient function."""

    def test_face_to_face_frequency_sufficient(self):
        """Tests for when the face to face contacts is sufficient."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ND.value,
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        self.assertTrue(
            us_nd_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

    def test_face_to_face_frequency_sufficient_contacts_before_supervision_start(
        self,
    ):
        """Tests for when the only face to face contacts is occurs prior to start of supervision."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                # Only contact happened before supervision started
                contact_date=supervision_period.start_date - relativedelta(days=100),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=3)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_nd_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_attempted_general_case(self):
        """Tests when the only face to face contact is attempted, but not completed."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_level=StateSupervisionLevel.MAXIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                # Only contact was not completed
                status=StateSupervisionContactStatus.ATTEMPTED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=2)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_nd_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_invalid_contact_type_general_case(
        self,
    ):
        """Tests when the only contact is not a valid type for face to face."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date,
                # Only contact is invalid type
                contact_type=StateSupervisionContactType.WRITTEN_MESSAGE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=5)
        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_nd_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_invalid_supervision_level(
        self,
    ):
        """Tests when the only contact is not a valid type for face to face."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date,
                # Only contact is invalid type
                contact_type=StateSupervisionContactType.WRITTEN_MESSAGE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=5)
        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_nd_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_minimum_level(
        self,
    ):
        """Tests face to face contacts for the minimum level case."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date + relativedelta(days=89),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=90)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_nd_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_medium_level(
        self,
    ):
        """Tests face to face contacts for the medium level case."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date + relativedelta(days=58),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=59)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_nd_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_face_to_face_frequency_sufficient_contacts_maximum_level(
        self,
    ):
        """Tests face to face contacts for the maximum level case."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date - relativedelta(days=24),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date + relativedelta(days=29),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=29)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        face_to_face_frequency_sufficient = (
            us_nd_supervision_compliance._face_to_face_contact_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)


class TestReassessmentRequirementAreMet(unittest.TestCase):
    """Tests the reassessment_requirements_are_met function."""

    def test_num_days_past_required_reassessment(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 2),
        )

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_past_reassessment = (
            us_nd_supervision_compliance._num_days_past_required_reassessment(
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
            state_code=StateCode.US_ND.value,
            start_date=date(2018, 3, 5),  # This was a Monday
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2010, 4, 2),
        )

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=[assessment],
            supervision_contacts=[],
        )

        days_past_reassessment = (
            us_nd_supervision_compliance._num_days_past_required_reassessment(
                evaluation_date,
                assessment.assessment_date,
                assessment.assessment_score,
            )
        )

        self.assertEqual(days_past_reassessment, 2682)


class TestHomeVisitsFrequencySufficient(unittest.TestCase):
    """Tests the home_visit_frequency_is_sufficient function."""

    def test_home_visit_frequency_sufficient_initial_visit_met(
        self,
    ):
        """Tests for when the initial home visit standard is met."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date + relativedelta(days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=59)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        home_visit_frequency_sufficient = (
            us_nd_supervision_compliance._home_visit_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(home_visit_frequency_sufficient)

    def test_home_visit_frequency_sufficient_initial_visit_not_met(
        self,
    ):
        """Tests for when the initial home visit standard is not met."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = []

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS + 1
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        home_visit_frequency_sufficient = (
            us_nd_supervision_compliance._home_visit_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(home_visit_frequency_sufficient)

    def test_home_visit_frequency_sufficient_initial_visit_not_at_residence(
        self,
    ):
        """Tests for when the only contact does not have location RESIDENCE."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date + relativedelta(days=1),
                contact_type=StateSupervisionContactType.FACE_TO_FACE,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.SUPERVISION_OFFICE,
            )
        ]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(
            days=NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS + 1
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        home_visit_frequency_sufficient = (
            us_nd_supervision_compliance._home_visit_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(home_visit_frequency_sufficient)

    def test_home_visit_frequency_sufficient_no_initial_visit_within_range(
        self,
    ):
        """Tests for when the home visit standard is met because the initial period has
        not yet passed."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = []

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=59)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        home_visit_frequency_sufficient = (
            us_nd_supervision_compliance._home_visit_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(home_visit_frequency_sufficient)

    def test_home_visit_frequency_sufficient_medium_level(
        self,
    ):
        """Tests home visit frequency for medium level"""
        contact_1 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 3, 8),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
            location=StateSupervisionContactLocation.RESIDENCE,
        )
        contact_2 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2019, 3, 6),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
            location=StateSupervisionContactLocation.RESIDENCE,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [contact_1, contact_2]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=375)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        home_visit_frequency_sufficient = (
            us_nd_supervision_compliance._home_visit_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertTrue(home_visit_frequency_sufficient)

    def test_home_visit_frequency_sufficient_medium_level_not_met(
        self,
    ):
        """Tests home visit frequency for medium level when standard is not met"""
        contact_1 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 3, 5),
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
            status=StateSupervisionContactStatus.COMPLETED,
            location=StateSupervisionContactLocation.RESIDENCE,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_contacts = [contact_1]

        start_of_supervision = supervision_period.start_date
        evaluation_date = start_of_supervision + relativedelta(days=375)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
        )

        home_visit_frequency_sufficient = (
            us_nd_supervision_compliance._home_visit_frequency_is_sufficient(
                evaluation_date
            )
        )

        self.assertFalse(home_visit_frequency_sufficient)
