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
from typing import List

from dateutil.relativedelta import relativedelta
from parameterized import parameterized

from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_delegate import (
    UsNdIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_compliance import (
    NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS,
    UsNdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
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
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StatePerson,
    StateSupervisionContact,
    StateSupervisionPeriod,
)
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_ip_index_for_tests,
)


class TestAssessmentsCompletedInComplianceMonth(unittest.TestCase):
    """Tests for assessments_in_compliance_month."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_ND")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsNdIncarcerationDelegate()
        )

    def test_completed_assessments_in_compliance_month(self) -> None:
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
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=assessments,
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        self.assertEqual(
            len(expected_assessments),
            us_nd_supervision_compliance._completed_assessments_on_date(
                evaluation_date
            ),
        )


class TestFaceToFaceContactsInComplianceMonth(unittest.TestCase):
    """Tests for face_to_face_contacts_in_compliance_month."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_ND")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsNdIncarcerationDelegate()
        )

    def test_face_to_face_contacts_in_compliance_month(self) -> None:
        evaluation_date = date(2018, 4, 30)
        contact_1 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 4, 1),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_2 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 4, 15),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_3 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_out_of_range = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 3, 30),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_incomplete = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.ATTEMPTED,
        )
        contact_wrong_type = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.COLLATERAL,
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
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=[],
            supervision_contacts=contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )
        self.assertEqual(
            len(expected_contacts),
            us_nd_supervision_compliance._face_to_face_contacts_on_date(
                evaluation_date
            ),
        )


class TestGuidelinesApplicableForCase(unittest.TestCase):
    """Tests the guidelines_applicable_for_case function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_ND")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsNdIncarcerationDelegate()
        )

    def test_guidelines_applicable_for_case_external_unknown(self) -> None:
        """The guidelines should not be applicable to people who are not classified."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.EXTERNAL_UNKNOWN,
            supervision_level_raw_text="5",
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case(
            start_of_supervision
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_interstate_compact(self) -> None:
        """The guidelines should not be applicable to people who are in interstate compact."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.INTERSTATE_COMPACT,
            supervision_level_raw_text="9",
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case(
            start_of_supervision
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_diversion(self) -> None:
        """The guidelines should not be applicable to people who are in diversion programs."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.DIVERSION,
            supervision_level_raw_text="7",
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case(
            start_of_supervision
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case(self) -> None:
        """The guidelines should be applicable to people who are not in interstate compact or not classified."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="2",
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case(
            start_of_supervision
        )

        self.assertTrue(applicable)

    def test_guidelines_applicable_for_case_invalid_supervision_level(self) -> None:
        """The guidelines should not be applicable to a supervision level other than MINIMUM,
        MEDIUM, MAXIMUM."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="SO HIGH",
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case(
            start_of_supervision
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_invalid_supervision_case_type(self) -> None:
        """The guidelines should not be applicable to a case type that is not GENERAL or SEX_OFFENSE."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="SO HIGH",
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        applicable = us_nd_supervision_compliance._guidelines_applicable_for_case(
            start_of_supervision
        )

        self.assertFalse(applicable)


class TestNextRecommendedFaceToFaceContactDate(unittest.TestCase):
    """Tests the next_recommended_face_to_face_date functions."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_ND")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsNdIncarcerationDelegate()
        )

    def generate_supervision_case_compliance(
        self,
        start_of_supervision: date,
        termination_date: date,
        supervision_level: StateSupervisionLevel,
        contact_dates: List[date],
    ) -> UsNdSupervisionCaseCompliance:
        """Tests face to face contacts for the maximum level case."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=termination_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=supervision_level,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=contact_date,
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
            for contact_date in contact_dates
        ]

        # note that this is outside 30 days but within the next calendar month
        # so we should be good
        return UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

    def test_next_recommended_face_to_face_date(self) -> None:
        """Tests for when the face to face contacts is sufficient."""
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code=StateCode.US_ND.value,
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        self.assertTrue(
            us_nd_supervision_compliance._next_recommended_face_to_face_date(
                start_of_supervision
            )
        )

    def test_next_recommended_face_to_face_date_contacts_before_supervision_start(
        self,
    ) -> None:
        """Tests for when the only face to face contacts is occurs prior to start of supervision."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                # Only contact happened before supervision started
                contact_date=start_of_supervision - relativedelta(days=100),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=3)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_face_to_face = (
            us_nd_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 6, 30))

    def test_next_recommended_face_to_face_date_contacts_attempted_general_case(
        self,
    ) -> None:
        """Tests when the only face to face contact is attempted, but not completed."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.DIRECT,
                # Only contact was not completed
                status=StateSupervisionContactStatus.ATTEMPTED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=2)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_face_to_face = (
            us_nd_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 4, 30))

    def test_next_recommended_face_to_face_date_contacts_invalid_contact_type_general_case(
        self,
    ) -> None:
        """Tests when the only contact is not a valid type for face to face."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=supervision_period.start_date,
                # Only contact is invalid type
                contact_type=StateSupervisionContactType.COLLATERAL,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=5)
        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_face_to_face = (
            us_nd_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 6, 30))

    def test_next_recommended_face_to_face_date_contacts_invalid_supervision_level(
        self,
    ) -> None:
        """Tests when the only contact is not a valid type for face to face."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=start_of_supervision,
                # Only contact is invalid type
                contact_type=StateSupervisionContactType.COLLATERAL,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=5)
        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_face_to_face = (
            us_nd_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 6, 30))

    @parameterized.expand(
        [
            (
                "minimum",
                StateSupervisionLevel.MINIMUM,
                [date(2018, 3, 5) + relativedelta(days=89)],
                date(2018, 3, 5) + relativedelta(days=170),
            ),
            (
                "medium",
                StateSupervisionLevel.MEDIUM,
                [date(2018, 3, 5) + relativedelta(days=58)],
                date(2018, 3, 5) + relativedelta(days=99),
            ),
            # note that the evaluation date is outside 30 days but within the next
            # calendar month so the contact requirement should still be fulfilled
            (
                "maximum",
                StateSupervisionLevel.MAXIMUM,
                [
                    date(2018, 3, 20),
                    date(2018, 4, 2),
                ],
                date(2018, 5, 28),
            ),
        ]
    )
    def test_next_recommended_face_to_face_date_contacts_per_level(
        self,
        _name: str,
        supervision_level: StateSupervisionLevel,
        contact_dates: List[date],
        evaluation_date: date,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)

        us_nd_supervision_compliance = self.generate_supervision_case_compliance(
            start_of_supervision=start_of_supervision,
            termination_date=date(2018, 5, 19),
            supervision_level=supervision_level,
            contact_dates=contact_dates,
        )

        next_recommended_face_to_face_date = (
            us_nd_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertIsNotNone(next_recommended_face_to_face_date)

    @parameterized.expand(
        [
            (
                "minimum",
                StateSupervisionLevel.MINIMUM,
                [date(2018, 3, 5) + relativedelta(days=89)],
                date(2018, 3, 5) + relativedelta(days=290),
                date(2018, 9, 30),
            ),
            (
                "medium",
                StateSupervisionLevel.MEDIUM,
                [date(2018, 4, 30)],
                date(2018, 7, 1),
                date(2018, 6, 30),
            ),
            (
                "maximum",
                StateSupervisionLevel.MAXIMUM,
                [date(2018, 4, 2)],
                date(2018, 6, 1),
                date(2018, 5, 31),
            ),
        ]
    )
    def test_next_recommended_face_to_face_date_insufficient_contacts_per_level(
        self,
        _name: str,
        supervision_level: StateSupervisionLevel,
        contact_dates: List[date],
        evaluation_date: date,
        expected_contact_date: date,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)

        us_nd_supervision_compliance = self.generate_supervision_case_compliance(
            start_of_supervision=start_of_supervision,
            termination_date=date(2018, 5, 19),
            supervision_level=supervision_level,
            contact_dates=contact_dates,
        )

        next_face_to_face = (
            us_nd_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, expected_contact_date)


class TestReassessmentRequirementAreMet(unittest.TestCase):
    """Tests the reassessment_requirements_are_met function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_ND")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsNdIncarcerationDelegate()
        )

    def test_next_recommended_reassessment(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessment_date = date(2018, 4, 2)
        assessment_score = 25
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_reassessment = us_nd_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score
        )

        self.assertEqual(next_reassessment, date(2018, 10, 31))

    def test_reassessment_requirements_are_not_met(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ND.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessment_date = date(2010, 4, 2)
        assessment_score = 25
        assessment = StateAssessment.new_with_defaults(
            state_code=StateCode.US_ND.value,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=assessment_date,
            assessment_score=assessment_score,
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[assessment],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        reassessment_deadline = (
            us_nd_supervision_compliance._next_recommended_reassessment(
                assessment_date,
                assessment_score,
            )
        )

        self.assertEqual(reassessment_deadline, date(2010, 10, 31))


class TestNextRecommendedHomeVisitDate(unittest.TestCase):
    """Tests the next_recommended_home_visit_date function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_ND")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsNdIncarcerationDelegate()
        )

    def test_next_recommended_home_visit_date_initial_visit_met(
        self,
    ) -> None:
        """Tests for when the initial home visit standard is met."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=start_of_supervision + relativedelta(days=1),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(days=59)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_recommended_home_visit_date = (
            us_nd_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertIsNone(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_initial_visit_not_met(
        self,
    ) -> None:
        """Tests for when the initial home visit standard is not met."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts: List[StateSupervisionContact] = []

        evaluation_date = start_of_supervision + relativedelta(
            days=NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS + 1
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_recommended_home_visit_date = (
            us_nd_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertFalse(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_initial_visit_not_at_residence(
        self,
    ) -> None:
        """Tests for when the only contact does not have location RESIDENCE."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ND",
                contact_date=start_of_supervision + relativedelta(days=1),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.SUPERVISION_OFFICE,
            )
        ]

        evaluation_date = start_of_supervision + relativedelta(
            days=NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS + 1
        )

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_recommended_home_visit_date = (
            us_nd_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertFalse(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_no_initial_visit_within_range(
        self,
    ) -> None:
        """Tests for when the home visit standard is met because the initial period has
        not yet passed."""
        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_contacts: List[StateSupervisionContact] = []

        evaluation_date = start_of_supervision + relativedelta(days=59)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_recommended_home_visit_date = (
            us_nd_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertIsNone(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_medium_level(
        self,
    ) -> None:
        """Tests home visit frequency for medium level"""
        contact_1 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 3, 8),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
            location=StateSupervisionContactLocation.RESIDENCE,
        )
        contact_2 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2019, 3, 6),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
            location=StateSupervisionContactLocation.RESIDENCE,
        )

        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts = [contact_1, contact_2]

        evaluation_date = start_of_supervision + relativedelta(days=375)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_recommended_home_visit_date = (
            us_nd_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertTrue(next_recommended_home_visit_date)

    def test_next_recommended_home_visit_date_medium_level_not_met(
        self,
    ) -> None:
        """Tests home visit frequency for medium level when standard is not met"""
        contact_1 = StateSupervisionContact.new_with_defaults(
            state_code="US_ND",
            contact_date=date(2018, 3, 5),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
            location=StateSupervisionContactLocation.RESIDENCE,
        )

        start_of_supervision = date(2018, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts = [contact_1]

        evaluation_date = start_of_supervision + relativedelta(days=375)

        us_nd_supervision_compliance = UsNdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        next_recommended_home_visit_date = (
            us_nd_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertIsNotNone(next_recommended_home_visit_date)
