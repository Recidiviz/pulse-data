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
from typing import List, Optional

from dateutil.relativedelta import relativedelta
from parameterized import parameterized

from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_delegate import (
    UsPaIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_compliance import (
    NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
    NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS,
    UsPaSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_assessment import StateAssessmentType

# pylint: disable=protected-access
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
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
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
)
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_ip_index_for_tests,
)


class TestAssessmentsInComplianceMonth(unittest.TestCase):
    """Tests for _completed_assessments_on_date."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_PA")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsPaIncarcerationDelegate()
        )

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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsPaIncarcerationDelegate()
        )

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
            contact_type=StateSupervisionContactType.COLLATERAL,
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsPaIncarcerationDelegate()
        )

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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
                contact_type=StateSupervisionContactType.COLLATERAL,
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 7))

    @parameterized.expand(
        [
            (
                "monitored_with_contact",
                StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
                [date(2018, 3, 5) + relativedelta(months=1, days=5)],
                date(2018, 3, 5) + relativedelta(months=12),
                None,
            ),
            (
                "monitored_no_contacts",
                StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
                [],
                date(2018, 3, 5)
                + relativedelta(
                    days=(NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10)
                ),
                None,
            ),
            (
                "limited",
                StateSupervisionLevel.LIMITED,
                [date(2018, 3, 5) + relativedelta(months=1, days=5)],
                date(2018, 3, 5) + relativedelta(months=12),
                None,
            ),
            (
                "minimum",
                StateSupervisionLevel.MINIMUM,
                [date(2018, 3, 5) + relativedelta(months=1, days=1)],
                date(2018, 3, 5) + relativedelta(months=3),
                date(2018, 7, 5),
            ),
            (
                "minimum_no_contacts",
                StateSupervisionLevel.MINIMUM,
                [],
                date(2018, 3, 5)
                + relativedelta(
                    days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10
                ),
                date(2018, 3, 7),
            ),
            (
                "minimum_out_of_bounds",
                StateSupervisionLevel.MINIMUM,
                [date(2018, 3, 5) + relativedelta(days=30)],
                date(2018, 3, 5) + relativedelta(days=180),
                date(2018, 7, 3),
            ),
            (
                "medium",
                StateSupervisionLevel.MEDIUM,
                [date(2018, 3, 5) + relativedelta(months=1, days=1)],
                date(2018, 3, 5) + relativedelta(months=2),
                date(2018, 5, 6),
            ),
            (
                "medium_no_contacts",
                StateSupervisionLevel.MEDIUM,
                [],
                date(2018, 3, 5)
                + relativedelta(
                    days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10
                ),
                date(2018, 3, 7),
            ),
            (
                "medium_out_of_bounds",
                StateSupervisionLevel.MEDIUM,
                [date(2018, 3, 5) + relativedelta(days=20)],
                date(2018, 3, 5) + relativedelta(days=60),
                date(2018, 4, 24),
            ),
            (
                "maximum",
                StateSupervisionLevel.MAXIMUM,
                [
                    date(2018, 3, 5) + relativedelta(days=32),
                    date(2018, 3, 5) + relativedelta(days=40),
                ],
                date(2018, 3, 5) + relativedelta(days=60),
                date(2018, 5, 6),
            ),
            (
                "maximum_no_contacts",
                StateSupervisionLevel.MAXIMUM,
                [],
                date(2018, 3, 5)
                + relativedelta(
                    days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10
                ),
                date(2018, 3, 7),
            ),
            (
                "maximum_out_of_bounds",
                StateSupervisionLevel.MAXIMUM,
                [
                    date(2018, 3, 5) + relativedelta(days=20),
                    date(2018, 3, 5) + relativedelta(days=40),
                ],
                date(2018, 3, 5) + relativedelta(days=60),
                date(2018, 4, 24),
            ),
            (
                "high",
                StateSupervisionLevel.HIGH,
                [
                    date(2018, 3, 5) + relativedelta(months=1, days=5),
                    date(2018, 3, 5) + relativedelta(months=1, days=10),
                    date(2018, 3, 5) + relativedelta(months=1, days=15),
                    date(2018, 3, 5) + relativedelta(months=1, days=20),
                ],
                date(2018, 3, 5) + relativedelta(months=2),
                date(2018, 5, 10),
            ),
            (
                "high_no_contacts",
                StateSupervisionLevel.HIGH,
                [],
                date(2018, 3, 5)
                + relativedelta(
                    days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10
                ),
                date(2018, 3, 7),
            ),
            (
                "high_out_of_bounds",
                StateSupervisionLevel.HIGH,
                [
                    date(2018, 3, 5)
                    + relativedelta(
                        days=20
                    ),  # No in correct time window, need 4 within 30 days
                    date(2018, 3, 5) + relativedelta(months=1, days=10),
                ],
                date(2018, 3, 5) + relativedelta(months=2),
                date(2018, 4, 4),
            ),
        ]
    )
    def test_next_recommended_face_to_face_date_contacts_per_level(
        self,
        _name: str,
        supervision_level: StateSupervisionLevel,
        contact_dates: List[date],
        evaluation_date: date,
        expected_contact_date: Optional[date],
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
            supervision_level=supervision_level,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=contact_date,
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
            for contact_date in contact_dates
        ]

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_face_to_face = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, expected_contact_date)

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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_recommended_face_to_face_date = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertIsNotNone(next_recommended_face_to_face_date)

    def test_next_recommended_face_to_face_date_skipped_if_incarcerated(self) -> None:
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

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=default_normalized_ip_index_for_tests(
                incarceration_periods=[
                    StateIncarcerationPeriod.new_with_defaults(
                        incarceration_period_id=1,
                        state_code=StateCode.US_PA.value,
                        incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                        admission_date=date(2018, 4, 1),
                        release_date=date(2018, 4, 30),
                        admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    )
                ],
                incarceration_delegate=UsPaIncarcerationDelegate(),
            ),
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_face_to_face_date = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                date(2018, 4, 2)
            )
        )

        self.assertIsNone(next_face_to_face_date)

    def test_next_recommended_face_to_face_date_skipped_if_in_parole_board_hold(
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

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=default_normalized_ip_index_for_tests(
                incarceration_periods=[
                    StateIncarcerationPeriod.new_with_defaults(
                        incarceration_period_id=1,
                        state_code=StateCode.US_PA.value,
                        incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN,
                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                        admission_date=date(2018, 4, 1),
                        release_date=date(2018, 4, 30),
                        admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                    )
                ],
                incarceration_delegate=UsPaIncarcerationDelegate(),
            ),
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_face_to_face_date = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                date(2018, 4, 2)
            )
        )

        self.assertIsNone(next_face_to_face_date)

    def test_next_recommended_face_to_face_date_skipped_if_past_max_date(self) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 4, 2),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[
                StateIncarcerationSentence.new_with_defaults(
                    state_code=StateCode.US_PA.value,
                    external_id="sentence1",
                    status=StateSentenceStatus.SERVING,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    date_imposed=date(2017, 2, 1),
                    start_date=date(2017, 2, 3),
                    projected_max_release_date=date(2018, 3, 18),
                    completion_date=date(2018, 4, 2),
                )
            ],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_face_to_face_date = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                date(2018, 3, 20)
            )
        )

        self.assertIsNone(next_face_to_face_date)

    def test_next_recommended_face_to_face_date_skipped_if_actively_absconding(
        self,
    ) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 4, 2),
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_face_to_face_date = (
            us_pa_supervision_compliance._next_recommended_face_to_face_date(
                date(2018, 3, 20)
            )
        )

        self.assertIsNone(next_face_to_face_date)


class TestNextRecommendedHomeVisitDate(unittest.TestCase):
    """Tests the next_recommended_home_visit_date function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_PA")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsPaIncarcerationDelegate()
        )

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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_recommended_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertIsNotNone(next_recommended_home_visit_date)

    @parameterized.expand(
        [
            (
                "medium",
                StateSupervisionLevel.MEDIUM,
                [date(2000, 1, 21) + relativedelta(days=20)],
                date(2000, 1, 21) + relativedelta(days=30),
                date(2000, 4, 10),
            ),
            (
                "medium_out_of_bounds",
                StateSupervisionLevel.MEDIUM,
                [
                    date(2000, 1, 21) + relativedelta(days=10)
                ],  # Out of bounds in that it's not within 60 days of evaluation,
                date(2000, 1, 21) + relativedelta(days=70),
                date(2000, 3, 31),
            ),
            (
                "maximum",
                StateSupervisionLevel.MAXIMUM,
                [date(2000, 1, 21) + relativedelta(days=20)],
                date(2000, 1, 21) + relativedelta(days=30),
                date(2000, 3, 11),
            ),
            (
                "maximum_out_of_bounds",
                StateSupervisionLevel.MAXIMUM,
                [date(2000, 1, 21) + relativedelta(days=10)],
                date(2000, 1, 21) + relativedelta(days=45),
                date(2000, 3, 1),
            ),
            (
                "high",
                StateSupervisionLevel.HIGH,
                [date(2000, 1, 21) + relativedelta(days=20)],
                date(2000, 1, 21) + relativedelta(days=30),
                date(2000, 3, 11),
            ),
            (
                "high_out_of_bounds",
                StateSupervisionLevel.HIGH,
                [date(2000, 1, 21) + relativedelta(days=10)],
                date(2000, 1, 21) + relativedelta(days=45),
                date(2000, 3, 1),
            ),
        ]
    )
    def test_next_recommended_home_visit_date_contacts_per_level(
        self,
        _name: str,
        supervision_level: StateSupervisionLevel,
        contact_dates: List[date],
        evaluation_date: date,
        expected_contact_date: Optional[date],
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

        supervision_contacts: List[StateSupervisionContact] = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=contact_date,
                contact_type=StateSupervisionContactType.DIRECT,
                location=StateSupervisionContactLocation.RESIDENCE,
                status=StateSupervisionContactStatus.COMPLETED,
            )
            for contact_date in contact_dates
        ]

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_recommended_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertEqual(next_recommended_home_visit_date, expected_contact_date)

    def test_next_recommended_home_visit_date_skipped_if_in_jail(self) -> None:
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

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=default_normalized_ip_index_for_tests(
                incarceration_periods=[
                    StateIncarcerationPeriod.new_with_defaults(
                        incarceration_period_id=1,
                        state_code=StateCode.US_PA.value,
                        incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                        admission_date=date(2018, 4, 1),
                        release_date=date(2018, 4, 30),
                        admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    )
                ],
                incarceration_delegate=UsPaIncarcerationDelegate(),
            ),
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                date(2018, 4, 2)
            )
        )

        self.assertIsNone(next_home_visit_date)

    def test_next_recommended_home_visit_date_skipped_if_past_max_date(self) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 4, 2),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                date(2018, 3, 20)
            )
        )

        self.assertIsNone(next_home_visit_date)

    def test_next_recommended_home_visit_date_skipped_if_actively_absconding(
        self,
    ) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 4, 2),
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_home_visit_date = (
            us_pa_supervision_compliance._next_recommended_home_visit_date(
                date(2018, 3, 20)
            )
        )

        self.assertIsNone(next_home_visit_date)


class TestNextRecommendedTreatmentCollateralVisitDate(unittest.TestCase):
    """Tests the _next_recommended_treatment_collateral_contact_date function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_PA")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsPaIncarcerationDelegate()
        )

    def test_next_recommended_treatment_collateral_contact_date_contacts_not_required(
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
            supervision_level=StateSupervisionLevel.LIMITED,
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_recommended_treatment_collateral_contact_date = us_pa_supervision_compliance._next_recommended_treatment_collateral_contact_date(
            evaluation_date
        )

        self.assertIsNone(next_recommended_treatment_collateral_contact_date)

    @parameterized.expand(
        [
            ("minimum", StateSupervisionLevel.MINIMUM),
            ("medium", StateSupervisionLevel.MEDIUM),
            ("maximum", StateSupervisionLevel.MAXIMUM),
            ("high", StateSupervisionLevel.HIGH),
        ]
    )
    def test_next_recommended_treatment_collateral_contact_date_no_contacts(
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
            days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_recommended_treatment_collateral_date = us_pa_supervision_compliance._next_recommended_treatment_collateral_contact_date(
            evaluation_date
        )

        self.assertIsNotNone(next_recommended_treatment_collateral_date)

    @parameterized.expand(
        [
            (
                "high",
                StateSupervisionLevel.HIGH,
                [date(2000, 1, 21) + relativedelta(days=20)],
                date(2000, 1, 21) + relativedelta(days=30),
                date(2000, 2, 20),
            ),
            (
                "high_out_of_bounds",
                StateSupervisionLevel.HIGH,
                [date(2000, 1, 21) + relativedelta(days=20)],
                date(2000, 1, 21) + relativedelta(days=45),
                date(2000, 2, 20),
            ),
            (
                "maximum",
                StateSupervisionLevel.MAXIMUM,
                [date(2000, 1, 21) + relativedelta(days=20)],
                date(2000, 1, 21) + relativedelta(days=30),
                date(2000, 3, 11),
            ),
            (
                "maximum_out_of_bounds",
                StateSupervisionLevel.MAXIMUM,
                [date(2000, 1, 21) + relativedelta(days=10)],
                date(2000, 1, 21) + relativedelta(days=50),
                date(2000, 3, 1),
            ),
            (
                "medium",
                StateSupervisionLevel.MEDIUM,
                [date(2000, 1, 21) + relativedelta(days=20)],
                date(2000, 1, 21) + relativedelta(days=30),
                date(2000, 5, 10),
            ),
            (
                "medium_out_of_bounds",
                StateSupervisionLevel.MEDIUM,
                [date(2000, 1, 21) + relativedelta(days=20)],
                date(2000, 1, 21) + relativedelta(days=100),
                date(2000, 5, 10),
            ),
            (
                "minimum",
                StateSupervisionLevel.MINIMUM,
                [date(2000, 1, 21) + relativedelta(days=20)],
                date(2000, 1, 21) + relativedelta(days=30),
                date(2000, 5, 10),
            ),
            (
                "minimum_out_of_bounds",
                StateSupervisionLevel.MINIMUM,
                [date(2000, 1, 21) + relativedelta(days=20)],
                date(2000, 1, 21) + relativedelta(days=100),
                date(2000, 5, 10),
            ),
        ]
    )
    def test_next_recommended_treatment_collateral_contact_date_per_level(
        self,
        _name: str,
        supervision_level: StateSupervisionLevel,
        contact_dates: List[date],
        evaluation_date: date,
        expected_contact_date: date,
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

        supervision_contacts: List[StateSupervisionContact] = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_PA",
                contact_date=contact_date,
                contact_type=StateSupervisionContactType.COLLATERAL,
                location=StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
            for contact_date in contact_dates
        ]

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_recommended_treatment_collateral_date = us_pa_supervision_compliance._next_recommended_treatment_collateral_contact_date(
            evaluation_date
        )

        self.assertEqual(
            next_recommended_treatment_collateral_date, expected_contact_date
        )

    def test_next_recommended_treatment_collateral_contact_date_skipped_if_incarcerated(
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

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=default_normalized_ip_index_for_tests(
                incarceration_periods=[
                    StateIncarcerationPeriod.new_with_defaults(
                        incarceration_period_id=1,
                        state_code=StateCode.US_PA.value,
                        incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                        admission_date=date(2018, 4, 1),
                        release_date=date(2018, 4, 30),
                        admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    )
                ],
                incarceration_delegate=UsPaIncarcerationDelegate(),
            ),
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_treatment_collateral_date = us_pa_supervision_compliance._next_recommended_treatment_collateral_contact_date(
            date(2018, 4, 2)
        )

        self.assertIsNone(next_treatment_collateral_date)

    def test_next_recommended_treatment_collateral_contact_date_skipped_if_in_parole_board_hold(
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

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=default_normalized_ip_index_for_tests(
                incarceration_periods=[
                    StateIncarcerationPeriod.new_with_defaults(
                        incarceration_period_id=1,
                        state_code=StateCode.US_PA.value,
                        incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN,
                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                        admission_date=date(2018, 4, 1),
                        release_date=date(2018, 4, 30),
                        admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                    )
                ],
                incarceration_delegate=UsPaIncarcerationDelegate(),
            ),
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_treatment_collateral_date = us_pa_supervision_compliance._next_recommended_treatment_collateral_contact_date(
            date(2018, 4, 2)
        )

        self.assertIsNone(next_treatment_collateral_date)

    def test_next_recommended_treatment_collateral_contact_date_skipped_if_past_max_date(
        self,
    ) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 4, 2),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[
                StateIncarcerationSentence.new_with_defaults(
                    state_code=StateCode.US_PA.value,
                    external_id="sentence1",
                    status=StateSentenceStatus.SERVING,
                    incarceration_type=StateIncarcerationType.STATE_PRISON,
                    date_imposed=date(2017, 2, 1),
                    start_date=date(2017, 2, 3),
                    projected_max_release_date=date(2018, 3, 18),
                    completion_date=date(2018, 4, 2),
                )
            ],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_treatment_collateral_date = us_pa_supervision_compliance._next_recommended_treatment_collateral_contact_date(
            date(2018, 3, 20)
        )

        self.assertIsNone(next_treatment_collateral_date)

    def test_next_recommended_treatment_collateral_contact_date_present_if_actively_absconding(
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

        supervision_contacts: List[StateSupervisionContact] = []

        evaluation_date = start_of_supervision + relativedelta(
            days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10
        )

        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        next_recommended_treatment_collateral_date = us_pa_supervision_compliance._next_recommended_treatment_collateral_contact_date(
            evaluation_date
        )

        self.assertIsNotNone(next_recommended_treatment_collateral_date)


class TestGuidelinesApplicableForCase(unittest.TestCase):
    """Tests the guidelines_applicable_for_case function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_PA")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsPaIncarcerationDelegate()
        )

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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        applicable = us_pa_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertFalse(applicable)


class TestNextRecommendedReassessment(unittest.TestCase):
    """Tests the _next_recommended_reassessment function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_XX")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsPaIncarcerationDelegate()
        )

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
            supervision_level=StateSupervisionLevel.MEDIUM,
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertIsNone(reassessment_date)

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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertIsNone(reassessment_date)

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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertIsNone(reassessment_date)

    def test_next_recommended_reassessment_can_skip_reassessment_due_to_incarcerated(
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=default_normalized_ip_index_for_tests(
                incarceration_periods=[
                    StateIncarcerationPeriod.new_with_defaults(
                        incarceration_period_id=1,
                        state_code=StateCode.US_PA.value,
                        incarceration_type=StateIncarcerationType.COUNTY_JAIL,
                        admission_date=date(2018, 4, 1),
                        release_date=date(2018, 4, 30),
                        admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    )
                ],
                incarceration_delegate=UsPaIncarcerationDelegate(),
            ),
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertIsNone(reassessment_date)

    def test_next_recommended_reassessment_can_skip_reassessment_due_to_in_parole_board_hold(
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=default_normalized_ip_index_for_tests(
                incarceration_periods=[
                    StateIncarcerationPeriod.new_with_defaults(
                        incarceration_period_id=1,
                        state_code=StateCode.US_PA.value,
                        incarceration_type=StateIncarcerationType.EXTERNAL_UNKNOWN,
                        specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                        admission_date=date(2018, 4, 1),
                        release_date=date(2018, 4, 30),
                        admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                    )
                ],
                incarceration_delegate=UsPaIncarcerationDelegate(),
            ),
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertIsNone(reassessment_date)

    def test_next_recommended_reassessment_can_skip_reassessment_due_to_past_max_date(
        self,
    ) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 4, 2),
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
            incarceration_sentences=[
                StateIncarcerationSentence.new_with_defaults(
                    state_code=StateCode.US_PA.value,
                    external_id="sentence1",
                    incarceration_sentence_id=1111,
                    start_date=start_of_supervision,
                    projected_max_release_date=date(2018, 3, 29),
                    status=StateSentenceStatus.SERVING,
                )
            ],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 1)
        )

        self.assertIsNone(reassessment_date)

    def test_next_recommended_reassessment_can_skip_reassessment_due_to_within_max_date(
        self,
    ) -> None:
        start_of_supervision = date(2017, 3, 5)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_PA.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 6, 30),
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
            incarceration_sentences=[
                StateIncarcerationSentence.new_with_defaults(
                    state_code=StateCode.US_PA.value,
                    external_id="sentence1",
                    incarceration_sentence_id=1111,
                    start_date=start_of_supervision,
                    projected_max_release_date=date(2018, 4, 20),
                    status=StateSentenceStatus.SERVING,
                )
            ],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )

        reassessment_date = us_pa_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score, date(2018, 4, 2)
        )

        self.assertIsNone(reassessment_date)


class TestSupervisionDowngrades(unittest.TestCase):
    """Tests the _get_recommended_supervision_downgrade_level function."""

    def setUp(self) -> None:
        self.start_of_supervision = date(2018, 3, 5)
        self.evaluation_date = date(2021, 1, 1)
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsPaIncarcerationDelegate()
        )

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
        us_pa_supervision_compliance = UsPaSupervisionCaseCompliance(
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )
        self.assertIsNone(
            us_pa_supervision_compliance._get_recommended_supervision_downgrade_level(
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
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
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsPaSupervisionDelegate(),
        )
        recommended_level = (
            compliance_downgrade._get_recommended_supervision_downgrade_level(
                self.evaluation_date
            )
        )
        assert recommended_level is not None
        self.assertTrue(recommended_level < level)
