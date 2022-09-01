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
from typing import List, Optional

from dateutil.relativedelta import relativedelta
from parameterized import parameterized

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_delegate import (
    UsIdIncarcerationDelegate,
)
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
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_delegate import (
    UsIdSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType

# pylint: disable=protected-access
from recidiviz.common.constants.state.state_person import StateGender
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
)
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_ip_index_for_tests,
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
        self.person = StatePerson.new_with_defaults(state_code="US_ID")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsIdIncarcerationDelegate()
        )

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
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
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

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=assessments,
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
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
        self.person = StatePerson.new_with_defaults(state_code="US_ID")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsIdIncarcerationDelegate()
        )

    def test_face_to_face_contacts_in_compliance_month(self) -> None:
        evaluation_date = date(2018, 4, 30)
        contact_1 = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 4, 1),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_2 = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 4, 15),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_3 = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_out_of_range = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 3, 30),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.COMPLETED,
        )
        contact_incomplete = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.DIRECT,
            status=StateSupervisionContactStatus.ATTEMPTED,
        )
        contact_wrong_type = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            contact_date=date(2018, 4, 30),
            contact_type=StateSupervisionContactType.COLLATERAL,
            status=StateSupervisionContactStatus.COMPLETED,
        )

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
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

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=evaluation_date,
            assessments=[],
            supervision_contacts=contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )
        self.assertEqual(
            len(expected_contacts),
            us_id_supervision_compliance._face_to_face_contacts_on_date(
                evaluation_date
            ),
        )


class TestNextRecommendedContactDate(unittest.TestCase):
    """Tests the following functions:
        - _next_recommended_face_to_face_date
        - _next_recommended_home_visit_date
        - _next_recommended_treatment_collateral_contact_date
    function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_ID")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsIdIncarcerationDelegate()
        )

    def test_next_recommended_face_to_face_date_start_of_supervision_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level_raw_text="MODERATE",
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.DIRECT,
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_next_recommended_face_to_face_date_contacts_before_supervision_start_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                # Only contact happened before supervision started
                contact_date=start_of_supervision - relativedelta(days=100),
                contact_type=StateSupervisionContactType.DIRECT,
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_face_to_face = (
            us_id_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 8))

    def test_next_recommended_face_to_face_date_contacts_attempted_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                contact_type=StateSupervisionContactType.DIRECT,
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_face_to_face = (
            us_id_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 8))

    def test_next_recommended_face_to_face_date_contacts_invalid_contact_type_general_case(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=supervision_period.start_date,
                # Only contact is invalid type
                contact_type=StateSupervisionContactType.COLLATERAL,
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_face_to_face = (
            us_id_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, date(2018, 3, 8))

    @parameterized.expand(
        [
            (
                "minimum",
                StateSupervisionLevel.MINIMUM,
                "LOW",
                [date(2018, 3, 6)],
                date(2018, 3, 5)
                + relativedelta(
                    days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10
                ),
                date(2018, 9, 2),
            ),
            (
                "minimum_deprecated",
                StateSupervisionLevel.MINIMUM,
                "LEVEL 1",
                [date(2018, 3, 6)],
                date(2018, 3, 5)
                + relativedelta(
                    days=NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS + 10
                ),
                None,
            ),
            (
                "maximum_up_to_date_one_contact",
                StateSupervisionLevel.MAXIMUM,
                "LEVEL 4",
                [date(2018, 3, 6)],
                date(2018, 3, 5)
                + relativedelta(
                    days=(
                        DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                        - 10
                    )
                ),
                date(2018, 4, 4),
            ),
            (
                "maximum_up_to_date_two_contacts",
                StateSupervisionLevel.MAXIMUM,
                "LEVEL 4",
                [
                    date(2018, 3, 5) + relativedelta(days=30),
                    date(2018, 3, 5) + relativedelta(days=40),
                ],
                date(2018, 3, 5) + relativedelta(days=50),
                date(2018, 5, 4),
            ),
            (
                "maximum_not_up_to_date",
                StateSupervisionLevel.MAXIMUM,
                "LEVEL 4",
                [
                    date(2018, 3, 5)
                    + relativedelta(
                        days=DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                        + 1
                    )
                ],
                date(2018, 3, 5)
                + relativedelta(
                    days=DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                    + 10
                ),
                date(2018, 3, 8),
            ),
            (
                "high",
                StateSupervisionLevel.HIGH,
                "HIGH",
                [
                    date(2018, 3, 5)
                    + relativedelta(
                        days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10
                    ),
                    date(2018, 3, 5)
                    + relativedelta(
                        days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 5
                    ),
                ],
                date(2018, 3, 5)
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                ),
                date(2018, 3, 30),
            ),
            (
                "high_deprecated",
                StateSupervisionLevel.HIGH,
                "LEVEL 3",
                [
                    date(2018, 3, 5)
                    + relativedelta(
                        days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                        - 10
                    ),
                    date(2018, 3, 5)
                    + relativedelta(
                        days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                        - 5
                    ),
                ],
                date(2018, 3, 5)
                + relativedelta(
                    days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                ),
                date(2018, 4, 29),
            ),
            (
                "high_up_to_date",
                StateSupervisionLevel.HIGH,
                "HIGH",
                [date(2018, 3, 5)],
                date(2018, 3, 5)
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10
                ),
                date(2018, 3, 20),
            ),
            (
                "high_deprecated_up_to_date",
                StateSupervisionLevel.HIGH,
                "LEVEL 3",
                [date(2018, 3, 5)],
                date(2018, 3, 5)
                + relativedelta(
                    days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                    - 10
                ),
                date(2018, 4, 4),
            ),
            (
                "high_not_up_to_date",
                StateSupervisionLevel.HIGH,
                "HIGH",
                [
                    date(2018, 3, 5) + relativedelta(days=15),
                    date(2018, 3, 5)
                    + relativedelta(
                        days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 1
                    ),
                ],
                date(2018, 3, 5)
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                ),
                date(2018, 4, 4),
            ),
            (
                "high_not_up_to_date_deprecated",
                StateSupervisionLevel.HIGH,
                "LEVEL 3",
                [date(2018, 3, 5)],
                date(2018, 3, 5)
                + relativedelta(
                    days=DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                    + 10
                ),
                date(2018, 4, 4),
            ),
            (
                "medium_up_to_date",
                StateSupervisionLevel.MEDIUM,
                "MODERATE",
                [date(2018, 3, 5)],
                date(2018, 3, 5)
                + relativedelta(
                    days=(MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE - 10)
                ),
                date(2018, 4, 19),
            ),
            (
                "medium_up_to_date_deprecated",
                StateSupervisionLevel.MEDIUM,
                "LEVEL 2",
                [date(2018, 3, 5)],
                date(2018, 3, 5)
                + relativedelta(
                    days=DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                    - 10
                ),
                date(2018, 9, 1),
            ),
            (
                "medium_not_up_to_date",
                StateSupervisionLevel.MEDIUM,
                "MODERATE",
                [
                    date(2018, 3, 5) + relativedelta(days=15),
                    date(2018, 3, 5)
                    + relativedelta(
                        days=MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE + 1
                    ),
                ],
                date(2018, 3, 5)
                + relativedelta(
                    days=MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                ),
                date(2018, 5, 4),
            ),
            (
                "medium_not_up_to_date_deprecated",
                StateSupervisionLevel.MEDIUM,
                "LEVEL 2",
                [date(2018, 3, 5)],
                date(2018, 3, 5)
                + relativedelta(
                    days=DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
                    + 10
                ),
                date(2018, 9, 1),
            ),
        ]
    )
    def test_next_recommended_face_to_face_date_contacts_general_case_per_level(
        self,
        _name: str,
        supervision_level: StateSupervisionLevel,
        supervision_level_raw_text: str,
        contact_dates: List[date],
        evaluation_date: date,
        expected_contact_date: Optional[date],
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=supervision_level,
            supervision_level_raw_text=supervision_level_raw_text,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=contact_date,
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
            for contact_date in contact_dates
        ]

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_recommended_face_to_face_date = (
            us_id_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_recommended_face_to_face_date, expected_contact_date)

    def test_next_recommended_face_to_face_date_contacts_new_case_opened_on_friday_general_case(
        self,
    ) -> None:
        start_of_supervision = date(1999, 8, 13)  # This was a Friday
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    def test_next_recommended_face_to_face_date_contacts_new_case_opened_on_friday_deprecated_general_case(
        self,
    ) -> None:
        start_of_supervision = date(1999, 8, 13)  # This was a Friday
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="LEVEL 2",
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        face_to_face_frequency_sufficient = (
            us_id_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertTrue(face_to_face_frequency_sufficient)

    @parameterized.expand(
        [
            (
                "minimum",
                StateSupervisionLevel.MINIMUM,
                "SO LOW",
                [date(2018, 3, 5) + relativedelta(days=1)],
                date(2018, 3, 5)
                + relativedelta(
                    days=MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE - 1
                ),
                date(2018, 6, 4),
            ),
            (
                "minimum_case_not_met",
                StateSupervisionLevel.MINIMUM,
                "SO LOW",
                [date(2018, 3, 5) + relativedelta(days=1)],
                date(2018, 3, 5)
                + relativedelta(
                    days=MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE + 1
                ),
                date(2018, 6, 4),
            ),
            (
                "medium",
                StateSupervisionLevel.MEDIUM,
                "SO MODERATE",
                [date(2018, 3, 5) + relativedelta(days=1)],
                date(2018, 3, 5)
                + relativedelta(
                    days=MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE - 1
                ),
                date(2018, 4, 5),
            ),
            (
                "medium_case_not_met",
                StateSupervisionLevel.MEDIUM,
                "SO MODERATE",
                [date(2018, 3, 5) + relativedelta(days=1)],
                date(2018, 3, 5)
                + relativedelta(
                    days=MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE + 1
                ),
                date(2018, 4, 5),
            ),
            (
                "high",
                StateSupervisionLevel.HIGH,
                "SO HIGH",
                [
                    date(2018, 3, 5)
                    + relativedelta(
                        days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE
                        - 10
                    ),
                    date(2018, 3, 5)
                    + relativedelta(
                        days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE
                        - 20
                    ),
                ],
                date(2018, 3, 5)
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE
                ),
                date(2018, 4, 14),
            ),
            (
                "high_case_not_met",
                StateSupervisionLevel.HIGH,
                "SO HIGH",
                [
                    date(2018, 3, 5)
                    + relativedelta(
                        days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE
                        - 10
                    ),
                    date(2018, 3, 5)
                    + relativedelta(
                        days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE
                        + 10
                    ),
                ],
                date(2018, 3, 5)
                + relativedelta(
                    days=HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENSE_CASE
                ),
                date(2018, 4, 4),
            ),
        ]
    )
    def test_next_recommended_face_to_face_date_contacts_sex_offense_case_per_level(
        self,
        _name: str,
        supervision_level: StateSupervisionLevel,
        supervision_level_raw_text: str,
        contact_dates: List[date],
        evaluation_date: date,
        expected_contact_date: Optional[date],
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=supervision_level,
            supervision_level_raw_text=supervision_level_raw_text,
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=contact_date,
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
            )
            for contact_date in contact_dates
        ]

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.SEX_OFFENSE,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_face_to_face = (
            us_id_supervision_compliance._next_recommended_face_to_face_date(
                evaluation_date
            )
        )

        self.assertEqual(next_face_to_face, expected_contact_date)

    @parameterized.expand(
        [
            (
                StateSupervisionCaseType.GENERAL,
                StateSupervisionLevel.MINIMUM,
                date(2018, 4, 1),
                date(2019, 4, 1),
            ),  # 1 year
            (
                StateSupervisionCaseType.GENERAL,
                StateSupervisionLevel.MEDIUM,
                date(2018, 4, 1),
                date(2019, 4, 1),
            ),  # 1 year
            (
                StateSupervisionCaseType.GENERAL,
                StateSupervisionLevel.HIGH,
                date(2018, 4, 1),
                date(2018, 9, 28),
            ),  # 180 days
            (
                StateSupervisionCaseType.SEX_OFFENSE,
                StateSupervisionLevel.MINIMUM,
                date(2018, 4, 1),
                date(2018, 6, 30),
            ),  # 90 days
            (
                StateSupervisionCaseType.SEX_OFFENSE,
                StateSupervisionLevel.MEDIUM,
                date(2018, 4, 1),
                date(2018, 5, 31),
            ),  # 60 days
            (
                StateSupervisionCaseType.SEX_OFFENSE,
                StateSupervisionLevel.HIGH,
                date(2018, 4, 1),
                date(2018, 5, 1),
            ),  # 30 days
        ]
    )
    def test_next_recommended_home_visit_has_previous_contacts(
        self,
        case_type: StateSupervisionCaseType,
        supervision_level: StateSupervisionLevel,
        previous_contact_date: date,
        expected_next_contact: date,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=supervision_level,
            supervision_level_raw_text="MODERATE",
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=previous_contact_date,
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
        ]

        evaluation_date = date(2018, 5, 1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_home_visit = (
            us_id_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertEqual(next_home_visit, expected_next_contact)

    def test_next_recommended_home_visit_no_previous_contacts(self) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
        )

        evaluation_date = date(2018, 5, 1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_home_visit = (
            us_id_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertEqual(next_home_visit, date(2018, 4, 4))

    def test_next_recommended_home_visit_non_existent_level(self) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="MODERATE",
        )

        evaluation_date = date(2018, 5, 1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_home_visit = (
            us_id_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertIsNone(next_home_visit)

    def test_next_recommended_home_visit_contact_type_agnostic(self) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
        )

        evaluation_date = date(2019, 5, 1)

        # For US_ID, both collateral and direct contacts at location of residence
        # count as home visits
        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=date(2018, 4, 1),
                contact_type=StateSupervisionContactType.DIRECT,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=date(2019, 4, 1),
                contact_type=StateSupervisionContactType.COLLATERAL,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.RESIDENCE,
            ),
        ]

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_home_visit = (
            us_id_supervision_compliance._next_recommended_home_visit_date(
                evaluation_date
            )
        )

        self.assertEqual(next_home_visit, date(2020, 3, 31))

    @parameterized.expand(
        [
            (
                StateSupervisionLevel.MINIMUM,
                date(2018, 4, 1),
                date(2018, 6, 30),
            ),  # 90 days
            (
                StateSupervisionLevel.MEDIUM,
                date(2018, 4, 1),
                date(2018, 5, 1),
            ),  # 30 days
            (
                StateSupervisionLevel.HIGH,
                date(2018, 4, 1),
                date(2018, 5, 1),
            ),  # 30 days
        ]
    )
    def test_next_recommended_treatment_collateral_contact_has_previous_contacts(
        self,
        supervision_level: StateSupervisionLevel,
        previous_contact_date: date,
        expected_next_contact: date,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=supervision_level,
            supervision_level_raw_text="MODERATE",
        )

        supervision_contacts = [
            StateSupervisionContact.new_with_defaults(
                state_code="US_ID",
                contact_date=previous_contact_date,
                contact_type=StateSupervisionContactType.COLLATERAL,
                status=StateSupervisionContactStatus.COMPLETED,
                location=StateSupervisionContactLocation.TREATMENT_PROVIDER,
            ),
        ]

        evaluation_date = date(2018, 5, 1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=supervision_contacts,
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_treatment_contact = us_id_supervision_compliance._next_recommended_treatment_collateral_contact_date(
            evaluation_date
        )

        self.assertEqual(next_treatment_contact, expected_next_contact)

    def test_next_recommended_treatment_collateral_contact_no_previous_contacts(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MODERATE",
        )

        evaluation_date = date(2018, 5, 1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_treatment_contact = us_id_supervision_compliance._next_recommended_treatment_collateral_contact_date(
            evaluation_date
        )

        self.assertEqual(next_treatment_contact, date(2018, 3, 19))

    def test_next_recommended_treatment_collateral_contact_non_existent_level(
        self,
    ) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="MODERATE",
        )

        evaluation_date = date(2018, 5, 1)

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        next_treatment_contact = us_id_supervision_compliance._next_recommended_treatment_collateral_contact_date(
            evaluation_date
        )

        self.assertIsNone(next_treatment_contact)

    def test_is_new_level_system_case_sensitivity(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="SO HIGH",
        )
        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )
        self.assertTrue(us_id_supervision_compliance._is_new_level_system("LOW"))
        self.assertFalse(us_id_supervision_compliance._is_new_level_system("Low"))


class TestGuidelinesApplicableForCase(unittest.TestCase):
    """Tests the guidelines_applicable_for_case function."""

    def setUp(self) -> None:
        self.person = StatePerson.new_with_defaults(state_code="US_ID")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsIdIncarcerationDelegate()
        )

    def test_guidelines_applicable_for_case_general(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="LEVEL 2",
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        applicable = us_id_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertTrue(applicable)

    def test_guidelines_applicable_for_case_no_supervision_level_general(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=None,
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        applicable = us_id_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_invalid_supervision_type_general(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        applicable = us_id_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertFalse(applicable)

    def test_guidelines_applicable_for_case_invalid_case_type(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
        )

        case_type = StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        self.assertFalse(
            us_id_supervision_compliance._guidelines_applicable_for_case(start_date)
        )

    def test_guidelines_applicable_for_case_sex_offense(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="SO MODERATE",
        )

        case_type = StateSupervisionCaseType.SEX_OFFENSE

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        applicable = us_id_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertTrue(applicable)

    def test_guidelines_not_applicable_for_case_invalid_leveL_sex_offense(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="SO MAXIMUM",  # Fake string, not actually possible to have max sex offense.
        )

        case_type = StateSupervisionCaseType.SEX_OFFENSE

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        applicable = us_id_supervision_compliance._guidelines_applicable_for_case(
            start_date
        )

        self.assertFalse(applicable)

    def test_guidelines_not_applicable_for_case_invalid_supervision_type_sex_offense(
        self,
    ) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="SO MAXIMUM",  # Fake string, not actually possible to have max sex offense.
        )

        case_type = StateSupervisionCaseType.SEX_OFFENSE

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=case_type,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        self.assertFalse(
            us_id_supervision_compliance._guidelines_applicable_for_case(start_date)
        )

    # TODO(#9440): If we build support for calculating compliance for bench warrant
    #  cases then this will no longer be an invalid case
    def test_guidelines_applicable_for_case_invalid_bench_warrant(self) -> None:
        start_date = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_date,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.BENCH_WARRANT,
            supervision_type_raw_text="BW",
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text="LEVEL 3",
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_date,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        self.assertFalse(
            us_id_supervision_compliance._guidelines_applicable_for_case(start_date)
        )

    def test_guideline_applicability_around_date_change(self) -> None:
        start_of_supervision = date(2018, 3, 5)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MAXIMUM,
            supervision_level_raw_text="LEVEL 2",
        )

        us_id_supervision_compliance = UsIdSupervisionCaseCompliance(
            self.person,
            supervision_period=supervision_period,
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
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
        self.person = StatePerson.new_with_defaults(state_code="US_ID")
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsIdIncarcerationDelegate()
        )

    def test_next_recommended_reassessment_general_minimum(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        reassessment_date = us_id_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score
        )

        self.assertEqual(reassessment_date, None)

    def test_next_recommended_reassessment_sex_offense_with_score(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        reassessment_date = us_id_supervision_compliance._next_recommended_reassessment(
            assessment_date, assessment_score
        )

        self.assertEqual(reassessment_date, date(2019, 4, 2))

    def test_reassessment_requirements_are_not_met(self) -> None:
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        reassessment_date = us_id_supervision_compliance._next_recommended_reassessment(
            assessment_date,
            assessment_score,
        )

        self.assertEqual(reassessment_date, date(2011, 4, 2))

    @parameterized.expand(
        [
            (StateGender.MALE,),
            (StateGender.TRANS_MALE,),
            (StateGender.FEMALE,),
            (StateGender.TRANS_FEMALE,),
        ]
    )
    def test_reassessment_requirements_at_sex_offense_boundaries(
        self, gender: StateGender
    ) -> None:
        self.person.gender = gender
        start_of_supervision = date(2018, 3, 5)  # This was a Monday
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=StateCode.US_ID.value,
            start_date=start_of_supervision,
            termination_date=date(2018, 5, 19),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        boundary_deadline = (
            us_id_supervision_compliance_boundary._next_recommended_reassessment(
                assessment_date,
                assessment_boundary_score,
            )
        )

        self.assertEqual(boundary_deadline, date(2011, 4, 2))

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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        reassessment_deadline = (
            us_id_supervision_compliance_under_boundary._next_recommended_reassessment(
                assessment_date,
                assessment_boundary_score - 1,
            )
        )

        self.assertEqual(reassessment_deadline, None)


class TestSupervisionDowngrades(unittest.TestCase):
    """Tests the reassessment_requirements_are_met function."""

    def setUp(self) -> None:
        self.start_of_supervision = date(2018, 3, 5)
        self.empty_ip_index = default_normalized_ip_index_for_tests(
            incarceration_delegate=UsIdIncarcerationDelegate()
        )

    def _person_with_gender(self, gender: StateGender) -> StatePerson:
        return StatePerson.new_with_defaults(state_code="US_ID", gender=gender)

    def _supervision_period_with_level(
        self, supervision_level: StateSupervisionLevel
    ) -> NormalizedStateSupervisionPeriod:
        return NormalizedStateSupervisionPeriod.new_with_defaults(
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
            ("male_old", StateGender.MALE, date(2019, 12, 31)),
            ("trans_ma_oldle", StateGender.TRANS_MALE, date(2019, 12, 31)),
            ("fema_oldle", StateGender.FEMALE, date(2019, 12, 31)),
            ("trans_fema_oldle", StateGender.TRANS_FEMALE, date(2019, 12, 31)),
            ("male_new", StateGender.MALE, date(2020, 12, 31)),
            ("trans_male_new", StateGender.TRANS_MALE, date(2020, 12, 31)),
            ("female_new", StateGender.FEMALE, date(2020, 12, 31)),
            ("trans_female_new", StateGender.TRANS_FEMALE, date(2020, 12, 31)),
        ]
    )
    def test_minimum_no_downgrade(
        self,
        _name: str,
        gender: StateGender,
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )
        self.assertIsNone(
            us_id_supervision_compliance._get_recommended_supervision_downgrade_level(
                evaluation_date
            )
        )

    @parameterized.expand(
        [
            ("medium", StateSupervisionLevel.MEDIUM, 16, StateGender.EXTERNAL_UNKNOWN),
            ("high", StateSupervisionLevel.HIGH, 24, StateGender.EXTERNAL_UNKNOWN),
            (
                "maximum",
                StateSupervisionLevel.MAXIMUM,
                31,
                StateGender.EXTERNAL_UNKNOWN,
            ),
        ]
    )
    def test_old_downgrade_at_border(
        self, _name: str, level: StateSupervisionLevel, score: int, gender: StateGender
    ) -> None:
        compliance_no_downgrade = UsIdSupervisionCaseCompliance(
            self._person_with_gender(gender),
            supervision_period=self._supervision_period_with_level(level),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[self._assessment_with_score(score)],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )
        self.assertIsNone(
            compliance_no_downgrade._get_recommended_supervision_downgrade_level(
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )
        recommended_level = (
            compliance_downgrade._get_recommended_supervision_downgrade_level(
                date(2019, 12, 31)
            )
        )
        assert recommended_level is not None
        self.assertTrue(recommended_level < level)

    @parameterized.expand(
        [
            ("medium_female", StateSupervisionLevel.MEDIUM, 23, StateGender.FEMALE),
            (
                "medium_trans_female",
                StateSupervisionLevel.MEDIUM,
                23,
                StateGender.TRANS_FEMALE,
            ),
            ("medium_male", StateSupervisionLevel.MEDIUM, 21, StateGender.MALE),
            (
                "medium_trans_male",
                StateSupervisionLevel.MEDIUM,
                21,
                StateGender.TRANS_MALE,
            ),
            ("high_female", StateSupervisionLevel.HIGH, 31, StateGender.FEMALE),
            (
                "high_trans_female",
                StateSupervisionLevel.HIGH,
                31,
                StateGender.TRANS_FEMALE,
            ),
            ("high_male", StateSupervisionLevel.HIGH, 29, StateGender.MALE),
            ("high_trans_male", StateSupervisionLevel.HIGH, 29, StateGender.TRANS_MALE),
        ]
    )
    def test_new_downgrade_at_border(
        self, _name: str, level: StateSupervisionLevel, score: int, gender: StateGender
    ) -> None:
        compliance_no_downgrade = UsIdSupervisionCaseCompliance(
            self._person_with_gender(gender),
            supervision_period=self._supervision_period_with_level(level),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[self._assessment_with_score(score)],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )
        self.assertIsNone(
            compliance_no_downgrade._get_recommended_supervision_downgrade_level(
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
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )
        recommended_level = (
            compliance_downgrade._get_recommended_supervision_downgrade_level(
                date(2020, 12, 31)
            )
        )
        assert recommended_level is not None
        self.assertTrue(recommended_level < level)

    def test_no_assessment(self) -> None:
        evaluation_date = date(2020, 12, 31)

        person = self._person_with_gender(StateGender.MALE)

        no_assessments_no_downgrade = UsIdSupervisionCaseCompliance(
            person,
            supervision_period=self._supervision_period_with_level(
                StateSupervisionLevel.MEDIUM
            ),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )
        self.assertIsNone(
            no_assessments_no_downgrade._get_recommended_supervision_downgrade_level(
                evaluation_date
            )
        )

        no_assessments_should_downgrade = UsIdSupervisionCaseCompliance(
            person,
            supervision_period=self._supervision_period_with_level(
                StateSupervisionLevel.MAXIMUM
            ),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=self.start_of_supervision,
            assessments=[],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )
        self.assertEqual(
            no_assessments_should_downgrade._get_recommended_supervision_downgrade_level(
                evaluation_date
            ),
            StateSupervisionLevel.MEDIUM,
        )

    def test_no_assessment_intake(self) -> None:
        supervision_start = date(2021, 3, 15)
        evaluation_date = supervision_start + timedelta(days=40)

        person = self._person_with_gender(StateGender.MALE)

        intake_no_downgrade = UsIdSupervisionCaseCompliance(
            person,
            supervision_period=self._supervision_period_with_level(
                StateSupervisionLevel.MEDIUM
            ),
            case_type=StateSupervisionCaseType.GENERAL,
            start_of_supervision=supervision_start,
            assessments=[
                # this would result in a MINIMUM recommendation
                StateAssessment.new_with_defaults(
                    state_code=StateCode.US_ID.value,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_date=supervision_start - timedelta(days=14),
                    assessment_score=1,
                )
            ],
            supervision_contacts=[],
            violation_responses=[],
            incarceration_sentences=[],
            incarceration_period_index=self.empty_ip_index,
            supervision_delegate=UsIdSupervisionDelegate([]),
        )

        self.assertIsNone(
            intake_no_downgrade._get_recommended_supervision_downgrade_level(
                evaluation_date
            )
        )
