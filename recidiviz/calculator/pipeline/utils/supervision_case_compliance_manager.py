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
"""
    Manager for all supervision case compliance calculations. Delegates calcuations to state-specific logic,
    where necessary.
"""
import abc
from datetime import date
import logging
from typing import Optional, List

from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.assessment_utils import (
    find_most_recent_applicable_assessment_of_class_for_state,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentClass
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactType,
    StateSupervisionContactStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateAssessment,
    StateSupervisionContact,
)


class StateSupervisionCaseComplianceManager:
    """Interface for state-specific supervision case compliance calculations."""

    def __init__(
        self,
        supervision_period: StateSupervisionPeriod,
        case_type: StateSupervisionCaseType,
        start_of_supervision: date,
        assessments: List[StateAssessment],
        supervision_contacts: List[StateSupervisionContact],
    ):
        self.supervision_period = supervision_period
        self.case_type = case_type
        self.start_of_supervision = start_of_supervision
        self.assessments = assessments
        self.supervision_contacts = supervision_contacts

        self.guidelines_applicable_for_case = self._guidelines_applicable_for_case()

    def get_case_compliance_on_date(
        self, compliance_evaluation_date: date
    ) -> Optional[SupervisionCaseCompliance]:
        """
        Calculates several different compliance values for the supervision case represented by the supervision period,
        based on state specific compliance standards. Measures compliance values for the following types of supervision
        events:
            - Assessments
            - Face-to-Face Contacts

        For each event, we calculate two types of metrics when possible.
            - The total number of events that have occurred for this person this month (until the
              |compliance_evaluation_date|).
            - Whether or not the compliance standards have been met for this event type (this is set to None if we do
              not have clear, documented guidelines applicable to this case).

        Returns:
             A SupervisionCaseCompliance object containing information regarding the ways the case is or isn't in
             compliance with state standards on the given compliance_evaluation_date.
        """
        assessment_count = self._completed_assessments_in_compliance_month(
            compliance_evaluation_date
        )
        face_to_face_count = self._face_to_face_contacts_in_compliance_month(
            compliance_evaluation_date
        )

        most_recent_assessment_date = None
        num_days_assessment_overdue = None
        face_to_face_frequency_sufficient = None

        if self.guidelines_applicable_for_case:
            most_recent_assessment = (
                find_most_recent_applicable_assessment_of_class_for_state(
                    compliance_evaluation_date,
                    self.assessments,
                    assessment_class=StateAssessmentClass.RISK,
                    state_code=self.supervision_period.state_code,
                )
            )
            if most_recent_assessment is not None:
                most_recent_assessment_date = most_recent_assessment.assessment_date

            num_days_assessment_overdue = self._num_days_assessment_overdue(
                compliance_evaluation_date, most_recent_assessment
            )

            face_to_face_frequency_sufficient = (
                self._face_to_face_contact_frequency_is_sufficient(
                    compliance_evaluation_date
                )
            )

        return SupervisionCaseCompliance(
            date_of_evaluation=compliance_evaluation_date,
            assessment_count=assessment_count,
            most_recent_assessment_date=most_recent_assessment_date,
            num_days_assessment_overdue=num_days_assessment_overdue,
            face_to_face_count=face_to_face_count,
            most_recent_face_to_face_date=self._most_recent_face_to_face_contact(
                compliance_evaluation_date
            ),
            face_to_face_frequency_sufficient=face_to_face_frequency_sufficient,
        )

    def _num_days_assessment_overdue(
        self,
        compliance_evaluation_date: date,
        most_recent_assessment: Optional[StateAssessment],
    ) -> int:
        """Determines whether the supervision case represented by the given supervision_period has an "up-to-date
        assessment" according to state-specific guidelines."""
        days_since_start = (compliance_evaluation_date - self.start_of_supervision).days

        initial_assessment_number_of_days = (
            self._get_initial_assessment_number_of_days()
        )

        if days_since_start <= initial_assessment_number_of_days:
            # This is a recently started supervision period, and the person has not yet hit the number of days from
            # the start of their supervision at which the officer is required to have performed an assessment. This
            # assessment is up to date regardless of when the last assessment was taken.
            logging.debug(
                "[%d] Supervision period %d started %d days before the compliance date %s. "
                "Assessment is not overdue.",
                self.supervision_period.state_code,
                self.supervision_period.supervision_period_id,
                days_since_start,
                compliance_evaluation_date,
            )
            return 0

        days_past_initial_assessment_deadline = (
            days_since_start - initial_assessment_number_of_days
        )
        if (
            not most_recent_assessment
            or not most_recent_assessment.assessment_date
            or not most_recent_assessment.assessment_score
        ):
            # If they have not had an assessment and are expected to, then their assessment is not up to date.
            logging.debug(
                "[%d] Supervision period %d started %d days before the compliance date %s, and they have not had "
                "an assessment taken. Assessment is %d days overdue.",
                self.supervision_period.state_code,
                self.supervision_period.supervision_period_id,
                days_since_start,
                compliance_evaluation_date,
                days_past_initial_assessment_deadline,
            )
            return days_past_initial_assessment_deadline

        return self._num_days_past_required_reassessment(
            compliance_evaluation_date,
            most_recent_assessment.assessment_date,
            most_recent_assessment.assessment_score,
        )

    def _most_recent_face_to_face_contact(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Gets the most recent face to face contact date. If there is not any, it returns None."""
        applicable_contacts = self._get_applicable_face_to_face_contacts_between_dates(
            self.start_of_supervision, compliance_evaluation_date
        )
        contact_dates = [
            contact.contact_date
            for contact in applicable_contacts
            if contact.contact_date is not None
        ]
        if not contact_dates:
            return None

        return max(contact_dates)

    def _face_to_face_contacts_in_compliance_month(
        self, compliance_evaluation_date: date
    ) -> int:
        """Returns the number of face-to-face contacts that were completed between the first of the month of the
        compliance_evaluation_date and the compliance_evaluation_date (inclusive)."""

        # TODO(#5199): Implement in us_nd_supervision_compliance once face to face contacts are ingested.
        if self.supervision_period.state_code == StateCode.US_ND.value:
            return 0

        compliance_month = compliance_evaluation_date.month
        compliance_year = compliance_evaluation_date.year
        first_day_of_month = date(compliance_year, compliance_month, 1)

        applicable_contacts = self._get_applicable_face_to_face_contacts_between_dates(
            lower_bound_inclusive=first_day_of_month,
            upper_bound_inclusive=compliance_evaluation_date,
        )

        return len(applicable_contacts)

    def _get_applicable_face_to_face_contacts_between_dates(
        self, lower_bound_inclusive: date, upper_bound_inclusive: date
    ) -> List[StateSupervisionContact]:
        """Returns the completed contacts that can be counted as face-to-face contacts and occurred between the
        lower_bound_inclusive date and the upper_bound_inclusive date.
        """
        return [
            contact
            for contact in self.supervision_contacts
            # These are the types of contacts that can satisfy the face-to-face contact requirement
            if contact.contact_type
            in (
                StateSupervisionContactType.FACE_TO_FACE,
                StateSupervisionContactType.TELEPHONE,
                StateSupervisionContactType.VIRTUAL,
            )
            # Contact must be marked as completed
            and contact.status == StateSupervisionContactStatus.COMPLETED
            and contact.contact_date is not None
            and lower_bound_inclusive <= contact.contact_date <= upper_bound_inclusive
        ]

    def _completed_assessments_in_compliance_month(
        self, compliance_evaluation_date: date
    ) -> int:
        """Returns the number of assessments that were completed between the first of the month of the
        compliance_evaluation_date and the compliance_evaluation_date (inclusive)."""
        compliance_month = compliance_evaluation_date.month
        compliance_year = compliance_evaluation_date.year
        first_day_of_month = date(compliance_year, compliance_month, 1)

        num_assessments_this_month = [
            assessment
            for assessment in self.assessments
            if assessment.assessment_date is not None
            and assessment.assessment_score is not None
            and first_day_of_month
            <= assessment.assessment_date
            <= compliance_evaluation_date
        ]

        return len(num_assessments_this_month)

    @abc.abstractmethod
    def _guidelines_applicable_for_case(self) -> bool:
        """Returns whether the standard state guidelines are applicable for the given supervision case."""

    @abc.abstractmethod
    def _get_initial_assessment_number_of_days(self) -> int:
        """Returns the number of days that an initial assessment should take place, optionally given a `case_type` and
        `supervision_type`."""

    @abc.abstractmethod
    def _num_days_past_required_reassessment(
        self,
        compliance_evaluation_date: date,
        most_recent_assessment_date: date,
        most_recent_assessment_score: int,
    ) -> int:
        """Returns the number of days it has been since the required reassessment deadline. Returns 0
        if the reassessment is not overdue."""

    @abc.abstractmethod
    def _face_to_face_contact_frequency_is_sufficient(
        self, compliance_evaluation_date: date
    ) -> Optional[bool]:
        # TODO(#5199): Update to return `bool` once face to face contacts are ingested for US_ND.
        """Returns whether the frequency of face-to-face contacts between the officer and the person on supervision
        is sufficient with respect to the state standards for the level of supervision of the case."""

    @abc.abstractmethod
    def _num_days_compliance_evaluation_date_past_reassessment_deadline(
        self, compliance_evaluation_date: date, most_recent_assessment_date: date
    ) -> int:
        """Returns the number of days that the compliance evaluation is overdue, given the latest evaluation.
        Returns 0 if it is not overdue"""
