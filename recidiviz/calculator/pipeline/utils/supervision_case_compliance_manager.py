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
from datetime import date, timedelta
from typing import List, Optional

from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.assessment_utils import (
    find_most_recent_applicable_assessment_of_class_for_state,
)
from recidiviz.calculator.pipeline.utils.supervision_level_policy import (
    SupervisionLevelPolicy,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentClass
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StatePerson,
    StateSupervisionContact,
    StateSupervisionPeriod,
)


class StateSupervisionCaseComplianceManager:
    """Interface for state-specific supervision case compliance calculations."""

    def __init__(
        self,
        person: StatePerson,
        supervision_period: StateSupervisionPeriod,
        case_type: StateSupervisionCaseType,
        start_of_supervision: date,
        assessments: List[StateAssessment],
        supervision_contacts: List[StateSupervisionContact],
    ):
        self.person = person
        self.supervision_period = supervision_period
        self.case_type = case_type
        self.start_of_supervision = start_of_supervision
        self.assessments = assessments
        self.supervision_contacts = supervision_contacts

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
        assessment_count = self._completed_assessments_on_date(
            compliance_evaluation_date
        )
        face_to_face_count = self._face_to_face_contacts_on_date(
            compliance_evaluation_date
        )
        home_visit_count = self._home_visits_on_date(compliance_evaluation_date)

        most_recent_assessment = (
            find_most_recent_applicable_assessment_of_class_for_state(
                compliance_evaluation_date,
                self.assessments,
                assessment_class=StateAssessmentClass.RISK,
                state_code=self.supervision_period.state_code,
            )
        )
        most_recent_assessment_date = (
            most_recent_assessment.assessment_date
            if most_recent_assessment is not None
            else None
        )

        next_recommended_assessment_date = None
        face_to_face_frequency_sufficient = None
        home_visit_frequency_sufficient = None

        if self._guidelines_applicable_for_case(compliance_evaluation_date):
            next_recommended_assessment_date = self._next_recommended_assessment_date(
                most_recent_assessment
            )

            face_to_face_frequency_sufficient = (
                self._face_to_face_contact_frequency_is_sufficient(
                    compliance_evaluation_date
                )
            )

            home_visit_frequency_sufficient = self._home_visit_frequency_is_sufficient(
                compliance_evaluation_date
            )

        return SupervisionCaseCompliance(
            date_of_evaluation=compliance_evaluation_date,
            assessment_count=assessment_count,
            most_recent_assessment_date=most_recent_assessment_date,
            next_recommended_assessment_date=next_recommended_assessment_date,
            face_to_face_count=face_to_face_count,
            most_recent_face_to_face_date=self._most_recent_face_to_face_contact(
                compliance_evaluation_date
            ),
            face_to_face_frequency_sufficient=face_to_face_frequency_sufficient,
            most_recent_home_visit_date=self._most_recent_home_visit_contact(
                compliance_evaluation_date
            ),
            home_visit_count=home_visit_count,
            home_visit_frequency_sufficient=home_visit_frequency_sufficient,
            recommended_supervision_downgrade_level=self._get_recommended_supervision_downgrade_level(
                compliance_evaluation_date
            ),
        )

    def _next_recommended_assessment_date(
        self, most_recent_assessment: Optional[StateAssessment]
    ) -> Optional[date]:
        """Outputs what the next assessment date is for the person under supervision."""
        if (
            not most_recent_assessment
            or not most_recent_assessment.assessment_date
            or not most_recent_assessment.assessment_score
        ):
            # No assessment has been filed, so the next recommended assessment date
            # is the initial assessment date.
            return self.start_of_supervision + timedelta(
                days=self._get_initial_assessment_number_of_days()
            )

        return self._next_recommended_reassessment(
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

    def _face_to_face_contacts_on_date(self, compliance_evaluation_date: date) -> int:
        """Returns the number of face-to-face contacts that were completed on compliance_evaluation_date."""
        applicable_contacts = self._get_applicable_face_to_face_contacts_between_dates(
            lower_bound_inclusive=compliance_evaluation_date,
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

    def _completed_assessments_on_date(self, compliance_evaluation_date: date) -> int:
        """Returns the number of assessments that were completed on the compliance evaluation date."""
        assessments_on_compliance_date = [
            assessment
            for assessment in self.assessments
            if assessment.assessment_date is not None
            and assessment.assessment_score is not None
            and assessment.assessment_date == compliance_evaluation_date
        ]

        return len(assessments_on_compliance_date)

    def _face_to_face_contact_frequency_is_in_compliance(
        self, compliance_evaluation_date: date, required_contacts: int, period_days: int
    ) -> Optional[bool]:
        """Returns whether the face-to-face contacts within the period are compliant with respect to the state
        standards for the level of supervision of the case."""
        # Get applicable contacts that occurred between the start of supervision and the
        # compliance_evaluation_date (inclusive)
        applicable_contacts = self._get_applicable_face_to_face_contacts_between_dates(
            self.start_of_supervision, compliance_evaluation_date
        )

        if not applicable_contacts:
            # This person has been on supervision for longer than the allowed number of days without an initial contact.
            # The face-to-face contact standard is not in compliance.
            return False

        days_since_start = (compliance_evaluation_date - self.start_of_supervision).days

        if days_since_start < period_days:
            # If they've had a contact since the start of their supervision, and they have been on supervision for less
            # than the number of days in which they would need another contact, then the case is in compliance
            return True

        contacts_within_period = [
            contact
            for contact in applicable_contacts
            if contact.contact_date is not None
            and (compliance_evaluation_date - contact.contact_date).days < period_days
        ]

        return len(contacts_within_period) >= required_contacts

    def _home_visits_on_date(self, compliance_evaluation_date: date) -> int:
        """Returns the number of face-to-face contacts that were completed on compliance_evaluation_date."""
        applicable_visits = self._get_applicable_home_visits_between_dates(
            lower_bound_inclusive=compliance_evaluation_date,
            upper_bound_inclusive=compliance_evaluation_date,
        )
        return len(applicable_visits)

    def _get_applicable_home_visits_between_dates(
        self, lower_bound_inclusive: date, upper_bound_inclusive: date
    ) -> List[StateSupervisionContact]:
        """Returns the completed contacts that can be counted as home visits and occurred between the
        lower_bound_inclusive date and the upper_bound_inclusive date.
        """
        return [
            contact
            for contact in self.supervision_contacts
            # These are the types of contacts that can satisfy the home visit requirement
            if contact.location == StateSupervisionContactLocation.RESIDENCE
            # Contact must be marked as completed
            and contact.status == StateSupervisionContactStatus.COMPLETED
            and contact.contact_date is not None
            and lower_bound_inclusive <= contact.contact_date <= upper_bound_inclusive
        ]

    def _most_recent_home_visit_contact(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Gets the most recent home visit contact date. If there is not any, it returns None."""
        applicable_contacts = self._get_applicable_home_visits_between_dates(
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

    def _get_recommended_supervision_downgrade_level(
        self, evaluation_date: date
    ) -> Optional[StateSupervisionLevel]:
        """Determines whether the person under evaluation was eligible for a downgrade
        on this date."""
        policy = self._get_supervision_level_policy(evaluation_date)
        if not policy:
            return None

        if (
            not (current_level := self.supervision_period.supervision_level)
            or not current_level.is_comparable()
        ):
            return None

        most_recent_assessment = (
            find_most_recent_applicable_assessment_of_class_for_state(
                evaluation_date,
                self.assessments,
                assessment_class=StateAssessmentClass.RISK,
                state_code=self.supervision_period.state_code,
            )
        )
        if (
            most_recent_assessment is None
            or (last_score := most_recent_assessment.assessment_score) is None
        ):
            return None

        recommended_level = policy.recommended_supervision_level_for_person(
            self.person, last_score
        )
        if not recommended_level or recommended_level >= current_level:
            return None

        return recommended_level

    @abc.abstractmethod
    def _guidelines_applicable_for_case(self, evaluation_date: date) -> bool:
        """Returns whether the standard state guidelines are applicable for the given supervision case."""

    @abc.abstractmethod
    def _get_initial_assessment_number_of_days(self) -> int:
        """Returns the number of days that an initial assessment should take place, optionally given a `case_type` and
        `supervision_type`."""

    @abc.abstractmethod
    def _next_recommended_reassessment(
        self,
        most_recent_assessment_date: date,
        most_recent_assessment_score: int,
    ) -> Optional[date]:
        """Returns the next recommended reassessment date or None if no further reassessments are needed."""

    @abc.abstractmethod
    def _face_to_face_contact_frequency_is_sufficient(
        self, compliance_evaluation_date: date
    ) -> Optional[bool]:
        # TODO(#5199): Update to return `bool` once face to face contacts are ingested for US_ND.
        """Returns whether the frequency of face-to-face contacts between the officer and the person on supervision
        is sufficient with respect to the state standards for the level of supervision of the case."""

    @abc.abstractmethod
    def _home_visit_frequency_is_sufficient(
        self, compliance_evaluation_date: date
    ) -> Optional[bool]:
        """Returns whether the frequency of home visits between the officer and the person on supervision
        is sufficient with respect to the state standards for the level of supervision of the case."""

    @abc.abstractmethod
    def _get_supervision_level_policy(
        self, evaluation_date: date
    ) -> Optional[SupervisionLevelPolicy]:
        """Returns the SupervisionLevelPolicy associated with evaluation for the person
        under supervision on the specified date."""
