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
from typing import Callable, List, Optional

import numpy as np
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.assessment_utils import (
    find_most_recent_applicable_assessment_of_class_for_state,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
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
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionViolationResponse,
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
        violation_responses: List[StateSupervisionViolationResponse],
        incarceration_sentences: List[StateIncarcerationSentence],
        incarceration_period_index: PreProcessedIncarcerationPeriodIndex,
        supervision_delegate: StateSpecificSupervisionDelegate,
    ):
        self.person = person
        self.supervision_period = supervision_period
        self.case_type = case_type
        self.start_of_supervision = start_of_supervision
        self.assessments = assessments
        self.supervision_contacts = supervision_contacts
        self.violation_responses = violation_responses
        self.incarceration_sentences = incarceration_sentences
        self.incarceration_period_index = incarceration_period_index
        self.supervision_delegate = supervision_delegate

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
                supervision_delegate=self.supervision_delegate,
            )
        )
        most_recent_assessment_date = (
            most_recent_assessment.assessment_date
            if most_recent_assessment is not None
            else None
        )

        next_recommended_assessment_date = None
        next_recommended_face_to_face_date = None
        next_recommended_home_visit_date = None
        next_recommended_treatment_collateral_contact_date = None

        if self._guidelines_applicable_for_case(compliance_evaluation_date):
            next_recommended_assessment_date = self._next_recommended_assessment_date(
                most_recent_assessment, compliance_evaluation_date
            )

            next_recommended_face_to_face_date = (
                self._next_recommended_face_to_face_date(compliance_evaluation_date)
            )

            next_recommended_home_visit_date = self._next_recommended_home_visit_date(
                compliance_evaluation_date
            )

            next_recommended_treatment_collateral_contact_date = (
                self._next_recommended_treatment_collateral_contact_date(
                    compliance_evaluation_date
                )
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
            next_recommended_face_to_face_date=next_recommended_face_to_face_date,
            most_recent_home_visit_date=self._most_recent_home_visit_contact(
                compliance_evaluation_date
            ),
            next_recommended_home_visit_date=next_recommended_home_visit_date,
            home_visit_count=home_visit_count,
            most_recent_treatment_collateral_contact_date=self._most_recent_treatment_collateral_contact(
                compliance_evaluation_date
            ),
            next_recommended_treatment_collateral_contact_date=next_recommended_treatment_collateral_contact_date,
            recommended_supervision_downgrade_level=self._get_recommended_supervision_downgrade_level(
                compliance_evaluation_date
            ),
        )

    def _next_recommended_assessment_date(
        self,
        most_recent_assessment: Optional[StateAssessment],
        compliance_evaluation_date: Optional[date] = None,
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
            compliance_evaluation_date,
        )

    def _most_recent_face_to_face_contact(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Gets the most recent face to face contact date. If there is not any, it returns None."""
        return self._default_most_recent_contact_date(
            compliance_evaluation_date,
            self._get_applicable_face_to_face_contacts_between_dates,
        )

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
                StateSupervisionContactType.DIRECT,
                StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT,
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
            and contact.contact_type
            in (
                StateSupervisionContactType.DIRECT,
                StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT,
            )
            # Contact must be marked as completed
            and contact.status == StateSupervisionContactStatus.COMPLETED
            and contact.contact_date is not None
            and lower_bound_inclusive <= contact.contact_date <= upper_bound_inclusive
        ]

    def _most_recent_home_visit_contact(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Gets the most recent home visit contact date. If there is not any, it returns None."""
        return self._default_most_recent_contact_date(
            compliance_evaluation_date, self._get_applicable_home_visits_between_dates
        )

    def _most_recent_treatment_collateral_contact(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Gets the most recent face to face contact date. If there is not any, it returns None."""
        return self._default_most_recent_contact_date(
            compliance_evaluation_date,
            self._get_applicable_treatment_collateral_contacts_between_dates,
        )

    def _treatment_collateral_contacts_on_date(
        self, compliance_evaluation_date: date
    ) -> int:
        """Returns the number of treatment collateral contacts that were completed on compliance_evaluation_date."""
        applicable_contacts = (
            self._get_applicable_treatment_collateral_contacts_between_dates(
                lower_bound_inclusive=compliance_evaluation_date,
                upper_bound_inclusive=compliance_evaluation_date,
            )
        )

        return len(applicable_contacts)

    def _get_applicable_treatment_collateral_contacts_between_dates(
        self, lower_bound_inclusive: date, upper_bound_inclusive: date
    ) -> List[StateSupervisionContact]:
        """Returns the completed contacts that can be counted as treatment provider
        collateral contacts and occurred between the lower_bound_inclusive date and the
        upper_bound_inclusive date.
        """
        return [
            contact
            for contact in self.supervision_contacts
            if contact.contact_type
            in (
                StateSupervisionContactType.COLLATERAL,
                StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT,
            )
            and contact.location == StateSupervisionContactLocation.TREATMENT_PROVIDER
            # Contact must be marked as completed
            and contact.status == StateSupervisionContactStatus.COMPLETED
            and contact.contact_date is not None
            and lower_bound_inclusive <= contact.contact_date <= upper_bound_inclusive
        ]

    def _awaiting_new_intake_assessment(
        self,
        evaluation_date: date,  # pylint: disable=unused-argument
        most_recent_assessment_date: date,  # pylint: disable=unused-argument
    ) -> bool:
        # for states where this concept is applicable, override with necessary logic
        return False

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
                supervision_delegate=self.supervision_delegate,
            )
        )

        if (
            most_recent_assessment is None
            or most_recent_assessment.assessment_date is None
        ):
            # fall back to default when a person has never been assessed
            recommended_level = policy.pre_assessment_level
        else:
            # Scores can be missing for various reasons (incomplete assessments, etc),
            # in which case we can't proceed with a recommendation
            if (last_score := most_recent_assessment.assessment_score) is None:
                return None

            if (
                self._awaiting_new_intake_assessment(
                    evaluation_date, most_recent_assessment.assessment_date
                )
                and current_level == policy.pre_assessment_level
            ):
                return None

            # if we've gotten this far, we have an existing assessment score to work with
            recommended_level = policy.recommended_supervision_level_from_score(
                self.person, last_score
            )

        if not recommended_level or recommended_level >= current_level:
            return None

        return recommended_level

    def _default_most_recent_contact_date(
        self,
        compliance_evaluation_date: date,
        get_applicable_contacts_function: Callable[
            [date, date], List[StateSupervisionContact]
        ],
    ) -> Optional[date]:
        """Provides a base implementation, describing when the most recent contact
        occurred. Returns None if compliance standards are unknown or no contacts
        have occurred."""
        applicable_contacts = get_applicable_contacts_function(
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

    def _default_next_recommended_contact_date_given_requirements(
        self,
        compliance_evaluation_date: date,
        required_contacts_per_period: int,
        period_length_days: int,
        new_supervision_contact_deadline_days: int,
        get_applicable_contacts_function: Callable[
            [date, date], List[StateSupervisionContact]
        ],
        use_business_days: bool,
    ) -> Optional[date]:
        """Provides a base implementation, describing when the next contact
        should be. Returns None if compliance standards are unknown or no subsequent
        contacts are required."""

        if required_contacts_per_period == 0:
            return None

        contacts_since_supervision_start = get_applicable_contacts_function(
            self.start_of_supervision, compliance_evaluation_date
        )
        contact_dates = sorted(
            [
                contact.contact_date
                for contact in contacts_since_supervision_start
                if contact.contact_date is not None
            ]
        )
        if not contact_dates:
            # No contacts. First contact required is within NEW_SUPERVISION_CONTACT_DEADLINE_DAYS.
            return (
                np.busday_offset(
                    self.start_of_supervision,
                    new_supervision_contact_deadline_days,
                    roll="forward",
                ).astype(date)
                if use_business_days
                else (
                    self.start_of_supervision
                    + relativedelta(days=new_supervision_contact_deadline_days)
                )
            )

        # TODO(#8637): Have this method operate also take in a unit of time for face-to-face
        # frequency so we can fold in ND logic here as well.

        if len(contact_dates) < required_contacts_per_period:
            # Not enough contacts. Give the PO until the full period window has elapsed
            # to meet with their client.
            return self.start_of_supervision + timedelta(days=period_length_days)

        # If n contacts are required every k days, this looks at the nth-to-last contact
        # and returns the date k days after that, as the latest day that the next contact
        # can happen to still be in compliance.
        return contact_dates[-required_contacts_per_period] + timedelta(
            days=period_length_days
        )

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
        compliance_evaluation_date: date = None,
    ) -> Optional[date]:
        """Returns the next recommended reassessment date or None if no further reassessments are needed."""

    @abc.abstractmethod
    def _next_recommended_face_to_face_date(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Returns when the next face-to-face contact should be. Returns None if compliance standards are
        unknown or no subsequent face-to-face contacts are required."""

    @abc.abstractmethod
    def _next_recommended_home_visit_date(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Returns when the next home visit should be. Returns None if the compliance standards are unknown or
        no subsequent home visits are required."""

    @abc.abstractmethod
    def _next_recommended_treatment_collateral_contact_date(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Returns when the next treatment collateral contact should be. Returns None if
        compliance standards are unknown or no subsequent treatment collateral contacts
        are required."""

    @abc.abstractmethod
    def _get_supervision_level_policy(
        self, evaluation_date: date
    ) -> Optional[SupervisionLevelPolicy]:
        """Returns the SupervisionLevelPolicy associated with evaluation for the person
        under supervision on the specified date."""
