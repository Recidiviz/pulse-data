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
import itertools
from datetime import date, timedelta
from typing import Iterator, List, Optional, Set, cast

import attr
import numpy as np
from dateutil.relativedelta import relativedelta

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
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStatePerson,
    NormalizedStateSupervisionContact,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.metrics.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.pipelines.utils.assessment_utils import (
    find_most_recent_applicable_assessment_of_class_for_state,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.pipelines.utils.supervision_level_policy import SupervisionLevelPolicy
from recidiviz.utils.range_querier import RangeQuerier


@attr.s
class ContactFilter:
    contact_types: Optional[Set[StateSupervisionContactType]] = attr.ib(default=None)
    statuses: Optional[Set[StateSupervisionContactStatus]] = attr.ib(default=None)
    locations: Optional[Set[StateSupervisionContactLocation]] = attr.ib(default=None)
    verified_employment: Optional[bool] = attr.ib(default=None)

    def matches(self, contact: NormalizedStateSupervisionContact) -> bool:
        if self.contact_types and contact.contact_type not in self.contact_types:
            return False
        if self.statuses and contact.status not in self.statuses:
            return False
        if self.locations and contact.location not in self.locations:
            return False
        if (
            self.verified_employment is not None
            and contact.verified_employment is not self.verified_employment
        ):
            return False
        return True


class StateSupervisionCaseComplianceManager:
    """Interface for state-specific supervision case compliance calculations."""

    def __init__(
        self,
        person: NormalizedStatePerson,
        *,
        supervision_period: NormalizedStateSupervisionPeriod,
        case_type: StateSupervisionCaseType,
        start_of_supervision: date,
        assessments_by_date: RangeQuerier[date, NormalizedStateAssessment],
        supervision_contacts_by_date: RangeQuerier[
            date, NormalizedStateSupervisionContact
        ],
        violation_responses: List[NormalizedStateSupervisionViolationResponse],
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_delegate: StateSpecificSupervisionDelegate,
    ):
        self.person = person
        self.supervision_period = supervision_period
        self.case_type = case_type
        self.start_of_supervision = start_of_supervision
        self.assessments_by_date = assessments_by_date
        self.supervision_contacts_by_date = supervision_contacts_by_date
        self.violation_responses = violation_responses
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
        face_to_face_count = self._count_contacts_on_date(
            compliance_evaluation_date, self.filter_for_face_to_face_contacts()
        )
        home_visit_count = self._count_contacts_on_date(
            compliance_evaluation_date, self.filter_for_home_visit_contacts()
        )

        most_recent_assessment = (
            find_most_recent_applicable_assessment_of_class_for_state(
                compliance_evaluation_date,
                self.assessments_by_date,
                assessment_class=StateAssessmentClass.RISK,
                supervision_delegate=self.supervision_delegate,
            )
        )
        most_recent_assessment_date = (
            most_recent_assessment.assessment_date
            if most_recent_assessment is not None
            else None
        )

        most_recent_employment_verification_date = self._most_recent_contact_date(
            compliance_evaluation_date,
            self.filter_for_employment_verification_contacts(),
        )

        next_recommended_assessment_date = None
        next_recommended_face_to_face_date = None
        next_recommended_home_visit_date = None
        next_recommended_treatment_collateral_contact_date = None
        next_recommended_employment_verification_date = None

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

            next_recommended_employment_verification_date = (
                self._next_recommended_employment_verification_date(
                    compliance_evaluation_date, most_recent_employment_verification_date
                )
            )

        return SupervisionCaseCompliance(
            date_of_evaluation=compliance_evaluation_date,
            assessment_count=assessment_count,
            most_recent_assessment_date=most_recent_assessment_date,
            next_recommended_assessment_date=next_recommended_assessment_date,
            face_to_face_count=face_to_face_count,
            most_recent_face_to_face_date=self._most_recent_contact_date(
                compliance_evaluation_date, self.filter_for_face_to_face_contacts()
            ),
            next_recommended_face_to_face_date=next_recommended_face_to_face_date,
            most_recent_home_visit_date=self._most_recent_contact_date(
                compliance_evaluation_date, self.filter_for_home_visit_contacts()
            ),
            next_recommended_home_visit_date=next_recommended_home_visit_date,
            home_visit_count=home_visit_count,
            most_recent_treatment_collateral_contact_date=self._most_recent_contact_date(
                compliance_evaluation_date,
                self.filter_for_treatment_collateral_contacts(),
            ),
            next_recommended_treatment_collateral_contact_date=next_recommended_treatment_collateral_contact_date,
            recommended_supervision_downgrade_level=self._get_recommended_supervision_downgrade_level(
                compliance_evaluation_date
            ),
            most_recent_employment_verification_date=most_recent_employment_verification_date,
            next_recommended_employment_verification_date=next_recommended_employment_verification_date,
        )

    def _next_recommended_assessment_date(
        self,
        most_recent_assessment: Optional[NormalizedStateAssessment],
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

    def _most_recent_contact_date(
        self, compliance_evaluation_date: date, contact_filter: ContactFilter
    ) -> Optional[date]:
        """Gets the most contact date. If there is not any, it returns None."""
        most_recent_contact = next(
            self._reverse_sorted_contacts(
                start_inclusive=self.start_of_supervision,
                end_inclusive=compliance_evaluation_date,
                contact_filter=contact_filter,
                most_recent_n=1,
            ),
            None,
        )
        if most_recent_contact:
            return most_recent_contact.contact_date
        return None

    def _count_contacts_on_date(
        self, compliance_evaluation_date: date, contact_filter: ContactFilter
    ) -> int:
        """Returns the number of contacts on compliance_evaluation_date."""
        return len(
            list(
                self._reverse_sorted_contacts(
                    start_inclusive=compliance_evaluation_date,
                    end_inclusive=compliance_evaluation_date,
                    contact_filter=contact_filter,
                    most_recent_n=None,
                )
            )
        )

    @classmethod
    def filter_for_face_to_face_contacts(cls) -> ContactFilter:
        return ContactFilter(
            contact_types={
                StateSupervisionContactType.DIRECT,
                StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT,
            },
            statuses={StateSupervisionContactStatus.COMPLETED},
        )

    @classmethod
    def filter_for_home_visit_contacts(cls) -> ContactFilter:
        return ContactFilter(
            contact_types={
                StateSupervisionContactType.DIRECT,
                StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT,
            },
            statuses={StateSupervisionContactStatus.COMPLETED},
            locations={StateSupervisionContactLocation.RESIDENCE},
        )

    @classmethod
    def filter_for_treatment_collateral_contacts(cls) -> ContactFilter:
        return ContactFilter(
            contact_types={
                StateSupervisionContactType.COLLATERAL,
                StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT,
            },
            statuses={StateSupervisionContactStatus.COMPLETED},
            locations={StateSupervisionContactLocation.TREATMENT_PROVIDER},
        )

    @classmethod
    def filter_for_employment_verification_contacts(cls) -> ContactFilter:
        return ContactFilter(verified_employment=True)

    def _completed_assessments_on_date(self, compliance_evaluation_date: date) -> int:
        """Returns the number of assessments that were completed on the compliance evaluation date."""
        return len(
            [
                assessment
                for assessment in self.assessments_by_date.get_sorted_items_in_range(
                    start_inclusive=compliance_evaluation_date,
                    end_inclusive=compliance_evaluation_date,
                )
                if assessment.assessment_score is not None
            ]
        )

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
                self.assessments_by_date,
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

    def _reverse_sorted_contacts(
        self,
        *,
        start_inclusive: date,
        end_inclusive: date,
        contact_filter: ContactFilter,
        most_recent_n: Optional[int],
    ) -> Iterator[NormalizedStateSupervisionContact]:
        """Gets contacts between two dates ordered from most recent to oldest.

        The date parameters are inclusive, so to get contacts for a single date, the
        start and end should both be set to that date.

        If `most_recent_n` is set, only the first `most_recent_n` contacts will be
        returned and any older contacts that are still in the date range will be
        skipped. For instance, if `most_recent_n` is set to 1, only the most recent
        contact in the date range is returned. If it is not set, then all of the
        contacts in the range are returned.
        """
        return itertools.islice(
            filter(
                contact_filter.matches,
                reversed(
                    self.supervision_contacts_by_date.get_sorted_items_in_range(
                        start_inclusive, end_inclusive
                    )
                ),
            ),
            most_recent_n,
        )

    def _default_next_recommended_contact_date_given_requirements(
        self,
        compliance_evaluation_date: date,
        required_contacts_per_period: int,
        period_length_days: int,
        new_supervision_contact_deadline_days: int,
        contact_filter: ContactFilter,
        use_business_days: bool,
    ) -> Optional[date]:
        """Provides a base implementation, describing when the next contact
        should be. Returns None if compliance standards are unknown or no subsequent
        contacts are required."""

        if required_contacts_per_period == 0:
            return None

        contact_dates = [
            # The range querier filters out contacts without a date, but we need this
            # check to satisfy mypy.
            cast(date, contact.contact_date)
            for contact in self._reverse_sorted_contacts(
                start_inclusive=self.start_of_supervision,
                end_inclusive=compliance_evaluation_date,
                contact_filter=contact_filter,
                most_recent_n=required_contacts_per_period,
            )
        ]
        if not contact_dates:
            # No contacts. First contact required is within NEW_SUPERVISION_CONTACT_DEADLINE_DAYS.
            return (
                np.busday_offset(  # type: ignore[call-overload]
                    dates=self.start_of_supervision,
                    offsets=new_supervision_contact_deadline_days,
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
        return contact_dates[-1] + timedelta(days=period_length_days)

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
        compliance_evaluation_date: Optional[date] = None,
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

    @abc.abstractmethod
    def _next_recommended_employment_verification_date(
        self,
        compliance_evaluation_date: date,
        most_recent_employment_verification_date: Optional[date],
    ) -> Optional[date]:
        """Returns the next recommended reassessment date or None if no further reassessments are needed."""
