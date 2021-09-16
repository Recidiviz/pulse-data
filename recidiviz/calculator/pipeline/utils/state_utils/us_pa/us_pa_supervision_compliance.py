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
"""State-specific utils for determining compliance with supervision standards for US_PA."""
import logging
from datetime import date
from typing import Dict, Optional, Tuple

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.calculator.pipeline.utils.supervision_level_policy import (
    SupervisionLevelPolicy,
)
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)

NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS = 45
REASSESSMENT_DEADLINE_DAYS = 365

# Dictionary from case type -> supervision level -> tuple of number of times they must be contacted per time period.
# A tuple (x, y) should be interpreted as x home visits every y days.
SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionCaseType, Dict[StateSupervisionLevel, Tuple[int, int]]
] = {
    StateSupervisionCaseType.GENERAL: {
        StateSupervisionLevel.MINIMUM: (1, 90),
        StateSupervisionLevel.MEDIUM: (1, 30),
        StateSupervisionLevel.MAXIMUM: (2, 30),
        StateSupervisionLevel.HIGH: (4, 30),
    }
}
# Dictionary from supervision level -> tuple of number of times they must be contacted per time period.
# A tuple (x, y) should be interpreted as x home visits every y days.
SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionLevel, Tuple[int, int]
] = {
    StateSupervisionLevel.MEDIUM: (1, 60),
    StateSupervisionLevel.MAXIMUM: (1, 30),
    StateSupervisionLevel.HIGH: (1, 30),
}

NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS = 2
NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS = 10


RISK_SCORE_TO_SUPERVISION_LEVEL_POLICY_DATE = date(2011, 1, 4)

CURRENT_US_PA_ASSESSMENT_SCORE_RANGE: Dict[
    Gender, Dict[StateSupervisionLevel, Tuple[int, Optional[int]]]
] = {
    gender: {
        StateSupervisionLevel.MINIMUM: (0, 19),
        StateSupervisionLevel.MEDIUM: (20, 27),
        StateSupervisionLevel.MAXIMUM: (28, None),
    }
    for gender in Gender
}


class UsPaSupervisionCaseCompliance(StateSupervisionCaseComplianceManager):
    """US_PA specific calculations for supervision case compliance."""

    def _guidelines_applicable_for_case(self, _evaluation_date: date) -> bool:
        """Returns whether the standard state guidelines are applicable for the given supervision case based on the supervision level."""

        # Check supervision level is valid
        allowed_supervision_levels = [
            StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
            StateSupervisionLevel.LIMITED,
            StateSupervisionLevel.MINIMUM,
            StateSupervisionLevel.MEDIUM,
            StateSupervisionLevel.MAXIMUM,
            StateSupervisionLevel.HIGH,
        ]
        return self.supervision_period.supervision_level in allowed_supervision_levels

    def _get_initial_assessment_number_of_days(self) -> int:
        """Returns the number of days that an initial assessment should take place."""
        return NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS

    def _next_recommended_reassessment(
        self,
        most_recent_assessment_date: date,
        most_recent_assessment_score: int,
        compliance_evaluation_date: Optional[date] = None,
    ) -> Optional[date]:
        """Returns the next recommended reassessment date or None if no further reassessments are needed."""
        if not compliance_evaluation_date:
            raise ValueError("PA supervision compliance requires an evaluation date")
        if self._can_skip_reassessment(
            most_recent_assessment_score, compliance_evaluation_date
        ):
            # No reassessment is needed if criteria is met
            return None

        reassessment_deadline = most_recent_assessment_date + relativedelta(
            days=REASSESSMENT_DEADLINE_DAYS
        )
        logging.debug(
            "Last assessment was taken on %s. Re-assessment due by %s.",
            most_recent_assessment_date,
            reassessment_deadline,
        )
        return reassessment_deadline

    def _can_skip_reassessment(
        self,
        most_recent_assessment_score: int,
        compliance_evaluation_date: date,
    ) -> bool:
        """Determines whether regular reassessment is needed.
        According to PA policy documents, this includes:
            - Attained a low risk score (0-19) on their most recent LSI-R assessment, and
            - Were supervised on minimum supervision for a year or more, and
            - Have not incurred any medium or high level sanctions since the administration of their last LSI-R.
        """
        most_recent_score_is_low = most_recent_assessment_score < 20
        days_since_start = (compliance_evaluation_date - self.start_of_supervision).days
        supervised_on_minimum_year_plus = (
            self.supervision_period.supervision_level == StateSupervisionLevel.MINIMUM
            and days_since_start >= 365
        )
        incurred_medium_to_high_sanctions = (
            False  # TODO(#9105): Bring in violations to further calculate
        )
        return (
            most_recent_score_is_low
            and supervised_on_minimum_year_plus
            and not incurred_medium_to_high_sanctions
        )

    def _next_recommended_face_to_face_date(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Returns when the next face-to-face contact should be. Returns None if compliance standards are
        unknown or no subsequent face-to-face contacts are required."""
        # No contacts required for monitored supervision
        # As of June 28, 2021, contacts are no longer needed for administrative supervision.
        if self.supervision_period.supervision_level in (
            StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
            StateSupervisionLevel.LIMITED,
        ):
            return None

        (
            required_contacts,
            period_days,
        ) = self._get_required_face_to_face_contacts_and_period_days_for_level()

        return self._default_next_recommended_face_to_face_date_given_requirements(
            compliance_evaluation_date,
            required_contacts,
            period_days,
            NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
        )

    def _get_required_face_to_face_contacts_and_period_days_for_level(
        self,
    ) -> Tuple[int, int]:
        """Returns the number of face-to-face contacts that are required within time period (in days) for a supervision
        case with the given supervision level."""

        supervision_level = self.supervision_period.supervision_level
        if supervision_level is None:
            raise ValueError(
                "Supervision level not provided and so cannot calculate required face to face contact frequency."
            )
        return SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[self.case_type][
            supervision_level
        ]

    def _home_visit_frequency_is_sufficient(
        self, compliance_evaluation_date: date
    ) -> Optional[bool]:
        """Calculates whether the frequency of home visits between the officer and the person on supervision
        is sufficient with respect to the state standards for the level of supervision of the case.
        """
        # No home visits are required for these supervision levels
        if self.supervision_period.supervision_level in (
            StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
            StateSupervisionLevel.LIMITED,
            StateSupervisionLevel.MINIMUM,
        ):
            return True

        days_since_start = (compliance_evaluation_date - self.start_of_supervision).days

        if days_since_start <= NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS:
            # This is a recently started supervision period, and the person has not yet hit the number of days
            # from the start of their supervision at which the officer is required to have made a home visit.
            logging.debug(
                "Supervision period %d started %d business days before the compliance date %s. Home visit is not "
                "overdue.",
                self.supervision_period.supervision_period_id,
                days_since_start,
                compliance_evaluation_date,
            )
            return True

        # Get applicable home visits that occurred between the start of supervision and the
        # compliance_evaluation_date (inclusive)
        applicable_visits = self._get_applicable_home_visits_between_dates(
            self.start_of_supervision, compliance_evaluation_date
        )

        if not applicable_visits:
            # This person has been on supervision for longer than the allowed number of days without an initial
            # home visit. The initial home visit standard is not in compliance.
            return False

        (
            required_contacts,
            period_days,
        ) = self._get_required_home_visits_and_period_days()

        if days_since_start < period_days:
            # If they've had a visit since the start of their supervision, and they have been on supervision for less
            # than the number of days in which they would need another contact, then the case is in compliance
            return True

        visits_within_period = [
            visit
            for visit in applicable_visits
            if visit.contact_date is not None
            and (compliance_evaluation_date - visit.contact_date).days < period_days
        ]

        return len(visits_within_period) >= required_contacts

    def _get_required_home_visits_and_period_days(self) -> Tuple[int, int]:
        """Returns the number of home visits that are required within time period (in days) for a supervision case"""
        supervision_level: Optional[
            StateSupervisionLevel
        ] = self.supervision_period.supervision_level

        if supervision_level is None:
            raise ValueError(
                "Supervision level not provided and so cannot calculate required home visit frequency."
            )

        return SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS[supervision_level]

    def _get_supervision_level_policy(
        self, evaluation_date: date
    ) -> Optional[SupervisionLevelPolicy]:
        if evaluation_date < RISK_SCORE_TO_SUPERVISION_LEVEL_POLICY_DATE:
            return None

        if self.case_type != StateSupervisionCaseType.GENERAL:
            return None

        if not self.person.gender:
            return None

        return SupervisionLevelPolicy(
            level_mapping=CURRENT_US_PA_ASSESSMENT_SCORE_RANGE,
            start_date=RISK_SCORE_TO_SUPERVISION_LEVEL_POLICY_DATE,
        )
