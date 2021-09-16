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
"""State-specific utils for determining compliance with supervision standards for US_ND.
   We currently measure compliance for `GENERAL` and `SEX_OFFENSE` case types. Below are the expected requirements:
        - `MINIMUM`
            - Face to face contacts: 1x every 90 days
            - Home visit: within 90 days of start of supervision
        - `MEDIUM`
            - Face to face contacts: 1x every 60 days
            - Home visit: within 90 days of start of supervision, 1x every 365 days
        - `MAXIMUM`
            - Face to face contacts: 1x every 30 days
            - Home visit: within 90 days of start of supervision, 1x every 365 days
    Contact with treatment staff is currently excluded for `SEX_OFFENSE` case types.
"""
import logging
import sys
from datetime import date, timedelta
from typing import Dict, Optional, Tuple

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.calculator.pipeline.utils.supervision_level_policy import (
    SupervisionLevelPolicy,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)

# Please refer to http://go/nd-risk-assessment-policy/ for context on these values.
# These values were last verified on 02/23/2021.
LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE: int = 30
REASSESSMENT_DEADLINE_DAYS: int = 212

# Dictionary from supervision level -> tuple of number of times they must be contacted per time period
# time period is in number of calendar months
# TODO(#8637): Add unit of time to SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS
SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionLevel, Tuple[int, int]
] = {
    StateSupervisionLevel.MINIMUM: (1, 3),
    StateSupervisionLevel.MEDIUM: (1, 2),
    StateSupervisionLevel.MAXIMUM: (1, 1),
}

HOME_VISIT_CONTACT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionLevel, Tuple[int, int]
] = {
    StateSupervisionLevel.MINIMUM: (0, sys.maxsize),
    StateSupervisionLevel.MEDIUM: (1, 365),
    StateSupervisionLevel.MAXIMUM: (1, 365),
}
NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS = 90


class UsNdSupervisionCaseCompliance(StateSupervisionCaseComplianceManager):
    """US_ND specific calculations for supervision case compliance."""

    def _get_initial_assessment_number_of_days(self) -> int:
        """Returns the number of days that an initial assessment should take place."""
        return LSIR_INITIAL_NUMBER_OF_DAYS_COMPLIANCE

    def _guidelines_applicable_for_case(self, _evaluation_date: date) -> bool:
        """Returns whether the standard state guidelines are applicable for the given supervision case. The standard
        guidelines are not applicable for people who are not classified or who are in interstate compact."""
        # Check case type
        if self.case_type not in (
            StateSupervisionCaseType.GENERAL,
            StateSupervisionCaseType.SEX_OFFENSE,
        ):
            return False

        # Check supervision level
        allowed_supervision_levels = [
            StateSupervisionLevel.MINIMUM,
            StateSupervisionLevel.MEDIUM,
            StateSupervisionLevel.MAXIMUM,
        ]
        if self.supervision_period.supervision_level not in allowed_supervision_levels:
            return False

        return True

    def _next_recommended_reassessment(
        self,
        most_recent_assessment_date: date,
        most_recent_assessment_score: int,
        compliance_evaluation_date: Optional[date] = None,
    ) -> Optional[date]:
        """Returns the next recommended reassessment date or None if no further reassessments are needed."""
        reassessment_deadline = most_recent_assessment_date + relativedelta(
            days=REASSESSMENT_DEADLINE_DAYS
        )
        logging.debug(
            "Last assessment was taken on %s. Re-assessment due by %s.",
            most_recent_assessment_date,
            reassessment_deadline,
        )
        return reassessment_deadline

    def _get_required_face_to_face_contacts_and_period_months_for_level(
        self,
    ) -> Tuple[int, int]:
        """Returns the number of face-to-face contacts that are required within time period (in days) for a supervision
        case with the given supervision level.
        """
        supervision_level: Optional[
            StateSupervisionLevel
        ] = self.supervision_period.supervision_level

        if self.case_type not in (
            StateSupervisionCaseType.GENERAL,
            StateSupervisionCaseType.SEX_OFFENSE,
        ):
            raise ValueError(
                "Standard supervision compliance guidelines for face to face contacts are "
                f"not applicable for cases with a supervision level of {supervision_level}. "
                f"Should not be calculating compliance for this supervision case."
            )
        if supervision_level is None:
            raise ValueError(
                "Supervision level not provided and so cannot calculate required face to face contact frequency."
            )

        return SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[supervision_level]

    def _home_visit_frequency_is_sufficient(
        self, compliance_evaluation_date: date
    ) -> Optional[bool]:
        """Calculates whether the frequency of home visits between the officer and the person on supervision
        is sufficient with respect to the state standards for the level of supervision of the case.
        """
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

        if self.case_type not in (
            StateSupervisionCaseType.GENERAL,
            StateSupervisionCaseType.SEX_OFFENSE,
        ):
            raise ValueError(
                "Standard supervision compliance guidelines for home visits are not applicable "
                f"for cases with a supervision level of {supervision_level}. Should not be "
                f"calculating compliance for this supervision case."
            )
        if supervision_level is None:
            raise ValueError(
                "Supervision level not provided and so cannot calculate required home visit frequency."
            )

        return HOME_VISIT_CONTACT_FREQUENCY_REQUIREMENTS[supervision_level]

    def _next_recommended_face_to_face_date(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Returns when the next face-to-face contact should be. Returns None if compliance standards are
        unknown or no subsequent face-to-face contacts are required."""

        # North Dakota has contact rquirements by month. For example, Medium-level supervisees need f2f
        # contact once every 2 calendar months. So if the last contact was Feb 12, the contact requirement
        # wouldn't be in violation until May 1. Inversely, as long as we had a contact by the beginning
        # of the calendar month 2 months previous, we are in compliance.

        (
            required_contacts,
            period_months,
        ) = self._get_required_face_to_face_contacts_and_period_months_for_level()

        contacts_since_supervision_start = (
            self._get_applicable_face_to_face_contacts_between_dates(
                self.start_of_supervision, compliance_evaluation_date
            )
        )
        contact_dates = sorted(
            [
                contact.contact_date
                for contact in contacts_since_supervision_start
                if contact.contact_date is not None
            ]
        )

        if len(contact_dates) < required_contacts:
            # Not enough contacts. Give the PO until the full period window has elapsed
            # to meet with their client.
            candidate_day = self.start_of_supervision + relativedelta(
                months=period_months
            )
        else:
            # If n contacts are required every k month, this looks at the nth-to-last contact
            # and returns the date k months after that.
            candidate_day = contact_dates[-required_contacts] + relativedelta(
                months=period_months
            )

        # Need to project candidate_day to last day of the month.
        return (candidate_day + relativedelta(months=1)).replace(day=1) - timedelta(
            days=1
        )

    def _get_supervision_level_policy(
        self, evaluation_date: date
    ) -> Optional[SupervisionLevelPolicy]:
        return None
