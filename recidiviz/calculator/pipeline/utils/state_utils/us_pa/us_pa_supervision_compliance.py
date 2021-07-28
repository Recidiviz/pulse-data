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

import numpy
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
    StateSupervisionLevel, Tuple[int, int]
] = {
    StateSupervisionLevel.LIMITED: (1, 365),
    StateSupervisionLevel.MINIMUM: (1, 90),
    StateSupervisionLevel.MEDIUM: (1, 30),
    StateSupervisionLevel.MAXIMUM: (2, 30),
    StateSupervisionLevel.HIGH: (4, 30),
}
# Dictionary from supervision level -> tuple of number of times they must be contacted per time period.
# A tuple (x, y) should be interpreted as x home visits every y days.
SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionLevel, Tuple[int, int]
] = {
    StateSupervisionLevel.MINIMUM: (1, 180),
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

    def _face_to_face_contact_frequency_is_sufficient(
        self, compliance_evaluation_date: date
    ) -> Optional[bool]:
        """Calculates whether the frequency of face-to-face contacts between the officer and the person on supervision
        is sufficient with respect to the state standards for the level of supervision of the case.
        """
        # No contacts required for monitored supervision
        if (
            self.supervision_period.supervision_level
            == StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY
        ):
            return True

        business_days_since_start = numpy.busday_count(
            self.start_of_supervision, compliance_evaluation_date
        )

        if business_days_since_start <= NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS:
            # This is a recently started supervision period, and the person has not yet hit the number of business days
            # from the start of their supervision at which the officer is required to have been in contact with the
            # person. This face-to-face contact is up to date regardless of when the last contact was completed.
            logging.debug(
                "Supervision period %d started %d business days before the compliance date %s. Contact is not "
                "overdue.",
                self.supervision_period.supervision_period_id,
                business_days_since_start,
                compliance_evaluation_date,
            )
            return True

        # Get applicable contacts that occurred between the start of supervision and the
        # compliance_evaluation_date (inclusive)
        applicable_contacts = self._get_applicable_face_to_face_contacts_between_dates(
            self.start_of_supervision, compliance_evaluation_date
        )

        if not applicable_contacts:
            # This person has been on supervision for longer than the allowed number of days without an initial contact.
            # The face-to-face contact standard is not in compliance.
            return False

        (
            required_contacts,
            period_days,
        ) = self._get_required_face_to_face_contacts_and_period_days_for_level()

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
        return SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[supervision_level]

    def _home_visit_frequency_is_sufficient(
        self, compliance_evaluation_date: date
    ) -> Optional[bool]:
        # TODO(#7052) Update with appropriate policies
        return None

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
