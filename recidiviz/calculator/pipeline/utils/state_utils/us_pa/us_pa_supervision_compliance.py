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
from typing import Dict, List, Optional, Tuple

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.calculator.pipeline.utils.supervision_level_policy import (
    SupervisionLevelPolicy,
)
from recidiviz.common.constants.person_characteristics import Gender
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
)
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.entities import StateSupervisionContact

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

SUPERVISION_COLLATERAL_VISIT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionLevel, Tuple[int, int]
] = {
    StateSupervisionLevel.HIGH: (2, 30),
    StateSupervisionLevel.MAXIMUM: (1, 30),
    StateSupervisionLevel.MEDIUM: (1, 90),
    StateSupervisionLevel.MINIMUM: (1, 90),
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

MEDIUM_HIGH_SANCTION_DECISION_RAW_TEXT_CODES: List[str] = [
    "DFSE",
    "URIN",
    "ICRF",
    "GVPB",
    "COMS",
    "OPAT",
    "RECT",
    "EMOS",
    "MOTR",
    "DRPT",
    "IDOX",
    "AGPS",
    "CPCB",
    "IPAT",
    "IPMH",
    "VCCF",
    "HOTR",
    "ARR2",
]

DAYS_WITHIN_MAX_DATE = 90


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
            most_recent_assessment_date,
            most_recent_assessment_score,
            compliance_evaluation_date,
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
        most_recent_assessment_date: date,
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
        # Search for medium/high sanctions that occurred since the last assessment date
        incurred_medium_to_high_sanctions = (
            len(
                [
                    decision.decision_raw_text
                    for response in self.violation_responses
                    for decision in response.supervision_violation_response_decisions
                    if response.response_date
                    and response.response_date > most_recent_assessment_date
                    and decision.decision_raw_text
                    in MEDIUM_HIGH_SANCTION_DECISION_RAW_TEXT_CODES
                ]
            )
            > 0
        )
        return (
            (
                most_recent_score_is_low
                and supervised_on_minimum_year_plus
                and not incurred_medium_to_high_sanctions
            )
            or self._is_incarcerated(compliance_evaluation_date)
            or self._is_in_parole_board_hold(compliance_evaluation_date)
            or self._is_past_max_date_of_supervision(compliance_evaluation_date)
            or self._is_actively_absconding(compliance_evaluation_date)
            or self._is_within_max_date_of_supervision(compliance_evaluation_date)
        )

    def _can_skip_direct_contact(self, compliance_evaluation_date: date) -> bool:
        """Determines whether a contact can be skipped."""
        return (
            self._is_incarcerated(compliance_evaluation_date)
            or self._is_in_parole_board_hold(compliance_evaluation_date)
            or self._is_past_max_date_of_supervision(compliance_evaluation_date)
            or self._is_actively_absconding(compliance_evaluation_date)
        )

    def _next_recommended_face_to_face_date(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Returns when the next face-to-face contact should be. Returns None if
        compliance standards are unknown or no subsequent face-to-face contacts are required."""
        # No contacts required for monitored supervision
        # As of June 28, 2021, contacts are no longer needed for administrative supervision.
        if (
            self.supervision_period.supervision_level
            in (
                StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
                StateSupervisionLevel.LIMITED,
            )
            or self._can_skip_direct_contact(compliance_evaluation_date)
        ):
            return None

        (
            required_contacts,
            period_days,
        ) = self._get_required_face_to_face_contacts_and_period_days_for_level()

        return self._default_next_recommended_contact_date_given_requirements(
            compliance_evaluation_date,
            required_contacts,
            period_days,
            NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
            self._get_applicable_face_to_face_contacts_between_dates,
            use_business_days=True,
        )

    def _get_required_face_to_face_contacts_and_period_days_for_level(
        self,
    ) -> Tuple[int, int]:
        """Returns the number of face-to-face contacts that are required within time
        period (in days) for a supervision case with the given supervision level."""

        supervision_level = self.supervision_period.supervision_level
        if supervision_level is None:
            raise ValueError(
                "Supervision level not provided and so cannot calculate required face to face contact frequency."
            )
        return SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[self.case_type][
            supervision_level
        ]

    def _next_recommended_home_visit_date(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Returns when the next home visit should be. Returns None if compliance
        standards are unknown or no subsequent home visits are required."""
        if self.supervision_period.supervision_level is None:
            raise ValueError(
                "Supervision level not provided and therefore cannot calculate home visit contact frequency"
            )

        # No home visits are required for these supervision levels
        if (
            self.supervision_period.supervision_level
            in (
                StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY,
                StateSupervisionLevel.LIMITED,
                StateSupervisionLevel.MINIMUM,
            )
            or self._can_skip_direct_contact(compliance_evaluation_date)
            or self.supervision_period.supervision_level
            not in SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS
        ):
            return None

        (
            required_contacts,
            period_days,
        ) = SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS[
            self.supervision_period.supervision_level
        ]

        return self._default_next_recommended_contact_date_given_requirements(
            compliance_evaluation_date,
            required_contacts,
            period_days,
            NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS,
            self._get_applicable_home_visits_between_dates,
            use_business_days=False,
        )

    def _get_applicable_treatment_collateral_contacts_between_dates(
        self, lower_bound_inclusive: date, upper_bound_inclusive: date
    ) -> List[StateSupervisionContact]:
        """In PA, we will count all collateral contacts as treatment collateral contacts
        for compliance reasons, as policy currently doesn't distinguish between the two."""
        return [
            contact
            for contact in self.supervision_contacts
            if contact.contact_type
            in (
                StateSupervisionContactType.COLLATERAL,
                StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT,
            )
            # Contact must be marked as completed
            and contact.status == StateSupervisionContactStatus.COMPLETED
            and contact.contact_date is not None
            and lower_bound_inclusive <= contact.contact_date <= upper_bound_inclusive
        ]

    def _can_skip_collateral_contact_date(
        self, compliance_evaluation_date: date
    ) -> bool:
        """Returns whether or not a collateral contact can be skipped."""
        return (
            self._is_incarcerated(compliance_evaluation_date)
            or self._is_in_parole_board_hold(compliance_evaluation_date)
            or self._is_past_max_date_of_supervision(compliance_evaluation_date)
        )

    def _next_recommended_treatment_collateral_contact_date(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Returns when the next collateral contact should be. Returns None if compliance
        standards are unknown or no subsequent collateral contacts are required."""
        if self.supervision_period.supervision_level is None:
            raise ValueError(
                "Supervision level not provided and therefore cannot calculate collateral visit contact frequency"
            )

        # No collateral contacts are required for supervision levels not designated in
        # the map
        if (
            self.supervision_period.supervision_level
            not in SUPERVISION_COLLATERAL_VISIT_FREQUENCY_REQUIREMENTS
            or self._can_skip_collateral_contact_date(compliance_evaluation_date)
        ):
            return None

        (
            required_contacts,
            period_days,
        ) = SUPERVISION_COLLATERAL_VISIT_FREQUENCY_REQUIREMENTS[
            self.supervision_period.supervision_level
        ]

        return self._default_next_recommended_contact_date_given_requirements(
            compliance_evaluation_date,
            required_contacts,
            period_days,
            NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
            self._get_applicable_treatment_collateral_contacts_between_dates,
            use_business_days=False,
        )

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

    def _is_incarcerated(self, compliance_evaluation_date: date) -> bool:
        """Returns whether or not the client has an incarceration period that overlaps
        with the evaluation date. Specifically, we check if they are either in jail
        or in state prison."""
        incarceration_types = {
            incarceration_period.incarceration_type
            for incarceration_period in self.incarceration_period_index.incarceration_periods
            if incarceration_period.duration.contains_day(compliance_evaluation_date)
        }
        return (
            StateIncarcerationType.COUNTY_JAIL in incarceration_types
            or StateIncarcerationType.STATE_PRISON in incarceration_types
        )

    def _is_in_parole_board_hold(self, compliance_evaluation_date: date) -> bool:
        """Returns whether or not the client is in a parole board hold."""
        specialized_purposes_for_incarceration = {
            incarceration_period.specialized_purpose_for_incarceration
            for incarceration_period in self.incarceration_period_index.incarceration_periods
            if incarceration_period.duration.contains_day(compliance_evaluation_date)
        }
        return (
            StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
            in specialized_purposes_for_incarceration
        )

    def _max_completion_date_of_supervision(
        self, compliance_evaluation_date: date
    ) -> date:
        completion_dates = [
            supervision_sentence.projected_completion_date
            for supervision_sentence in self.supervision_period.supervision_sentences
            if supervision_sentence.projected_completion_date
            and supervision_sentence.start_date
            and DateRange.from_maybe_open_range(
                supervision_sentence.start_date,
                supervision_sentence.projected_completion_date,
            ).contains_day(compliance_evaluation_date)
        ]
        if completion_dates:
            return max(completion_dates)
        return date.max

    def _is_past_max_date_of_supervision(
        self, compliance_evaluation_date: date
    ) -> bool:
        """Returns whether the current supervision period is past the max date of the latest sentence
        projected completion date associated with the period."""
        return (
            self.supervision_period.duration.upper_bound_exclusive_date
            > self._max_completion_date_of_supervision(compliance_evaluation_date)
        )

    def _is_within_max_date_of_supervision(
        self, compliance_evaluation_date: date
    ) -> bool:
        """Returns whether the compliance evaluation date is within 90 days of the latest sentence
        projected completion date associated with the period."""
        max_completion_date = self._max_completion_date_of_supervision(
            compliance_evaluation_date
        )
        return (
            max_completion_date - relativedelta(days=DAYS_WITHIN_MAX_DATE)
            <= compliance_evaluation_date
            <= max_completion_date
        )

    def _is_actively_absconding(self, compliance_evaluation_date: date) -> bool:
        """Returns whether this current supervision period indicates an active absconsion."""
        return (
            self.supervision_period.duration.contains_day(compliance_evaluation_date)
            and self.supervision_period.admission_reason
            == StateSupervisionPeriodAdmissionReason.ABSCONSION
        )
