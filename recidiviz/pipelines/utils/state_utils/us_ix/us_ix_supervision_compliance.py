# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""State-specific utils for determining compliance with supervision standards for US_IX.
   We currently measure compliance for `GENERAL` and `SEX_OFFENSE` case types. Below are the expected requirements:
        - For `GENERAL` cases, there are two level systems:
            1. Deprecated system mapping (`StateSupervisionLevel`: raw string) and expected frequencies:
                - Initial compliance standards (same for all levels):
                    - LSI-R Assessment: within 45 days (if no assessment exists, or if one is past due)
                    - Face to face: within 3 days of start of supervision
                - `MINIMUM`:`LEVEL 1`
                    - Face to face contacts: none necessary
                    - LSI-R Assessment: none
                - `MEDIUM`:`LEVEL 2`
                    - Face to face contacts: 1x every 180 days
                    - LSI-R Assessment: 1x every 365 days
                - `HIGH`: `LEVEL 3`
                    - Face to face contacts: 1x every 30 days
                    - LSI-R Assessment: 1x every 365 days
                - `MAXIMUM`: `LEVEL 4`
                    - Face to face contacts: 2x every 30 days
                    - LSI-R Assessment: 1x every 365 days
            2. New system mapping (`StateSupervisionLevel`: raw string) and expected frequencies:
                - Initial compliance standards (same for all levels):
                    - LSI-R Assessment: within 45 days (if no assessment exists, or if one is past due)
                    - Face to face: within 3 days of start of supervision
                 - `MINIMUM`:`MINIMUM`
                    - Face to face contacts: 1x every 180 days
                    - LSI-R Assessment: none
                - `MEDIUM`:`MODERATE`
                    - Face to face contacts: 2x every 90 days
                    - LSI-R Assessment: 1x every 365 days
                - `HIGH`: `HIGH`
                    - Face to face contacts: 2x every 30 days
                    - LSI-R Assessment: 1x every 365 days
        - For `SEX_OFFENSE` cases, there is one level system with the following mapping and expected frequencies:
            - Initial compliance standards (same for all levels):
                - LSI-R Assessment: within 45 days if on probation, or within 90 days if on parole
                - Face to face: within 3 days of start of supervision
            - `MINIMUM`:`SO LEVEL 1`/`SO LOW`
                - Face to face contacts: 1x every 90 days
                - LSI-R Assessment: every 365 days if LSI-R > 16
            - `MEDIUM`:`SO LEVEL 2`/`SO MODERATE`
                - Face to face contacts: 1x every 30 days
                - LSI-R Assessment: every 365 days if LSI-R > 16
            - `HIGH`: `SO LEVEL 3`/`SO HIGH`
                - Face to face contacts: 2x every 30 days
                - LSI-R Assessment: every 365 days if LSI-R > 16
"""
import logging
import sys
from datetime import date, timedelta
from typing import Dict, Optional, Tuple

from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_person import StateGender
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactStatus,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.pipelines.metrics.utils.supervision_case_compliance_manager import (
    ContactFilter,
    StateSupervisionCaseComplianceManager,
)
from recidiviz.pipelines.utils.supervision_level_policy import SupervisionLevelPolicy

SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION = 45
SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE = 90

# Minimum score for people with sex offenses to require an annual LSI-R re-assessment.
# Current as of 2/17/2021, sourced from:
# http://forms.idoc.idaho.gov/WebLink/0/edoc/283396/Sex%20Offenders%20Supervision%20and%20Classification.pdf
SEX_OFFENSE_LSIR_MINIMUM_SCORE: Dict[StateGender, int] = {
    StateGender.FEMALE: 23,
    StateGender.TRANS_FEMALE: 23,
    StateGender.MALE: 21,
    StateGender.TRANS_MALE: 21,
}

NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS = 45
REASSESSMENT_DEADLINE_DAYS = 365

DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 30
DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 30
DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 180
DEPRECATED_MINIMUM_SUPERVISION_CONTACT_FREQUENCY_GENERAL_CASE = sys.maxsize

# Dictionary from case type -> supervision level -> tuple of number of times they must be contacted per time period.
# A tuple (x, y) should be interpreted as x home visits every y days.
SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionCaseType, Dict[StateSupervisionLevel, Tuple[int, int]]
] = {
    StateSupervisionCaseType.GENERAL: {
        StateSupervisionLevel.MINIMUM: (1, 180),
        StateSupervisionLevel.MEDIUM: (1, 45),
        StateSupervisionLevel.HIGH: (1, 15),
    },
    StateSupervisionCaseType.SEX_OFFENSE: {
        StateSupervisionLevel.MINIMUM: (1, 90),
        StateSupervisionLevel.MEDIUM: (1, 30),
        StateSupervisionLevel.HIGH: (2, 30),
    },
}
# Dictionary from case type -> supervision level -> tuple of number of times they must be contacted per time period.
# A tuple (x, y) should be interpreted as x home visits every y days.
US_IX_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionCaseType, Dict[StateSupervisionLevel, Tuple[int, int]]
] = {
    StateSupervisionCaseType.GENERAL: {
        StateSupervisionLevel.MINIMUM: (1, 365),
        StateSupervisionLevel.MEDIUM: (1, 365),
        StateSupervisionLevel.HIGH: (1, 90),
    },
    StateSupervisionCaseType.SEX_OFFENSE: {
        StateSupervisionLevel.MINIMUM: (1, 90),
        StateSupervisionLevel.MEDIUM: (1, 60),
        StateSupervisionLevel.HIGH: (1, 30),
    },
}

US_IX_SUPERVISION_TREATMENT_COLLATERAL_CONTACT_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionLevel, Tuple[int, int]
] = {
    StateSupervisionLevel.MINIMUM: (1, 90),
    StateSupervisionLevel.MEDIUM: (1, 30),
    StateSupervisionLevel.HIGH: (1, 30),
}

# There are only employment verification requirements for SEX_OFFENSE cases
US_IX_SUPERVISION_EMPLOYMENT_VERIFICATION_FREQUENCY_REQUIREMENTS: Dict[
    StateSupervisionCaseType, Dict[StateSupervisionLevel, Tuple[int, int]]
] = {
    StateSupervisionCaseType.SEX_OFFENSE: {
        StateSupervisionLevel.MINIMUM: (1, 90),
        StateSupervisionLevel.MEDIUM: (1, 60),
        StateSupervisionLevel.HIGH: (1, 30),
    },
}

NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS = 3
NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS = 30
NEW_SUPERVISION_TREATMENT_CONTACT_DEADLINE_DAYS = 14
# Employment verification is required upon initial sign up for both GENERAL and SEX_OFFENSE cases
NEW_SUPERVISION_EMPLOYMENT_VERIFICATION_DAYS = 30

# This is the date where Idaho switched its method of determining supervision
# levels, going from 4 levels to 3.
DATE_OF_SUPERVISION_LEVEL_SWITCH = date(2020, 7, 23)

# Note: This mapping doesn't contain details for EXTERNAL_UNKNOWN or NULL. All other types
# that we know about (as of Feb. 9, 2021) are reflected in this mapping.
# See
# http://forms.idoc.idaho.gov/WebLink/0/edoc/281944/Interim%20Standards%20to%20Probation%20and%20Parole%20Supervision%20Strategies.pdf
CURRENT_US_IX_ASSESSMENT_SCORE_RANGE: Dict[
    StateGender, Dict[StateSupervisionLevel, Tuple[int, Optional[int]]]
] = {
    StateGender.FEMALE: {
        StateSupervisionLevel.MINIMUM: (0, 22),
        StateSupervisionLevel.MEDIUM: (23, 30),
        StateSupervisionLevel.HIGH: (31, None),
    },
    StateGender.TRANS_FEMALE: {
        StateSupervisionLevel.MINIMUM: (0, 22),
        StateSupervisionLevel.MEDIUM: (23, 30),
        StateSupervisionLevel.HIGH: (31, None),
    },
    StateGender.MALE: {
        StateSupervisionLevel.MINIMUM: (0, 20),
        StateSupervisionLevel.MEDIUM: (21, 28),
        StateSupervisionLevel.HIGH: (29, None),
    },
    StateGender.TRANS_MALE: {
        StateSupervisionLevel.MINIMUM: (0, 20),
        StateSupervisionLevel.MEDIUM: (21, 28),
        StateSupervisionLevel.HIGH: (29, None),
    },
}

THROUGH_07_2020_US_IX_ASSESSMENT_SCORE_RANGE: Dict[
    StateGender, Dict[StateSupervisionLevel, Tuple[int, Optional[int]]]
] = {
    gender: {
        StateSupervisionLevel.MINIMUM: (0, 15),
        StateSupervisionLevel.MEDIUM: (16, 23),
        StateSupervisionLevel.HIGH: (24, 30),
        StateSupervisionLevel.MAXIMUM: (31, None),
    }
    for gender in StateGender
}


class UsIxSupervisionCaseCompliance(StateSupervisionCaseComplianceManager):
    """US_IX specific calculations for supervision case compliance."""

    # TODO(#12146): Exclude absconsion periods from US_IX in the
    #  UsIxSupervisionCaseCompliance delegate
    def _guidelines_applicable_for_case(self, evaluation_date: date) -> bool:
        """Returns whether the standard state guidelines are applicable for the given supervision case. The standard
        guidelines are only applicable for supervision cases of type GENERAL and SEX_OFFENSE, each with corresponding
        expected supervision levels and supervision types."""
        supervision_type = self.supervision_period.supervision_type

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
            StateSupervisionLevel.HIGH,
        ]
        if (
            self.case_type is StateSupervisionCaseType.GENERAL
            and evaluation_date < DATE_OF_SUPERVISION_LEVEL_SWITCH
        ):
            allowed_supervision_levels.append(StateSupervisionLevel.MAXIMUM)
        if self.supervision_period.supervision_level not in allowed_supervision_levels:
            return False

        # Check supervision type
        allowed_supervision_types = [
            StateSupervisionPeriodSupervisionType.DUAL,
            StateSupervisionPeriodSupervisionType.PROBATION,
            StateSupervisionPeriodSupervisionType.PAROLE,
        ]
        is_bench_warrant = (
            supervision_type == StateSupervisionPeriodSupervisionType.BENCH_WARRANT
        )
        # TODO(#9440): Build support for calculating compliance for bench warrant cases
        #  if necessary
        if supervision_type not in allowed_supervision_types or is_bench_warrant:
            return False

        return True

    def _get_initial_assessment_number_of_days(self) -> int:
        """Returns the number of days that an initial assessment should take place, given a `case_type` and
        `supervision_type`."""
        supervision_type: Optional[
            StateSupervisionPeriodSupervisionType
        ] = self.supervision_period.supervision_type
        if self.case_type == StateSupervisionCaseType.GENERAL:
            return NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
        if self.case_type == StateSupervisionCaseType.SEX_OFFENSE:
            if supervision_type == StateSupervisionPeriodSupervisionType.PROBATION:
                return SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
            if supervision_type in (
                StateSupervisionPeriodSupervisionType.PAROLE,
                StateSupervisionPeriodSupervisionType.DUAL,
            ):
                return SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE
            raise ValueError(f"Found unexpected supervision_type: [{supervision_type}]")

        raise ValueError(f"Found unexpected case_type: [{self.case_type}]")

    def _next_recommended_reassessment(
        self,
        most_recent_assessment_date: date,
        most_recent_assessment_score: int,
        compliance_evaluation_date: Optional[date] = None,
    ) -> Optional[date]:
        """Returns the next recommended reassessment date or None if no further reassessments are needed."""
        if self.case_type == StateSupervisionCaseType.GENERAL and (
            self.supervision_period.supervision_level == StateSupervisionLevel.MINIMUM
        ):
            # No reassessment needed.
            return None
        if self.case_type == StateSupervisionCaseType.SEX_OFFENSE:
            if (
                not (gender := self.person.gender)
                or (threshold_score := SEX_OFFENSE_LSIR_MINIMUM_SCORE.get(gender))
                is None
            ):
                logging.warning(
                    "No threshold sex offense LSIR minimum found for gender: %s",
                    gender,
                )

                # If we can't find a stored threshold, we take the most permissive threshold score that
                # we know of since it's better to err on the side of less assessments.
                threshold_score = max(SEX_OFFENSE_LSIR_MINIMUM_SCORE.values())

            if most_recent_assessment_score < threshold_score:
                # No reassessment needed.
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

    def _next_recommended_face_to_face_date(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Returns when the next face-to-face contact should be. Returns None if compliance standards are
        unknown or no subsequent face-to-face contacts are required."""
        (
            required_contacts,
            period_days,
        ) = self._get_required_face_to_face_contacts_and_period_days_for_level()

        return self._default_next_recommended_contact_date_given_requirements(
            compliance_evaluation_date,
            required_contacts,
            period_days,
            NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS,
            self.filter_for_face_to_face_contacts(),
            use_business_days=True,
        )

    def _get_required_face_to_face_contacts_and_period_days_for_level(
        self,
    ) -> Tuple[int, int]:
        """Returns the number of face-to-face contacts that are required within time period (in days) for a supervision
         case with the given supervision level.
        There are two supervision level systems, each with different face to face contact frequency expectations. The
        deprecated level system has four levels (which are associated with four numeric levels), and the new system has
        three levels.
        """
        supervision_level_raw_text: Optional[
            str
        ] = self.supervision_period.supervision_level_raw_text
        supervision_level: Optional[
            StateSupervisionLevel
        ] = self.supervision_period.supervision_level
        is_new_level_system = UsIxSupervisionCaseCompliance._is_new_level_system(
            supervision_level_raw_text
        )

        if self.case_type not in (
            StateSupervisionCaseType.GENERAL,
            StateSupervisionCaseType.SEX_OFFENSE,
        ):
            raise ValueError(
                "Standard supervision compliance guidelines not applicable for cases with a case"
                f" type of {self.case_type}. Should not be calculating compliance for this"
                f" supervision case."
            )

        if supervision_level is None:
            raise ValueError(
                "Supervision level not provided and so cannot calculate required face to face contact frequency."
            )
        if (
            self.case_type == StateSupervisionCaseType.GENERAL
            and not is_new_level_system
        ):
            if supervision_level == StateSupervisionLevel.MINIMUM:
                return 0, DEPRECATED_MINIMUM_SUPERVISION_CONTACT_FREQUENCY_GENERAL_CASE
            if supervision_level == StateSupervisionLevel.MEDIUM:
                return (
                    1,
                    DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE,
                )
            if supervision_level == StateSupervisionLevel.HIGH:
                return (
                    1,
                    DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE,
                )
            if supervision_level == StateSupervisionLevel.MAXIMUM:
                return (
                    2,
                    DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE,
                )

        return SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[self.case_type][
            supervision_level
        ]

    @staticmethod
    def _is_new_level_system(supervision_level_raw_text: Optional[str]) -> bool:
        """As of July 2020, Idaho has deprecated its previous supervision level system and now uses `LOW`, `MODERATE`,
        and `HIGH`. Returns whether the level system used is one of new values."""

        if not supervision_level_raw_text:
            raise ValueError(
                "a StateSupervisionPeriod should always have a value for supervision_level_raw_text."
            )

        return supervision_level_raw_text in ("LOW", "MODERATE", "HIGH")

    @classmethod
    def filter_for_home_visit_contacts(cls) -> ContactFilter:
        """In Idaho, home visits are considered location only and are collateral vs.
        face-to-face agnostic."""
        return ContactFilter(
            # Contact must be marked as completed
            statuses={StateSupervisionContactStatus.COMPLETED},
            # These are the types of contacts that can satisfy the home visit requirement
            locations={StateSupervisionContactLocation.RESIDENCE},
        )

    def _next_recommended_home_visit_date(
        self,
        compliance_evaluation_date: date,
    ) -> Optional[date]:
        """Returns when the next home visit should be. Returns None if compliance standards are
        unknown or no subsequent home visits are required."""
        if self.supervision_period.supervision_level is None:
            raise ValueError(
                "Supervision level not provided and so cannot calculate next recommended "
                "home visit."
            )

        if self.case_type not in (
            StateSupervisionCaseType.GENERAL,
            StateSupervisionCaseType.SEX_OFFENSE,
        ):
            return None

        if (
            self.supervision_period.supervision_level
            not in US_IX_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS[self.case_type]
        ):
            return None

        (
            required_contacts,
            period_days,
        ) = US_IX_SUPERVISION_HOME_VISIT_FREQUENCY_REQUIREMENTS[self.case_type][
            self.supervision_period.supervision_level
        ]

        return self._default_next_recommended_contact_date_given_requirements(
            compliance_evaluation_date,
            required_contacts,
            period_days,
            NEW_SUPERVISION_HOME_VISIT_DEADLINE_DAYS,
            self.filter_for_home_visit_contacts(),
            use_business_days=False,
        )

    def _next_recommended_treatment_collateral_contact_date(
        self, compliance_evaluation_date: date
    ) -> Optional[date]:
        """Returns when the next treatment collateral contact should be. Returns None if
        compliance standards are unknown or no subsequent home visits are required."""
        if self.supervision_period.supervision_level is None:
            raise ValueError(
                "Supervision level not provided and so cannot calculate next recommended "
                "treatment collateral contact."
            )

        if (
            self.supervision_period.supervision_level
            not in US_IX_SUPERVISION_TREATMENT_COLLATERAL_CONTACT_FREQUENCY_REQUIREMENTS
        ):
            return None

        (
            required_contacts,
            period_days,
        ) = US_IX_SUPERVISION_TREATMENT_COLLATERAL_CONTACT_FREQUENCY_REQUIREMENTS[
            self.supervision_period.supervision_level
        ]

        return self._default_next_recommended_contact_date_given_requirements(
            compliance_evaluation_date,
            required_contacts,
            period_days,
            NEW_SUPERVISION_TREATMENT_CONTACT_DEADLINE_DAYS,
            self.filter_for_treatment_collateral_contacts(),
            use_business_days=False,
        )

    def _get_supervision_level_policy(
        self, evaluation_date: date
    ) -> Optional[SupervisionLevelPolicy]:
        if self.case_type != StateSupervisionCaseType.GENERAL:
            return None

        if evaluation_date < DATE_OF_SUPERVISION_LEVEL_SWITCH:
            return SupervisionLevelPolicy(
                level_mapping=THROUGH_07_2020_US_IX_ASSESSMENT_SCORE_RANGE,
                end_date_exclusive=DATE_OF_SUPERVISION_LEVEL_SWITCH,
            )

        return SupervisionLevelPolicy(
            level_mapping=CURRENT_US_IX_ASSESSMENT_SCORE_RANGE,
            start_date=DATE_OF_SUPERVISION_LEVEL_SWITCH,
            pre_assessment_level=StateSupervisionLevel.MEDIUM,
        )

    def _awaiting_new_intake_assessment(
        self, evaluation_date: date, most_recent_assessment_date: date
    ) -> bool:
        return (
            # no assessment has been made since period start
            most_recent_assessment_date < self.start_of_supervision
            and
            # initial assessment deadline has not passed yet
            self.start_of_supervision
            + timedelta(days=self._get_initial_assessment_number_of_days())
            > evaluation_date
        )

    def _next_recommended_employment_reverification(
        self,
        compliance_evaluation_date: date,
    ) -> Optional[date]:
        """Returns when the next employment verification should be for people who have
        already had one employment verification. Returns None if compliance standards
        are unknown or no subsequent employment verifications are required.
        """

        if (
            self.case_type
            not in US_IX_SUPERVISION_EMPLOYMENT_VERIFICATION_FREQUENCY_REQUIREMENTS
        ):
            # If the case_type is not in this map then there are not recurring
            # employment verification requirements for this case type.
            return None

        frequency_by_supervision_level = (
            US_IX_SUPERVISION_EMPLOYMENT_VERIFICATION_FREQUENCY_REQUIREMENTS[
                self.case_type
            ]
        )

        if self.supervision_period.supervision_level is None:
            raise ValueError(
                "Supervision level not provided and so cannot calculate next "
                "recommended next recommended employment check."
            )

        if (
            self.supervision_period.supervision_level
            not in frequency_by_supervision_level
        ):
            # People on this supervision level and case type do not require recurring
            # employment checks.
            return None

        (
            required_contacts_per_period,
            period_days,
        ) = frequency_by_supervision_level[self.supervision_period.supervision_level]

        return self._default_next_recommended_contact_date_given_requirements(
            compliance_evaluation_date,
            required_contacts_per_period,
            period_days,
            NEW_SUPERVISION_EMPLOYMENT_VERIFICATION_DAYS,
            self.filter_for_employment_verification_contacts(),
            use_business_days=False,
        )

    def _next_recommended_employment_verification_date(
        self,
        compliance_evaluation_date: date,
        most_recent_employment_verification_date: Optional[date] = None,
    ) -> Optional[date]:
        """Returns when the next employment verification should be. Returns None if
        compliance standards are unknown or no subsequent employment verification
        are required.

        For all people on supervision, employment verification is required as soon as
        supervision starts if no verification has yet been made.
        """

        if not most_recent_employment_verification_date:
            # No employment verification has been done, so the next recommended
            # employment verification date is the beginning of the supervision term.
            return self.start_of_supervision + timedelta(
                days=NEW_SUPERVISION_EMPLOYMENT_VERIFICATION_DAYS
            )

        return self._next_recommended_employment_reverification(
            compliance_evaluation_date
        )
