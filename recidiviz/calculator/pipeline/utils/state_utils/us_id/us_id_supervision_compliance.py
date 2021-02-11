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
"""State-specific utils for determining compliance with supervision standards for US_ID.
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
from datetime import date
from typing import Dict, Optional, Tuple

import numpy
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import \
    StateSupervisionCaseComplianceManager
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType, \
    StateSupervisionLevel

SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION = 45
SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE = 90
SEX_OFFENSE_LSIR_MINIMUM_SCORE = 16

NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS = 45
REASSESSMENT_DEADLINE_DAYS = 365

DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 30
DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 30
DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 180
DEPRECATED_MINIMUM_SUPERVISION_CONTACT_FREQUENCY_GENERAL_CASE = sys.maxsize

# Dictionary from case type -> supervision level -> tuple of number of times they must be contacted per time period
SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS: \
    Dict[StateSupervisionCaseType, Dict[StateSupervisionLevel, Tuple[int, int]]] = {
        StateSupervisionCaseType.GENERAL: {
            StateSupervisionLevel.MINIMUM: (1, 180),
            StateSupervisionLevel.MEDIUM: (2, 90),
            StateSupervisionLevel.HIGH: (2, 30),
        },
        StateSupervisionCaseType.SEX_OFFENSE: {
            StateSupervisionLevel.MINIMUM: (1, 90),
            StateSupervisionLevel.MEDIUM: (1, 30),
            StateSupervisionLevel.HIGH: (2, 30),
        },
    }

NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS = 3


class UsIdSupervisionCaseCompliance(StateSupervisionCaseComplianceManager):
    """US_ID specific calculations for supervision case compliance."""

    def _guidelines_applicable_for_case(self) -> bool:
        """Returns whether the standard state guidelines are applicable for the given supervision case. The standard
        guidelines are only applicable for supervision cases of type GENERAL and SEX_OFFENSE, each with corresponding
        expected supervision levels and supervision types."""
        supervision_type = self.supervision_period.supervision_period_supervision_type

        # Check case type
        if self.case_type not in (StateSupervisionCaseType.GENERAL, StateSupervisionCaseType.SEX_OFFENSE):
            return False

        # Check supervision level
        allowed_supervision_levels = [StateSupervisionLevel.MINIMUM, StateSupervisionLevel.MEDIUM,
                                      StateSupervisionLevel.HIGH]
        if self.case_type is StateSupervisionCaseType.GENERAL:
            allowed_supervision_levels.append(StateSupervisionLevel.MAXIMUM)
        if self.supervision_period.supervision_level not in allowed_supervision_levels:
            return False

        # Check supervision type
        allowed_supervision_types = [StateSupervisionPeriodSupervisionType.DUAL,
                                     StateSupervisionPeriodSupervisionType.PROBATION,
                                     StateSupervisionPeriodSupervisionType.PAROLE]
        is_bench_warrant = supervision_type == StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN and \
            self.supervision_period.supervision_type_raw_text == 'BW'
        if supervision_type not in allowed_supervision_types and not is_bench_warrant:
            return False

        return True

    def _get_initial_assessment_number_of_days(self) -> int:
        """Returns the number of days that an initial assessment should take place, given a `case_type` and
        `supervision_type`."""
        supervision_type: Optional[StateSupervisionPeriodSupervisionType] = self.supervision_period.\
            supervision_period_supervision_type
        if self.case_type == StateSupervisionCaseType.GENERAL:
            return NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
        if self.case_type == StateSupervisionCaseType.SEX_OFFENSE:
            if supervision_type == StateSupervisionPeriodSupervisionType.PROBATION:
                return SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
            if supervision_type in (StateSupervisionPeriodSupervisionType.PAROLE,
                                    StateSupervisionPeriodSupervisionType.DUAL):
                return SEX_OFFENSE_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE
            raise ValueError(f"Found unexpected supervision_type: [{supervision_type}]")

        raise ValueError(f"Found unexpected case_type: [{self.case_type}]")

    def _num_days_past_required_reassessment(self,
                                             compliance_evaluation_date: date,
                                             most_recent_assessment_date: date,
                                             most_recent_assessment_score: int) -> int:
        """Returns the number of days it has been since the required reassessment deadline. Returns 0
        if the reassessment is not overdue."""
        if self.case_type == StateSupervisionCaseType.GENERAL:
            if self.supervision_period.supervision_level == StateSupervisionLevel.MINIMUM:
                return 0
            return self._num_days_compliance_evaluation_date_past_reassessment_deadline(
                compliance_evaluation_date, most_recent_assessment_date)
        if self.case_type == StateSupervisionCaseType.SEX_OFFENSE:
            if most_recent_assessment_score > SEX_OFFENSE_LSIR_MINIMUM_SCORE:
                return self._num_days_compliance_evaluation_date_past_reassessment_deadline(
                    compliance_evaluation_date,
                    most_recent_assessment_date)
        return 0

    def _face_to_face_contact_frequency_is_sufficient(self, compliance_evaluation_date: date) -> Optional[bool]:
        """Calculates whether the frequency of face-to-face contacts between the officer and the person on supervision
        is sufficient with respect to the state standards for the level of supervision of the case.
        """
        business_days_since_start = numpy.busday_count(self.start_of_supervision, compliance_evaluation_date)

        if business_days_since_start <= NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS:
            # This is a recently started supervision period, and the person has not yet hit the number of business days
            # from the start of their supervision at which the officer is required to have been in contact with the
            # person. This face-to-face contact is up to date regardless of when the last contact was completed.
            logging.debug(
                "Supervision period %d started %d business days before the compliance date %s. Contact is not "
                "overdue.",
                self.supervision_period.supervision_period_id, business_days_since_start, compliance_evaluation_date)
            return True

        # Get applicable contacts that occurred between the start of supervision and the
        # compliance_evaluation_date (inclusive)
        applicable_contacts = self._get_applicable_face_to_face_contacts_between_dates(self.start_of_supervision,
                                                                                       compliance_evaluation_date)

        if not applicable_contacts:
            # This person has been on supervision for longer than the allowed number of days without an initial contact.
            # The face-to-face contact standard is not in compliance.
            return False

        required_contacts, period_days = self._get_required_face_to_face_contacts_and_period_days_for_level()

        days_since_start = (compliance_evaluation_date - self.start_of_supervision).days

        if days_since_start < period_days:
            # If they've had a contact since the start of their supervision, and they have been on supervision for less
            # than the number of days in which they would need another contact, then the case is in compliance
            return True

        contacts_within_period = [
            contact for contact in applicable_contacts
            if
            contact.contact_date is not None and (compliance_evaluation_date - contact.contact_date).days <
            period_days
        ]

        return len(contacts_within_period) >= required_contacts

    def _num_days_compliance_evaluation_date_past_reassessment_deadline(self,
                                                                        compliance_evaluation_date: date,
                                                                        most_recent_assessment_date: date) -> int:
        """Computes the number of days the compliance is overdue for a reassessment. Returns 0 if it is
        not overdue."""
        reassessment_deadline = most_recent_assessment_date + relativedelta(days=REASSESSMENT_DEADLINE_DAYS)
        logging.debug(
            "Last assessment was taken on %s. Re-assessment due by %s, and the compliance evaluation date is %s",
            most_recent_assessment_date, reassessment_deadline, compliance_evaluation_date)
        return max(0, (compliance_evaluation_date - reassessment_deadline).days)

    def _get_required_face_to_face_contacts_and_period_days_for_level(self) -> \
            Tuple[int, int]:
        """Returns the number of face-to-face contacts that are required within time period (in days) for a supervision
         case with the given supervision level.
        There are two supervision level systems, each with different face to face contact frequency expectations. The
        deprecated level system has four levels (which are associated with four numeric levels), and the new system has
        three levels.
        """
        supervision_level_raw_text: Optional[str] = self.supervision_period.supervision_level_raw_text
        supervision_level: Optional[StateSupervisionLevel] = self.supervision_period.supervision_level
        is_new_level_system = UsIdSupervisionCaseCompliance._is_new_level_system(supervision_level_raw_text)

        if self.case_type not in (StateSupervisionCaseType.GENERAL, StateSupervisionCaseType.SEX_OFFENSE):
            raise ValueError("Standard supervision compliance guidelines not applicable for cases with a supervision"
                             f" level of {supervision_level}. Should not be calculating compliance for this"
                             f" supervision case.")
        if supervision_level is None:
            raise ValueError(
                'Supervision level not provided and so cannot calculate required face to face contact frequency.')
        if self.case_type == StateSupervisionCaseType.GENERAL and not is_new_level_system:
            if supervision_level == StateSupervisionLevel.MINIMUM:
                return 0, DEPRECATED_MINIMUM_SUPERVISION_CONTACT_FREQUENCY_GENERAL_CASE
            if supervision_level == StateSupervisionLevel.MEDIUM:
                return 1, DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
            if supervision_level == StateSupervisionLevel.HIGH:
                return 1, DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
            if supervision_level == StateSupervisionLevel.MAXIMUM:
                return 2, DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE

        return SUPERVISION_CONTACT_FREQUENCY_REQUIREMENTS[self.case_type][supervision_level]

    @staticmethod
    def _is_new_level_system(supervision_level_raw_text: Optional[str]) -> bool:
        """As of July 2020, Idaho has deprecated its previous supervision level system and now uses `LOW`, `MODERATE`,
        and `HIGH`. Returns whether the level system used is one of new values."""

        if not supervision_level_raw_text:
            raise ValueError("a StateSupervisionPeriod should always have a value for supervision_level_raw_text.")

        return supervision_level_raw_text in ("LOW", "MODERATE", "HIGH")
