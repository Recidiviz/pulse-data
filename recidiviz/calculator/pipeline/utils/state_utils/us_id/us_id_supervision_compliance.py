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
"""State-specific utils for determining compliance with supervision standards for US_ID."""
import logging
import sys
from datetime import date
from typing import Optional, List, Tuple

import numpy
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import SupervisionCaseCompliance
from recidiviz.calculator.pipeline.utils.assessment_utils import \
    find_most_recent_applicable_assessment_of_class_for_state
from recidiviz.common.constants.state.state_assessment import StateAssessmentClass
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_contact import StateSupervisionContactType, \
    StateSupervisionContactStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType, \
    StateSupervisionLevel
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod, StateAssessment, StateSupervisionContact


# TODO(#3938): rename to SEX_OFFENSE.
SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION = 45
SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE = 90
SEX_OFFENDER_LSIR_MINIMUM_SCORE = 16
MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE = 90
MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE = 30
HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE = 30

NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS = 45
REASSESSMENT_DEADLINE_DAYS = 365

DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 30
DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 30
DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 180
DEPRECATED_MINIMUM_SUPERVISION_CONTACT_FREQUENCY_GENERAL_CASE = sys.maxsize

MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 180
MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 90
HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE = 30

NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS = 3


def us_id_case_compliance_on_date(supervision_period: StateSupervisionPeriod,
                                  case_type: StateSupervisionCaseType,
                                  start_of_supervision: date,
                                  compliance_evaluation_date: date,
                                  assessments: List[StateAssessment],
                                  supervision_contacts: List[StateSupervisionContact]) -> \
        Optional[SupervisionCaseCompliance]:
    """
    Calculates several different compliance values for the supervision case represented by the supervision period, based
    on US_ID compliance standards. Measures compliance values for the following types of supervision events:
        - Assessments
        - Face-to-Face Contacts
    TODO(#3938): rename to SEX_OFFENSE here and below
    We currently measure compliance for `GENERAL` and `SEX_OFFENDER` case types. Below are the expected requirements:
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
        - For `SEX_OFFENDER` cases, there is one level system with the following mapping and expected frequencies:
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
    For each event, we calculate two types of metrics when possible.
        - The total number of events that have occurred for this person this month (until the
          |compliance_evaluation_date|).
        - Whether or not the compliance standards have been met for this event type (this is set to None if we do not
          have clear, documented guidelines applicable to this case).

    Args:
        supervision_period: The supervision_period representing the supervision case
        case_type: The "most severe" case type for the given supervision period
        start_of_supervision: The date the person started serving this supervision
        compliance_evaluation_date: The date that the compliance of the given case is being evaluated
        assessments: The risk assessments completed on this person
        supervision_contacts: The instances of contact between supervision officers and the person on supervision

    Returns:
         A SupervisionCaseCompliance object containing information regarding the ways the case is or isn't in compliance
         with state standards on the given compliance_evaluation_date.
    """
    assessment_count = _assessments_in_compliance_month(compliance_evaluation_date, assessments)
    face_to_face_count = _face_to_face_contacts_in_compliance_month(compliance_evaluation_date, supervision_contacts)

    assessment_is_up_to_date = None
    face_to_face_frequency_sufficient = None

    if _guidelines_applicable_for_case(supervision_period, case_type):
        most_recent_assessment = find_most_recent_applicable_assessment_of_class_for_state(
            compliance_evaluation_date,
            assessments,
            assessment_class=StateAssessmentClass.RISK,
            state_code=supervision_period.state_code
        )

        assessment_is_up_to_date = _assessment_is_up_to_date(case_type,
                                                             supervision_period,
                                                             start_of_supervision,
                                                             compliance_evaluation_date,
                                                             most_recent_assessment)

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(case_type,
                                                                                          supervision_period,
                                                                                          start_of_supervision,
                                                                                          compliance_evaluation_date,
                                                                                          supervision_contacts)

    return SupervisionCaseCompliance(
        date_of_evaluation=compliance_evaluation_date,
        assessment_count=assessment_count,
        assessment_up_to_date=assessment_is_up_to_date,
        face_to_face_count=face_to_face_count,
        face_to_face_frequency_sufficient=face_to_face_frequency_sufficient
    )


def _assessments_in_compliance_month(compliance_evaluation_date: date, assessments: List[StateAssessment]) -> int:
    """Returns the number of assessments that were conducted between the first of the month of the
    compliance_evaluation_date and the compliance_evaluation_date (inclusive)."""
    compliance_month = compliance_evaluation_date.month
    compliance_year = compliance_evaluation_date.year
    first_day_of_month = date(compliance_year, compliance_month, 1)

    num_assessments_this_month = [
        assessment for assessment in assessments
        if assessment.assessment_date is not None
        and first_day_of_month <= assessment.assessment_date <= compliance_evaluation_date
    ]

    return len(num_assessments_this_month)


def _face_to_face_contacts_in_compliance_month(compliance_evaluation_date: date,
                                               supervision_contacts: List[StateSupervisionContact]) -> int:
    """Returns the number of face-to-face contacts that were completed between the first of the month of the
    compliance_evaluation_date and the compliance_evaluation_date (inclusive)."""
    compliance_month = compliance_evaluation_date.month
    compliance_year = compliance_evaluation_date.year
    first_day_of_month = date(compliance_year, compliance_month, 1)

    applicable_contacts = _get_applicable_face_to_face_contacts_between_dates(
        supervision_contacts=supervision_contacts,
        lower_bound_inclusive=first_day_of_month,
        upper_bound_inclusive=compliance_evaluation_date)

    return len(applicable_contacts)


def _get_applicable_face_to_face_contacts_between_dates(supervision_contacts: List[StateSupervisionContact],
                                                        lower_bound_inclusive: date,
                                                        upper_bound_inclusive: date) -> List[StateSupervisionContact]:
    """Returns the completed contacts that can be counted as face-to-face contacts and occurred between the
    lower_bound_inclusive date and the upper_bound_inclusive date."""
    return [
        contact for contact in supervision_contacts
        # These are the types of contacts that can satisfy the face-to-face contact requirement
        if contact.contact_type in (StateSupervisionContactType.FACE_TO_FACE, StateSupervisionContactType.TELEPHONE)
        # Contact must be marked as completed
        and contact.status == StateSupervisionContactStatus.COMPLETED
        and contact.contact_date is not None
        and lower_bound_inclusive <= contact.contact_date <= upper_bound_inclusive
    ]


def _guidelines_applicable_for_case(supervision_period: StateSupervisionPeriod,
                                    case_type: StateSupervisionCaseType) -> bool:
    """Returns whether the standard state guidelines are applicable for the given supervision case. The standard
    guidelines are only applicable for supervision cases of type GENERAL and SEX_OFFENDER, each with corresponding
    expected supervision levels and supervision types."""
    supervision_type = supervision_period.supervision_period_supervision_type

    # Check case type
    if case_type not in (StateSupervisionCaseType.GENERAL, StateSupervisionCaseType.SEX_OFFENDER):
        return False

    # Check supervision level
    allowed_supervision_levels = [StateSupervisionLevel.MINIMUM, StateSupervisionLevel.MEDIUM,
                                  StateSupervisionLevel.HIGH]
    if case_type is StateSupervisionCaseType.GENERAL:
        allowed_supervision_levels.append(StateSupervisionLevel.MAXIMUM)
    if supervision_period.supervision_level not in allowed_supervision_levels:
        return False

    # Check supervision type
    allowed_supervision_types = [StateSupervisionPeriodSupervisionType.DUAL,
                                 StateSupervisionPeriodSupervisionType.PROBATION,
                                 StateSupervisionPeriodSupervisionType.PAROLE]
    is_bench_warrant = supervision_type == StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN and \
                       supervision_period.supervision_type_raw_text == 'BW'
    if supervision_type not in allowed_supervision_types and not is_bench_warrant:
        return False

    return True


def _assessment_is_up_to_date(case_type: StateSupervisionCaseType,
                              supervision_period: StateSupervisionPeriod,
                              start_of_supervision: date,
                              compliance_evaluation_date: date,
                              most_recent_assessment: Optional[StateAssessment]) -> bool:
    """Determines whether the supervision case represented by the given supervision_period has an "up-to-date
    assessment" according to US_ID guidelines."""
    supervision_type = supervision_period.supervision_period_supervision_type
    most_recent_assessment_date = most_recent_assessment.assessment_date if most_recent_assessment else None

    days_since_start = (compliance_evaluation_date - start_of_supervision).days

    initial_assessment_number_of_days = _get_initial_assessment_number_of_days(case_type, supervision_type)
    if days_since_start <= initial_assessment_number_of_days:
        # This is a recently started supervision period, and the person has not yet hit the number of days from
        # the start of their supervision at which the officer is required to have performed an assessment. This
        # assessment is up to date regardless of when the last assessment was taken.
        logging.debug("Supervision period %d started %d days before the compliance date %s. Assessment is not overdue.",
                      supervision_period.supervision_period_id, days_since_start, compliance_evaluation_date)
        return True

    if not most_recent_assessment_date:
        # If they have not had an assessment and are expected to, then their assessment is not up to date.
        logging.debug("Supervision period %d started %d days before the compliance date %s, and they have not had an"
                      "assessment taken. Assessment is overdue.", supervision_period.supervision_period_id,
                      days_since_start, compliance_evaluation_date)
        return False

    if not most_recent_assessment:
        # If they have not had an assessment and are expected to, then their assessment is not up to date.
        logging.debug("Supervision period %d started %d days before the compliance date %s, and they have not had an"
                      "assessment taken. Assessment is overdue.", supervision_period.supervision_period_id,
                      days_since_start, compliance_evaluation_date)
        return False

    return _reassessment_requirements_are_met(case_type, supervision_period.supervision_level, most_recent_assessment,
                                              compliance_evaluation_date)


def _get_initial_assessment_number_of_days(case_type: StateSupervisionCaseType,
                                           supervision_type: Optional[StateSupervisionPeriodSupervisionType]) -> int:
    """Returns the number of days that an initial assessment should take place, given a `case_type` and
    `supervision_type`."""
    if case_type == StateSupervisionCaseType.GENERAL:
        return NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
    if case_type == StateSupervisionCaseType.SEX_OFFENDER:
        if supervision_type == StateSupervisionPeriodSupervisionType.PROBATION:
            return SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PROBATION
        if supervision_type in (StateSupervisionPeriodSupervisionType.PAROLE,
                                StateSupervisionPeriodSupervisionType.DUAL):
            return SEX_OFFENDER_NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS_PAROLE
        raise ValueError(f"Found unexpected supervision_type: [{supervision_type}]")

    raise ValueError(f"Found unexpected case_type: [{case_type}]")


def _reassessment_requirements_are_met(case_type: StateSupervisionCaseType,
                                       supervision_level: Optional[StateSupervisionLevel],
                                       most_recent_assessment: StateAssessment,
                                       compliance_evaluation_date: date) -> bool:
    if case_type == StateSupervisionCaseType.GENERAL:
        if supervision_level == StateSupervisionLevel.MINIMUM:
            return True
        return _compliance_evaluation_date_before_reassessment_deadline(most_recent_assessment.assessment_date,
                                                                        compliance_evaluation_date)
    if case_type == StateSupervisionCaseType.SEX_OFFENDER:
        if not most_recent_assessment.assessment_score:
            return False
        if most_recent_assessment.assessment_score > SEX_OFFENDER_LSIR_MINIMUM_SCORE:
            return _compliance_evaluation_date_before_reassessment_deadline(most_recent_assessment.assessment_date,
                                                                            compliance_evaluation_date)
    return True


def _compliance_evaluation_date_before_reassessment_deadline(most_recent_assessment_date: Optional[date],
                                                             compliance_evaluation_date: date) -> bool:
    # Their assessment is up to date if the compliance_evaluation_date is within REASSESSMENT_DEADLINE_DAYS
    # number of days since the last assessment date.
    if most_recent_assessment_date is None:
        return False
    reassessment_deadline = most_recent_assessment_date + relativedelta(days=REASSESSMENT_DEADLINE_DAYS)
    logging.debug(
        "Last assessment was taken on %s. Re-assessment due by %s, and the compliance evaluation date is %s",
        most_recent_assessment_date, reassessment_deadline, compliance_evaluation_date)
    return compliance_evaluation_date < reassessment_deadline


def _face_to_face_contact_frequency_is_sufficient(case_type: StateSupervisionCaseType,
                                                  supervision_period: StateSupervisionPeriod,
                                                  start_of_supervision: date,
                                                  compliance_evaluation_date: date,
                                                  supervision_contacts: List[StateSupervisionContact]) -> bool:
    """Calculates whether the frequency of face-to-face contacts between the officer and the person on supervision is
    sufficient with respect to the state standards for the level of supervision of the case.
    """
    business_days_since_start = numpy.busday_count(start_of_supervision, compliance_evaluation_date)

    if business_days_since_start <= NEW_SUPERVISION_CONTACT_DEADLINE_BUSINESS_DAYS:
        # This is a recently started supervision period, and the person has not yet hit the number of business days from
        # the start of their supervision at which the officer is required to have been in contact with the person. This
        # face-to-face contact is up to date regardless of when the last contact was completed.
        logging.debug("Supervision period %d started %d business days before the compliance date %s. Contact is not "
                      "overdue.",
                      supervision_period.supervision_period_id, business_days_since_start, compliance_evaluation_date)
        return True

    # Get applicable contacts that occurred between the start of supervision and the
    # compliance_evaluation_date (inclusive)
    applicable_contacts = _get_applicable_face_to_face_contacts_between_dates(supervision_contacts,
                                                                              start_of_supervision,
                                                                              compliance_evaluation_date)

    if not applicable_contacts:
        # This person has been on supervision for longer than the allowed number of days without an initial contact.
        # The face-to-face contact standard is not in compliance.
        return False

    required_contacts, period_days = _get_required_face_to_face_contacts_and_period_days_for_level(
        case_type, supervision_period.supervision_level, supervision_period.supervision_level_raw_text)

    days_since_start = (compliance_evaluation_date - start_of_supervision).days

    if days_since_start < period_days:
        # If they've had a contact since the start of their supervision, and they have been on supervision for less than
        # the number of days in which they would need another contact, then the case is in compliance
        return True

    contacts_within_period = [
        contact for contact in applicable_contacts
        if contact.contact_date is not None and (compliance_evaluation_date - contact.contact_date).days < period_days
    ]

    return len(contacts_within_period) >= required_contacts


def _get_required_face_to_face_contacts_and_period_days_for_level(case_type: StateSupervisionCaseType,
                                                                  supervision_level: Optional[StateSupervisionLevel],
                                                                  supervision_level_raw_text: Optional[str]) -> \
        Tuple[int, int]:
    """Returns the number of face-to-face contacts that are required within time period (in days) for a supervision case
    with the given supervision level.

    There are two supervision level systems, each with different face to face contact frequency expectations. The
    deprecated level system has four levels (which are associated with four numeric levels), and the new system has
    three levels.
    """
    is_new_level_system = _is_new_level_system(supervision_level_raw_text)

    if case_type == StateSupervisionCaseType.GENERAL:
        if is_new_level_system:
            if supervision_level == StateSupervisionLevel.MINIMUM:
                return 1, MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
            if supervision_level == StateSupervisionLevel.MEDIUM:
                return 2, MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
            if supervision_level == StateSupervisionLevel.HIGH:
                return 2, HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
        else:
            if supervision_level == StateSupervisionLevel.MINIMUM:
                return 0, DEPRECATED_MINIMUM_SUPERVISION_CONTACT_FREQUENCY_GENERAL_CASE
            if supervision_level == StateSupervisionLevel.MEDIUM:
                return 1, DEPRECATED_MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
            if supervision_level == StateSupervisionLevel.HIGH:
                return 1, DEPRECATED_HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
            if supervision_level == StateSupervisionLevel.MAXIMUM:
                return 2, DEPRECATED_MAXIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_GENERAL_CASE
    elif case_type == StateSupervisionCaseType.SEX_OFFENDER:
        if supervision_level == StateSupervisionLevel.MINIMUM:
            return 1, MINIMUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE
        if supervision_level == StateSupervisionLevel.MEDIUM:
            return 1, MEDIUM_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE
        if supervision_level == StateSupervisionLevel.HIGH:
            return 2, HIGH_SUPERVISION_CONTACT_FREQUENCY_DAYS_SEX_OFFENDER_CASE

    raise ValueError("Standard supervision compliance guidelines not applicable for cases with a supervision level"
                     f"of {supervision_level}. Should not be calculating compliance for this supervision case.")


def _is_new_level_system(supervision_level_raw_text: Optional[str]) -> bool:
    """As of July 2020, Idaho has deprecated its previous supervision level system and now uses `LOW`, `MODERATE`,
    and `HIGH`. Returns whether the level system used is one of new values."""

    if not supervision_level_raw_text:
        raise ValueError("a StateSupervisionPeriod should always have a value for supervision_level_raw_text.")

    return supervision_level_raw_text in ("LOW", "MODERATE", "HIGH")
