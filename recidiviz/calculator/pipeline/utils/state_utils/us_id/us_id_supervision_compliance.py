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
from recidiviz.calculator.pipeline.utils.assessment_utils import find_most_recent_assessment
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_contact import StateSupervisionContactType, \
    StateSupervisionContactStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType, \
    StateSupervisionLevel
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod, StateAssessment, StateSupervisionContact

NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS = 45
REASSESSMENT_DEADLINE_DAYS = 365

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
        most_recent_assessment = find_most_recent_assessment(compliance_evaluation_date, assessments)

        assessment_is_up_to_date = _assessment_is_up_to_date(supervision_period,
                                                             start_of_supervision,
                                                             compliance_evaluation_date,
                                                             most_recent_assessment)

        face_to_face_frequency_sufficient = _face_to_face_contact_frequency_is_sufficient(supervision_period,
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
    guidelines are only applicable for supervision cases of GENERAL case type with a supervision level of
    MINIMUM, MEDIUM, HIGH, or MAXIMUM and a supervision_type of DUAL, PROBATION, PAROLE, or INTERNAL_UNKNOWN
    (BW - Bench Warrant)."""
    supervision_type = supervision_period.supervision_period_supervision_type

    return (case_type == StateSupervisionCaseType.GENERAL
            and supervision_period.supervision_level in (
                StateSupervisionLevel.MINIMUM,
                StateSupervisionLevel.MEDIUM,
                StateSupervisionLevel.HIGH,
                StateSupervisionLevel.MAXIMUM
            )
            and (supervision_type in (
                StateSupervisionPeriodSupervisionType.DUAL,
                StateSupervisionPeriodSupervisionType.PAROLE,
                StateSupervisionPeriodSupervisionType.PROBATION,
            ) or (supervision_type == StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
                  and supervision_period.supervision_type_raw_text == 'BW'))
            )


def _assessment_is_up_to_date(supervision_period: StateSupervisionPeriod,
                              start_of_supervision: date,
                              compliance_evaluation_date: date,
                              most_recent_assessment: Optional[StateAssessment]) -> bool:
    """Determines whether the supervision case represented by the given supervision_period has an "up-to-date
    assessment" according to US_ID guidelines.

    For individuals on parole, they must have a new assessment taken within NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
    number of days after the start of their parole.

    For individuals on probation, they must have a new assessment taken within NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS
    number of days after the start of their probation, unless they have a recent assessment that was taken during a
    previous time on probation within the REASSESSMENT_DEADLINE_DAYS number of days.

    Once a person has had an assessment done, they need to be re-assessed every REASSESSMENT_DEADLINE_DAYS number of
    days. However, individuals on a MINIMUM supervision level do not need to be re-assessed once the initial assessment
    has been completed during the time on supervision."""
    supervision_type = supervision_period.supervision_period_supervision_type
    most_recent_assessment_date = most_recent_assessment.assessment_date if most_recent_assessment else None

    days_since_start = (compliance_evaluation_date - start_of_supervision).days

    if days_since_start <= NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS:
        # This is a recently started supervision period, and the person has not yet hit the number of days from
        # the start of their supervision at which the officer is required to have performed an assessment. This
        # assessment is up to date regardless of when the last assessment was taken.
        logging.debug("Supervision period %d started %d days before the compliance date %s. Assessment is not overdue.",
                      supervision_period.supervision_period_id, days_since_start, compliance_evaluation_date)
        return True

    if not most_recent_assessment_date:
        # They have passed the deadline for a new supervision case having an assessment. Their assessment is not
        # up to date.
        logging.debug("Supervision period %d started %d days before the compliance date %s, and they have not had an"
                      "assessment taken. Assessment is overdue.", supervision_period.supervision_period_id,
                      days_since_start, compliance_evaluation_date)
        return False

    if (supervision_type in (StateSupervisionPeriodSupervisionType.PAROLE,
                             StateSupervisionPeriodSupervisionType.DUAL)
            and most_recent_assessment_date < start_of_supervision):
        # If they have been on parole for more than NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS and they have not
        # had an assessment since the start of their parole, then their assessment is not up to date.
        logging.debug("Parole supervision period %d started %d days before the compliance date %s, and their most"
                      "recent assessment was taken before the start of this parole. Assessment is overdue.",
                      supervision_period.supervision_period_id, days_since_start, compliance_evaluation_date)
        return False

    if supervision_period.supervision_level == StateSupervisionLevel.MINIMUM:
        # People on MINIMUM supervision do not need to be re-assessed.
        logging.debug("Supervision period %d has a MINIMUM supervision level. Does not need to be re-assessed.",
                      supervision_period.supervision_period_id)
        return True

    # Their assessment is up to date if the compliance_evaluation_date is within REASSESSMENT_DEADLINE_DAYS
    # number of days since the last assessment date.
    reassessment_deadline = most_recent_assessment_date + relativedelta(days=REASSESSMENT_DEADLINE_DAYS)
    logging.debug("Last assessment was taken on %s. Re-assessment due by %s, and the compliance evaluation date is %s",
                  most_recent_assessment_date, reassessment_deadline, compliance_evaluation_date)
    return compliance_evaluation_date < reassessment_deadline


def _face_to_face_contact_frequency_is_sufficient(supervision_period: StateSupervisionPeriod,
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
        supervision_period.supervision_level, supervision_period.supervision_level_raw_text)

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


def _get_required_face_to_face_contacts_and_period_days_for_level(supervision_level: Optional[StateSupervisionLevel],
                                                                  supervision_level_raw_text: Optional[str]) -> \
        Tuple[int, int]:
    """Returns the number of face-to-face contacts that are required within time period (in days) for a supervision case
    with the given supervision level.

    There are two supervision level systems, each with different face to face contact frequency expectations. The
    deprecated level system has four levels (which are associated with four numeric levels), and the new system has
    three levels.
    """
    is_deprecated_level_system = _is_deprecated_level_system(supervision_level_raw_text)

    if is_deprecated_level_system:
        if supervision_level == StateSupervisionLevel.MINIMUM:
            return 0, sys.maxsize
        if supervision_level == StateSupervisionLevel.MEDIUM:
            return 1, 180
        if supervision_level == StateSupervisionLevel.HIGH:
            return 1, 30
        if supervision_level == StateSupervisionLevel.MAXIMUM:
            return 2, 30
    else:
        if supervision_level == StateSupervisionLevel.MINIMUM:
            return 1, 180
        if supervision_level == StateSupervisionLevel.MEDIUM:
            return 2, 90
        if supervision_level == StateSupervisionLevel.HIGH:
            return 2, 30

    raise ValueError("Standard supervision compliance guidelines not applicable for cases with a supervision level"
                     f"of {supervision_level}. Should not be calculating compliance for this supervision case.")


def _is_deprecated_level_system(supervision_level_raw_text: Optional[str]) -> bool:
    """As of July 2020, Idaho has deprecated its previous supervision level system, which used the values: LEVEL 1,
    LEVEL 2, LEVEL 3, LEVEL 4. Returns whether the level system used is one of deprecated values."""

    if not supervision_level_raw_text:
        raise ValueError("a StateSupervisionPeriod should always have a value for supervision_level_raw_text.")

    return supervision_level_raw_text in ("LEVEL 1", "LEVEL 2", "LEVEL 3", "LEVEL 4")
