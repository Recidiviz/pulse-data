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
from datetime import date
from typing import Optional

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import SupervisionCaseCompliance
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType, \
    StateSupervisionLevel
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod, StateAssessment

NEW_SUPERVISION_ASSESSMENT_DEADLINE_DAYS = 45
REASSESSMENT_DEADLINE_DAYS = 365


def us_id_case_compliance_on_date(supervision_period: StateSupervisionPeriod,
                                  start_of_supervision: date,
                                  compliance_evaluation_date: date,
                                  most_recent_assessment: Optional[StateAssessment]) -> SupervisionCaseCompliance:
    """Determines whether the supervision case represented by the given supervision_period is in compliance with US_ID
    state standards. Measures compliance with the following standards:
        - Up-to-date Assessments
        - TODO(3304): Implement residence verification and contact compliance measures

    Args:
        supervision_period: The supervision_period representing the supervision case
        start_of_supervision: The date the person started serving this supervision
        compliance_evaluation_date: The date that the compliance of the given case is being evaluated
        most_recent_assessment: The most recent assessment taken before the compliance_evaluation_date

    Returns:
         A SupervisionCaseCompliance object containing information regarding the ways the case is or isn't in compliance
         with state standards on the given compliance_evaluation_date.
    """
    assessment_is_up_to_date = _assessment_is_up_to_date(supervision_period,
                                                         start_of_supervision,
                                                         compliance_evaluation_date,
                                                         most_recent_assessment)

    return SupervisionCaseCompliance(
        date_of_evaluation=compliance_evaluation_date,
        assessment_up_to_date=assessment_is_up_to_date
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
        # People on minimum supervision do not need to be re-assessed.
        logging.debug("Supervision period %d has a MINIMUM supervision level. Does not need to be re-assessed.",
                      supervision_period.supervision_period_id)
        return True

    # Their assessment is up to date if the compliance_evaluation_date is within REASSESSMENT_DEADLINE_DAYS
    # number of days since the last assessment date.
    reassessment_deadline = most_recent_assessment_date + relativedelta(days=REASSESSMENT_DEADLINE_DAYS)
    logging.debug("Last assessment was taken on %s. Re-assessment due by %s, and the compliance evaluation date is %s",
                  most_recent_assessment_date, reassessment_deadline, compliance_evaluation_date)
    return compliance_evaluation_date < reassessment_deadline
