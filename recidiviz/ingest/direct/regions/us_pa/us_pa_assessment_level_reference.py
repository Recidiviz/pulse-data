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
"""Helper functions for determining assessment_level information on US_PA assessments."""
from collections import OrderedDict
import datetime

from recidiviz.common import str_field_utils
from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel
from recidiviz.common.str_field_utils import parse_int
from recidiviz.ingest.models.ingest_info import StateAssessment

_DATE_SPECIFIC_ORDERED_LSIR_LEVELS = OrderedDict([
    (datetime.date(2008, 12, 31), {
        20: StateAssessmentLevel.LOW,
        28: StateAssessmentLevel.MEDIUM,
        54: StateAssessmentLevel.HIGH
    }),
    (datetime.date(2014, 12, 3), {
        17: StateAssessmentLevel.LOW,
        26: StateAssessmentLevel.MEDIUM,
        54: StateAssessmentLevel.HIGH
    }),
    (datetime.date.max, {
        19: StateAssessmentLevel.LOW,
        27: StateAssessmentLevel.MEDIUM,
        54: StateAssessmentLevel.HIGH
    })
])


def set_date_specific_lsir_fields(assessment: StateAssessment) -> StateAssessment:
    """Over time, US_PA has updated the mapping between an LSIR score and the associated assessment level. This function
    sets the appropriate assessment_level and assessment_score according to the score and the date of the |assessment|,
    as defined by _DATE_SPECIFIC_ORDERED_LSIR_LEVELS.

    Returns the updated StateAssessment object.
    """
    if not assessment.assessment_score:
        return assessment

    assessment_score = parse_int(assessment.assessment_score)

    if assessment_score == 60:
        # This value indicates the scoring was not completed
        assessment.assessment_score = None
        assessment.assessment_level = 'UNKNOWN (60-ATTEMPTED_INCOMPLETE)'
    elif assessment_score == 70:
        # This person either refused to be assessed or did not need to be assessed because they chose not to be released
        # onto parole
        assessment.assessment_score = None
        assessment.assessment_level = 'UNKNOWN (70-REFUSED)'
    elif assessment_score > 55:
        # Assessment score number is over the max value of 54, and isn't one of the expected special-case
        # codes (60, 70, 55)
        assessment.assessment_level = f"UNKNOWN ({assessment_score}-SCORE_OUT_OF_RANGE)"
        assessment.assessment_score = None
    else:
        if assessment_score == 55:
            # This should be treated as a 54
            assessment_score = 54
            assessment.assessment_score = '54'

        assessment_date_raw = assessment.assessment_date
        assessment_date = str_field_utils.parse_date(assessment_date_raw) if assessment_date_raw else None

        if not assessment_date:
            # At this point we need a valid assessment_date to determine the date-specific LSIR level
            assessment.assessment_level = "UNKNOWN (NO_DATE)"
            return assessment
        for cutoff_date, score_level_map in _DATE_SPECIFIC_ORDERED_LSIR_LEVELS.items():
            if assessment_date <= cutoff_date:
                for cutoff_score, level in score_level_map.items():
                    if assessment_score <= cutoff_score:
                        assessment.assessment_level = level.value
                        return assessment

        raise ValueError(f"Unhandled assessment_score {assessment_score} with assessment_date {assessment_date}")

    return assessment
