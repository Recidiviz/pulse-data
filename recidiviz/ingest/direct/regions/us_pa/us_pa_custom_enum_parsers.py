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
"""Custom enum parsers functions for US_PA. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_pa_custom_enum_parsers.<function name>
"""

import datetime
from collections import OrderedDict

from recidiviz.common import str_field_utils
from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel
from recidiviz.common.constants.state.state_person import StateResidencyStatus
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
)
from recidiviz.common.str_field_utils import parse_int
from recidiviz.ingest.models.ingest_info import StateAssessment


def residency_status_from_address(raw_text: str) -> StateResidencyStatus:
    normalized_address = raw_text.upper()
    no_stable_housing_indicators = ["HOMELESS", "TRANSIENT"]
    for indicator in no_stable_housing_indicators:
        if indicator in normalized_address:
            # TODO(#9301): Use the term NO_STABLE_HOUSING in the schema instead of
            #  HOMELESS / TRANSIENT.
            return StateResidencyStatus.HOMELESS

    return StateResidencyStatus.PERMANENT


def supervision_contact_location_mapper(
    raw_text: str,
) -> StateSupervisionContactLocation:
    """Maps a supervision_contact_location_raw_text to the corresponding
    StateSupervisionContactLocation, if applicable."""
    if raw_text:
        collateral_type, method = raw_text.split("†")
        if collateral_type == "Treatment Provider":
            return StateSupervisionContactLocation.TREATMENT_PROVIDER
        if collateral_type == "Employer":
            return StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT
        if collateral_type == "Court/Probation Staf":
            return StateSupervisionContactLocation.COURT
        if collateral_type == "Law Enforcement":
            return StateSupervisionContactLocation.LAW_ENFORCEMENT_AGENCY
        if method == "Field":
            return StateSupervisionContactLocation.FIELD
        if method == "Office":
            return StateSupervisionContactLocation.SUPERVISION_OFFICE
        if method == "Home":
            return StateSupervisionContactLocation.RESIDENCE
        if method == "Work":
            return StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT
    return StateSupervisionContactLocation.INTERNAL_UNKNOWN


# dictionary of specific dates for set_date_specific_lsir_fields
_DATE_SPECIFIC_ORDERED_LSIR_LEVELS = OrderedDict(
    [
        (
            datetime.date(2008, 12, 31),
            {
                20: StateAssessmentLevel.LOW,
                28: StateAssessmentLevel.MEDIUM,
                54: StateAssessmentLevel.HIGH,
            },
        ),
        (
            datetime.date(2014, 12, 3),
            {
                17: StateAssessmentLevel.LOW,
                26: StateAssessmentLevel.MEDIUM,
                54: StateAssessmentLevel.HIGH,
            },
        ),
        (
            datetime.date.max,
            {
                19: StateAssessmentLevel.LOW,
                27: StateAssessmentLevel.MEDIUM,
                54: StateAssessmentLevel.HIGH,
            },
        ),
    ]
)


def assessment_level_mapper(raw_text: str) -> StateAssessmentLevel:
    """Maps an assessment_level_raw_text code to the corresponding StateAssessmentLevel, if applicable."""
    date, desc, score = raw_text.split("††")
    state_assessment = StateAssessment(
        assessment_date=date, assessment_type=desc, assessment_score=score
    )
    output = set_date_specific_lsir_fields(state_assessment)
    level = str(output.assessment_level)
    if level.startswith("UNKNOWN") or "NONE" in score:
        return StateAssessmentLevel.EXTERNAL_UNKNOWN
    return StateAssessmentLevel(level)


def set_date_specific_lsir_fields(assessment: StateAssessment) -> StateAssessment:
    """Over time, US_PA has updated the mapping between an LSIR score and the associated assessment level. This function
    sets the appropriate assessment_level and assessment_score according to the score and the date of the |assessment|,
    as defined by _DATE_SPECIFIC_ORDERED_LSIR_LEVELS.
    Returns the updated StateAssessment object.
    """
    if not assessment.assessment_score or "NONE" in assessment.assessment_score:
        return assessment
    assessment_score = parse_int(assessment.assessment_score)
    if assessment_score == 60:
        # This value indicates the scoring was not completed
        assessment.assessment_score = None
        assessment.assessment_level = "UNKNOWN (60-ATTEMPTED_INCOMPLETE)"
    elif assessment_score == 70:
        # This person either refused to be assessed or did not need to be assessed because they chose not to be released
        # onto parole
        assessment.assessment_score = None
        assessment.assessment_level = "UNKNOWN (70-REFUSED)"
    elif assessment_score > 55:
        # Assessment score number is over the max value of 54, and isn't one of the expected special-case
        # codes (60, 70, 55)
        assessment.assessment_level = f"UNKNOWN ({assessment_score}-SCORE_OUT_OF_RANGE)"
        assessment.assessment_score = None
    else:
        if assessment_score == 55:
            # This should be treated as a 54
            assessment_score = 54
            assessment.assessment_score = "54"
        assessment_date_raw = assessment.assessment_date
        assessment_date = (
            str_field_utils.parse_date(assessment_date_raw)
            if assessment_date_raw
            else None
        )
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
        raise ValueError(
            f"Unhandled assessment_score {assessment_score} with assessment_date {assessment_date}"
        )
    return assessment
