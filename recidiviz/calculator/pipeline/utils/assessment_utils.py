# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for dealing with assessment data in the calculation pipelines."""
from datetime import date
from typing import List, Tuple, Optional, Dict

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentType,
    StateAssessmentLevel,
    StateAssessmentClass,
)
from recidiviz.persistence.entity.state.entities import StateAssessment

_ASSESSMENT_TYPES_TO_INCLUDE_FOR_CLASS: Dict[
    StateAssessmentClass, Dict[str, List[StateAssessmentType]]
] = {
    StateAssessmentClass.RISK: {
        "US_ID": [StateAssessmentType.LSIR],
        "US_MO": [
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
        ],
        "US_ND": [StateAssessmentType.LSIR],
        "US_PA": [StateAssessmentType.LSIR],
    }
}


def _assessment_types_of_class_for_state(
    assessment_class: StateAssessmentClass, state_code: str
) -> Optional[List[StateAssessmentType]]:
    """Returns the StateAssessmentType values that should be included in metric output for the given |assessment_class|
    and |state_code|."""
    types_for_class = _ASSESSMENT_TYPES_TO_INCLUDE_FOR_CLASS.get(assessment_class)

    if types_for_class:
        assessment_types_to_include = types_for_class.get(state_code)

        if not assessment_types_to_include:
            raise ValueError(
                f"Unsupported state_code {state_code} in _ASSESSMENT_TYPES_TO_INCLUDE_FOR_CLASS."
            )

        return assessment_types_to_include

    return None


def find_most_recent_applicable_assessment_of_class_for_state(
    cutoff_date: date,
    assessments: List[StateAssessment],
    assessment_class: StateAssessmentClass,
    state_code: str,
) -> Optional[StateAssessment]:
    """Finds the assessment that happened before or on the given date and has the date closest to the given date.
    Disregards any assessments of types that are not applicable for the given `pipeline` and `state_code`, and any
    assessments without set assessment_score attributes.

    Returns the assessment."""
    if assessments:
        assessment_types_to_include = _assessment_types_of_class_for_state(
            assessment_class, state_code
        )

        if assessment_types_to_include:
            applicable_assessments_before_date = [
                assessment
                for assessment in assessments
                if assessment.assessment_type in assessment_types_to_include
                and assessment.assessment_score is not None
                and assessment.assessment_date is not None
                and assessment.assessment_date <= cutoff_date
            ]

            return max(
                applicable_assessments_before_date,
                key=lambda a: a.assessment_date
                if a and a.assessment_date
                else date.min,
                default=None,
            )

    return None


def most_recent_applicable_assessment_attributes_for_class(
    cutoff_date: date,
    assessments: List[StateAssessment],
    assessment_class: StateAssessmentClass,
    state_code: str,
) -> Tuple[
    Optional[int], Optional[StateAssessmentLevel], Optional[StateAssessmentType]
]:
    """Returns the assessment score, assessment level, and the assessment type of the most recent relevant assessment
    for the given `pipeline` and `state_code`."""
    most_recent_assessment = find_most_recent_applicable_assessment_of_class_for_state(
        cutoff_date, assessments, assessment_class, state_code
    )

    if most_recent_assessment:
        return (
            most_recent_assessment.assessment_score,
            most_recent_assessment.assessment_level,
            most_recent_assessment.assessment_type,
        )

    return None, None, None
