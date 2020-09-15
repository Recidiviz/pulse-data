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
import logging
from datetime import date
from typing import List, Tuple, Optional

from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType, StateAssessmentLevel
from recidiviz.persistence.entity.state.entities import StateAssessment

ASSESSMENT_TYPES_TO_INCLUDE = {
    'program': {
        'US_ID': [StateAssessmentType.LSIR],
        'US_MO': [
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING
        ],
        'US_ND': [StateAssessmentType.LSIR],
    },
    'supervision': {
        'US_ID': [StateAssessmentType.LSIR],
        'US_MO': [
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING
        ],
        'US_ND': [StateAssessmentType.LSIR],
    },
}


def find_most_recent_assessment(cutoff_date: date, assessments: List[StateAssessment],
                                assessment_type: Optional[StateAssessmentType]) -> Optional[StateAssessment]:
    """Finds the assessment (of type `assessment_type`, if applicable) that happened before or on the given date and has
     the date closest to the given date. Returns the assessment."""
    if assessments:
        filtered_assessments = assessments
        if assessment_type:
            filtered_assessments = [assessment for assessment in assessments if
                                    assessment.assessment_type == assessment_type]

        # Only pick assessments that have a score associated with them.
        filtered_assessments = [assessment for assessment in filtered_assessments if
                                assessment.assessment_score is not None]

        assessments_before_date = [
            assessment for assessment in filtered_assessments
            if assessment.assessment_date is not None
            and assessment.assessment_date <= cutoff_date
        ]

        if assessments_before_date:
            assessments_before_date.sort(
                key=lambda b: b.assessment_date)

            most_recent_assessment = assessments_before_date[-1]
            return most_recent_assessment

    return None


def most_recent_assessment_attributes(cutoff_date: date, assessments: List[StateAssessment]) -> \
        Tuple[Optional[int], Optional[StateAssessmentLevel], Optional[StateAssessmentType]]:
    """Finds the assessment that happened before or on the given date and has the date closest to the given date.
    Returns the assessment score, assessment level, and the assessment type."""
    most_recent_assessment = find_most_recent_assessment(cutoff_date, assessments, None)

    if most_recent_assessment:
        return most_recent_assessment.assessment_score, \
               most_recent_assessment.assessment_level, \
               most_recent_assessment.assessment_type

    return None, None, None


def include_assessment_in_metric(pipeline: str, state_code: str, assessment_type: StateAssessmentType) -> bool:
    """Returns whether assessment data from the assessment with the given StateAssessmentType should be included in
    the results for metrics in the given pipeline with the given state_code."""
    values_for_pipeline = ASSESSMENT_TYPES_TO_INCLUDE.get(pipeline)

    if values_for_pipeline:
        assessment_types_to_include = values_for_pipeline.get(state_code)

        if assessment_types_to_include:
            return assessment_type is not None and assessment_type in assessment_types_to_include

        logging.warning("Unsupported state_code: %s for pipeline type: %s."
                        " Update ASSESSMENT_TYPES_TO_INCLUDE.", state_code, pipeline)

    logging.warning("Unsupported pipeline type: %s for state_code: %s. Update ASSESSMENT_TYPES_TO_INCLUDE.",
                    pipeline, state_code)
    return False
