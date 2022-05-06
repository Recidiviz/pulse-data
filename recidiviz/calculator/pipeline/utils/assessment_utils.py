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
from typing import List, Optional, Tuple

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.persistence.entity.state.entities import StateAssessment

DEFAULT_ASSESSMENT_SCORE_BUCKET = "NOT_ASSESSED"


def find_most_recent_applicable_assessment_of_class_for_state(
    cutoff_date: date,
    assessments: List[StateAssessment],
    assessment_class: StateAssessmentClass,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Optional[StateAssessment]:
    """Finds the assessment that happened before or on the given date and has the date closest to the given date.
    Disregards any assessments of types that are not applicable for the given `pipeline` and `state_code`, and any
    assessments without set assessment_score attributes.

    Returns the assessment."""
    assessment_types_to_include = (
        supervision_delegate.assessment_types_to_include_for_class(assessment_class)
    )

    if not assessment_types_to_include:
        return None

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
        key=lambda a: (
            # This key sorts by date and then by external id.
            # In order to get something that behaves like integer sorting
            # and to have consistent sorting between dataflow and sessions,
            # we assume longer ids always come after shorter ones.
            a.assessment_date if a and a.assessment_date else date.min,
            len(a.external_id) if a and a.external_id else 0,
            a.external_id if a else None,
        ),
        default=None,
    )


def most_recent_applicable_assessment_attributes_for_class(
    cutoff_date: date,
    assessments: List[StateAssessment],
    assessment_class: StateAssessmentClass,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Tuple[
    Optional[int], Optional[StateAssessmentLevel], Optional[StateAssessmentType]
]:
    """Returns the assessment score, assessment level, and the assessment type of the most recent relevant assessment
    for the given `pipeline` and `state_code`."""
    most_recent_assessment = find_most_recent_applicable_assessment_of_class_for_state(
        cutoff_date, assessments, assessment_class, supervision_delegate
    )

    if most_recent_assessment:
        return (
            most_recent_assessment.assessment_score,
            most_recent_assessment.assessment_level,
            most_recent_assessment.assessment_type,
        )

    return None, None, None


def assessment_score_bucket(
    assessment_type: Optional[StateAssessmentType],
    assessment_score: Optional[int],
    assessment_level: Optional[StateAssessmentLevel],
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> str:
    """Calculates the assessment score bucket that applies to measurement
    based on the assessment type. If various fields are not provided, then
    it falls to the default state of not being assessed.

    NOTE: Only LSIR and ORAS buckets are currently supported
    TODO(#2742): Add calculation support for all supported StateAssessmentTypes

    Returns:
        A string representation of the assessment score for the person.
        DEFAULT_ASSESSMENT_SCORE_BUCKET if the assessment type is not supported or if the object is missing
            assessment information."""
    if assessment_type:
        if assessment_type == StateAssessmentType.LSIR:
            lsir_bucket = supervision_delegate.set_lsir_assessment_score_bucket(
                assessment_score, assessment_level
            )
            if lsir_bucket:
                return lsir_bucket
        elif assessment_type in [
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            StateAssessmentType.ORAS_MISDEMEANOR_ASSESSMENT,
            StateAssessmentType.ORAS_MISDEMEANOR_SCREENING,
            StateAssessmentType.ORAS_PRE_TRIAL,
            StateAssessmentType.ORAS_PRISON_SCREENING,
            StateAssessmentType.ORAS_PRISON_INTAKE,
            StateAssessmentType.ORAS_REENTRY,
            StateAssessmentType.ORAS_SUPPLEMENTAL_REENTRY,
        ]:
            if assessment_level:
                return assessment_level.value
        elif assessment_type == StateAssessmentType.STRONG_R:
            if assessment_level:
                return assessment_level.value
        elif assessment_type in [
            StateAssessmentType.EXTERNAL_UNKNOWN,
            StateAssessmentType.INTERNAL_UNKNOWN,
            StateAssessmentType.CSSM,
            StateAssessmentType.HIQ,
            StateAssessmentType.J_SOAP,
            StateAssessmentType.ODARA,
            StateAssessmentType.OYAS,
            StateAssessmentType.PA_RST,
            StateAssessmentType.PSA,
            StateAssessmentType.SORAC,
            StateAssessmentType.SOTIPS,
            StateAssessmentType.SPIN_W,
            StateAssessmentType.STABLE,
            StateAssessmentType.STATIC_99,
            StateAssessmentType.TCU_DRUG_SCREEN,
        ]:
            logging.warning("Assessment type %s is unsupported.", assessment_type)
        else:
            raise ValueError(
                f"Unexpected unsupported StateAssessmentType: {assessment_type}"
            )

    return DEFAULT_ASSESSMENT_SCORE_BUCKET
