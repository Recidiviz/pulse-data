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
import sys
from datetime import date
from typing import List, Optional

from recidiviz.common.constants.state.state_assessment import StateAssessmentClass
from recidiviz.pipelines.normalization.utils.normalized_entities import (
    NormalizedStateAssessment,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)


def find_most_recent_applicable_assessment_of_class_for_state(
    cutoff_date: date,
    assessments: List[NormalizedStateAssessment],
    assessment_class: StateAssessmentClass,
    supervision_delegate: StateSpecificSupervisionDelegate,
) -> Optional[NormalizedStateAssessment]:
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
        key=lambda a: (a.sequence_num if a and a.sequence_num else -sys.maxsize),
        default=None,
    )
