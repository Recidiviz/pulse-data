# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains US_XX implementation of the StateSpecificAssessmentNormalizationDelegate."""
import datetime
from typing import List, Optional

from more_itertools import one

from recidiviz.common.constants.state.state_person import StateGender
from recidiviz.persistence.entity.state.entities import StateAssessment, StatePerson
from recidiviz.pipelines.normalization.utils.normalization_managers.assessment_normalization_manager import (
    StateSpecificAssessmentNormalizationDelegate,
)


class UsIdAssessmentNormalizationDelegate(StateSpecificAssessmentNormalizationDelegate):
    """US_ID implementation of the StateSpecificAssessmentNormalizationDelegate."""

    def __init__(self, persons: List[StatePerson]) -> None:
        self.gender = one(persons).gender

    def set_lsir_assessment_score_bucket(
        self, assessment: StateAssessment
    ) -> Optional[str]:
        assessment_date = assessment.assessment_date
        assessment_score = assessment.assessment_score
        if assessment_date and assessment_score:
            if assessment_date < datetime.date(2020, 7, 21):
                if assessment_score <= 15:
                    return "LEVEL_1"
                if assessment_score <= 23:
                    return "LEVEL_2"
                if assessment_score <= 30:
                    return "LEVEL_3"
                return "LEVEL_4"
            if self.gender in {StateGender.MALE, StateGender.TRANS_MALE}:
                if assessment_score <= 20:
                    return "LOW"
                if assessment_score <= 28:
                    return "MODERATE"
                return "HIGH"
            if self.gender in {StateGender.FEMALE, StateGender.TRANS_FEMALE}:
                if assessment_score <= 22:
                    return "LOW"
                if assessment_score <= 30:
                    return "MODERATE"
                return "HIGH"
        return None
