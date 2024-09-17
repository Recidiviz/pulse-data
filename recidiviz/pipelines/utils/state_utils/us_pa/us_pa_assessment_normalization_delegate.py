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
from typing import Optional

from recidiviz.persistence.entity.state.entities import StateAssessment
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager import (
    StateSpecificAssessmentNormalizationDelegate,
)


class UsPaAssessmentNormalizationDelegate(StateSpecificAssessmentNormalizationDelegate):
    """US_PA implementation of the StateSpecificAssessmentNormalizationDelegate."""

    def set_lsir_assessment_score_bucket(
        self,
        assessment: StateAssessment,
    ) -> Optional[str]:
        """In US_PA, the score buckets for LSIR have changed over time, so in order to
        set the bucket, the assessment level is used instead."""
        assessment_level = assessment.assessment_level
        if assessment_level:
            return assessment_level.value
        return None
