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
"""Tests the us_pa_assessment_normalization_delegate.py"""
import unittest
from typing import List, Tuple

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.assessment_normalization_manager import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
    AssessmentNormalizationManager,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_assessment_normalization_delegate import (
    UsPaAssessmentNormalizationDelegate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.persistence.entity.state.entities import StateAssessment

STATE_CODE = "US_PA"


class TestNormalizedAssessmentPeriodsForCalculations(unittest.TestCase):
    """Tests the US_PA-specific aspects of the normalized_assessments_and_additional_attributes
    function on the AssessmentNormalizationManager."""

    @staticmethod
    def _normalized_assessments_for_calculations(
        assessments: List[StateAssessment],
    ) -> Tuple[List[StateAssessment], AdditionalAttributesMap]:
        """Helper function for testing the normalized_assessments_and_additional_attributes
        for US_PA."""
        assessments_normalization_manager = AssessmentNormalizationManager(
            assessments=assessments,
            delegate=UsPaAssessmentNormalizationDelegate(),
        )

        return (
            assessments_normalization_manager.normalized_assessments_and_additional_attributes()
        )

    def test_normalized_assessments_score_bucket_level(self) -> None:
        _, additional_attributes = self._normalized_assessments_for_calculations(
            assessments=[
                StateAssessment(
                    assessment_id=1,
                    state_code=STATE_CODE,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_level=StateAssessmentLevel.LOW,
                )
            ]
        )

        self.assertEqual(
            additional_attributes,
            {
                StateAssessment.__name__: {
                    1: {
                        "assessment_score_bucket": StateAssessmentLevel.LOW.value,
                        "conducting_staff_id": None,
                        "sequence_num": 0,
                    }
                }
            },
        )

    def test_normalized_assessments_score_bucket_no_level(self) -> None:
        _, additional_attributes = self._normalized_assessments_for_calculations(
            assessments=[
                StateAssessment(
                    assessment_id=1,
                    state_code=STATE_CODE,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_level=None,
                )
            ]
        )

        self.assertEqual(
            additional_attributes,
            {
                StateAssessment.__name__: {
                    1: {
                        "assessment_score_bucket": DEFAULT_ASSESSMENT_SCORE_BUCKET,
                        "conducting_staff_id": None,
                        "sequence_num": 0,
                    }
                }
            },
        )
