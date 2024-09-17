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
"""Tests for assessments_normalization_manager.py"""
import datetime
import unittest
from typing import List, Optional, Tuple

from parameterized import parameterized

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.persistence.entity.state.entities import StateAssessment
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
    AssessmentNormalizationManager,
)
from recidiviz.pipelines.utils.execution_utils import (
    build_staff_external_id_to_staff_id_map,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_assessment_normalization_delegate import (
    UsXxAssessmentNormalizationDelegate,
)
from recidiviz.tests.pipelines.normalization.utils.entity_normalization_manager_utils_test import (
    STATE_PERSON_TO_STATE_STAFF_LIST,
)


class TestPrepareAssessmentsForCalculations(unittest.TestCase):
    """State-agnostic tests for pre-processing that happens to all assessments regarldess
    of state (sorting, setting additional attributes)."""

    def setUp(self) -> None:
        self.state_code = "US_XX"
        self.delegate = UsXxAssessmentNormalizationDelegate()

    def _normalized_assessments_for_calculations(
        self, assessments: List[StateAssessment]
    ) -> Tuple[List[StateAssessment], AdditionalAttributesMap]:
        entity_normalization_manager = AssessmentNormalizationManager(
            assessments=assessments,
            delegate=self.delegate,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            processed_assessments,
            additional_attributes,
        ) = (
            entity_normalization_manager.normalized_assessments_and_additional_attributes()
        )

        return (
            processed_assessments,
            additional_attributes,
        )

    def test_sort_assessments(self) -> None:
        assessment_1 = StateAssessment.new_with_defaults(
            state_code=self.state_code,
            external_id="a1",
            assessment_date=datetime.date(2000, 1, 1),
            assessment_id=1,
        )
        assessment_2 = StateAssessment.new_with_defaults(
            state_code=self.state_code,
            external_id="a2",
            assessment_date=datetime.date(2001, 1, 1),
            assessment_id=2,
        )
        assessment_3 = StateAssessment.new_with_defaults(
            state_code=self.state_code, external_id="1", assessment_id=3
        )
        assessment_4 = StateAssessment.new_with_defaults(
            state_code=self.state_code, external_id="10", assessment_id=4
        )
        assessment_5 = StateAssessment.new_with_defaults(
            state_code=self.state_code, external_id="16", assessment_id=5
        )

        assessments = [
            assessment_1,
            assessment_2,
            assessment_3,
            assessment_4,
            assessment_5,
        ]

        normalized_assessments, _ = self._normalized_assessments_for_calculations(
            assessments
        )
        self.assertEqual(
            # External Id length and then sorting, and then date
            [assessment_3, assessment_4, assessment_5, assessment_1, assessment_2],
            normalized_assessments,
        )

    def test_default_sorted_assessments_with_additional_attributes(self) -> None:
        assessment_1 = StateAssessment.new_with_defaults(
            state_code=self.state_code,
            external_id="external_id",
            assessment_date=datetime.date(2000, 1, 1),
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=10,
            assessment_id=1,
            conducting_staff_external_id="EMP3",
            conducting_staff_external_id_type="US_XX_STAFF_ID",
        )
        assessment_2 = StateAssessment.new_with_defaults(
            state_code=self.state_code,
            external_id="external_id2",
            assessment_date=datetime.date(2000, 1, 1),
            assessment_type=StateAssessmentType.LSIR,
            assessment_id=2,
            conducting_staff_external_id="EMP3",
            conducting_staff_external_id_type="US_XX_STAFF_ID",
        )

        assessments = [assessment_1, assessment_2]

        _, additional_attributes = self._normalized_assessments_for_calculations(
            assessments=assessments
        )

        expected_additional_attributes = {
            StateAssessment.__name__: {
                1: {
                    "assessment_score_bucket": "0-23",
                    "conducting_staff_id": 30000,
                    "sequence_num": 0,
                },
                2: {
                    "assessment_score_bucket": "NOT_ASSESSED",
                    "conducting_staff_id": 30000,
                    "sequence_num": 1,
                },
            }
        }

        self.assertEqual(expected_additional_attributes, additional_attributes)

    @parameterized.expand(
        [
            ("low", 19, "0-23"),
            ("med", 27, "24-29"),
            ("high", 30, "30-38"),
            ("max", 39, "39+"),
        ]
    )
    def test_default_assessments_with_score_buckets_lsir(
        self, _name: str, score: int, bucket: str
    ) -> None:
        _, additional_attributes = self._normalized_assessments_for_calculations(
            assessments=[
                StateAssessment(
                    state_code=self.state_code,
                    external_id="external_id",
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_score=score,
                    assessment_id=1,
                )
            ]
        )

        expected_additional_attributes = {
            StateAssessment.__name__: {
                1: {
                    "assessment_score_bucket": bucket,
                    "conducting_staff_id": None,
                    "sequence_num": 0,
                }
            }
        }
        self.assertEqual(expected_additional_attributes, additional_attributes)

    @parameterized.expand(
        [
            (
                "oras",
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                StateAssessmentLevel.MEDIUM,
                StateAssessmentLevel.MEDIUM.value,
            ),
            (
                "oras_no_level",
                StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                None,
                DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            (
                "strong_r",
                StateAssessmentType.STRONG_R,
                StateAssessmentLevel.HIGH,
                StateAssessmentLevel.HIGH.value,
            ),
            (
                "unsupported",
                StateAssessmentType.CSSM,
                None,
                DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            ("no_type", None, None, DEFAULT_ASSESSMENT_SCORE_BUCKET),
        ]
    )
    def test_default_assessments_with_score_buckets_other_types(
        self,
        _name: str,
        assessment_type: Optional[StateAssessmentType],
        assessment_level: Optional[StateAssessmentLevel],
        bucket: str,
    ) -> None:
        _, additional_attributes = self._normalized_assessments_for_calculations(
            assessments=[
                StateAssessment(
                    state_code=self.state_code,
                    external_id="external_id",
                    assessment_type=assessment_type,
                    assessment_level=assessment_level,
                    assessment_id=1,
                    conducting_staff_external_id="EMP3",
                    conducting_staff_external_id_type="US_XX_STAFF_ID",
                )
            ]
        )

        expected_additional_attributes = {
            StateAssessment.__name__: {
                1: {
                    "assessment_score_bucket": bucket,
                    "conducting_staff_id": 30000,
                    "sequence_num": 0,
                }
            }
        }
        self.assertEqual(expected_additional_attributes, additional_attributes)

    # test that all assessment type enums are handled by the normalization manager
    def test_assessment_types(self) -> None:
        assessments = [
            StateAssessment.new_with_defaults(
                state_code=self.state_code,
                external_id="external_id",
                assessment_date=datetime.date(2000, 1, 1),
                assessment_id=i,
                assessment_type=a,
            )
            for i, a in enumerate(StateAssessmentType, 1)
        ]

        normalized_assessments, _ = self._normalized_assessments_for_calculations(
            assessments
        )

        self.assertEqual(
            assessments,
            normalized_assessments,
        )
