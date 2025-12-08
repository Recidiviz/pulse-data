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
"""Tests the us_ix_assessment_normalization_delegate.py"""
import datetime
import unittest
from typing import List, Tuple

from parameterized import parameterized

from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_person import StateSex
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.persistence.entity.state.entities import StateAssessment, StatePerson
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
    AssessmentNormalizationManager,
)
from recidiviz.pipelines.utils.execution_utils import (
    build_staff_external_id_to_staff_id_map,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_assessment_normalization_delegate import (
    UsIxAssessmentNormalizationDelegate,
)
from recidiviz.tests.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager_test import (
    STATE_PERSON_TO_STATE_STAFF_LIST,
)

STATE_CODE = "US_IX"


class TestNormalizedAssessmentPeriodsForCalculations(unittest.TestCase):
    """Tests the US_IX-specific aspects of the normalized_assessments_and_additional_attributes
    function on the AssessmentNormalizationManager."""

    @staticmethod
    def _normalized_assessments_for_calculations(
        assessments: List[StateAssessment], person: StatePerson
    ) -> Tuple[List[StateAssessment], AdditionalAttributesMap]:
        """Helper function for testing the normalized_assessments_and_additional_attributes
        for US_PA."""
        assessments_normalization_manager = AssessmentNormalizationManager(
            assessments=assessments,
            delegate=UsIxAssessmentNormalizationDelegate(person=person),
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        return (
            assessments_normalization_manager.normalized_assessments_and_additional_attributes()
        )

    @parameterized.expand(
        [
            ("level 1", 14, "LEVEL_1"),
            ("level 2", 20, "LEVEL_2"),
            ("level 3", 26, "LEVEL_3"),
            ("level 4", 35, "LEVEL_4"),
        ]
    )
    def test_normalized_assessments_score_pre_july_2020(
        self, _name: str, score: int, bucket: str
    ) -> None:
        _, additional_attributes = self._normalized_assessments_for_calculations(
            assessments=[
                StateAssessment(
                    assessment_id=1,
                    external_id="a1",
                    state_code=STATE_CODE,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_score=score,
                    assessment_date=datetime.date(2019, 1, 1),
                )
            ],
            person=StatePerson(state_code=STATE_CODE, person_id=2, gender=None),
        )

        self.assertEqual(
            additional_attributes,
            {
                StateAssessment.__name__: {
                    1: {
                        "assessment_score_bucket": bucket,
                        "conducting_staff_id": None,
                        "sequence_num": 0,
                    }
                }
            },
        )

    @parameterized.expand(
        [
            ("low female", StateSex.FEMALE, 20, "LOW"),
            ("moderate female", StateSex.FEMALE, 25, "MODERATE"),
            ("high female", StateSex.FEMALE, 32, "HIGH"),
            ("low male", StateSex.MALE, 15, "LOW"),
            ("moderate male", StateSex.MALE, 25, "MODERATE"),
            ("high male", StateSex.MALE, 30, "HIGH"),
        ]
    )
    def test_normalized_assessments_score_bucket_post_july_2020(
        self, _name: str, sex: StateSex, score: int, bucket: str
    ) -> None:
        _, additional_attributes = self._normalized_assessments_for_calculations(
            assessments=[
                StateAssessment(
                    assessment_id=1,
                    external_id="a1",
                    state_code=STATE_CODE,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_score=score,
                    assessment_date=datetime.date(2020, 11, 1),
                )
            ],
            person=StatePerson(state_code=STATE_CODE, person_id=2, sex=sex),
        )

        self.assertEqual(
            additional_attributes,
            {
                StateAssessment.__name__: {
                    1: {
                        "assessment_score_bucket": bucket,
                        "conducting_staff_id": None,
                        "sequence_num": 0,
                    }
                }
            },
        )

    def test_normalized_assessments_score_no_gender_post_july_2020(self) -> None:
        _, additional_attributes = self._normalized_assessments_for_calculations(
            assessments=[
                StateAssessment(
                    assessment_id=1,
                    external_id="a1",
                    state_code=STATE_CODE,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_score=10,
                    assessment_date=datetime.date(2020, 11, 1),
                )
            ],
            person=StatePerson(state_code=STATE_CODE, person_id=2, gender=None),
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

    def test_normalized_assessments_score_no_score(self) -> None:
        _, additional_attributes = self._normalized_assessments_for_calculations(
            assessments=[
                StateAssessment(
                    assessment_id=1,
                    external_id="a1",
                    state_code=STATE_CODE,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_score=None,
                    assessment_date=datetime.date(2020, 11, 1),
                ),
                StateAssessment(
                    assessment_id=2,
                    external_id="a2",
                    state_code=STATE_CODE,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_score=None,
                    assessment_date=datetime.date(2017, 11, 1),
                ),
            ],
            person=StatePerson(state_code=STATE_CODE, person_id=2, sex=StateSex.FEMALE),
        )

        self.assertEqual(
            additional_attributes,
            {
                StateAssessment.__name__: {
                    1: {
                        "assessment_score_bucket": DEFAULT_ASSESSMENT_SCORE_BUCKET,
                        "conducting_staff_id": None,
                        "sequence_num": 1,
                    },
                    2: {
                        "assessment_score_bucket": DEFAULT_ASSESSMENT_SCORE_BUCKET,
                        "conducting_staff_id": None,
                        "sequence_num": 0,
                    },
                }
            },
        )

    def test_normalized_assessments_score_no_date(self) -> None:
        _, additional_attributes = self._normalized_assessments_for_calculations(
            assessments=[
                StateAssessment(
                    assessment_id=1,
                    external_id="a1",
                    state_code=STATE_CODE,
                    assessment_type=StateAssessmentType.LSIR,
                    assessment_score=10,
                    assessment_date=None,
                )
            ],
            person=StatePerson(state_code=STATE_CODE, person_id=2, sex=StateSex.FEMALE),
        )

        self.assertEqual(
            additional_attributes,
            {
                StateAssessment.__name__: {
                    1: {
                        "assessment_score_bucket": DEFAULT_ASSESSMENT_SCORE_BUCKET,
                        "conducting_staff_id": None,
                        "sequence_num": 0,
                    },
                }
            },
        )
