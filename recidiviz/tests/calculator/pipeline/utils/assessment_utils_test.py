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
"""Tests the functions in the assessment_utils file."""
import unittest
from datetime import date
from typing import List, Optional

from parameterized import parameterized

from recidiviz.calculator.pipeline.utils import assessment_utils
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_delegate import (
    UsIdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.persistence.entity.state.entities import StateAssessment
from recidiviz.tests.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate_test import (
    UsXxSupervisionDelegate,
)


# pylint: disable=protected-access
class TestFindMostRecentApplicableAssessment(unittest.TestCase):
    """Tests the find_most_recent_applicable_assessment_for_pipeline_and_state function."""

    class LsirOnlySupervisionDelegate(UsXxSupervisionDelegate):
        def assessment_types_to_include_for_class(
            self, _assessment_class: StateAssessmentClass
        ) -> Optional[List[StateAssessmentType]]:
            return [StateAssessmentType.LSIR]

    def test_find_most_recent_applicable_assessment_LSIR(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17,
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
        )

        assessments = [assessment_1, assessment_2]

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                assessments,
                StateAssessmentClass.RISK,
                self.LsirOnlySupervisionDelegate(),
            )
        )

        self.assertEqual(most_recent_assessment, assessment_1)

    def test_find_most_recent_applicable_assessment_LSIR_no_matches(self):
        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
        )

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                [assessment],
                StateAssessmentClass.RISK,
                self.LsirOnlySupervisionDelegate(),
            )
        )

        self.assertIsNone(most_recent_assessment)

    class NoRiskAssessmentSupervisionDelegate(UsXxSupervisionDelegate):
        def assessment_types_to_include_for_class(
            self, _assessment_class: StateAssessmentClass
        ) -> Optional[List[StateAssessmentType]]:
            return None

    def test_find_most_recent_applicable_assessment_no_assessment_types_for_pipeline(
        self,
    ):
        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
        )

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                [assessment],
                StateAssessmentClass.RISK,
                self.NoRiskAssessmentSupervisionDelegate(),
            )
        )

        self.assertIsNone(most_recent_assessment, assessment)

    def test_find_most_recent_applicable_assessment_no_assessment_score(self):
        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_date=date(2018, 4, 29),
            assessment_score=None,
        )

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                [assessment],
                StateAssessmentClass.RISK,
                UsXxSupervisionDelegate(),
            )
        )

        self.assertIsNone(most_recent_assessment)

    def test_find_most_recent_applicable_assessment_US_ID(self):
        state_code = "US_ID"

        lsir_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17,
        )

        oras_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
        )

        assessments = [lsir_assessment, oras_assessment]

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                assessments,
                StateAssessmentClass.RISK,
                UsIdSupervisionDelegate(),
            )
        )

        self.assertEqual(most_recent_assessment, lsir_assessment)

    def test_find_most_recent_applicable_assessment_US_ND(self):
        state_code = "US_ND"

        lsir_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17,
        )

        oras_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
        )

        assessments = [lsir_assessment, oras_assessment]

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                assessments,
                StateAssessmentClass.RISK,
                UsNdSupervisionDelegate(),
            )
        )

        self.assertEqual(most_recent_assessment, lsir_assessment)

    def test_find_most_recent_applicable_assessment_US_MO(self):
        state_code = "US_MO"

        lsir_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17,
        )

        oras_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
        )

        applicable_assessment_types = [
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
        ]

        for assessment_type in applicable_assessment_types:
            oras_assessment.assessment_type = assessment_type
            most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                [lsir_assessment, oras_assessment],
                StateAssessmentClass.RISK,
                UsMoSupervisionDelegate(),
            )

            self.assertEqual(most_recent_assessment, oras_assessment)

    def test_same_dates(self) -> None:
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17,
            external_id="2",
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=21,
            external_id="10",
        )

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                [assessment_1, assessment_2],
                StateAssessmentClass.RISK,
                UsXxSupervisionDelegate(),
            )
        )

        self.assertEqual(most_recent_assessment, assessment_2)


class TestAssessmentScoreBucket(unittest.TestCase):
    """Tests the assessment_score_bucket function."""

    def test_assessment_score_bucket_all_assessment_types(self) -> None:
        for assessment_type in StateAssessmentType:
            _ = assessment_utils.assessment_score_bucket(
                assessment_type=assessment_type,
                assessment_score=None,
                assessment_level=None,
                supervision_delegate=UsXxSupervisionDelegate(),
            )

    @parameterized.expand(
        [
            ("low", 19, "0-23"),
            ("med", 27, "24-29"),
            ("high", 30, "30-38"),
            ("max", 39, "39+"),
        ]
    )
    def test_assessment_score_bucket_lsir(
        self, _name: str, score: int, bucket: str
    ) -> None:
        self.assertEqual(
            assessment_utils.assessment_score_bucket(
                assessment_type=StateAssessmentType.LSIR,
                assessment_score=score,
                assessment_level=None,
                supervision_delegate=UsXxSupervisionDelegate(),
            ),
            bucket,
        )

    def test_assessment_score_bucket_lsir_no_score(self) -> None:
        self.assertEqual(
            assessment_utils.assessment_score_bucket(
                assessment_type=StateAssessmentType.LSIR,
                assessment_level=None,
                assessment_score=None,
                supervision_delegate=UsXxSupervisionDelegate(),
            ),
            assessment_utils.DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )

    def test_assessment_score_bucket_oras(self) -> None:
        self.assertEqual(
            assessment_utils.assessment_score_bucket(
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                assessment_score=10,
                assessment_level=StateAssessmentLevel.MEDIUM,
                supervision_delegate=UsXxSupervisionDelegate(),
            ),
            StateAssessmentLevel.MEDIUM.value,
        )

    def test_assessment_score_bucket_oras_no_level(self) -> None:
        self.assertEqual(
            assessment_utils.assessment_score_bucket(
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                assessment_score=10,
                assessment_level=None,
                supervision_delegate=UsXxSupervisionDelegate(),
            ),
            assessment_utils.DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )

    def test_assessment_score_bucket_unsupported_assessment_type(self) -> None:
        self.assertEqual(
            assessment_utils.assessment_score_bucket(
                assessment_type=StateAssessmentType.PSA,
                assessment_score=10,
                assessment_level=None,
                supervision_delegate=UsXxSupervisionDelegate(),
            ),
            assessment_utils.DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )

    def test_assessment_score_bucket_no_type(self) -> None:
        self.assertEqual(
            assessment_utils.assessment_score_bucket(
                assessment_type=None,
                assessment_score=10,
                assessment_level=None,
                supervision_delegate=UsXxSupervisionDelegate(),
            ),
            assessment_utils.DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )
