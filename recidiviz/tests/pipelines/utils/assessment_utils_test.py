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

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.assessment_normalization_manager import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.pipelines.utils import assessment_utils
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_supervision_delegate import (
    UsIxSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.utils.range_querier import RangeQuerier


# pylint: disable=protected-access
class TestFindMostRecentApplicableAssessment(unittest.TestCase):
    """Tests the find_most_recent_applicable_assessment_for_pipeline_and_state function."""

    class LsirOnlySupervisionDelegate(UsXxSupervisionDelegate):
        def assessment_types_to_include_for_class(
            self, _assessment_class: StateAssessmentClass
        ) -> Optional[List[StateAssessmentType]]:
            return [StateAssessmentType.LSIR]

    def test_find_most_recent_applicable_assessment_LSIR(self) -> None:
        assessment_1 = NormalizedStateAssessment(
            state_code="US_XX",
            external_id="a1",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17,
            assessment_score_bucket="0-23",
            sequence_num=0,
        )

        assessment_2 = NormalizedStateAssessment(
            state_code="US_XX",
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        assessments = [assessment_1, assessment_2]

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                StateAssessmentClass.RISK,
                self.LsirOnlySupervisionDelegate(),
            )
        )

        self.assertEqual(most_recent_assessment, assessment_1)

    def test_find_most_recent_applicable_assessment_LSIR_no_matches(self) -> None:
        assessment = NormalizedStateAssessment(
            state_code="US_XX",
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                RangeQuerier(
                    [assessment], lambda assessment: assessment.assessment_date
                ),
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
    ) -> None:
        assessment = NormalizedStateAssessment(
            state_code="US_XX",
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                RangeQuerier(
                    [assessment], lambda assessment: assessment.assessment_date
                ),
                StateAssessmentClass.RISK,
                self.NoRiskAssessmentSupervisionDelegate(),
            )
        )

        self.assertIsNone(most_recent_assessment, assessment)

    def test_find_most_recent_applicable_assessment_no_assessment_score(self) -> None:
        assessment = NormalizedStateAssessment(
            state_code="US_XX",
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_date=date(2018, 4, 29),
            assessment_score=None,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                RangeQuerier(
                    [assessment], lambda assessment: assessment.assessment_date
                ),
                StateAssessmentClass.RISK,
                UsXxSupervisionDelegate(),
            )
        )

        self.assertIsNone(most_recent_assessment)

    def test_find_most_recent_applicable_assessment_US_IX(self) -> None:
        state_code = "US_IX"

        lsir_assessment = NormalizedStateAssessment(
            state_code=state_code,
            external_id="a1",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17,
            assessment_score_bucket="0-23",
            sequence_num=0,
        )

        oras_assessment = NormalizedStateAssessment(
            state_code=state_code,
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        assessments = [lsir_assessment, oras_assessment]

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                StateAssessmentClass.RISK,
                UsIxSupervisionDelegate(),
            )
        )

        self.assertEqual(most_recent_assessment, lsir_assessment)

    def test_find_most_recent_applicable_assessment_US_ND(self) -> None:
        state_code = "US_ND"

        lsir_assessment = NormalizedStateAssessment(
            state_code=state_code,
            external_id="a1",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17,
            assessment_score_bucket="0-23",
            sequence_num=0,
        )

        oras_assessment = NormalizedStateAssessment(
            state_code=state_code,
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        assessments = [lsir_assessment, oras_assessment]

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                StateAssessmentClass.RISK,
                UsNdSupervisionDelegate(),
            )
        )

        self.assertEqual(most_recent_assessment, lsir_assessment)

    def test_find_most_recent_applicable_assessment_US_MO(self) -> None:
        state_code = "US_MO"

        lsir_assessment = NormalizedStateAssessment(
            state_code=state_code,
            external_id="a1",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17,
            assessment_score_bucket="0-23",
            sequence_num=0,
        )

        oras_assessment = NormalizedStateAssessment(
            state_code=state_code,
            external_id="a2",
            assessment_date=date(2018, 4, 29),
            assessment_score=17,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        applicable_assessment_types = [
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
        ]

        for assessment_type in applicable_assessment_types:
            oras_assessment.assessment_type = assessment_type
            most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                RangeQuerier(
                    [lsir_assessment, oras_assessment],
                    lambda assessment: assessment.assessment_date,
                ),
                StateAssessmentClass.RISK,
                UsMoSupervisionDelegate(),
            )

            self.assertEqual(most_recent_assessment, oras_assessment)

    def test_same_dates(self) -> None:
        assessment_1 = NormalizedStateAssessment(
            state_code="US_XX",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17,
            external_id="2",
            assessment_score_bucket="0-23",
            sequence_num=0,
        )

        assessment_2 = NormalizedStateAssessment(
            state_code="US_XX",
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=21,
            external_id="10",
            assessment_score_bucket="0-23",
            sequence_num=1,
        )

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                RangeQuerier(
                    [assessment_1, assessment_2],
                    lambda assessment: assessment.assessment_date,
                ),
                StateAssessmentClass.RISK,
                UsXxSupervisionDelegate(),
            )
        )

        self.assertEqual(most_recent_assessment, assessment_2)
