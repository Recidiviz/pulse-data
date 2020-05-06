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

from recidiviz.calculator.pipeline.utils import assessment_utils
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType, StateAssessmentLevel
from recidiviz.persistence.entity.state.entities import StateAssessment


def test_assessment_score_bucket():
    assert assessment_utils.assessment_score_bucket(
        19, None, StateAssessmentType.LSIR) == '0-23'
    assert assessment_utils.assessment_score_bucket(
        27, None, StateAssessmentType.LSIR) == '24-29'
    assert assessment_utils.assessment_score_bucket(
        30, None, StateAssessmentType.LSIR) == '30-38'
    assert assessment_utils.assessment_score_bucket(
        39, None, StateAssessmentType.LSIR) == '39+'
    assert assessment_utils.assessment_score_bucket(
        23, StateAssessmentLevel.MEDIUM, StateAssessmentType.ORAS) == \
        StateAssessmentLevel.MEDIUM.value
    assert not assessment_utils.assessment_score_bucket(
        13, None, StateAssessmentType.PSA
    )


class TestFindAssessmentScoreChange(unittest.TestCase):
    """Tests the find_assessment_score_change function."""
    def test_find_assessment_score_change(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_score=33,
            assessment_date=date(2015, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2015, 11, 2)
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=23,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_date=date(2016, 1, 13)
        )

        assessments = [assessment_1,
                       assessment_2,
                       assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            assessment_utils.find_assessment_score_change(
                start_date,
                termination_date,
                assessments
            )

        self.assertEqual(-6, assessment_score_change)
        self.assertEqual(assessment_3.assessment_score, end_assessment_score)
        self.assertEqual(assessment_3.assessment_level, end_assessment_level)
        self.assertEqual(assessment_3.assessment_type, end_assessment_type)

    def test_find_assessment_score_change_insufficient_assessments(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2)
        )

        assessments = [assessment_1,
                       assessment_2]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            assessment_utils.find_assessment_score_change(
                start_date,
                termination_date,
                assessments
            )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_different_type(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2)
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=23,
            assessment_date=date(2016, 1, 13)
        )

        assessments = [assessment_1,
                       assessment_2,
                       assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            assessment_utils.find_assessment_score_change(
                start_date,
                termination_date,
                assessments
            )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_same_date(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2)
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=23,
            assessment_date=date(2016, 11, 2)
        )

        assessments = [assessment_1,
                       assessment_2,
                       assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            assessment_utils.find_assessment_score_change(
                start_date,
                termination_date,
                assessments
            )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_no_assessments(self):
        assessments = []

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            assessment_utils.find_assessment_score_change(
                start_date,
                termination_date,
                assessments
            )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_outside_boundary(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2011, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2)
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code='CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=23,
            assessment_date=date(2016, 1, 13)
        )

        assessments = [assessment_1,
                       assessment_2,
                       assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            assessment_utils.find_assessment_score_change(
                start_date,
                termination_date,
                assessments
            )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)


class TestIncludeAssessmentInMetric(unittest.TestCase):
    """Tests the include_assessment_in_metric function."""

    def test_include_assessment_in_metric(self):
        pipeline = 'supervision'
        state_code = 'US_MO'
        assessment_type = StateAssessmentType.ORAS_COMMUNITY_SUPERVISION

        include_assessment = assessment_utils.include_assessment_in_metric(pipeline, state_code, assessment_type)

        self.assertTrue(include_assessment)

        assessment_type = StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING

        include_assessment = assessment_utils.include_assessment_in_metric(pipeline, state_code, assessment_type)

        self.assertTrue(include_assessment)

    def test_include_assessment_in_metric_exclude_type(self):
        pipeline = 'supervision'
        state_code = 'US_MO'
        assessment_type = StateAssessmentType.ORAS_MISDEMEANOR_ASSESSMENT

        include_assessment = assessment_utils.include_assessment_in_metric(pipeline, state_code, assessment_type)

        self.assertFalse(include_assessment)

    def test_include_assessment_in_metric_unsupported_pipeline(self):
        pipeline = 'not_supported_pipeline'
        state_code = 'US_MO'
        assessment_type = StateAssessmentType.ORAS_MISDEMEANOR_ASSESSMENT

        include_assessment = assessment_utils.include_assessment_in_metric(pipeline, state_code, assessment_type)

        self.assertFalse(include_assessment)

    def test_include_assessment_in_metric_unsupported_state_code(self):
        pipeline = 'supervision'
        state_code = 'US_XX'
        assessment_type = StateAssessmentType.ORAS_MISDEMEANOR_ASSESSMENT

        include_assessment = assessment_utils.include_assessment_in_metric(pipeline, state_code, assessment_type)

        self.assertFalse(include_assessment)
