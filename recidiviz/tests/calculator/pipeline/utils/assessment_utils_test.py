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
from recidiviz.persistence.entity.state.entities import StateAssessment
from recidiviz.common.constants.state.state_assessment import StateAssessmentType


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


class TestFindMostRecentAssessment(unittest.TestCase):
    """Tests the find_most_recent_assessment function."""

    def test_find_most_recent_assessment_filters_assessment_type_matches_LSIR(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17
        )

        assessments = [assessment_1, assessment_2]

        most_recent_assessment = assessment_utils.find_most_recent_assessment(date(2018, 4, 30), assessments,
                                                                              StateAssessmentType.LSIR)

        self.assertEqual(most_recent_assessment, assessment_1)

    def test_find_most_recent_assessment_filters_assessment_type_no_matches_LSIR(self):
        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17
        )

        most_recent_assessment = assessment_utils.find_most_recent_assessment(date(2018, 4, 30), [assessment],
                                                                              StateAssessmentType.LSIR)

        self.assertIsNone(most_recent_assessment)

    def test_find_most_recent_assessment_without_assessment_type_filter(self):
        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17
        )

        most_recent_assessment = assessment_utils.find_most_recent_assessment(date(2018, 4, 30), [assessment],
                                                                              None)

        self.assertEqual(most_recent_assessment, assessment)

    def test_find_most_recent_assessment_with_assessment_type_filter_no_assessment_score(self):
        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 29),
            assessment_score=None
        )

        most_recent_assessment = assessment_utils.find_most_recent_assessment(date(2018, 4, 30), [assessment],
                                                                              StateAssessmentType.LSIR)

        self.assertIsNone(most_recent_assessment)

    def test_find_most_recent_assessment_without_assessment_type_filter_no_assessment_score(self):
        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 29),
            assessment_score=None
        )

        most_recent_assessment = assessment_utils.find_most_recent_assessment(date(2018, 4, 30), [assessment],
                                                                              None)

        self.assertIsNone(most_recent_assessment)
