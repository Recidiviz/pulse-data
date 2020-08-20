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

from recidiviz.calculator.pipeline.utils import assessment_utils
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
