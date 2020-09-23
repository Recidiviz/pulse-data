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
from unittest import mock

from recidiviz.calculator.pipeline.utils import assessment_utils
from recidiviz.persistence.entity.state.entities import StateAssessment
from recidiviz.common.constants.state.state_assessment import StateAssessmentType, StateAssessmentClass


# pylint: disable=protected-access
class TestFindMostRecentApplicableAssessment(unittest.TestCase):
    """Tests the find_most_recent_applicable_assessment_for_pipeline_and_state function."""

    @mock.patch('recidiviz.calculator.pipeline.utils.assessment_utils._assessment_types_of_class_for_state')
    def test_find_most_recent_applicable_assessment_LSIR(self, mock_assessment_types):
        mock_assessment_types.return_value = [StateAssessmentType.LSIR]

        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17
        )

        assessments = [assessment_1, assessment_2]

        most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
            date(2018, 4, 30),
            assessments,
            StateAssessmentClass.RISK,
            'US_XX'
        )

        self.assertEqual(most_recent_assessment, assessment_1)

    @mock.patch('recidiviz.calculator.pipeline.utils.assessment_utils._assessment_types_of_class_for_state')
    def test_find_most_recent_applicable_assessment_LSIR_no_matches(self, mock_assessment_types):
        mock_assessment_types.return_value = [StateAssessmentType.LSIR]
        assessment = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17
        )

        most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
            date(2018, 4, 30),
            [assessment],
            StateAssessmentClass.RISK,
            'US_XX')

        self.assertIsNone(most_recent_assessment)

    @mock.patch('recidiviz.calculator.pipeline.utils.assessment_utils._assessment_types_of_class_for_state')
    def test_find_most_recent_applicable_assessment_no_assessment_types_for_pipeline(self, mock_assessment_types):
        mock_assessment_types.return_value = None

        assessment = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17
        )

        most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
            date(2018, 4, 30),
            [assessment],
            StateAssessmentClass.RISK,
            'US_XX')

        self.assertIsNone(most_recent_assessment, assessment)

    @mock.patch('recidiviz.calculator.pipeline.utils.assessment_utils._assessment_types_of_class_for_state')
    def test_find_most_recent_applicable_assessment_no_assessment_score(
            self, mock_assessment_types):
        mock_assessment_types.return_value = [StateAssessmentType.ORAS_COMMUNITY_SUPERVISION]

        assessment = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_date=date(2018, 4, 29),
            assessment_score=None
        )

        most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
            date(2018, 4, 30),
            [assessment],
            StateAssessmentClass.RISK,
            'US_XX')

        self.assertIsNone(most_recent_assessment)

    def test_find_most_recent_applicable_assessment_US_ID(self):
        state_code = 'US_ID'

        lsir_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17
        )

        oras_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17
        )

        assessments = [lsir_assessment, oras_assessment]

        for pipeline in assessment_utils._ASSESSMENT_TYPES_TO_INCLUDE_FOR_CLASS:
            most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                assessments,
                pipeline,
                state_code
            )

            self.assertEqual(most_recent_assessment, lsir_assessment)

    def test_find_most_recent_applicable_assessment_US_ND(self):
        state_code = 'US_ND'

        lsir_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17
        )

        oras_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
            assessment_date=date(2018, 4, 29),
            assessment_score=17
        )

        assessments = [lsir_assessment, oras_assessment]

        for pipeline in assessment_utils._ASSESSMENT_TYPES_TO_INCLUDE_FOR_CLASS:
            most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                date(2018, 4, 30),
                assessments,
                pipeline,
                state_code
            )

            self.assertEqual(most_recent_assessment, lsir_assessment)

    def test_find_most_recent_applicable_assessment_US_MO(self):
        state_code = 'US_MO'

        lsir_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.LSIR,
            assessment_date=date(2018, 4, 28),
            assessment_score=17
        )

        oras_assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_date=date(2018, 4, 29),
            assessment_score=17
        )

        assessments = [lsir_assessment, oras_assessment]

        applicable_assessment_types = [StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                                       StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING]

        for assessment_type in applicable_assessment_types:
            oras_assessment.assessment_type = assessment_type

            for pipeline in assessment_utils._ASSESSMENT_TYPES_TO_INCLUDE_FOR_CLASS:
                most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                    date(2018, 4, 30),
                    assessments,
                    pipeline,
                    state_code
                )

                self.assertEqual(most_recent_assessment, oras_assessment)
