# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests the functions in the us_pa_assessment_level_reference file."""
import unittest
from datetime import date

import pytest
from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel
from recidiviz.ingest.direct.regions.us_pa import us_pa_assessment_level_reference
from recidiviz.ingest.models.ingest_info import StateAssessment


class TestSetDateSpecificAssessmentLevel(unittest.TestCase):
    """Tests the set_date_specific_lsir_fields function that determines what StateAssessmentLevel a score was classified
    as at a given point in time in US_PA, and sets the assessment_level and assessment_score accordingly."""
    def test_set_date_specific_lsir_fields(self) -> None:
        assessment = StateAssessment(
            assessment_score='33',
            assessment_date='03012018'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual(StateAssessmentLevel.HIGH.value, updated_assessment.assessment_level)
        self.assertEqual('33', updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_pre_2009(self) -> None:
        assessment = StateAssessment(
            assessment_score='19',  # This was considered LOW before 2009, but since then has been MEDIUM or HIGH
            assessment_date='03012000'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual(StateAssessmentLevel.LOW.value, updated_assessment.assessment_level)
        self.assertEqual('19', updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_pre_2014_12_3(self) -> None:
        assessment = StateAssessment(
            assessment_score='18',   # This was considered MEDIUM until 2014-12-3
            assessment_date='03012010'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual(StateAssessmentLevel.MEDIUM.value, updated_assessment.assessment_level)
        self.assertEqual('18', updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_on_2014_12_3(self) -> None:
        assessment = StateAssessment(
            assessment_score='27',   # This was considered HIGH until 2014-12-3, but since then has been MEDIUM
            assessment_date='12032014'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual(StateAssessmentLevel.HIGH.value, updated_assessment.assessment_level)
        self.assertEqual('27', updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_post_2014_12_3(self) -> None:
        assessment = StateAssessment(
            assessment_score='27',   # This was considered HIGH until 2014-12-3, but since then has been MEDIUM
            assessment_date='12042014'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual(StateAssessmentLevel.MEDIUM.value, updated_assessment.assessment_level)
        self.assertEqual('27', updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_no_score(self) -> None:
        assessment = StateAssessment(
            assessment_score=None,
            assessment_date='12042014'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertIsNone(updated_assessment.assessment_level)
        self.assertIsNone(updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_54(self) -> None:
        assessment = StateAssessment(
            assessment_score='54',   # This is the maximum score
            assessment_date='12032014'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual(StateAssessmentLevel.HIGH.value, updated_assessment.assessment_level)
        self.assertEqual('54', updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_55_as_54(self) -> None:
        assessment = StateAssessment(
            assessment_score='55',   # This should be treated (and cast) as a 54
            assessment_date='12032014'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual(StateAssessmentLevel.HIGH.value, updated_assessment.assessment_level)
        self.assertEqual('54', updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_undetermined_60(self) -> None:
        assessment = StateAssessment(
            assessment_score='60',  # This score should be cleared, with the level set as ATTEMPTED_INCOMPLETE
            assessment_date='12032014'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual('UNKNOWN (60-ATTEMPTED_INCOMPLETE)', updated_assessment.assessment_level)
        self.assertIsNone(updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_undetermined_60_no_date(self) -> None:
        assessment = StateAssessment(
            assessment_score='60',  # This score should be cleared,with the level set as ATTEMPTED_INCOMPLETE
            assessment_date=None  # Date does not matter for this score
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual('UNKNOWN (60-ATTEMPTED_INCOMPLETE)', updated_assessment.assessment_level)
        self.assertIsNone(updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_non_applicable(self) -> None:
        assessment = StateAssessment(
            assessment_score='70',  # This score should be cleared, with the level set as REFUSED
            assessment_date='12032014'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual('UNKNOWN (70-REFUSED)', updated_assessment.assessment_level)
        self.assertIsNone(updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_non_applicable_no_date(self) -> None:
        assessment = StateAssessment(
            assessment_score='70',  # This score should be cleared, with the level set as REFUSED
            assessment_date=None  # Date does not matter for this score
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual('UNKNOWN (70-REFUSED)', updated_assessment.assessment_level)
        self.assertIsNone(updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_undetermined_general(self) -> None:
        assessment = StateAssessment(
            assessment_score='351',  # Scores above 54 indicate typos in the data entry
            assessment_date='03012010'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual('UNKNOWN (351-SCORE_OUT_OF_RANGE)', updated_assessment.assessment_level)
        self.assertIsNone(updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_undetermined_general_no_date(self) -> None:
        assessment = StateAssessment(
            assessment_score='351',  # Scores above 54 indicate typos in the data entry
            assessment_date=None  # Date does not matter for this score
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual('UNKNOWN (351-SCORE_OUT_OF_RANGE)', updated_assessment.assessment_level)
        self.assertIsNone(updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_date_in_future(self) -> None:
        # Should use the current classification
        assessment_future = StateAssessment(
            assessment_score='33',
            assessment_date=date.strftime(date.today() + relativedelta(days=100), '%m%d%Y')
        )

        updated_assessment_future = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment_future)

        assessment_today = StateAssessment(
            assessment_score='33',
            assessment_date=date.strftime(date.today(), '%m%d%Y')
        )

        updated_assessment_today = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment_today)

        self.assertEqual(updated_assessment_future.assessment_level, updated_assessment_today.assessment_level)
        self.assertEqual(updated_assessment_future.assessment_score, updated_assessment_today.assessment_score)

    def test_set_date_specific_lsir_fields_no_date(self) -> None:
        assessment = StateAssessment(
            assessment_score='31',
            assessment_date=None
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual('UNKNOWN (NO_DATE)', updated_assessment.assessment_level)
        self.assertEqual('31', updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_timestamp(self) -> None:
        assessment = StateAssessment(
            assessment_score='33',
            assessment_date='12/19/2016 15:21:56'
        )

        updated_assessment = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)

        self.assertEqual(StateAssessmentLevel.HIGH.value, updated_assessment.assessment_level)
        self.assertEqual('33', updated_assessment.assessment_score)

    def test_set_date_specific_lsir_fields_non_int_score(self) -> None:
        assessment = StateAssessment(
            assessment_score='X9K',  # This should throw
            assessment_date='12042014'
        )

        with pytest.raises(ValueError):
            _ = us_pa_assessment_level_reference.set_date_specific_lsir_fields(assessment)
