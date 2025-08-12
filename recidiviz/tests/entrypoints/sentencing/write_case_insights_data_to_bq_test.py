# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for write_case_insights_data_to_bq.py"""
import datetime
import json
import unittest
from unittest.mock import Mock, call, patch

import freezegun
import pandas as pd
from dateutil.tz import tzlocal

from recidiviz.entrypoints.sentencing import write_case_insights_data_to_bq


class TestWriteCaseInsightsDataToBQ(unittest.TestCase):
    """Tests for writing case insights data to BQ."""

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_get_gendered_assessment_score_bucket_range(self) -> None:
        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 0})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(0, score_bucket_start)
        self.assertEqual(22, score_bucket_end)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 2})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(0, score_bucket_start)
        self.assertEqual(22, score_bucket_end)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 22})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(0, score_bucket_start)
        self.assertEqual(22, score_bucket_end)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 23})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(23, score_bucket_start)
        self.assertEqual(30, score_bucket_end)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 30})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(23, score_bucket_start)
        self.assertEqual(30, score_bucket_end)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 31})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(31, score_bucket_start)
        self.assertEqual(-1, score_bucket_end)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 35})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(31, score_bucket_start)
        self.assertEqual(-1, score_bucket_end)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 0})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(0, score_bucket_start)
        self.assertEqual(20, score_bucket_end)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 2})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(0, score_bucket_start)
        self.assertEqual(20, score_bucket_end)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 20})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(0, score_bucket_start)
        self.assertEqual(20, score_bucket_end)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 21})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(21, score_bucket_start)
        self.assertEqual(28, score_bucket_end)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 28})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(21, score_bucket_start)
        self.assertEqual(28, score_bucket_end)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 29})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(29, score_bucket_start)
        self.assertEqual(-1, score_bucket_end)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 35})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(29, score_bucket_start)
        self.assertEqual(-1, score_bucket_end)

        input_row = pd.Series({"gender": "MALE", "assessment_score": None})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(-1, score_bucket_start)
        self.assertEqual(-1, score_bucket_end)

        input_row = pd.Series({"gender": "MALE", "assessment_score": -1})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(-1, score_bucket_start)
        self.assertEqual(-1, score_bucket_end)

        input_row = pd.Series({"gender": "MALE", "assessment_score": -5})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(-1, score_bucket_start)
        self.assertEqual(-1, score_bucket_end)

        input_row = pd.Series({"gender": "EXTERNAL_UNKNOWN", "assessment_score": 1})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(0, score_bucket_start)
        self.assertEqual(-1, score_bucket_end)

        input_row = pd.Series({"gender": "TRANS_FEMALE", "assessment_score": 1})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(0, score_bucket_start)
        self.assertEqual(-1, score_bucket_end)

        input_row = pd.Series({"gender": "TRANS_MALE", "assessment_score": 1})
        (
            score_bucket_start,
            score_bucket_end,
        ) = write_case_insights_data_to_bq.get_gendered_assessment_score_bucket_range(
            input_row
        )
        self.assertEqual(0, score_bucket_start)
        self.assertEqual(-1, score_bucket_end)

    def test_adjust_any_is_sex_offense(self) -> None:
        # any_is_sex_offense should override to True for three categories
        input_row = pd.Series(
            {
                "any_is_sex_offense": True,
                "most_severe_ncic_category_uniform": "General Crimes",
                "most_severe_description": "Assault",
            }
        )
        any_is_sex_offense = write_case_insights_data_to_bq.adjust_any_is_sex_offense(
            input_row
        )
        self.assertEqual(True, any_is_sex_offense)

        input_row = pd.Series(
            {
                "any_is_sex_offense": False,
                "most_severe_ncic_category_uniform": "General Crimes",
                "most_severe_description": "Assault",
            }
        )
        any_is_sex_offense = write_case_insights_data_to_bq.adjust_any_is_sex_offense(
            input_row
        )
        self.assertEqual(False, any_is_sex_offense)

        input_row = pd.Series(
            {
                "any_is_sex_offense": False,
                "most_severe_ncic_category_uniform": "Sexual Assault",
                "most_severe_description": "Assault",
            }
        )
        any_is_sex_offense = write_case_insights_data_to_bq.adjust_any_is_sex_offense(
            input_row
        )
        self.assertEqual(True, any_is_sex_offense)

        input_row = pd.Series(
            {
                "any_is_sex_offense": False,
                "most_severe_ncic_category_uniform": "Sexual Assualt",
                "most_severe_description": "Assault",
            }
        )
        any_is_sex_offense = write_case_insights_data_to_bq.adjust_any_is_sex_offense(
            input_row
        )
        self.assertEqual(True, any_is_sex_offense)

        input_row = pd.Series(
            {
                "any_is_sex_offense": False,
                "most_severe_ncic_category_uniform": "Sex Offense",
                "most_severe_description": "Assault",
            }
        )
        any_is_sex_offense = write_case_insights_data_to_bq.adjust_any_is_sex_offense(
            input_row
        )
        self.assertEqual(True, any_is_sex_offense)

        # any_is_sex_offense should override to True if most_severe_description contains "sexual"
        input_row = pd.Series(
            {
                "any_is_sex_offense": False,
                "most_severe_ncic_category_uniform": "General Crimes",
                "most_severe_description": "Assault",
            }
        )
        any_is_sex_offense = write_case_insights_data_to_bq.adjust_any_is_sex_offense(
            input_row
        )
        self.assertEqual(False, any_is_sex_offense)

        input_row = pd.Series(
            {
                "any_is_sex_offense": False,
                "most_severe_ncic_category_uniform": "General Crimes",
                "most_severe_description": "Sexual Assault",
            }
        )
        any_is_sex_offense = write_case_insights_data_to_bq.adjust_any_is_sex_offense(
            input_row
        )
        self.assertEqual(True, any_is_sex_offense)

    def test_adjust_ncic_category(self) -> None:
        input_row = pd.Series(
            {
                "most_severe_ncic_category_uniform": "General Crimes",
                "most_severe_description": "ASSAULT",
            }
        )
        ncic_category = write_case_insights_data_to_bq.adjust_ncic_category(input_row)
        self.assertEqual("General Crimes", ncic_category)

        input_row = pd.Series(
            {
                "most_severe_ncic_category_uniform": "Bribery",
                "most_severe_description": "CHILD SEXUAL ABUSE OF A MINOR UNDER 16 YEARS OF AGE",
            }
        )
        ncic_category = write_case_insights_data_to_bq.adjust_ncic_category(input_row)
        self.assertEqual("Sexual Assault", ncic_category)

        input_row = pd.Series(
            {
                "most_severe_ncic_category_uniform": "Bribery",
                "most_severe_description": "CHILDREN-SEXUAL BATTERY OF MINOR CHILD 16 TO 17 YEARS OF AGE",
            }
        )
        ncic_category = write_case_insights_data_to_bq.adjust_ncic_category(input_row)
        self.assertEqual("Sexual Assault", ncic_category)

        input_row = pd.Series(
            {
                "most_severe_ncic_category_uniform": "Bribery",
                "most_severe_description": "SEXUAL CONTACT WITH AN ADULT INMATE OR JUVENILE OFFENDER",
            }
        )
        ncic_category = write_case_insights_data_to_bq.adjust_ncic_category(input_row)
        self.assertEqual("Sexual Assault", ncic_category)

        input_row = pd.Series(
            {
                "most_severe_ncic_category_uniform": "Bribery",
                "most_severe_description": "CHILD SEXUALLY EXPLOITATIVE MATERIAL-KNOWINGLY DISTRIBUTES BY ANY MEANS",
            }
        )
        ncic_category = write_case_insights_data_to_bq.adjust_ncic_category(input_row)
        self.assertEqual("Commercial Sex", ncic_category)

        input_row = pd.Series(
            {
                "most_severe_ncic_category_uniform": "Bribery",
                "most_severe_description": "CHILDREN-SEXUAL EXPLOITATION OF A CHILD",
            }
        )
        ncic_category = write_case_insights_data_to_bq.adjust_ncic_category(input_row)
        self.assertEqual("Commercial Sex", ncic_category)

        input_row = pd.Series(
            {
                "most_severe_ncic_category_uniform": "Sexual Assualt",
                "most_severe_description": "SEXUAL IMPOSITION",
            }
        )
        ncic_category = write_case_insights_data_to_bq.adjust_ncic_category(input_row)
        self.assertEqual("Sexual Assault", ncic_category)

    def test_get_combined_offense_category(self) -> None:
        input_row = pd.Series(
            {
                "any_is_violent": True,
                "any_is_drug": True,
                "any_is_sex_offense": True,
            }
        )
        combined_offense_category = (
            write_case_insights_data_to_bq.get_combined_offense_category(input_row)
        )
        self.assertEqual(
            "Violent offense, Drug offense, Sex offense", combined_offense_category
        )

        input_row = pd.Series(
            {
                "any_is_violent": True,
                "any_is_drug": False,
                "any_is_sex_offense": True,
            }
        )
        combined_offense_category = (
            write_case_insights_data_to_bq.get_combined_offense_category(input_row)
        )
        self.assertEqual("Violent offense, Sex offense", combined_offense_category)

        input_row = pd.Series(
            {
                "any_is_violent": False,
                "any_is_drug": True,
                "any_is_sex_offense": True,
            }
        )
        combined_offense_category = (
            write_case_insights_data_to_bq.get_combined_offense_category(input_row)
        )
        self.assertEqual("Drug offense, Sex offense", combined_offense_category)

        input_row = pd.Series(
            {
                "any_is_violent": True,
                "any_is_drug": True,
                "any_is_sex_offense": False,
            }
        )
        combined_offense_category = (
            write_case_insights_data_to_bq.get_combined_offense_category(input_row)
        )
        self.assertEqual("Violent offense, Drug offense", combined_offense_category)

        input_row = pd.Series(
            {
                "any_is_violent": True,
                "any_is_drug": False,
                "any_is_sex_offense": False,
            }
        )
        combined_offense_category = (
            write_case_insights_data_to_bq.get_combined_offense_category(input_row)
        )
        self.assertEqual("Violent offense", combined_offense_category)

        input_row = pd.Series(
            {
                "any_is_violent": False,
                "any_is_drug": True,
                "any_is_sex_offense": False,
            }
        )
        combined_offense_category = (
            write_case_insights_data_to_bq.get_combined_offense_category(input_row)
        )
        self.assertEqual("Drug offense", combined_offense_category)

        input_row = pd.Series(
            {
                "any_is_violent": False,
                "any_is_drug": False,
                "any_is_sex_offense": True,
            }
        )
        combined_offense_category = (
            write_case_insights_data_to_bq.get_combined_offense_category(input_row)
        )
        self.assertEqual("Sex offense", combined_offense_category)

        input_row = pd.Series(
            {
                "any_is_violent": False,
                "any_is_drug": False,
                "any_is_sex_offense": False,
            }
        )
        combined_offense_category = (
            write_case_insights_data_to_bq.get_combined_offense_category(input_row)
        )
        self.assertEqual(
            "Non-drug, Non-violent, Non-sex offense", combined_offense_category
        )

    def test_get_disposition_df(self) -> None:
        cohort_df = pd.DataFrame(
            {
                "state_code": [
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                ],
                "person_id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "gender": [
                    "FEMALE",
                    "FEMALE",
                    "MALE",
                    "MALE",
                    "FEMALE",
                    "FEMALE",
                    "FEMALE",
                    "MALE",
                    "MALE",
                ],
                "assessment_score_bucket_start": [0, 0, 29, 29, 0, 0, 0, 29, 29],
                "assessment_score_bucket_end": [22, 22, -1, -1, 22, 22, 22, -1, -1],
                "most_severe_description": [
                    "DUI DRIVING",
                    "DUI DRIVING",
                    "DUI DRIVING",
                    "DUI DRIVING",
                    "BURGLARY",
                    "BURGLARY",
                    "BURGLARY",
                    "BURGLARY",
                    "BURGLARY",
                ],
                "cohort_group": [
                    "PROBATION",
                    "RIDER",
                    "RIDER",
                    "TERM",
                    "PROBATION",
                    "RIDER",
                    "TERM",
                    "PROBATION",
                    "PROBATION",
                ],
            }
        )

        expected_disposition_df = pd.DataFrame(
            {
                "state_code": ["US_IX", "US_IX", "US_IX", "US_IX"],
                "gender": [
                    "FEMALE",
                    "MALE",
                    "FEMALE",
                    "MALE",
                ],
                "assessment_score_bucket_start": [0, 29, 0, 29],
                "assessment_score_bucket_end": [22, -1, 22, -1],
                "most_severe_description": [
                    "DUI DRIVING",
                    "DUI DRIVING",
                    "BURGLARY",
                    "BURGLARY",
                ],
                "disposition_probation_pc": [1.0 / 2, 0.0, 1.0 / 3, 1.0],
                "disposition_rider_pc": [1.0 / 2, 1.0 / 2, 1.0 / 3, 0.0],
                "disposition_term_pc": [0.0, 1.0 / 2, 1.0 / 3, 0.0],
                "disposition_num_records": [2, 2, 3, 2],
            }
        )

        disposition_df = write_case_insights_data_to_bq.get_disposition_df(
            cohort_df, "US_IX"
        )
        pd.testing.assert_frame_equal(
            expected_disposition_df.sort_values(
                [
                    "state_code",
                    "gender",
                    "assessment_score_bucket_start",
                    "assessment_score_bucket_end",
                    "most_severe_description",
                ]
            ).reset_index(drop=True),
            disposition_df.sort_values(
                [
                    "state_code",
                    "gender",
                    "assessment_score_bucket_start",
                    "assessment_score_bucket_end",
                    "most_severe_description",
                ]
            ).reset_index(drop=True),
        )

    def test_add_placeholder_cohort_rows(self) -> None:
        cohort_df = pd.DataFrame(
            {
                "state_code": ["US_IX"],
                "person_id": [1],
                "gender": ["FEMALE"],
                "assessment_score": [5],
                "cohort_group": ["TERM"],
                "cohort_start_date": pd.to_datetime("2024-01-01").date(),
                "most_severe_description": ["ASSAULT OR BATTERY"],
                "most_severe_ncic_category_uniform": ["Assault"],
                "any_is_violent": [True],
                "any_is_drug": [False],
                "any_is_sex_offense": [False],
            }
        )
        cohort_df = write_case_insights_data_to_bq.add_placeholder_cohort_rows(
            cohort_df
        )

        expected_cohort_df = pd.DataFrame(
            {
                "state_code": [
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                ],
                "person_id": [1, -1, -2, -3, -4, -5, -6, -7, -8],
                "gender": [
                    "FEMALE",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                ],
                "assessment_score": [5, -1, -1, -1, -1, -1, -1, -1, -1],
                "cohort_group": [
                    "TERM",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                ],
                "cohort_start_date": pd.to_datetime(
                    [
                        "2024-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                    ]
                ).date,
                "most_severe_description": [
                    "ASSAULT OR BATTERY",
                    "[PLACEHOLDER] True, True, True",
                    "[PLACEHOLDER] True, True, False",
                    "[PLACEHOLDER] True, False, True",
                    "[PLACEHOLDER] True, False, False",
                    "[PLACEHOLDER] False, True, True",
                    "[PLACEHOLDER] False, True, False",
                    "[PLACEHOLDER] False, False, True",
                    "[PLACEHOLDER] False, False, False",
                ],
                "most_severe_ncic_category_uniform": [
                    "Assault",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                ],
                "any_is_violent": [
                    True,
                    True,
                    True,
                    True,
                    True,
                    False,
                    False,
                    False,
                    False,
                ],
                "any_is_drug": [
                    False,
                    True,
                    True,
                    False,
                    False,
                    True,
                    True,
                    False,
                    False,
                ],
                "any_is_sex_offense": [
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                ],
            }
        )

        pd.testing.assert_frame_equal(expected_cohort_df, cohort_df)

        # With more than one state
        cohort_df = pd.DataFrame(
            {
                "state_code": ["US_IX", "US_IX", "US_ND"],
                "person_id": [1, 2, 3],
                "gender": ["FEMALE", "MALE", "FEMALE"],
                "assessment_score": [5, 6, 7],
                "cohort_group": ["TERM", "RIDER", "TERM"],
                "cohort_start_date": [
                    pd.to_datetime("2024-01-01").date(),
                    pd.to_datetime("2024-01-02").date(),
                    pd.to_datetime("2024-01-03").date(),
                ],
                "most_severe_description": [
                    "ASSAULT OR BATTERY",
                    "SEXUAL ASSAULT",
                    "ASSAULT OR BATTERY",
                ],
                "most_severe_ncic_category_uniform": [
                    "Assault",
                    "Sex Offense",
                    "Assault",
                ],
                "any_is_violent": [True, True, True],
                "any_is_drug": [False, False, False],
                "any_is_sex_offense": [False, True, False],
            }
        )
        cohort_df = write_case_insights_data_to_bq.add_placeholder_cohort_rows(
            cohort_df
        )

        expected_cohort_df = pd.DataFrame(
            {
                "state_code": [
                    "US_IX",
                    "US_IX",
                    "US_ND",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_ND",
                    "US_ND",
                    "US_ND",
                    "US_ND",
                    "US_ND",
                    "US_ND",
                    "US_ND",
                    "US_ND",
                ],
                "person_id": [
                    1,
                    2,
                    3,
                    -1,
                    -2,
                    -3,
                    -4,
                    -5,
                    -6,
                    -7,
                    -8,
                    -9,
                    -10,
                    -11,
                    -12,
                    -13,
                    -14,
                    -15,
                    -16,
                ],
                "gender": [
                    "FEMALE",
                    "MALE",
                    "FEMALE",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                ],
                "assessment_score": [
                    5,
                    6,
                    7,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                    -1,
                ],
                "cohort_group": [
                    "TERM",
                    "RIDER",
                    "TERM",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                    "PROBATION",
                ],
                "cohort_start_date": pd.to_datetime(
                    [
                        "2024-01-01",
                        "2024-01-02",
                        "2024-01-03",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                        "1970-01-01",
                    ]
                ).date,
                "most_severe_description": [
                    "ASSAULT OR BATTERY",
                    "SEXUAL ASSAULT",
                    "ASSAULT OR BATTERY",
                    "[PLACEHOLDER] True, True, True",
                    "[PLACEHOLDER] True, True, False",
                    "[PLACEHOLDER] True, False, True",
                    "[PLACEHOLDER] True, False, False",
                    "[PLACEHOLDER] False, True, True",
                    "[PLACEHOLDER] False, True, False",
                    "[PLACEHOLDER] False, False, True",
                    "[PLACEHOLDER] False, False, False",
                    "[PLACEHOLDER] True, True, True",
                    "[PLACEHOLDER] True, True, False",
                    "[PLACEHOLDER] True, False, True",
                    "[PLACEHOLDER] True, False, False",
                    "[PLACEHOLDER] False, True, True",
                    "[PLACEHOLDER] False, True, False",
                    "[PLACEHOLDER] False, False, True",
                    "[PLACEHOLDER] False, False, False",
                ],
                "most_severe_ncic_category_uniform": [
                    "Assault",
                    "Sex Offense",
                    "Assault",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                    "[PLACEHOLDER] NCIC CATEGORY",
                ],
                "any_is_violent": [
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    False,
                    False,
                    False,
                    False,
                    True,
                    True,
                    True,
                    True,
                    False,
                    False,
                    False,
                    False,
                ],
                "any_is_drug": [
                    False,
                    False,
                    False,
                    True,
                    True,
                    False,
                    False,
                    True,
                    True,
                    False,
                    False,
                    True,
                    True,
                    False,
                    False,
                    True,
                    True,
                    False,
                    False,
                ],
                "any_is_sex_offense": [
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                ],
            }
        )

        pd.testing.assert_frame_equal(expected_cohort_df, cohort_df)

    def test_add_missing_offenses(self) -> None:
        cohort_df = pd.DataFrame(
            {
                "state_code": ["US_IX"],
                "person_id": [1],
                "gender": ["FEMALE"],
                "assessment_score": [5],
                "cohort_group": ["TERM"],
                "cohort_start_date": pd.to_datetime("2024-01-01").date(),
                "most_severe_description": ["ASSAULT OR BATTERY"],
                "most_severe_ncic_category_uniform": ["Assault"],
                "any_is_violent": [True],
                "any_is_drug": [False],
                "any_is_sex_offense": [False],
            }
        )
        charge_df = pd.DataFrame(
            {
                "state_code": ["US_IX", "US_IX"],
                "charge": ["NEW_CHARGE1", "NEW_CHARGE2"],
                "is_violent": [False, False],
                "is_drug": [True, False],
                "is_sex_offense": [False, True],
                "ncic_category_uniform": ["CATEGORY1", "CATEGORY2"],
                "frequency": [3, 4],
                "mandatory_minimums": [None, None],
            }
        )
        cohort_df = write_case_insights_data_to_bq.add_missing_offenses(
            cohort_df, charge_df
        )

        expected_cohort_df = pd.DataFrame(
            {
                "state_code": [
                    "US_IX",
                    "US_IX",
                    "US_IX",
                ],
                "person_id": [1, -10001, -10002],
                "gender": [
                    "FEMALE",
                    "INTERNAL_UNKNOWN",
                    "INTERNAL_UNKNOWN",
                ],
                "assessment_score": [5, -1, -1],
                "cohort_group": [
                    "TERM",
                    "PROBATION",
                    "PROBATION",
                ],
                "cohort_start_date": pd.to_datetime(
                    [
                        "2024-01-01",
                        "1970-01-01",
                        "1970-01-01",
                    ]
                ).date,
                "most_severe_description": [
                    "ASSAULT OR BATTERY",
                    "NEW_CHARGE1",
                    "NEW_CHARGE2",
                ],
                "most_severe_ncic_category_uniform": [
                    "Assault",
                    "CATEGORY1",
                    "CATEGORY2",
                ],
                "any_is_violent": [
                    True,
                    False,
                    False,
                ],
                "any_is_drug": [
                    False,
                    True,
                    False,
                ],
                "any_is_sex_offense": [
                    False,
                    False,
                    True,
                ],
            }
        )

        pd.testing.assert_frame_equal(expected_cohort_df, cohort_df)

    def test_add_all_combinations(self) -> None:
        disposition_df = pd.DataFrame(
            {
                "state_code": ["US_IX", "US_IX", "US_IX", "US_IX"],
                "gender": [
                    "FEMALE",
                    "FEMALE",
                    "FEMALE",
                    "MALE",
                ],
                "assessment_score_bucket_start": [0, 23, 0, 29],
                "assessment_score_bucket_end": [22, 30, 22, -1],
                "most_severe_description": [
                    "DUI DRIVING",
                    "DUI DRIVING",
                    "BURGLARY",
                    "BURGLARY",
                ],
                "disposition_probation_pc": [0.1, 0.2, 0.3, 0.4],
                "disposition_rider_pc": [0.5, 0.6, 0.7, 0.8],
                "disposition_term_pc": [0.0, 0.1, 0.2, 0.3],
                "disposition_num_records": [100, 50, 100, 50],
            }
        )

        expected_all_combinations_df = pd.DataFrame(
            {
                "state_code": ["US_IX", "US_IX", "US_IX", "US_IX", "US_IX", "US_IX"],
                "gender": [
                    "FEMALE",
                    "FEMALE",
                    "FEMALE",
                    "MALE",
                    "FEMALE",
                    "MALE",
                ],
                "assessment_score_bucket_start": [0, 23, 0, 29, 23, 29],
                "assessment_score_bucket_end": [22, 30, 22, -1, 30, -1],
                "most_severe_description": [
                    "DUI DRIVING",
                    "DUI DRIVING",
                    "BURGLARY",
                    "BURGLARY",
                    "BURGLARY",
                    "DUI DRIVING",
                ],
                "disposition_probation_pc": pd.Series([0.1, 0.2, 0.3, 0.4, 0, 0]),
                "disposition_rider_pc": pd.Series([0.5, 0.6, 0.7, 0.8, 0, 0]),
                "disposition_term_pc": pd.Series([0.0, 0.1, 0.2, 0.3, 0, 0]),
                "disposition_num_records": pd.Series([100, 50, 100, 50, 0, 0]),
            }
        )

        all_combinations_df = write_case_insights_data_to_bq.add_all_combinations(
            disposition_df, "US_IX"
        )
        pd.testing.assert_frame_equal(
            expected_all_combinations_df.sort_values(
                [
                    "state_code",
                    "gender",
                    "assessment_score_bucket_start",
                    "assessment_score_bucket_end",
                    "most_severe_description",
                ]
            ).reset_index(drop=True),
            all_combinations_df.sort_values(
                [
                    "state_code",
                    "gender",
                    "assessment_score_bucket_start",
                    "assessment_score_bucket_end",
                    "most_severe_description",
                ]
            ).reset_index(drop=True),
        )

    @patch(
        "recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.add_attributes_to_index"
    )
    @patch(
        "recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.get_recidivism_series_df_for_rollup_level"
    )
    def test_get_all_rollup_aggregated_df(
        self,
        mock_get_recidivism_series_df_for_rollup_level: Mock,
        mock_add_attributes_to_index: Mock,
    ) -> None:
        mock_rollup_attributes = {
            "US_IX": [
                ["state_code", "most_severe_description"],
                ["state_code", "most_severe_ncic_category_uniform"],
            ]
        }
        recidivism_df = pd.DataFrame(
            {
                "state_code": ["US_IX", "US_IX"],
                "most_severe_description": [
                    "ASSAULT OR BATTERY",
                    "MISUSE OF PUBLIC MONEY",
                ],
                "most_severe_ncic_category_uniform": ["Assault", "Bribery"],
            }
        )
        index_df = pd.DataFrame(
            data={
                "state_code": ["US_IX", "US_IX"],
                "most_severe_description": [
                    "ASSAULT OR BATTERY",
                    "MISUSE OF PUBLIC MONEY",
                ],
                "most_severe_ncic_category_uniform": ["Assault", "Fraud"],
            }
        )
        returned_recidivism_series_dfs = [
            pd.DataFrame(
                index=pd.MultiIndex.from_arrays(
                    [
                        ["US_IX", "US_IX"],
                        ["ASSAULT OR BATTERY", "MISUSE OF PUBLIC MONEY"],
                    ],
                    names=[
                        "state_code",
                        "most_severe_description",
                    ],
                ),
                data=[
                    [
                        "fake_event_rate_dict_str1",
                        "fake_event_rate_dict_str2",
                        15,
                        None,
                        0.2,
                        None,
                    ],
                    [
                        "fake_event_rate_dict_str3",
                        "fake_event_rate_dict_str4",
                        10,
                        20,
                        0.5,
                        0.1,
                    ],
                ],
                columns=pd.MultiIndex.from_product(
                    [
                        ["event_rate_dict", "cohort_size", "final_ci_size"],
                        ["PROBATION", "TERM"],
                    ],
                    names=["metric", "cohort_group"],
                ),
            ),
            pd.DataFrame(
                index=pd.MultiIndex.from_arrays(
                    [
                        ["US_IX", "US_IX"],
                        ["ASSAULT OR BATTERY", "MISUSE OF PUBLIC MONEY"],
                    ],
                    names=[
                        "state_code",
                        "most_severe_description",
                    ],
                ),
                data=[
                    [
                        "fake_event_rate_dict_str5",
                        "fake_event_rate_dict_str6",
                        20,
                        10,
                        0.1,
                        0.2,
                    ],
                    [
                        "fake_event_rate_dict_str7",
                        "fake_event_rate_dict_str8",
                        25,
                        None,
                        0.4,
                        None,
                    ],
                ],
                columns=pd.MultiIndex.from_product(
                    [
                        ["event_rate_dict", "cohort_size", "final_ci_size"],
                        ["PROBATION", "TERM"],
                    ],
                    names=["metric", "cohort_group"],
                ),
            ),
        ]
        # These are the same as returned_recidivism_series_dfs but with indices added to the column names
        recidivism_series_dfs_with_levels = [
            pd.DataFrame(
                index=pd.MultiIndex.from_arrays(
                    [
                        ["ASSAULT OR BATTERY", "MISUSE OF PUBLIC MONEY"],
                        ["US_IX", "US_IX"],
                    ],
                    names=[
                        "most_severe_description",
                        "state_code",
                    ],
                ),
                data=[
                    [
                        "fake_event_rate_dict_str1",
                        "fake_event_rate_dict_str2",
                        15,
                        None,
                        0.2,
                        None,
                    ],
                    [
                        "fake_event_rate_dict_str3",
                        "fake_event_rate_dict_str4",
                        10,
                        20,
                        0.5,
                        0.1,
                    ],
                ],
                columns=pd.MultiIndex.from_product(
                    [
                        ["cohort_size", "event_rate_dict", "final_ci_size"],
                        ["PROBATION", "TERM"],
                        [0],
                    ],
                    names=["metric", "cohort_group", "rollup_level"],
                ),
            ),
            pd.DataFrame(
                index=pd.MultiIndex.from_arrays(
                    [
                        ["ASSAULT OR BATTERY", "MISUSE OF PUBLIC MONEY"],
                        ["US_IX", "US_IX"],
                    ],
                    names=[
                        "most_severe_description",
                        "state_code",
                    ],
                ),
                data=[
                    [
                        "fake_event_rate_dict_str5",
                        "fake_event_rate_dict_str6",
                        20,
                        10,
                        0.1,
                        0.2,
                    ],
                    [
                        "fake_event_rate_dict_str7",
                        "fake_event_rate_dict_str8",
                        25,
                        None,
                        0.4,
                        None,
                    ],
                ],
                columns=pd.MultiIndex.from_product(
                    [
                        ["cohort_size", "event_rate_dict", "final_ci_size"],
                        ["PROBATION", "TERM"],
                        [1],
                    ],
                    names=["metric", "cohort_group", "rollup_level"],
                ),
            ),
        ]
        # These are the same as the first recidivism_series_dfs_with_levels but with NCIC categories added to the index
        returned_add_attributes_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    ["US_IX", "US_IX"],
                    ["ASSAULT OR BATTERY", "MISUSE OF PUBLIC MONEY"],
                    ["Assault", "Fraud"],
                ],
                names=[
                    "state_code",
                    "most_severe_description",
                    "most_severe_ncic_category_uniform",
                ],
            ),
            data=[
                [
                    "fake_event_rate_dict_str1",
                    "fake_event_rate_dict_str2",
                    15,
                    None,
                    0.2,
                    None,
                ],
                [
                    "fake_event_rate_dict_str3",
                    "fake_event_rate_dict_str4",
                    10,
                    20,
                    0.5,
                    0.1,
                ],
            ],
            columns=pd.MultiIndex.from_product(
                [
                    ["cohort_size", "event_rate_dict", "final_ci_size"],
                    ["PROBATION", "TERM"],
                    [0],
                ],
                names=["metric", "cohort_group", "rollup_level"],
            ),
        )
        mock_get_recidivism_series_df_for_rollup_level.side_effect = (
            returned_recidivism_series_dfs
        )
        mock_add_attributes_to_index.return_value = returned_add_attributes_df

        with patch(
            "recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.ROLLUP_ATTRIBUTES",
            mock_rollup_attributes,
        ):
            all_rollup_levels_df = (
                write_case_insights_data_to_bq.get_all_rollup_aggregated_df(
                    recidivism_df, [0, 3], index_df, "US_IX"
                )
            )

        expected_all_rollup_levels_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    ["US_IX", "US_IX"],
                    ["ASSAULT OR BATTERY", "MISUSE OF PUBLIC MONEY"],
                    ["Assault", "Fraud"],
                ],
                names=[
                    "state_code",
                    "most_severe_description",
                    "most_severe_ncic_category_uniform",
                ],
            ),
            data=[
                [
                    "fake_event_rate_dict_str1",
                    "fake_event_rate_dict_str2",
                    15,
                    None,
                    0.2,
                    None,
                    "fake_event_rate_dict_str5",
                    "fake_event_rate_dict_str6",
                    20,
                    10,
                    0.1,
                    0.2,
                ],
                [
                    "fake_event_rate_dict_str3",
                    "fake_event_rate_dict_str4",
                    10,
                    20,
                    0.5,
                    0.1,
                    "fake_event_rate_dict_str7",
                    "fake_event_rate_dict_str8",
                    25,
                    None,
                    0.4,
                    None,
                ],
            ],
            columns=pd.MultiIndex.from_tuples(
                [
                    ("cohort_size", "PROBATION", 0),
                    ("cohort_size", "TERM", 0),
                    ("event_rate_dict", "PROBATION", 0),
                    ("event_rate_dict", "TERM", 0),
                    ("final_ci_size", "PROBATION", 0),
                    ("final_ci_size", "TERM", 0),
                    ("cohort_size", "PROBATION", 1),
                    ("cohort_size", "TERM", 1),
                    ("event_rate_dict", "PROBATION", 1),
                    ("event_rate_dict", "TERM", 1),
                    ("final_ci_size", "PROBATION", 1),
                    ("final_ci_size", "TERM", 1),
                ],
                names=["metric", "cohort_group", "rollup_level"],
            ),
        )
        mock_get_recidivism_series_df_for_rollup_level.assert_has_calls(
            [
                call(recidivism_df, [0, 3], ["state_code", "most_severe_description"]),
                call(
                    recidivism_df,
                    [0, 3],
                    ["state_code", "most_severe_ncic_category_uniform"],
                ),
            ]
        )
        # Use assert_frame_equal for add_attributes_to_index args because assert_called_with doesn't handle DataFrames
        mock_add_attributes_to_index.assert_called_once()
        pd.testing.assert_frame_equal(
            recidivism_series_dfs_with_levels[0],
            mock_add_attributes_to_index.call_args[1]["target_df"],
        )
        pd.testing.assert_frame_equal(
            recidivism_df, mock_add_attributes_to_index.call_args[1]["reference_df"]
        )

        pd.testing.assert_frame_equal(
            expected_all_rollup_levels_df, all_rollup_levels_df
        )

    def test_add_attributes_to_index(self) -> None:
        reference_df = pd.DataFrame(
            {
                "state_code": [
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                    "US_IX",
                ],
                "gender": [
                    "FEMALE",
                    "FEMALE",
                    "FEMALE",
                    "FEMALE",
                    "FEMALE",
                    "MALE",
                    "MALE",
                    "MALE",
                ],
                "assessment_score_bucket_start": [0, 0, 0, 0, 0, 0, 0, 0],
                "assessment_score_bucket_end": [20, 20, 20, 20, 20, 20, 20, 20],
                "most_severe_description": [
                    "DUI DRIVING",
                    "DUI DRIVING",
                    "FORGERY",
                    "INSURANCE FRAUD",
                    "ELUDING A POLICE OFFICER IN A MOTOR VEHICLE",
                    "FORGERY",
                    "INSURANCE FRAUD",
                    "DUI DRIVING",
                ],
                "most_severe_ncic_category_uniform": [
                    "Traffic Offenses",
                    "Traffic Offenses",
                    "Fraud",
                    "Fraud",
                    "Traffic Offenses",
                    "Fraud",
                    "Fraud",
                    "Traffic Offenses",
                ],
                "combined_offense_category": [
                    "Non-drug, non-violent, non-sex offense",
                    "Non-drug, non-violent, non-sex offense",
                    "Non-drug, non-violent, non-sex offense",
                    "Non-drug, non-violent, non-sex offense",
                    "Non-drug, non-violent, non-sex offense",
                    "Non-drug, non-violent, non-sex offense",
                    "Non-drug, non-violent, non-sex offense",
                    "Non-drug, non-violent, non-sex offense",
                ],
                "any_is_violent": [
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                ],
            }
        )
        target_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    ["US_IX", "US_IX", "US_IX", "US_IX", "US_IX", "US_IX"],
                    ["FEMALE", "FEMALE", "FEMALE", "FEMALE", "MALE", "MALE"],
                    [0, 0, 0, 0, 0, 0],
                    [20, 20, 20, 20, 20, 20],
                    [
                        "ELUDING A POLICE OFFICER IN A MOTOR VEHICLE",
                        "DUI DRIVING",
                        "FORGERY",
                        "INSURANCE FRAUD",
                        "FORGERY",
                        "DUI DRIVING",
                    ],
                ],
                names=[
                    "state_code",
                    "gender",
                    "assessment_score_bucket_start",
                    "assessment_score_bucket_end",
                    "most_severe_description",
                ],
            ),
            data=[5, 6, 7, 8, 9, 10],
            columns=pd.MultiIndex.from_arrays(
                [["cohort_size"], ["PROBATION"], [0]],
                names=["metric", "cohort_group", "rollup_level"],
            ),
        )
        attribute_mapping = {
            "most_severe_description": [
                "most_severe_ncic_category_uniform",
                "combined_offense_category",
                "any_is_violent",
            ]
        }
        added_attributes_df = write_case_insights_data_to_bq.add_attributes_to_index(
            target_df, reference_df, attribute_mapping
        )
        expected_added_attributes_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    ["US_IX", "US_IX", "US_IX", "US_IX", "US_IX", "US_IX"],
                    ["FEMALE", "FEMALE", "FEMALE", "FEMALE", "MALE", "MALE"],
                    [0, 0, 0, 0, 0, 0],
                    [20, 20, 20, 20, 20, 20],
                    [
                        "ELUDING A POLICE OFFICER IN A MOTOR VEHICLE",
                        "DUI DRIVING",
                        "FORGERY",
                        "INSURANCE FRAUD",
                        "FORGERY",
                        "DUI DRIVING",
                    ],
                    [
                        "Traffic Offenses",
                        "Traffic Offenses",
                        "Fraud",
                        "Fraud",
                        "Fraud",
                        "Traffic Offenses",
                    ],
                    [
                        "Non-drug, non-violent, non-sex offense",
                        "Non-drug, non-violent, non-sex offense",
                        "Non-drug, non-violent, non-sex offense",
                        "Non-drug, non-violent, non-sex offense",
                        "Non-drug, non-violent, non-sex offense",
                        "Non-drug, non-violent, non-sex offense",
                    ],
                    [False, False, False, False, False, False],
                ],
                names=[
                    "state_code",
                    "gender",
                    "assessment_score_bucket_start",
                    "assessment_score_bucket_end",
                    "most_severe_description",
                    "most_severe_ncic_category_uniform",
                    "combined_offense_category",
                    "any_is_violent",
                ],
            ),
            data=[5, 6, 7, 8, 9, 10],
            columns=pd.MultiIndex.from_product(
                [["cohort_size"], ["PROBATION"], [0]],
                names=["metric", "cohort_group", "rollup_level"],
            ),
        )

        pd.testing.assert_frame_equal(expected_added_attributes_df, added_attributes_df)

        # An exception should be raised if an attribute in the mapping isn't in the first level of ROLLUP_ATTRIBUTES
        incorrect_attribute_mapping = {"random_attribute": ["another_random_attribute"]}
        with self.assertRaises(ValueError):
            write_case_insights_data_to_bq.add_attributes_to_index(
                target_df, reference_df, incorrect_attribute_mapping
            )

    def test_add_offense_attributes(self) -> None:
        cohort_df = pd.DataFrame(
            {
                "gender": ["FEMALE", "MALE", "FEMALE"],
                "assessment_score": [5, 5, 5],
                "most_severe_ncic_category_uniform": [
                    "Assault",
                    "Sex Offense",
                    "Homicide",
                ],
                "most_severe_description": [
                    "ASSAULT OR BATTERY",
                    "SEXUAL ASSAULT",
                    "MURDER II",
                ],
                "any_is_violent": [True, False, True],
                "any_is_drug": [True, False, False],
                "any_is_sex_offense": [False, True, False],
            }
        )

        added_attributes_df = write_case_insights_data_to_bq.add_offense_attributes(
            cohort_df
        )

        expected_added_attributes_df = pd.DataFrame(
            {
                "gender": ["FEMALE", "MALE", "FEMALE"],
                "assessment_score": [5, 5, 5],
                "most_severe_ncic_category_uniform": [
                    "Assault",
                    "Sex Offense",
                    "Homicide",
                ],
                "most_severe_description": [
                    "ASSAULT OR BATTERY",
                    "SEXUAL ASSAULT",
                    "MURDER II",
                ],
                "any_is_violent": [True, False, True],
                "any_is_drug": [True, False, False],
                "any_is_sex_offense": [False, True, False],
                "assessment_score_bucket_start": [0, 0, 0],
                "assessment_score_bucket_end": [22, 20, 22],
                "combined_offense_category": [
                    "Violent offense, Drug offense",
                    "Sex offense",
                    "Violent offense",
                ],
            }
        )
        pd.testing.assert_frame_equal(expected_added_attributes_df, added_attributes_df)

    def test_extract_rate_dicts_info(self) -> None:
        event_rate_dicts_df = pd.DataFrame(
            {
                "event_rate_dicts": [
                    {"event_rate": 0.5, "cohort_size": 10, "ci_size": 0.2},
                    {"event_rate": 0.25, "cohort_size": 20, "ci_size": 0.1},
                ]
            }
        )
        extracted_info = write_case_insights_data_to_bq.extract_rate_dicts_info(
            event_rate_dicts_df, "event_rate_dicts"
        )
        expected_extracted_info = {
            "event_rate_dict": '[{"event_rate": 0.5, "cohort_size": 10, "ci_size": 0.2}, {"event_rate": 0.25, "cohort_size": 20, "ci_size": 0.1}]',
            "cohort_size": 20,
            "final_ci_size": 0.1,
        }
        self.assertDictEqual(expected_extracted_info, extracted_info)

    @patch(
        "recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.concatenate_recidivism_series"
    )
    @patch(
        "recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.add_recidivism_rate_dicts"
    )
    @patch("recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.add_cis")
    @patch(
        "recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.gen_aggregated_cohort_event_df"
    )
    @freezegun.freeze_time(datetime.datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=tzlocal()))
    def test_get_recidivism_series_df_for_rollup_level(
        self,
        mock_gen_aggregated_cohort_event_df: Mock,
        mock_add_cis: Mock,
        mock_add_recidivism_rate_dicts: Mock,
        mock_concatenate_recidivism_series: Mock,
    ) -> None:
        recidivism_df = pd.DataFrame(
            {
                "state_code": ["US_IX", "US_IX"],
                "most_severe_description": [
                    "ASSAULT OR BATTERY",
                    "MISUSE OF PUBLIC MONEY",
                ],
                "most_severe_ncic_category_uniform": ["Assault", "Bribery"],
            }
        )
        returned_aggregated_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    [0, 3, 0, 3],
                    ["US_IX", "US_IX", "US_IX", "US_IX"],
                    [
                        "ASSAULT OR BATTERY",
                        "ASSAULT OR BATTERY",
                        "MISUSE OF PUBLIC MONEY",
                        "MISUSE OF PUBLIC MONEY",
                    ],
                ],
                names=[
                    "cohort_months",
                    "state_code",
                    "most_severe_description",
                ],
            ),
            data={
                "event_count": [0, 1, 0, 1],
                "cohort_size": [1, 2, 1, 4],
                "event_rate": [0, 0.5, 0, 0.25],
            },
        )
        returned_concatenated_series_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    ["US_IX", "US_IX"],
                    ["ASSAULT OR BATTERY", "MISUSE OF PUBLIC MONEY"],
                ],
                names=[
                    "state_code",
                    "most_severe_description",
                ],
            ),
            data={
                (("event_rate_dict", "PROBATION")): [
                    "fake_event_rate_dict_str1",
                    "fake_event_rate_dict_str2",
                ],
                (("event_rate_dict", "TERM")): [
                    "fake_event_rate_dict_str3",
                    "fake_event_rate_dict_str4",
                ],
                (("cohort_size", "PROBATION")): [15, 10],
                (("cohort_size", "TERM")): [None, 20],
                (("final_ci_size", "PROBATION")): [0.2, 0.5],
                (("final_ci_size", "TERM")): [None, 0.1],
            },
        )
        mock_gen_aggregated_cohort_event_df.return_value = returned_aggregated_df
        # The Mock mock_add_cis just returns the same DataFrame as the input
        mock_add_cis.return_value = returned_aggregated_df
        # The Mock add_recidivism_rate_dicts just returns the same DataFrame as the input
        mock_add_recidivism_rate_dicts.return_value = returned_aggregated_df
        mock_concatenate_recidivism_series.return_value = (
            returned_concatenated_series_df
        )

        concatenated_recidivism_series_df = (
            write_case_insights_data_to_bq.get_recidivism_series_df_for_rollup_level(
                recidivism_df, [0, 3], ["state_code", "cohort_group", "gender"]
            )
        )

        mock_gen_aggregated_cohort_event_df.assert_called_with(
            recidivism_df,
            cohort_date_field="cohort_start_date",
            event_date_field="recidivism_date",
            time_index=[0, 3],
            time_unit="months",
            cohort_attribute_col=["state_code", "cohort_group", "gender"],
            last_day_of_data=datetime.datetime.now(),
            full_observability=True,
        )
        mock_add_cis.assert_called_with(df=returned_aggregated_df)
        mock_add_recidivism_rate_dicts.assert_called_with(df=returned_aggregated_df)
        mock_concatenate_recidivism_series.assert_called_with(
            returned_aggregated_df, ["state_code", "cohort_group", "gender"]
        )
        pd.testing.assert_frame_equal(
            returned_concatenated_series_df, concatenated_recidivism_series_df
        )

    def test_extract_rollup_columns(self) -> None:
        mock_rollup_attributes = {
            "US_IX": [
                ["state_code", "most_severe_description"],
                ["state_code"],
            ]
        }
        # The second level should be extracted for each row
        all_rollup_levels_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    ["US_IX", "US_IX"],
                    ["ASSAULT OR BATTERY", "MISUSE OF PUBLIC MONEY"],
                    ["Assault", "Fraud"],
                ],
                names=[
                    "state_code",
                    "most_severe_description",
                    "most_severe_ncic_category_uniform",
                ],
            ),
            data=[
                [
                    10,
                    15,
                    20,
                    "event_rate_dict_probation_0_0",
                    "event_rate_dict_rider_0_0",
                    "event_rate_dict_term_0_0",
                    0.25,
                    0.1,
                    0.05,
                    15,
                    20,
                    25,
                    "event_rate_dict_probation_0_1",
                    "event_rate_dict_rider_0_1",
                    "event_rate_dict_term_0_1",
                    0.1,
                    0.05,
                    0.03,
                ],
                [
                    11,
                    16,
                    21,
                    "event_rate_dict_probation_1_0",
                    "event_rate_dict_rider_1_0",
                    "event_rate_dict_term_1_0",
                    0.24,
                    0.09,
                    0.04,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                ],
            ],
            columns=pd.MultiIndex.from_tuples(
                [
                    ("cohort_size", "PROBATION", 0),
                    ("cohort_size", "RIDER", 0),
                    ("cohort_size", "TERM", 0),
                    ("event_rate_dict", "PROBATION", 0),
                    ("event_rate_dict", "RIDER", 0),
                    ("event_rate_dict", "TERM", 0),
                    ("final_ci_size", "PROBATION", 0),
                    ("final_ci_size", "RIDER", 0),
                    ("final_ci_size", "TERM", 0),
                    ("cohort_size", "PROBATION", 1),
                    ("cohort_size", "RIDER", 1),
                    ("cohort_size", "TERM", 1),
                    ("event_rate_dict", "PROBATION", 1),
                    ("event_rate_dict", "RIDER", 1),
                    ("event_rate_dict", "TERM", 1),
                    ("final_ci_size", "PROBATION", 1),
                    ("final_ci_size", "RIDER", 1),
                    ("final_ci_size", "TERM", 1),
                ],
                names=["metric", "cohort_group", "rollup_level"],
            ),
        )

        with patch(
            "recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.ROLLUP_ATTRIBUTES",
            mock_rollup_attributes,
        ):
            rollup_df_with_extracted_columns = (
                write_case_insights_data_to_bq.extract_rollup_columns(
                    all_rollup_levels_df, "US_IX"
                )
            )

        expected_rollup_df_with_extracted_columns = pd.DataFrame(
            data=[
                [
                    "US_IX",
                    "ASSAULT OR BATTERY",
                    "Assault",
                    1,
                    15,
                    20,
                    25,
                    "event_rate_dict_probation_0_1",
                    "event_rate_dict_rider_0_1",
                    "event_rate_dict_term_0_1",
                    0.1,
                    0.05,
                    0.03,
                    60,
                    '{"state_code": "US_IX"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX"}',
                ],
            ],
            columns=[
                "state_code",
                "most_severe_description",
                "most_severe_ncic_category_uniform",
                "rollup_level",
                ("cohort_size", "PROBATION"),
                ("cohort_size", "RIDER"),
                ("cohort_size", "TERM"),
                "recidivism_probation_series",
                "recidivism_rider_series",
                "recidivism_term_series",
                ("final_ci_size", "PROBATION"),
                ("final_ci_size", "RIDER"),
                ("final_ci_size", "TERM"),
                "recidivism_num_records",
                "recidivism_rollup",
            ],
        )
        pd.testing.assert_frame_equal(
            expected_rollup_df_with_extracted_columns, rollup_df_with_extracted_columns
        )

    def test_add_combined_attributes_to_rollup(self) -> None:
        rolled_up_recidivism_df = pd.DataFrame(
            data=[
                [
                    "US_IX",
                    "ASSAULT OR BATTERY",
                    "Assault",
                    1,
                    15,
                    20,
                    25,
                    "event_rate_dict_probation_0_1",
                    "event_rate_dict_rider_0_1",
                    "event_rate_dict_term_0_1",
                    0.1,
                    0.05,
                    0.03,
                    60,
                    '{"state_code": "US_IX"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "most_severe_ncic_category_uniform": "Sex offense"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Violent offense"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Sex offense"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Drug offense"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Violent offense, Sex offense"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Violent offense, Drug offense"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Drug offense, Sex offense"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Violent offense, Drug offense, Sex offense"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Non-drug, Non-violent, Non-sex offense"}',
                ],
            ],
            columns=[
                "state_code",
                "most_severe_description",
                "most_severe_ncic_category_uniform",
                "rollup_level",
                ("cohort_size", "PROBATION"),
                ("cohort_size", "RIDER"),
                ("cohort_size", "TERM"),
                "recidivism_probation_series",
                "recidivism_rider_series",
                "recidivism_term_series",
                ("final_ci_size", "PROBATION"),
                ("final_ci_size", "RIDER"),
                ("final_ci_size", "TERM"),
                "recidivism_num_records",
                "recidivism_rollup",
            ],
        )

        rolled_up_recidivism_df_with_added_attributes = (
            write_case_insights_data_to_bq.add_combined_attributes_to_rollup(
                rolled_up_recidivism_df
            )
        )

        expected_rolled_up_recidivism_with_added_attributes_df = pd.DataFrame(
            data=[
                [
                    "US_IX",
                    "ASSAULT OR BATTERY",
                    "Assault",
                    1,
                    15,
                    20,
                    25,
                    "event_rate_dict_probation_0_1",
                    "event_rate_dict_rider_0_1",
                    "event_rate_dict_term_0_1",
                    0.1,
                    0.05,
                    0.03,
                    60,
                    '{"state_code": "US_IX", "rollupCombinedOffenseCategory": null}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "most_severe_ncic_category_uniform": "Sex offense", "rollupCombinedOffenseCategory": null}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Violent offense", "rollupCombinedOffenseCategory": {"isViolent": true, "isDrug": false, "isSex": false}}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Sex offense", "rollupCombinedOffenseCategory": {"isViolent": false, "isDrug": false, "isSex": true}}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Drug offense", "rollupCombinedOffenseCategory": {"isViolent": false, "isDrug": true, "isSex": false}}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Violent offense, Sex offense", "rollupCombinedOffenseCategory": {"isViolent": true, "isDrug": false, "isSex": true}}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Violent offense, Drug offense", "rollupCombinedOffenseCategory": {"isViolent": true, "isDrug": true, "isSex": false}}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Drug offense, Sex offense", "rollupCombinedOffenseCategory": {"isViolent": false, "isDrug": true, "isSex": true}}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Violent offense, Drug offense, Sex offense", "rollupCombinedOffenseCategory": {"isViolent": true, "isDrug": true, "isSex": true}}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX", "combined_offense_category": "Non-drug, Non-violent, Non-sex offense", "rollupCombinedOffenseCategory": {"isViolent": false, "isDrug": false, "isSex": false}}',
                ],
            ],
            columns=[
                "state_code",
                "most_severe_description",
                "most_severe_ncic_category_uniform",
                "rollup_level",
                ("cohort_size", "PROBATION"),
                ("cohort_size", "RIDER"),
                ("cohort_size", "TERM"),
                "recidivism_probation_series",
                "recidivism_rider_series",
                "recidivism_term_series",
                ("final_ci_size", "PROBATION"),
                ("final_ci_size", "RIDER"),
                ("final_ci_size", "TERM"),
                "recidivism_num_records",
                "recidivism_rollup",
            ],
        )

        pd.testing.assert_frame_equal(
            expected_rolled_up_recidivism_with_added_attributes_df,
            rolled_up_recidivism_df_with_added_attributes,
        )

    def test_add_cis(self) -> None:
        df = pd.DataFrame(
            {
                "event_rate": [0.0, 0.0, 0.2, 0.5, 0.9, 1.0, 1.0],
                "cohort_size": [1, 10, 10, 100, 1000, 10, 1],
            }
        )

        write_case_insights_data_to_bq.add_cis(df)

        expected_df_with_cis = pd.DataFrame(
            {
                "event_rate": [0.0, 0.0, 0.2, 0.5, 0.9, 1.0, 1.0],
                "cohort_size": [1, 10, 10, 100, 1000, 10, 1],
                "lower_ci": [
                    0.0,
                    0.0,
                    0.02521072632683336,
                    0.39832112950330106,
                    0.8797120634813074,
                    0.6915028921812392,
                    0.025,
                ],
                "upper_ci": [
                    0.975,
                    0.30849710781876083,
                    0.5560954623076415,
                    0.6016788704966989,
                    0.91789466564442,
                    1.0,
                    1.0,
                ],
                "ci_size": [
                    0.975,
                    0.30849710781876083,
                    0.5308847359808081,
                    0.20335774099339782,
                    0.038182602163112644,
                    0.30849710781876083,
                    0.975,
                ],
            }
        )

        pd.testing.assert_frame_equal(expected_df_with_cis, df)

    def test_add_recidivism_rate_dicts(self) -> None:
        df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [[0, 3, 6, 9, 12], ["US_IX", "US_IX", "US_IX", "US_IX", "US_IX"]],
                names=["cohort_months", "state_code"],
            ),
            data={
                "event_rate": [0.0, 0.2, 0.5, 0.9, 1.0],
                "cohort_size": [10, 10, 100, 1000, 10],
                "lower_ci": [0.0, 0.0, 0.402002, 0.881406, 1.0],
                "upper_ci": [0.0, 0.447918, 0.597998, 0.918594, 1.0],
                "ci_size": [0.0, 0.447918, 0.195996, 0.037188, 0.0],
            },
        )

        df = write_case_insights_data_to_bq.add_recidivism_rate_dicts(df)

        expected_df_with_recidivism_rate_dicts = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [[0, 3, 6, 9, 12], ["US_IX", "US_IX", "US_IX", "US_IX", "US_IX"]],
                names=["cohort_months", "state_code"],
            ),
            data={
                "event_rate": [0.0, 0.2, 0.5, 0.9, 1.0],
                "cohort_size": [10, 10, 100, 1000, 10],
                "lower_ci": [0.0, 0.0, 0.402002, 0.881406, 1.0],
                "upper_ci": [0.0, 0.447918, 0.597998, 0.918594, 1.0],
                "ci_size": [0.0, 0.447918, 0.195996, 0.037188, 0.0],
                "event_rate_dict": [
                    {
                        "cohort_months": 0,
                        "event_rate": 0.0,
                        "lower_ci": 0.0,
                        "upper_ci": 0.0,
                        "ci_size": 0.0,
                        "cohort_size": 10,
                    },
                    {
                        "cohort_months": 3,
                        "event_rate": 0.2,
                        "lower_ci": 0.0,
                        "upper_ci": 0.447918,
                        "ci_size": 0.447918,
                        "cohort_size": 10,
                    },
                    {
                        "cohort_months": 6,
                        "event_rate": 0.5,
                        "lower_ci": 0.402002,
                        "upper_ci": 0.597998,
                        "ci_size": 0.195996,
                        "cohort_size": 100,
                    },
                    {
                        "cohort_months": 9,
                        "event_rate": 0.9,
                        "lower_ci": 0.881406,
                        "upper_ci": 0.918594,
                        "ci_size": 0.037188,
                        "cohort_size": 1000,
                    },
                    {
                        "cohort_months": 12,
                        "event_rate": 1.0,
                        "lower_ci": 1.0,
                        "upper_ci": 1.0,
                        "ci_size": 0.0,
                        "cohort_size": 10,
                    },
                ],
            },
        )

        pd.testing.assert_frame_equal(expected_df_with_recidivism_rate_dicts, df)

    def test_concatenate_recidivism_series(self) -> None:
        aggregated_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    ["US_IX", "US_IX", "US_IX", "US_IX", "US_IX"],
                    ["PROBATION", "PROBATION", "PROBATION", "TERM", "TERM"],
                    [
                        "MALE",
                        "FEMALE",
                        "FEMALE",
                        "MALE",
                        "MALE",
                    ],
                    [0, 0, 0, 0, 0],
                    [20, 22, 22, 20, 20],
                    ["Assault", "Bribery", "Bribery", "Burglary", "Burglary"],
                ],
                names=[
                    "state_code",
                    "cohort_group",
                    "gender",
                    "assessment_score_bucket_start",
                    "assessment_score_bucket_end",
                    "most_severe_description",
                ],
            ),
            data={
                "cohort_months": [0, 0, 3, 0, 3],
                "event_rate_dict": [
                    {
                        "cohort_months": 0,
                        "event_rate": 0.0,
                        "lower_ci": 0.0,
                        "upper_ci": 0.5,
                        "ci_size": 0.5,
                        "cohort_size": 10,
                    },
                    {
                        "cohort_months": 0,
                        "event_rate": 0.0,
                        "lower_ci": 0.0,
                        "upper_ci": 0.5,
                        "ci_size": 0.5,
                        "cohort_size": 15,
                    },
                    {
                        "cohort_months": 3,
                        "event_rate": 0.1,
                        "lower_ci": 0.0,
                        "upper_ci": 0.2,
                        "ci_size": 0.2,
                        "cohort_size": 15,
                    },
                    {
                        "cohort_months": 0,
                        "event_rate": 0.0,
                        "lower_ci": 0.0,
                        "upper_ci": 0.5,
                        "ci_size": 0.5,
                        "cohort_size": 20,
                    },
                    {
                        "cohort_months": 3,
                        "event_rate": 0.2,
                        "lower_ci": 0.15,
                        "upper_ci": 0.25,
                        "ci_size": 0.1,
                        "cohort_size": 20,
                    },
                ],
            },
        )

        recidivism_series_df = (
            write_case_insights_data_to_bq.concatenate_recidivism_series(
                aggregated_df,
                [
                    "state_code",
                    "cohort_group",
                    "gender",
                    "assessment_score_bucket_start",
                    "assessment_score_bucket_end",
                    "most_severe_description",
                ],
            )
        )

        expected_recidivism_series_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    ["US_IX", "US_IX", "US_IX"],
                    ["FEMALE", "MALE", "MALE"],
                    [0, 0, 0],
                    [22, 20, 20],
                    ["Bribery", "Assault", "Burglary"],
                ],
                names=[
                    "state_code",
                    "gender",
                    "assessment_score_bucket_start",
                    "assessment_score_bucket_end",
                    "most_severe_description",
                ],
            ),
            data=[
                [
                    '[{"cohort_months": 0, "event_rate": 0.0, "lower_ci": 0.0, "upper_ci": 0.5, "ci_size": 0.5, '
                    + '"cohort_size": 15}, {"cohort_months": 3, "event_rate": 0.1, "lower_ci": 0.0, "upper_ci": 0.2, '
                    + '"ci_size": 0.2, "cohort_size": 15}]',
                    float("nan"),
                    15,
                    None,
                    0.2,
                    None,
                ],
                [
                    '[{"cohort_months": 0, "event_rate": 0.0, "lower_ci": 0.0, "upper_ci": 0.5, "ci_size": 0.5, '
                    + '"cohort_size": 10}]',
                    float("nan"),
                    10,
                    None,
                    0.5,
                    None,
                ],
                [
                    float("nan"),
                    '[{"cohort_months": 0, "event_rate": 0.0, "lower_ci": 0.0, "upper_ci": 0.5, "ci_size": 0.5, '
                    + '"cohort_size": 20}, {"cohort_months": 3, "event_rate": 0.2, "lower_ci": 0.15, "upper_ci": 0.25, '
                    + '"ci_size": 0.1, "cohort_size": 20}]',
                    None,
                    20,
                    None,
                    0.1,
                ],
            ],
            columns=pd.MultiIndex.from_product(
                [
                    ["event_rate_dict", "cohort_size", "final_ci_size"],
                    ["PROBATION", "TERM"],
                ],
                names=[None, "cohort_group"],
            ),
        )
        pd.testing.assert_frame_equal(
            expected_recidivism_series_df, recidivism_series_df
        )

    def test_create_final_table(self) -> None:
        mock_rollup_attributes = {
            "US_IX": [
                ["state_code", "cohort_group", "most_severe_description"],
                ["state_code", "cohort_group"],
            ]
        }
        disposition_df = pd.DataFrame(
            {
                "state_code": ["US_IX", "US_IX"],
                "most_severe_description": [
                    "ASSAULT OR BATTERY",
                    "MISUSE OF PUBLIC MONEY",
                ],
                "disposition_probation_pc": [0.9, 0.0],
                "disposition_rider_pc": [0.06, 0.4],
                "disposition_term_pc": [0.04, 0.6],
            }
        )

        rolled_up_recidivism_df = pd.DataFrame(
            data=[
                [
                    "US_IX",
                    "ASSAULT OR BATTERY",
                    "Assault",
                    1,
                    15,
                    20,
                    25,
                    "event_rate_dict_probation_0_1",
                    "event_rate_dict_rider_0_1",
                    "event_rate_dict_term_0_1",
                    0.1,
                    0.05,
                    0.03,
                    60,
                    '{"state_code": "US_IX"}',
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX"}',
                ],
            ],
            columns=[
                "state_code",
                "most_severe_description",
                "most_severe_ncic_category_uniform",
                "rollup_level",
                ("cohort_size", "PROBATION"),
                ("cohort_size", "RIDER"),
                ("cohort_size", "TERM"),
                "recidivism_probation_series",
                "recidivism_rider_series",
                "recidivism_term_series",
                ("final_ci_size", "PROBATION"),
                ("final_ci_size", "RIDER"),
                ("final_ci_size", "TERM"),
                "recidivism_num_records",
                "recidivism_rollup",
            ],
        )
        with patch(
            "recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.ROLLUP_ATTRIBUTES",
            mock_rollup_attributes,
        ):
            final_table = write_case_insights_data_to_bq.create_final_table(
                rolled_up_recidivism_df, disposition_df, "US_IX"
            )

        expected_final_table = pd.DataFrame(
            data=[
                [
                    "US_IX",
                    "ASSAULT OR BATTERY",
                    "Assault",
                    1,
                    15,
                    20,
                    25,
                    "event_rate_dict_probation_0_1",
                    "event_rate_dict_rider_0_1",
                    "event_rate_dict_term_0_1",
                    0.1,
                    0.05,
                    0.03,
                    60,
                    '{"state_code": "US_IX"}',
                    0.9,
                    0.06,
                    0.04,
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    "event_rate_dict_probation_1_1",
                    "event_rate_dict_rider_1_1",
                    "event_rate_dict_term_1_1",
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_IX"}',
                    0.0,
                    0.4,
                    0.6,
                ],
            ],
            columns=[
                "state_code",
                "most_severe_description",
                "most_severe_ncic_category_uniform",
                "rollup_level",
                ("cohort_size", "PROBATION"),
                ("cohort_size", "RIDER"),
                ("cohort_size", "TERM"),
                "recidivism_probation_series",
                "recidivism_rider_series",
                "recidivism_term_series",
                ("final_ci_size", "PROBATION"),
                ("final_ci_size", "RIDER"),
                ("final_ci_size", "TERM"),
                "recidivism_num_records",
                "recidivism_rollup",
                "disposition_probation_pc",
                "disposition_rider_pc",
                "disposition_term_pc",
            ],
        )

        pd.testing.assert_frame_equal(expected_final_table, final_table)

    def test_consolidate_cohort_columns(self) -> None:
        mock_inclusion_ci_thresholds = {"US_ND": 0.6}
        final_table = pd.DataFrame(
            data=[
                [
                    "US_IX",
                    "ASSAULT OR BATTERY",
                    "Assault",
                    1,
                    15,
                    20,
                    25,
                    '[{"event_rate": 0.1, "ci_size": 0.1}, {"event_rate": 0.4, "ci_size": 0.1}]',
                    '[{"event_rate": 0.2, "ci_size": 0.3}, {"event_rate": 0.5, "ci_size": 0.3}]',
                    '[{"event_rate": 0.2, "ci_size": 0.8}, {"event_rate": 0.5, "ci_size": 0.7}]',  # will be excluded
                    '[{"event_rate": 0.3, "ci_size": 0.5}, {"event_rate": 0.6, "ci_size": 0.5}]',
                    0.1,
                    0.05,
                    0.03,
                    60,
                    '{"state_code": "US_ND"}',
                    0.9,
                    0.06,
                    0.01,
                    0.03,
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    '[{"event_rate": 0.2, "ci_size": 0.1}, {"event_rate": 0.9, "ci_size": 0.1}]',
                    '[{"event_rate": 0.3, "ci_size": 0.3}, {"event_rate": 0.8, "ci_size": 0.3}]',
                    '[{"event_rate": 0.3, "ci_size": 0.8}, {"event_rate": 0.8, "ci_size": 0.7}]',  # will be excluded
                    '[{"event_rate": 0.4, "ci_size": 0.5}, {"event_rate": 0.7, "ci_size": 0.5}]',
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_ND"}',
                    0.0,
                    0.4,
                    0.01,
                    0.59,
                ],
            ],
            columns=[
                "state_code",
                "most_severe_description",
                "most_severe_ncic_category_uniform",
                "rollup_level",
                ("cohort_size", "PROBATION"),
                ("cohort_size", "RIDER"),
                ("cohort_size", "TERM"),
                "recidivism_PROBATION_series",
                "recidivism_INCARCERATION|0-1 years_series",
                "recidivism_INCARCERATION|1-3 years_series",
                "recidivism_INCARCERATION|6-? years_series",
                ("final_ci_size", "PROBATION"),
                ("final_ci_size", "RIDER"),
                ("final_ci_size", "TERM"),
                "recidivism_num_records",
                "recidivism_rollup",
                "disposition_PROBATION_pc",
                "recidivism_INCARCERATION|0-1 years_pc",
                "recidivism_INCARCERATION|1-3 years_pc",
                "recidivism_INCARCERATION|6-? years_pc",
            ],
        )

        expected_recidivism_series = pd.Series(
            [
                json.dumps(
                    [
                        {
                            "sentence_type": "Probation",
                            "data_points": [
                                {"event_rate": 0.1, "ci_size": 0.1},
                                {"event_rate": 0.4, "ci_size": 0.1},
                            ],
                        },
                        {
                            "sentence_type": "Incarceration",
                            "sentence_length_bucket_start": 0,
                            "sentence_length_bucket_end": 1,
                            "data_points": [
                                {"event_rate": 0.2, "ci_size": 0.3},
                                {"event_rate": 0.5, "ci_size": 0.3},
                            ],
                        },
                        {
                            "sentence_type": "Incarceration",
                            "sentence_length_bucket_start": 6,
                            "sentence_length_bucket_end": -1,
                            "data_points": [
                                {"event_rate": 0.3, "ci_size": 0.5},
                                {"event_rate": 0.6, "ci_size": 0.5},
                            ],
                        },
                    ]
                ),
                json.dumps(
                    [
                        {
                            "sentence_type": "Probation",
                            "data_points": [
                                {"event_rate": 0.2, "ci_size": 0.1},
                                {"event_rate": 0.9, "ci_size": 0.1},
                            ],
                        },
                        {
                            "sentence_type": "Incarceration",
                            "sentence_length_bucket_start": 0,
                            "sentence_length_bucket_end": 1,
                            "data_points": [
                                {"event_rate": 0.3, "ci_size": 0.3},
                                {"event_rate": 0.8, "ci_size": 0.3},
                            ],
                        },
                        {
                            "sentence_type": "Incarceration",
                            "sentence_length_bucket_start": 6,
                            "sentence_length_bucket_end": -1,
                            "data_points": [
                                {"event_rate": 0.4, "ci_size": 0.5},
                                {"event_rate": 0.7, "ci_size": 0.5},
                            ],
                        },
                    ]
                ),
            ]
        )

        with patch(
            "recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.INCLUSION_CI_THRESHOLDS",
            mock_inclusion_ci_thresholds,
        ):
            recidivism_series = (
                write_case_insights_data_to_bq.consolidate_cohort_columns(
                    final_table,
                    "US_ND",
                    write_case_insights_data_to_bq.RECIDIVISM_SERIES_COLUMN_REGEX,
                    "data_points",
                )
            )

        pd.testing.assert_series_equal(expected_recidivism_series, recidivism_series)

    def test_add_consolidated_columns(self) -> None:
        mock_inclusion_ci_thresholds = {"US_ND": 0.6}
        final_table = pd.DataFrame(
            data=[
                [
                    "US_IX",
                    "ASSAULT OR BATTERY",
                    "Assault",
                    1,
                    15,
                    20,
                    25,
                    '[{"event_rate": 0.1, "ci_size": 0.1}, {"event_rate": 0.4, "ci_size": 0.1}]',
                    '[{"event_rate": 0.2, "ci_size": 0.3}, {"event_rate": 0.5, "ci_size": 0.3}]',
                    '[{"event_rate": 0.2, "ci_size": 0.8}, {"event_rate": 0.5, "ci_size": 0.7}]',  # will be excluded
                    '[{"event_rate": 0.3, "ci_size": 0.5}, {"event_rate": 0.6, "ci_size": 0.5}]',
                    0.1,
                    0.05,
                    0.03,
                    60,
                    '{"state_code": "US_ND"}',
                    0.9,
                    0.06,
                    0.01,
                    0.03,
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    '[{"event_rate": 0.2, "ci_size": 0.1}, {"event_rate": 0.9, "ci_size": 0.1}]',
                    '[{"event_rate": 0.3, "ci_size": 0.3}, {"event_rate": 0.8, "ci_size": 0.3}]',
                    '[{"event_rate": 0.3, "ci_size": 0.8}, {"event_rate": 0.8, "ci_size": 0.7}]',  # will be excluded
                    '[{"event_rate": 0.4, "ci_size": 0.5}, {"event_rate": 0.7, "ci_size": 0.5}]',
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_ND"}',
                    0.0,
                    0.4,
                    0.01,
                    0.59,
                ],
            ],
            columns=[
                "state_code",
                "most_severe_description",
                "most_severe_ncic_category_uniform",
                "rollup_level",
                ("cohort_size", "PROBATION"),
                ("cohort_size", "RIDER"),
                ("cohort_size", "TERM"),
                "recidivism_PROBATION_series",
                "recidivism_INCARCERATION|0-1 years_series",
                "recidivism_INCARCERATION|1-3 years_series",
                "recidivism_INCARCERATION|6-? years_series",
                ("final_ci_size", "PROBATION"),
                ("final_ci_size", "RIDER"),
                ("final_ci_size", "TERM"),
                "recidivism_num_records",
                "recidivism_rollup",
                "disposition_PROBATION_pc",
                "disposition_INCARCERATION|0-1 years_pc",
                "disposition_INCARCERATION|1-3 years_pc",
                "disposition_INCARCERATION|6-? years_pc",
            ],
        )

        expected_final_table = pd.DataFrame(
            data=[
                [
                    "US_IX",
                    "ASSAULT OR BATTERY",
                    "Assault",
                    1,
                    15,
                    20,
                    25,
                    '[{"event_rate": 0.1, "ci_size": 0.1}, {"event_rate": 0.4, "ci_size": 0.1}]',
                    '[{"event_rate": 0.2, "ci_size": 0.3}, {"event_rate": 0.5, "ci_size": 0.3}]',
                    '[{"event_rate": 0.2, "ci_size": 0.8}, {"event_rate": 0.5, "ci_size": 0.7}]',
                    '[{"event_rate": 0.3, "ci_size": 0.5}, {"event_rate": 0.6, "ci_size": 0.5}]',
                    0.1,
                    0.05,
                    0.03,
                    60,
                    '{"state_code": "US_ND"}',
                    0.9,
                    0.06,
                    0.01,
                    0.03,
                    json.dumps(
                        [
                            {
                                "sentence_type": "Probation",
                                "data_points": [
                                    {"event_rate": 0.1, "ci_size": 0.1},
                                    {"event_rate": 0.4, "ci_size": 0.1},
                                ],
                            },
                            {
                                "sentence_type": "Incarceration",
                                "sentence_length_bucket_start": 0,
                                "sentence_length_bucket_end": 1,
                                "data_points": [
                                    {"event_rate": 0.2, "ci_size": 0.3},
                                    {"event_rate": 0.5, "ci_size": 0.3},
                                ],
                            },
                            {
                                "sentence_type": "Incarceration",
                                "sentence_length_bucket_start": 6,
                                "sentence_length_bucket_end": -1,
                                "data_points": [
                                    {"event_rate": 0.3, "ci_size": 0.5},
                                    {"event_rate": 0.6, "ci_size": 0.5},
                                ],
                            },
                        ]
                    ),
                    json.dumps(
                        [
                            {"sentence_type": "Probation", "percentage": 0.9},
                            {
                                "sentence_type": "Incarceration",
                                "sentence_length_bucket_start": 0,
                                "sentence_length_bucket_end": 1,
                                "percentage": 0.06,
                            },
                            {
                                "sentence_type": "Incarceration",
                                "sentence_length_bucket_start": 1,
                                "sentence_length_bucket_end": 3,
                                "percentage": 0.01,
                            },
                            {
                                "sentence_type": "Incarceration",
                                "sentence_length_bucket_start": 6,
                                "sentence_length_bucket_end": -1,
                                "percentage": 0.03,
                            },
                        ]
                    ),
                ],
                [
                    "US_IX",
                    "MISUSE OF PUBLIC MONEY",
                    "Fraud",
                    1,
                    16,
                    21,
                    26,
                    '[{"event_rate": 0.2, "ci_size": 0.1}, {"event_rate": 0.9, "ci_size": 0.1}]',
                    '[{"event_rate": 0.3, "ci_size": 0.3}, {"event_rate": 0.8, "ci_size": 0.3}]',
                    '[{"event_rate": 0.3, "ci_size": 0.8}, {"event_rate": 0.8, "ci_size": 0.7}]',
                    '[{"event_rate": 0.4, "ci_size": 0.5}, {"event_rate": 0.7, "ci_size": 0.5}]',
                    0.09,
                    0.04,
                    0.02,
                    63,
                    '{"state_code": "US_ND"}',
                    0.0,
                    0.4,
                    0.01,
                    0.59,
                    json.dumps(
                        [
                            {
                                "sentence_type": "Probation",
                                "data_points": [
                                    {"event_rate": 0.2, "ci_size": 0.1},
                                    {"event_rate": 0.9, "ci_size": 0.1},
                                ],
                            },
                            {
                                "sentence_type": "Incarceration",
                                "sentence_length_bucket_start": 0,
                                "sentence_length_bucket_end": 1,
                                "data_points": [
                                    {"event_rate": 0.3, "ci_size": 0.3},
                                    {"event_rate": 0.8, "ci_size": 0.3},
                                ],
                            },
                            {
                                "sentence_type": "Incarceration",
                                "sentence_length_bucket_start": 6,
                                "sentence_length_bucket_end": -1,
                                "data_points": [
                                    {"event_rate": 0.4, "ci_size": 0.5},
                                    {"event_rate": 0.7, "ci_size": 0.5},
                                ],
                            },
                        ]
                    ),
                    json.dumps(
                        [
                            {"sentence_type": "Probation", "percentage": 0.0},
                            {
                                "sentence_type": "Incarceration",
                                "sentence_length_bucket_start": 0,
                                "sentence_length_bucket_end": 1,
                                "percentage": 0.4,
                            },
                            {
                                "sentence_type": "Incarceration",
                                "sentence_length_bucket_start": 1,
                                "sentence_length_bucket_end": 3,
                                "percentage": 0.01,
                            },
                            {
                                "sentence_type": "Incarceration",
                                "sentence_length_bucket_start": 6,
                                "sentence_length_bucket_end": -1,
                                "percentage": 0.59,
                            },
                        ]
                    ),
                ],
            ],
            columns=[
                "state_code",
                "most_severe_description",
                "most_severe_ncic_category_uniform",
                "rollup_level",
                ("cohort_size", "PROBATION"),
                ("cohort_size", "RIDER"),
                ("cohort_size", "TERM"),
                "recidivism_PROBATION_series",
                "recidivism_INCARCERATION|0-1 years_series",
                "recidivism_INCARCERATION|1-3 years_series",
                "recidivism_INCARCERATION|6-? years_series",
                ("final_ci_size", "PROBATION"),
                ("final_ci_size", "RIDER"),
                ("final_ci_size", "TERM"),
                "recidivism_num_records",
                "recidivism_rollup",
                "disposition_PROBATION_pc",
                "disposition_INCARCERATION|0-1 years_pc",
                "disposition_INCARCERATION|1-3 years_pc",
                "disposition_INCARCERATION|6-? years_pc",
                "recidivism_series",
                "dispositions",
            ],
        )

        with patch(
            "recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq.INCLUSION_CI_THRESHOLDS",
            mock_inclusion_ci_thresholds,
        ):
            final_table_with_added_columns = (
                write_case_insights_data_to_bq.add_consolidated_columns(
                    final_table, "US_ND"
                )
            )

        pd.testing.assert_frame_equal(
            expected_final_table, final_table_with_added_columns
        )
