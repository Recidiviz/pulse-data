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
import unittest
import uuid
from unittest import mock
from unittest.mock import Mock, create_autospec

import pandas as pd

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.entrypoints.ingest import update_state_dataset
from recidiviz.entrypoints.sentencing import write_case_insights_data_to_bq

REFRESH_ENTRYPOINT_PACKAGE_NAME = update_state_dataset.__name__


@mock.patch(
    f"{REFRESH_ENTRYPOINT_PACKAGE_NAME}.BigQueryClientImpl",
    create_autospec(BigQueryClientImpl),
)
@mock.patch("time.sleep", Mock(side_effect=lambda _: None))
@mock.patch(
    "uuid.uuid4", Mock(return_value=uuid.UUID("8367379ff8674b04adfb9b595b277dc3"))
)
class TestWriteCaseInsightsDataToBQ(unittest.TestCase):
    """Tests for writing case insights data to BQ."""

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_get_gendered_assessment_score_bucket(self) -> None:
        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 0})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("0-22", score_bucket)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 2})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("0-22", score_bucket)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 22})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("0-22", score_bucket)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 23})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("23-30", score_bucket)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 30})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("23-30", score_bucket)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 31})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("31+", score_bucket)

        input_row = pd.Series({"gender": "FEMALE", "assessment_score": 35})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("31+", score_bucket)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 0})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("0-20", score_bucket)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 2})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("0-20", score_bucket)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 20})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("0-20", score_bucket)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 21})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("21-28", score_bucket)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 28})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("21-28", score_bucket)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 29})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("29+", score_bucket)

        input_row = pd.Series({"gender": "MALE", "assessment_score": 35})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("29+", score_bucket)

        input_row = pd.Series({"gender": "MALE", "assessment_score": None})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("UNKNOWN", score_bucket)

        input_row = pd.Series({"gender": "MALE", "assessment_score": -1})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("UNKNOWN", score_bucket)

        input_row = pd.Series({"gender": "EXTERNAL_UNKNOWN", "assessment_score": 1})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )

        input_row = pd.Series({"gender": "TRANS_FEMALE", "assessment_score": 1})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("UNKNOWN", score_bucket)

        input_row = pd.Series({"gender": "TRANS_MALE", "assessment_score": 1})
        score_bucket = (
            write_case_insights_data_to_bq.get_gendered_assessment_score_bucket(
                input_row
            )
        )
        self.assertEqual("UNKNOWN", score_bucket)

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
                "assessment_score_bucket": [
                    "0-22",
                    "0-22",
                    "29+",
                    "29+",
                    "0-22",
                    "0-22",
                    "0-22",
                    "29+",
                    "29+",
                ],
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
                "assessment_score_bucket": [
                    "0-22",
                    "29+",
                    "0-22",
                    "29+",
                ],
                "most_severe_description": [
                    "DUI DRIVING",
                    "DUI DRIVING",
                    "BURGLARY",
                    "BURGLARY",
                ],
                "disposition_probation_pc": [1.0 / 2, 0.0, 1.0 / 3, 1.0],
                "disposition_rider_pc": [1.0 / 2, 1.0 / 2, 1.0 / 3, 0.0],
                "disposition_term_pc": [0.0, 1.0 / 2, 1.0 / 3, 0.0],
            }
        )

        disposition_df = write_case_insights_data_to_bq.get_disposition_df(cohort_df)
        pd.testing.assert_frame_equal(
            expected_disposition_df.sort_values(
                [
                    "state_code",
                    "gender",
                    "assessment_score_bucket",
                    "most_severe_description",
                ]
            ).reset_index(drop=True),
            disposition_df.sort_values(
                [
                    "state_code",
                    "gender",
                    "assessment_score_bucket",
                    "most_severe_description",
                ]
            ).reset_index(drop=True),
        )

    def test_add_cis(self) -> None:
        df = pd.DataFrame(
            {
                "event_rate": [0.0, 0.2, 0.5, 0.9, 1.0],
                "cohort_size": [10, 10, 100, 1000, 10],
            }
        )

        write_case_insights_data_to_bq.add_cis(df)

        expected_df_with_cis = pd.DataFrame(
            {
                "event_rate": [0.0, 0.2, 0.5, 0.9, 1.0],
                "cohort_size": [10, 10, 100, 1000, 10],
                "lower_ci": [0.0, 0.0, 0.402002, 0.881406, 1.0],
                "upper_ci": [0.0, 0.447918, 0.597998, 0.918594, 1.0],
                "ci_size": [0.0, 0.447918, 0.195996, 0.037188, 0.0],
            }
        )

        pd.testing.assert_frame_equal(expected_df_with_cis, df)

    def test_add_recidivism_rate_dicts(self) -> None:
        df = pd.DataFrame(
            {
                "cohort_months": [0, 3, 6, 9, 12],
                "event_rate": [0.0, 0.2, 0.5, 0.9, 1.0],
                "cohort_size": [10, 10, 100, 1000, 10],
                "lower_ci": [0.0, 0.0, 0.402002, 0.881406, 1.0],
                "upper_ci": [0.0, 0.447918, 0.597998, 0.918594, 1.0],
                "ci_size": [0.0, 0.447918, 0.195996, 0.037188, 0.0],
            }
        )

        write_case_insights_data_to_bq.add_recidivism_rate_dicts(df)

        expected_df_with_recidivism_rate_dicts = pd.DataFrame(
            {
                "cohort_months": [0, 3, 6, 9, 12],
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
                    },
                    {
                        "cohort_months": 3,
                        "event_rate": 0.2,
                        "lower_ci": 0.0,
                        "upper_ci": 0.447918,
                    },
                    {
                        "cohort_months": 6,
                        "event_rate": 0.5,
                        "lower_ci": 0.402002,
                        "upper_ci": 0.597998,
                    },
                    {
                        "cohort_months": 9,
                        "event_rate": 0.9,
                        "lower_ci": 0.881406,
                        "upper_ci": 0.918594,
                    },
                    {
                        "cohort_months": 12,
                        "event_rate": 1.0,
                        "lower_ci": 1.0,
                        "upper_ci": 1.0,
                    },
                ],
            }
        )

        pd.testing.assert_frame_equal(expected_df_with_recidivism_rate_dicts, df)

    def test_get_recidivism_series(self) -> None:
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
                    [
                        "0-20",
                        "0-22",
                        "0-22",
                        "0-20",
                        "0-20",
                    ],
                    ["Assault", "Bribery", "Bribery", "Burglary", "Burglary"],
                ],
                names=[
                    "state_code",
                    "cohort_group",
                    "gender",
                    "assessment_score_bucket",
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
                        "upper_ci": 0.0,
                    },
                    {
                        "cohort_months": 0,
                        "event_rate": 0.0,
                        "lower_ci": 0.0,
                        "upper_ci": 0.0,
                    },
                    {
                        "cohort_months": 3,
                        "event_rate": 0.1,
                        "lower_ci": 0.0,
                        "upper_ci": 0.2,
                    },
                    {
                        "cohort_months": 0,
                        "event_rate": 0.0,
                        "lower_ci": 0.0,
                        "upper_ci": 0.0,
                    },
                    {
                        "cohort_months": 3,
                        "event_rate": 0.2,
                        "lower_ci": 0.15,
                        "upper_ci": 0.25,
                    },
                ],
            },
        )

        recidivism_series_df = write_case_insights_data_to_bq.get_recidivism_series(
            aggregated_df
        )

        expected_recidivism_series_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    ["US_IX", "US_IX", "US_IX"],
                    ["FEMALE", "MALE", "MALE"],
                    ["0-22", "0-20", "0-20"],
                    ["Bribery", "Assault", "Burglary"],
                ],
                names=[
                    "state_code",
                    "gender",
                    "assessment_score_bucket",
                    "most_severe_description",
                ],
            ),
            data={
                "recidivism_probation_series": [
                    '[{"cohort_months": 0, "event_rate": 0.0, "lower_ci": 0.0, "upper_ci": 0.0},'
                    + ' {"cohort_months": 3, "event_rate": 0.1, "lower_ci": 0.0, "upper_ci": 0.2}]',
                    '[{"cohort_months": 0, "event_rate": 0.0, "lower_ci": 0.0, "upper_ci": 0.0}]',
                    None,
                ],
                "recidivism_term_series": [
                    None,
                    None,
                    '[{"cohort_months": 0, "event_rate": 0.0, "lower_ci": 0.0, "upper_ci": 0.0},'
                    + ' {"cohort_months": 3, "event_rate": 0.2, "lower_ci": 0.15, "upper_ci": 0.25}]',
                ],
            },
        )
        pd.testing.assert_frame_equal(
            expected_recidivism_series_df, recidivism_series_df
        )

    def test_create_final_table(self) -> None:
        disposition_df = pd.DataFrame(
            {
                "state_code": ["US_IX", "US_IX", "US_IX"],
                "gender": ["FEMALE", "MALE", "MALE"],
                "assessment_score_bucket": [
                    "0-22",
                    "0-20",
                    "0-20",
                ],
                "most_severe_description": ["Bribery", "Assault", "Burglary"],
                "disposition_probation_pc": [0.9, 0.6, 0.0],
                "disposition_rider_pc": [0.06, 0.3, 0.4],
                "disposition_term_pc": [0.04, 0.1, 0.6],
            }
        )

        aggregated_df = pd.DataFrame(
            index=pd.MultiIndex.from_arrays(
                [
                    ["US_IX", "US_IX", "US_IX", "US_IX", "US_IX"],
                    ["PROBATION", "PROBATION", "PROBATION", "TERM", "TERM"],
                    ["MALE", "FEMALE", "FEMALE", "MALE", "MALE"],
                    [
                        "0-20",
                        "0-22",
                        "0-22",
                        "0-20",
                        "0-20",
                    ],
                    ["Assault", "Bribery", "Bribery", "Burglary", "Burglary"],
                ],
                names=[
                    "state_code",
                    "cohort_group",
                    "gender",
                    "assessment_score_bucket",
                    "most_severe_description",
                ],
            ),
            data={
                "cohort_months": [0, 0, 3, 0, 3],
                "cohort_size": [50, 25, 25, 15, 15],
                "event_rate_dict": [
                    {
                        "cohort_months": 0,
                        "event_rate": 0.0,
                        "lower_ci": 0.0,
                        "upper_ci": 0.0,
                    },
                    {
                        "cohort_months": 0,
                        "event_rate": 0.0,
                        "lower_ci": 0.0,
                        "upper_ci": 0.0,
                    },
                    {
                        "cohort_months": 3,
                        "event_rate": 0.1,
                        "lower_ci": 0.0,
                        "upper_ci": 0.2,
                    },
                    {
                        "cohort_months": 0,
                        "event_rate": 0.0,
                        "lower_ci": 0.0,
                        "upper_ci": 0.0,
                    },
                    {
                        "cohort_months": 3,
                        "event_rate": 0.2,
                        "lower_ci": 0.15,
                        "upper_ci": 0.25,
                    },
                ],
            },
        )

        final_table = write_case_insights_data_to_bq.create_final_table(
            aggregated_df, disposition_df
        )

        expected_final_table = pd.DataFrame(
            {
                "state_code": ["US_IX", "US_IX", "US_IX"],
                "gender": ["FEMALE", "MALE", "MALE"],
                "assessment_score_bucket": ["0-22", "0-20", "0-20"],
                "most_severe_description": ["Bribery", "Assault", "Burglary"],
                "recidivism_probation_series": [
                    '[{"cohort_months": 0, "event_rate": 0.0, "lower_ci": 0.0, "upper_ci": 0.0},'
                    + ' {"cohort_months": 3, "event_rate": 0.1, "lower_ci": 0.0, "upper_ci": 0.2}]',
                    '[{"cohort_months": 0, "event_rate": 0.0, "lower_ci": 0.0, "upper_ci": 0.0}]',
                    float("nan"),
                ],
                "recidivism_term_series": [
                    float("nan"),
                    float("nan"),
                    '[{"cohort_months": 0, "event_rate": 0.0, "lower_ci": 0.0, "upper_ci": 0.0},'
                    + ' {"cohort_months": 3, "event_rate": 0.2, "lower_ci": 0.15, "upper_ci": 0.25}]',
                ],
                "cohort_size": [25, 50, 15],
                "disposition_probation_pc": [0.9, 0.6, 0.0],
                "disposition_rider_pc": [0.06, 0.3, 0.4],
                "disposition_term_pc": [0.04, 0.1, 0.6],
                "disposition_num_records": [25, 50, 15],
                "recidivism_num_records": [25, 50, 15],
                "recidivism_rollup": ["Bribery", "Assault", "Burglary"],
            }
        )

        pd.testing.assert_frame_equal(expected_final_table, final_table)
