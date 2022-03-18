# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for big_query_utils.py"""
import unittest
from datetime import date, time

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from recidiviz.big_query.big_query_utils import (
    make_bq_compatible_types_for_df,
    normalize_column_name_for_bq,
)


class BigQueryUtilsTest(unittest.TestCase):
    """TestCase for BigQuery utils"""

    def setUp(self) -> None:
        self.head_whitespace = "  FIELD_NAME_532"
        self.tail_whitespace = "FIELD_NAME_532  "
        self.non_printable_characters = "FIELD_\x16NAME_532"
        self.name_with_spaces = "FIELD NAME 532"
        self.name_with_chars = "FIELD?NAME*532"
        self.valid_column_name = "FIELD_NAME_532"
        self.column_names = [
            self.head_whitespace,
            self.tail_whitespace,
            self.non_printable_characters,
            self.name_with_spaces,
            self.name_with_chars,
            self.valid_column_name,
        ]

        self.df = pd.DataFrame(
            {
                "string_col": [None, "val a", "Y"],
                "int_col": [2, 3, 10],
                "time_col": ["4:56:00", "12:34:56", None],
                "date_col": pd.Series(
                    ["2022-01-01 01:23:45", None, "2022-03-04"],
                    dtype="datetime64[ns]",
                ),
                "bool_col": [None, "Y", "N"],
            }
        )

    def test_normalize_column_name_for_bq_lead_whitespace(self) -> None:
        for column_name in self.column_names:
            normalized = normalize_column_name_for_bq(column_name)
            self.assertEqual(normalized, self.valid_column_name)

    def test_make_bq_compatible_types_for_df_convert_to_date(self) -> None:
        df = make_bq_compatible_types_for_df(
            self.df,
            convert_datetime_to_date=True,
            bool_map={"Y": True, "N": False},
        )
        expected_df = pd.DataFrame(
            {
                "string_col": pd.Series([pd.NA, "val a", "Y"], dtype="string"),
                "int_col": pd.Series([2, 3, 10], dtype="Int64"),
                "time_col": [time(4, 56), time(12, 34, 56), pd.NaT],
                "date_col": [date(2022, 1, 1), pd.NA, date(2022, 3, 4)],
                "bool_col": [pd.NA, True, False],
            }
        )

        assert_frame_equal(df, expected_df, check_column_type=True)

    def test_make_bq_compatible_types_for_df_no_convert_to_date(self) -> None:
        df = make_bq_compatible_types_for_df(
            self.df,
            convert_datetime_to_date=False,
            bool_map={"Y": True, "N": False},
        )
        expected_df = pd.DataFrame(
            {
                "string_col": pd.Series([pd.NA, "val a", "Y"], dtype="string"),
                "int_col": pd.Series([2, 3, 10], dtype="Int64"),
                "time_col": [time(4, 56), time(12, 34, 56), pd.NaT],
                "date_col": [
                    np.datetime64("2022-01-01 01:23:45"),
                    pd.NaT,
                    np.datetime64("2022-03-04"),
                ],
                "bool_col": [pd.NA, True, False],
            }
        )

        assert_frame_equal(df, expected_df, check_column_type=True)
