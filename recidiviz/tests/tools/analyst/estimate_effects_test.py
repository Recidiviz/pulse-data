# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for effects estimation methods"""

import unittest

import pandas as pd

from recidiviz.tools.analyst.estimate_effects import validate_df

# create simulated dataframes for testing
_DUMMY_DF = pd.DataFrame(
    {
        "outcome": [1, 2, 3, 4, 5],
        "unit_of_analysis": ["a", "b", "c", "d", "e"],
        "start_date": pd.to_datetime(["2020-01-01"] * 5),
        "weights": [1] * 5,
        "other_column": ["a", "b", "c", "d", "e"],
        "other_column_2": ["a", "b", "c", "d", "e"],
        "excluded_column": ["a", "b", "c", "d", "e"],
        "bad_outcome": ["a", "b", "c", "d", "e"],
        "bad_unit_of_analysis": [1, 2, 3, 4, 5],
        "bad_start_date": [1, 2, 3, 4, 5],
    }
)
_EXPECTED_COLUMNS = [
    "outcome",
    "unit_of_analysis",
    "start_date",
    "weights",
]
_OTHER_COLUMNS = ["other_column", "other_column_2"]


class TestValidateDf(unittest.TestCase):
    """Tests for validate_df() method in estimate_effects.py"""

    def test_validate_df(self) -> None:
        """Verify that validate_df() works as expected"""

        # get output of validate_df() run on dummy df
        validated_df = validate_df(
            df=_DUMMY_DF,
            outcome_column="outcome",
            unit_of_analysis_column="unit_of_analysis",
            date_column="start_date",
            weight_column="weights",
            other_columns=_OTHER_COLUMNS,
        )

        # check that validate_df() returns a df
        self.assertIsInstance(validated_df, pd.DataFrame)

        # verify correct columns are returned
        self.assertListEqual(
            validated_df.columns.tolist(),
            _EXPECTED_COLUMNS + _OTHER_COLUMNS,
        )

        # verify correct number of rows are returned
        self.assertEqual(validated_df.shape[0], _DUMMY_DF.shape[0])

    def test_validate_df_no_weights(self) -> None:
        """Verify that validate_df() works as expected when no weights are provided"""

        # get output of validate_df() run on dummy df
        validated_df = validate_df(
            df=_DUMMY_DF,
            outcome_column="outcome",
            unit_of_analysis_column="unit_of_analysis",
            date_column="start_date",
            other_columns=_OTHER_COLUMNS,
        )

        # verify correct columns are returned
        self.assertListEqual(
            validated_df.columns.tolist(),
            _EXPECTED_COLUMNS + _OTHER_COLUMNS,
        )

        # verify that weights are added
        self.assertListEqual(validated_df["weights"].tolist(), [1] * _DUMMY_DF.shape[0])

    def test_validate_df_no_other_columns(self) -> None:
        """Verify that validate_df() works as expected when no other columns are provided"""

        # get output of validate_df() run on dummy df
        validated_df = validate_df(
            df=_DUMMY_DF,
            outcome_column="outcome",
            unit_of_analysis_column="unit_of_analysis",
            date_column="start_date",
        )

        # verify correct columns are returned
        self.assertListEqual(
            validated_df.columns.tolist(),
            _EXPECTED_COLUMNS,
        )

    def test_validate_df_wrong_types(self) -> None:
        """Validate that error is thrown if wrong datatypes present in key columns"""

        # non-numeric outcome column
        with self.assertRaises(TypeError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="bad_outcome",
                unit_of_analysis_column="unit_of_analysis",
                date_column="start_date",
            )

        # non-string unit_of_analysis column
        with self.assertRaises(TypeError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="outcome",
                unit_of_analysis_column="bad_unit_of_analysis",
                date_column="start_date",
            )

        # non-datetime date column
        with self.assertRaises(TypeError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="outcome",
                unit_of_analysis_column="unit_of_analysis",
                date_column="bad_start_date",
            )

    def test_validate_df_missing_columns(self) -> None:
        """Validate that error is thrown if missing key columns in df"""

        # missing outcome column
        with self.assertRaises(ValueError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="missing_outcome",
                unit_of_analysis_column="unit_of_analysis",
                date_column="start_date",
            )

        # missing unit_of_analysis column
        with self.assertRaises(ValueError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="outcome",
                unit_of_analysis_column="missing_unit_of_analysis",
                date_column="start_date",
            )

        # missing date column
        with self.assertRaises(ValueError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="outcome",
                unit_of_analysis_column="unit_of_analysis",
                date_column="missing_date",
            )
