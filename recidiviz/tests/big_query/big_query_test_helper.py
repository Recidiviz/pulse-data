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
"""Defines a base interface shared by the postgres and bigquery test implementations."""

import abc
import datetime
import logging
from typing import Dict, List, Type, Union

import pandas as pd
from more_itertools import one
from pandas._testing import assert_frame_equal


# TODO(#15020): Get rid of this interface once the postgres implementation is deleted.
class BigQueryTestHelper:
    """Interface for functions needed by tests that use BigQuery."""

    @abc.abstractmethod
    def query(self, query: str) -> pd.DataFrame:
        """Returns results from the given query"""

    @classmethod
    def compare_expected_and_result_dfs(
        cls, *, expected: pd.DataFrame, results: pd.DataFrame
    ) -> None:
        """Compares the results in the |expected| dataframe to |results|."""
        if sorted(results.columns) != sorted(expected.columns):
            raise ValueError(
                f"Columns in expected and actual results do not match (order agnostic). "
                f"Expected: {expected.columns}. Actual: {results.columns}."
            )

        data_types = {
            column: BigQueryTestHelper.fixture_comparison_data_type_for_column(
                results, column
            )
            for column in results.columns.tolist()
        }
        dimensions = results.columns.tolist()

        # Reorder the columns in the expected results to match the actual result column
        # order.
        expected = expected[results.columns]

        expected = cls.apply_types_and_sort(expected, data_types, dimensions)

        results = cls.apply_types_and_sort(results, data_types, dimensions)

        print("**** EXPECTED ****")
        print(expected.info())
        print(expected)
        print("**** ACTUAL ****")
        print(results.info())
        print(results)
        assert_frame_equal(expected, results)

    @staticmethod
    def apply_types_and_sort(
        df: pd.DataFrame,
        data_types: Dict[str, Union[Type, str]],
        sort_dimensions: List[str],
    ) -> pd.DataFrame:
        """Converts the columns in the provided dataframe to the provided datatypes and
        sorts the rows according the columns in |sort_dimensions|.
        """
        if not data_types:
            raise ValueError("Found empty data_types")

        if not sort_dimensions:
            raise ValueError("Found empty dimensions")

        for col in df.columns:
            if data_types.get(col) == bool and df[col].dtype != bool:
                # Pandas interprets "true" and "false" string values as truthy, so will
                # convert these incorrectly when converting the column to a bool type.
                # We map here to boolean True/False values so that the column type can
                # be converted properly.
                df[col] = df[col].map(
                    {"true": True, "false": False, "True": True, "False": False}
                )

        # Convert values in dataframe to specified types.
        df = df.astype(data_types)

        # Sets the dimension columns as index columns so we can sort by the values
        # in those columns. This REMOVES the columns from the dataframe data.
        df = df.set_index(sort_dimensions)
        df = df.sort_index()

        # Remove the index columns, which adds those columns back into the Dataframe
        # data.
        df = df.reset_index()

        return df

    @staticmethod
    def fixture_comparison_data_type_for_column(
        df: pd.DataFrame, column: str
    ) -> Union[Type, pd.StringDtype]:
        """Inspects data in the provided |column| in |df| to determine the column data
        type (dtype) that should be used to compare this column in |df| to a column in
        a DataFrame read from a fixture file.
        """
        column_value_types = {
            # Collect types, filtering out None, pd.NaT, np.nan values.
            type(val)
            for val in df[column].tolist()
            if isinstance(val, list) or not pd.isnull(val)
        }

        has_null_values = any(
            not isinstance(val, list) and pd.isnull(val) for val in df[column].tolist()
        )

        if len(column_value_types) == 0:
            # There are no values in this column so we can't conclude what the type
            # is.
            return object

        if len(column_value_types) > 1:
            raise ValueError(
                f"Found multiple conflicting types [{column_value_types}] in column "
                f"[{column}] with values: {df[column].tolist()}"
            )

        python_type = one(column_value_types)

        if python_type == int:
            return int

        if python_type == float:
            return float

        if python_type == bool:
            if has_null_values:
                # Pandas does not allow null values in boolean columns - nulls will get
                # converted to False. If we want to mimic a NULLABLE boolean column, we
                # have to convert to strings for comparison.
                return pd.StringDtype()
            return bool

        # TODO(#21124): pass in flag to conditionally compare datetimes as datetimes (and not strings).
        if python_type in (
            str,
            # Pandas doesn't support dates like 9999-12-31 which are often
            # present in query results (and valid in BQ). We convert these types
            # back to strings for comparison.
            datetime.datetime,
            datetime.date,
            pd.Timestamp,
            # Collection types are not hashable so there are issues with setting
            # collection type columns as index columns.
            list,
            set,
            dict,
        ):
            print(f"PYTHON TYPE for {column}: {python_type}")
            return pd.StringDtype()

        raise ValueError(
            f"Found unhandled data type [{python_type}] for column [{column}] "
            f"with values [{df[column].tolist()}]"
        )


def query_view(
    helper: BigQueryTestHelper, view_name: str, view_query: str
) -> pd.DataFrame:
    """Returns results from view based on the given query"""
    results = helper.query(view_query)
    # Log results to debug log level, to see them pass --log-level DEBUG to pytest
    logging.debug("Results for `%s`:\n%s", view_name, results.to_string())
    return results
