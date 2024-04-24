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
"""Defines a base interface shared by the postgres and bigquery test implementations."""

import abc
import datetime
import logging
from typing import Dict, List, Type, Union

import db_dtypes
import numpy
import pandas as pd
import sqlglot
import sqlglot.expressions
from more_itertools import one
from pandas._testing import assert_frame_equal

DTYPES = {
    "integer": {int, pd.Int64Dtype, numpy.dtypes.Int64DType, numpy.int64},
    "bool": {bool, pd.BooleanDtype, numpy.dtypes.BoolDType, numpy.bool_},
}


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
                f"Columns in expected and actual results do not match (order "
                f"agnostic).\n"
                f"Expected results to contain these columns, but they did not:\n"
                f"{set(expected.columns).difference(results.columns)}\n"
                f"Results contained these additional columns that were not expected:\n"
                f"{set(results.columns).difference(expected.columns)}\n"
                f"Full results:\n"
                f"Expected: {expected.columns}.\n"
                f"Actual: {results.columns}.\n"
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
            if (
                data_types.get(col) in DTYPES["bool"]
                and type(df[col].dtype) not in DTYPES["bool"]
            ):
                # Pandas interprets "true" and "false" string values as truthy, so will
                # convert these incorrectly when converting the column to a bool type.
                # We map here to boolean True/False values so that the column type can
                # be converted properly.
                df[col] = df[col].map(
                    {
                        "true": True,
                        "false": False,
                        "True": True,
                        "False": False,
                        True: True,
                        False: False,
                    }
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
            # There are no values in this column, defer decision to results dataframe
            column_value_types.add(type(df.dtypes[column]))

        if len(column_value_types) > 1:
            raise ValueError(
                f"Found multiple conflicting types [{column_value_types}] in column "
                f"[{column}] with values: {df[column].tolist()}"
            )

        python_type = one(column_value_types)

        if python_type == float:
            return float

        if python_type in DTYPES["integer"]:
            # Columns that contain NaN cannot be coerced to integer, must use float
            if has_null_values:
                return float

            return int

        if python_type in DTYPES["bool"]:
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
            numpy.datetime64,
            db_dtypes.DateDtype,
            numpy.dtypes.ObjectDType,
            numpy.dtypes.DateTime64DType,
            # Collection types are not hashable so there are issues with setting
            # collection type columns as index columns.
            list,
            set,
            dict,
            int,
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


def check_for_ctes_with_no_comments(query: str, query_name: str) -> None:
    """
    Raises a ValueError with all CTEs that do not have a comment.

    Args:
        query: (str) is the actual text of the query.
        query_name: (str) is a helpful identifier of the query if/when there
                    is an error.

    We expect to have queries documented like:
        WITH
        -- this explains table 1
        table_1 AS (
            SELECT * FROM A
        ),
        -- this explains table 2
        table_2 AS (
            SELECT * FROM B JOIN C USING(col)
        )
        SELECT * FROM table_1 UNION ALL SELECT * FROM table_2
    """
    tree = sqlglot.parse_one(query, dialect="bigquery")
    if not isinstance(tree, sqlglot.expressions.Query):
        raise ValueError("Non-Query SQL expression built from ViewBuilder")
    undocumented_ctes = ", ".join(
        cte.alias
        for cte in tree.ctes
        # TODO(#29272) Update DirectIngestViewQueryBuilder to self document generated views
        if "generated_view" not in cte.alias and not cte.args["alias"].comments
    )
    if undocumented_ctes:
        raise ValueError(
            f"Query {query_name} has undocumented CTEs: {undocumented_ctes}"
        )
