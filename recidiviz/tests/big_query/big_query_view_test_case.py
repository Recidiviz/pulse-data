# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Utility class for testing BQ views against Postgres"""
import datetime
import logging
import re
import unittest
from typing import Dict, List, Sequence, Type, Union

import pandas as pd
import pytest
from more_itertools import one
from pandas._testing import assert_frame_equal

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.tests.big_query.fakes.fake_big_query_database import FakeBigQueryDatabase
from recidiviz.tests.big_query.fakes.fake_table_schema import PostgresTableSchema
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils import csv


@pytest.mark.uses_db
class BigQueryViewTestCase(unittest.TestCase):
    """This is a utility class that allows BQ views to be tested using Postgres instead.

    This is NOT fully featured and has some shortcomings, most notably:
    1. It uses naive regexes to rewrite parts of the query. This works for the most part but may produce invalid
       queries in some cases. For instance, the lazy capture groups may capture the wrong tokens in nested function
       calls.
    2. Postgres can only use ORDINALS when unnesting and indexing into arrays, while BigQuery uses OFFSETS (or both).
       This does not translate the results (add or subtract one). So long as the query consistently uses one or the
       other, it should produce correct results.
    3. This does not (yet) support chaining of views. To test a view query, any tables or views that it queries from
       must be created and seeded with data using `create_table`.
    4. Not all BigQuery SQL syntax has been translated, and it is possible that some features may not have equivalent
       functionality in Postgres and therefore can't be translated.

    Given these, it may not make sense to use this for all of our views. If it prevents you from using BQ features that
    would be helpful, or creates more headaches than value it provides, it may not be necessary.
    """

    temp_db_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        # View specific regex patterns to replace in the BigQuery SQL for the Postgres server. These are applied before
        # the rest of the SQL rewrites.
        self.sql_regex_replacements: Dict[str, str] = {}
        self.fake_bq_db = FakeBigQueryDatabase()

    def tearDown(self) -> None:
        self.fake_bq_db.teardown_databases()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def create_mock_bq_table(
        self,
        dataset_id: str,
        table_id: str,
        mock_schema: PostgresTableSchema,
        mock_data: pd.DataFrame,
    ) -> None:
        self.fake_bq_db.create_mock_bq_table(
            dataset_id=dataset_id,
            table_id=table_id,
            mock_schema=mock_schema,
            mock_data=mock_data,
        )

    def create_view(self, view_builder: BigQueryViewBuilder) -> None:
        self.fake_bq_db.create_view(view_builder)

    def query_view(
        self, table_address: BigQueryAddress, view_query: str
    ) -> pd.DataFrame:
        if self.sql_regex_replacements:
            for bq_sql_regex_pattern, pg_sql in self.sql_regex_replacements.items():
                view_query = re.sub(bq_sql_regex_pattern, pg_sql, view_query)

        results = self.fake_bq_db.run_query(view_query)

        # Log results to debug log level, to see them pass --log-level DEBUG to pytest
        logging.debug("Results for `%s`:\n%s", table_address, results.to_string())
        return results

    def query_view_for_builder(
        self,
        view_builder: BigQueryViewBuilder,
        data_types: Dict[str, Union[Type, str]],
        dimensions: List[str],
    ) -> pd.DataFrame:
        if isinstance(view_builder, DirectIngestPreProcessedIngestViewBuilder):
            raise ValueError(
                f"Found view builder type [{type(view_builder)}] - use "
                f"query_ingest_view_for_builder() for this type instead."
            )

        view: BigQueryView = view_builder.build()
        results = self.query_view(view.table_for_query, view.view_query)

        # TODO(#5533): If we add `dimensions` to all `BigQueryViewBuilder`, instead of
        # just `MetricBigQueryViewBuilder`, then we can reuse that here instead of
        # forcing the caller to specify them manually.

        return self.apply_types_and_sort(
            results, data_types=data_types, sort_dimensions=dimensions
        )

    @staticmethod
    def apply_types_and_sort(
        df: pd.DataFrame,
        data_types: Dict[str, Union[Type, str]],
        sort_dimensions: List[str],
    ) -> pd.DataFrame:
        if not data_types:
            raise ValueError("Found empty data_types")

        if not sort_dimensions:
            raise ValueError("Found empty dimensions")

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

    def query_view_chain(
        self,
        view_builders: Sequence[BigQueryViewBuilder],
        data_types: Dict[str, Union[Type, str]],
        dimensions: List[str],
    ) -> pd.DataFrame:
        for view_builder in view_builders[:-1]:
            self.create_view(view_builder)
        return self.query_view_for_builder(view_builders[-1], data_types, dimensions)

    @staticmethod
    def fixture_comparison_data_type_for_column(
        df: pd.DataFrame, column: str
    ) -> Union[Type, pd.StringDtype]:
        """Inspects data in the provided |column| in |df| to determine the column data
        type (dtype) that should be used to compare this column in |df| to a column in
        a DataFrame read from a fixture file.
        """
        column_value_types = {
            type(val) for val in df[column].tolist() if val is not None
        }

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

        if python_type in (
            str,
            # Boolean values are represented in fixtures as 'True' and 'False' -
            # we convert these back to strings in the result for comparison
            # to fixture data.
            bool,
            # Pandas doesn't support dates like 9999-12-31 which are often
            # present in query results (and valid in BQ). We convert these types
            # back to strings for comparison.
            datetime.datetime,
            datetime.date,
            # Collection types are not hashable so there are issues with setting
            # collection type columns as index columns.
            list,
            set,
            dict,
        ):
            return pd.StringDtype()

        raise ValueError(
            f"Found unhandled data type [{python_type}] for column [{column}] "
            f"with values [{df[column].tolist()}]"
        )

    def compare_results_to_fixture(
        self, results: pd.DataFrame, expected_output_fixture_path: str
    ) -> None:
        """Compares the results in the given Dataframe that have been presumably read
        out of a local, fake BQ instance (i.e. backed by Postgres) and compares them
        to the data in the fixture file at the provided path.
        """

        print(f"Loading expected results from path [{expected_output_fixture_path}]")
        expected_output = list(
            csv.get_rows_as_tuples(expected_output_fixture_path, skip_header_row=False)
        )

        expected_columns = [column.lower() for column in expected_output.pop(0)]
        expected = pd.DataFrame(expected_output, columns=expected_columns)

        if sorted(results.columns) != sorted(expected.columns):
            raise ValueError(
                f"Columns in expected and actual results do not match (order agnostic). "
                f"Expected: {expected.columns}. Actual: {results.columns}."
            )

        data_types = {
            column: self.fixture_comparison_data_type_for_column(results, column)
            for column in results.columns.tolist()
        }
        dimensions = results.columns.tolist()

        # Reorder the columns in the expected results to match the actual result column
        # order.
        expected = expected[results.columns]

        # Nulls in fixture files are represented as the empty string - replace with
        # real nulls.
        expected = expected.applymap(lambda x: None if x == "" else x)
        expected = self.apply_types_and_sort(expected, data_types, dimensions)

        results = self.apply_types_and_sort(results, data_types, dimensions)

        print("**** EXPECTED ****")
        print(expected.info())
        print(expected)
        print("**** ACTUAL ****")
        print(results.info())
        print(results)
        assert_frame_equal(expected, results)
