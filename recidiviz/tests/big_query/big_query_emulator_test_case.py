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
"""An implementation of TestCase that can be used for tests that talk to the BigQuery
emulator.
"""
import datetime
import os
import tempfile
import unittest
from concurrent import futures
from typing import Any, Dict, Iterable, List
from unittest.mock import Mock, patch

import db_dtypes
import grpc
import numpy
import pandas as pd
import pytest
import requests
from google.api_core.exceptions import GoogleAPICallError, from_http_response
from google.cloud import bigquery
from google.cloud.bigquery_storage_v1 import BigQueryReadClient
from google.cloud.bigquery_storage_v1.services.big_query_read.transports import (
    BigQueryReadGrpcTransport,
)
from more_itertools import one
from pandas._testing import assert_frame_equal
from pandas_gbq import read_gbq as og_read_gbq

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClientImpl,
)
from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_input_schema_json import (
    write_emulator_source_tables_json,
)
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID
from recidiviz.tests.utils.big_query_emulator_control import BigQueryEmulatorControl
from recidiviz.tests.utils.big_query_emulator_log_parser import (
    BigQueryEmulatorLogParser,
)
from recidiviz.utils import environment, metadata

DTYPES = {
    "integer": {int, pd.Int64Dtype, numpy.dtypes.Int64DType, numpy.int64},
    "bool": {bool, pd.BooleanDtype, numpy.dtypes.BoolDType, numpy.bool_},
}


def _fail_500(response: requests.Response) -> None:
    """The BigQuery client retries when it receives a 500 from google.
    However, using the emulator means we end up in an infinite try loop.
    This function is used to mock the underlying error response and
    fails if it is a 500 response.
    """
    original_error: GoogleAPICallError = from_http_response(response)
    if original_error.response.status_code == 500:
        raise RuntimeError(
            "The BigQueryEmulator has failed with a 500 status code. "
            "To investigate: set the class attribute "
            "show_emulator_logs_on_failure=True, re-run, and then check emulator's logs. "
            f"Original error message: {original_error.message}"
        )
    raise original_error


@pytest.mark.uses_bq_emulator
class BigQueryEmulatorTestCase(unittest.TestCase):
    """An implementation of TestCase that can be used for tests that talk to the
    BigQuery emulator.

    DISCLAIMER: The BQ emulator currently supports a large subset of BigQuery SQL
    features, but not all of them. If you are trying to use the emulator and running
    into issues, you should post in #platform-team.
    """

    control: BigQueryEmulatorControl

    # Deletes all tables / views in the emulator after each test
    # Subclasses can choose to override this as it may not always be necessary
    wipe_emulator_data_on_teardown = True

    # Subclasses can override this to prevent rebuilding of input JSON
    input_json_schema_path: str | None = None

    # Subclasses can override this to keep the input file when debugging tests
    delete_json_input_schema_on_teardown = True

    # If the test failed, output the emulator logs prior to exiting
    show_emulator_logs_on_failure = False

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return []

    @classmethod
    def setUpClass(cls) -> None:
        cls.control = BigQueryEmulatorControl.build()
        cls.control.pull_image()

        input_schema_json_path = None
        if cls.input_json_schema_path is not None:
            input_schema_json_path = cls.input_json_schema_path
        elif source_tables := cls.get_source_tables():
            with tempfile.NamedTemporaryFile(
                dir=os.path.join(os.path.dirname(__file__), "fixtures"), delete=False
            ) as file:
                cls.input_json_schema_path = file.name
                input_schema_json_path = write_emulator_source_tables_json(
                    source_table_collections=source_tables,
                    file_name=cls.input_json_schema_path,
                )

        cls.control.start_emulator(input_schema_json_path=input_schema_json_path)

    def setUp(self) -> None:
        self.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id",
            Mock(return_value=BQ_EMULATOR_PROJECT_ID),
        )
        self.project_id = self.project_id_patcher.start().return_value
        self.bq_error_handling_patcher = patch(
            "google.cloud.exceptions.from_http_response", _fail_500
        )
        self.bq_error_handling_patcher.start()
        self.bq_client = BigQueryClientImpl()
        self.bq_client.apply_row_level_permissions = Mock(  # type: ignore
            return_value="Row-level permissions not supported in BQ Emulator"
        )
        self.read_gbq_patcher = patch(
            "pandas_gbq.read_gbq",
            self._read_gbq_with_emulator,
        )
        self.read_gbq_patcher.start()
        self.to_gbq_patcher = patch(
            "pandas_gbq.to_gbq",
            self._fail_to_gbq_call,
        )
        self.to_gbq_patcher.start()

        # Patch BigQuery Client to always create a new emulator storage client
        def _create_bqstorage_client() -> Any:
            channel = grpc.insecure_channel(f"localhost:{self.control.grpc_port}")
            transport = BigQueryReadGrpcTransport(channel=channel)
            return BigQueryReadClient(
                transport=transport,
                client_options={
                    "api_endpoint": f"localhost:{self.control.grpc_port}",
                },
            )

        self.bqstorage_patcher = patch.object(
            bigquery.Client,
            "_ensure_bqstorage_client",
            side_effect=_create_bqstorage_client,
        )
        self.bqstorage_patcher.start()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        if self.wipe_emulator_data_on_teardown:
            self._wipe_emulator_data()
        self.bq_error_handling_patcher.stop()
        self.read_gbq_patcher.stop()
        self.to_gbq_patcher.stop()
        self.bqstorage_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        logs = cls.control.get_logs()

        parser = BigQueryEmulatorLogParser()
        parser.parse_logs(logs)
        print(f"\n\nStats for {cls.__name__}")
        print("=" * 80)
        parser.print_stats(n=10)
        print("=" * 80)

        if cls.show_emulator_logs_on_failure:
            print(logs)

        cls.control.stop_emulator()

        if cls.input_json_schema_path and cls.delete_json_input_schema_on_teardown:
            os.remove(cls.input_json_schema_path)

    def query(self, query: str) -> pd.DataFrame:
        return self.bq_client.run_query_async(
            query_str=query, use_query_cache=True
        ).to_dataframe()

    def _read_gbq_with_emulator(self, *args, **kwargs):  # type: ignore
        return og_read_gbq(
            *args,
            **kwargs,
            bigquery_client=self.bq_client.client,
        )

    # Ran into some issues following the same pattern for reading with pandas.
    # I think this is because writing to BigQuery potentially needs access to other
    # resources like GCS buckets (depending on how the data is written).
    # It's really easy to just write to *actual* BigQuery on accident though,
    # so raising an error will help prevent that.
    def _fail_to_gbq_call(self, *args, **kwargs):  # type: ignore
        raise RuntimeError(
            "Writing to the emulator from pandas is not currently supported."
        )

    def _clear_emulator_table_data(self) -> None:
        """Clears the data out of emulator tables but does not delete any tables."""
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            to_delete = [
                executor.submit(
                    self.bq_client.delete_from_table_async,
                    BigQueryAddress(
                        dataset_id=dataset_list_item.dataset_id,
                        table_id=table_list_item.table_id,
                    ),
                )
                for dataset_list_item in self.bq_client.list_datasets()
                for table_list_item in self.bq_client.list_tables(
                    dataset_list_item.dataset_id
                )
            ]

        for future in futures.as_completed(to_delete):
            future.result()

    def _wipe_emulator_data(self) -> None:
        """Fully deletes all tables and datasets loaded into the emulator."""
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            to_delete = [
                executor.submit(
                    self.bq_client.delete_dataset,
                    dataset_list_item.dataset_id,
                    delete_contents=True,
                    not_found_ok=True,
                )
                for dataset_list_item in self.bq_client.list_datasets()
            ]

        for future in futures.as_completed(to_delete):
            future.result()

    def run_query_test(
        self,
        query_str: str,
        expected_result: Iterable[Dict[str, Any]],
        enforce_order: bool = True,
    ) -> None:
        query_job = self.bq_client.run_query_async(
            query_str=query_str, use_query_cache=True
        )
        contents_iterator: Iterable[Dict[str, Any]] = BigQueryResultsContentsHandle(
            query_job
        ).get_contents_iterator()
        if enforce_order:
            self.assertEqual(expected_result, list(contents_iterator))
        else:
            self.assertSetEqual(
                {frozenset(expected.items()) for expected in expected_result},
                {frozenset(actual.items()) for actual in contents_iterator},
            )

    def create_mock_table(
        self,
        address: BigQueryAddress,
        schema: List[bigquery.SchemaField],
        check_exists: bool | None = True,
        create_dataset: bool | None = True,
    ) -> None:
        if create_dataset:
            self.bq_client.create_dataset_if_necessary(address.dataset_id)

        if check_exists and self.bq_client.table_exists(address):
            raise ValueError(
                f"Table [{address}] already exists. Test cleanup not working properly."
            )

        self.bq_client.create_table_with_schema(address=address, schema_fields=schema)

    def load_rows_into_table(
        self,
        address: BigQueryAddress,
        data: List[Dict[str, Any]],
    ) -> None:
        self.bq_client.stream_into_table(address, rows=data)

    def compare_table_to_fixture(
        self,
        address: BigQueryAddress,
        columns_to_ignore: list[str],
        expected_output_fixture_path: str,
        expect_missing_fixtures_on_empty_results: bool,
        create_expected: bool,
        expect_unique_output_rows: bool,
    ) -> None:
        project_specific_address = address.to_project_specific_address(
            metadata.project_id()
        )

        if columns_to_ignore:
            columns_str = ", ".join(columns_to_ignore)
            query = project_specific_address.select_query(
                select_statement=f"SELECT * EXCEPT({columns_str})"
            )
        else:
            query = project_specific_address.select_query()

        table_contents_df = self.query(query)
        self.compare_results_to_fixture(
            results=table_contents_df,
            expected_output_fixture_path=expected_output_fixture_path,
            create_expected=create_expected,
            expect_missing_fixtures_on_empty_results=expect_missing_fixtures_on_empty_results,
            expect_unique_output_rows=expect_unique_output_rows,
        )

    @classmethod
    def compare_results_to_fixture(
        cls,
        results: pd.DataFrame,
        expected_output_fixture_path: str,
        expect_missing_fixtures_on_empty_results: bool,
        create_expected: bool,
        expect_unique_output_rows: bool,
    ) -> None:
        """Compares the results in the given Dataframe that have been presumably read
        out of the BQ emulator and compares them to the data in the fixture file at the
        provided path.

        Args:
            results: A Dataframe containing the query results we want to compare to the
                provided fixture.
            expected_output_fixture_path: The path to the fixture file containing the
                expected results we expect to compare to the |results| Dataframe.
            expect_missing_fixtures_on_empty_results: If True, we expect a file to exist
                at |expected_output_fixture_path| if and only if the |results| Dataframe
                is empty.
            create_expected: If True, running this function will update all the
                |expected_output_fixture_path| to contain contents that match |results|.
        """

        fixture_should_not_exist = (
            results.empty and expect_missing_fixtures_on_empty_results
        )

        if create_expected:
            if environment.in_ci():
                raise AssertionError(
                    "`create_expected` should only be used when writing or updating the test."
                )

            if fixture_should_not_exist:
                if os.path.exists(expected_output_fixture_path):
                    os.remove(expected_output_fixture_path)
            else:
                # Make output directory if it doesn't yet exist
                output_directory = os.path.dirname(expected_output_fixture_path)
                os.makedirs(output_directory, exist_ok=True)

                sorted_results = cls.apply_types_and_sort(
                    results,
                    cls._get_fixture_comparison_data_types(results),
                    results.columns.tolist(),
                )
                sorted_results.to_csv(expected_output_fixture_path, index=False)

        if fixture_should_not_exist:
            if os.path.exists(expected_output_fixture_path):
                raise ValueError(
                    f"Found fixture [{expected_output_fixture_path}] but there were no "
                    f"results produced - this fixture should be deleted."
                )
            # Fixture does not exist and results are empty - check passes
            return
        pd.options.display.width = 9999
        pd.options.display.max_columns = 999
        pd.options.display.max_rows = 999
        pd.options.display.max_colwidth = 999

        if expect_unique_output_rows and results.duplicated().any():
            raise ValueError(
                f"Expected unique output rows in results, but found duplicates: "
                f"{expected_output_fixture_path=} \n\n {results}"
            )

        print(f"Loading expected results from path [{expected_output_fixture_path}]")
        expected = load_dataframe_from_path(
            expected_output_fixture_path, fixture_columns=None, allow_comments=True
        )
        cls.compare_expected_and_result_dfs(expected=expected, results=results)

    @staticmethod
    def _get_fixture_comparison_data_types(
        results: pd.DataFrame,
    ) -> Dict[str, type | str]:
        return {
            column: BigQueryEmulatorTestCase.fixture_comparison_data_type_for_column(
                results, column
            )
            for column in results.columns.tolist()
        }

    @classmethod
    def compare_expected_and_result_dfs(
        cls, *, expected: pd.DataFrame, results: pd.DataFrame
    ) -> None:
        """Compares the results in the |expected| dataframe to |results|.

        Args:
            expected: The expected dataframe from the fixture.
            results: The actual results dataframe.
        """
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
            column: BigQueryEmulatorTestCase.fixture_comparison_data_type_for_column(
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

        try:
            assert_frame_equal(expected, results)
        except Exception:
            print("**** EXPECTED ****")
            print(expected.to_csv(index=False))
            print("**** ACTUAL ****")
            print(results.to_csv(index=False))
            print("\n**** DETAILED ROW-BY-ROW DIFF ****")
            cls._print_row_by_row_diff(expected, results)
            raise

    @staticmethod
    def _print_row_by_row_diff(expected: pd.DataFrame, actual: pd.DataFrame) -> None:
        """Prints a detailed row-by-row diff showing which rows and columns differ.

        This method assumes both dataframes have already been normalized via
        apply_types_and_sort(), which ensures consistent data types and null handling.

        Args:
            expected: The expected dataframe.
            actual: The actual dataframe.
        """
        if len(expected) != len(actual):
            print(
                f"Row count mismatch: expected {len(expected)} rows, got {len(actual)} rows"
            )

        # Compare rows at matching indices
        matching_rows = 0
        differing_rows = []
        for idx in range(min(len(expected), len(actual))):
            expected_row = expected.iloc[idx]
            actual_row = actual.iloc[idx]

            # Check if rows are equal
            if expected_row.equals(actual_row):
                matching_rows += 1
            else:
                # Find which columns differ
                differing_cols = []
                for col in expected.columns:
                    exp_val = expected_row[col]
                    act_val = actual_row[col]

                    # Both nulls are considered equal
                    if pd.isna(exp_val) and pd.isna(act_val):
                        continue

                    # If one is null, or they're different, they're different
                    if (pd.isna(exp_val) or pd.isna(act_val)) or exp_val != act_val:
                        differing_cols.append((col, exp_val, act_val))

                differing_rows.append((idx, differing_cols))

        # Print summary
        print(f"Rows matching: {matching_rows} / {min(len(expected), len(actual))}")
        print(f"Rows differing: {len(differing_rows)}")

        if len(expected) != len(actual):
            print("  (Note: DataFrames have different lengths)")

        # Print details for differing rows (limit to first 20 for readability)
        if differing_rows:
            print("\nFirst differing rows:")
            for idx, differing_cols in differing_rows[:20]:
                print(f"\n  Row {idx}:")
                for col, exp_val, act_val in differing_cols:
                    print(f"    {col}:")
                    print(f"      Expected: {exp_val!r}")
                    print(f"      Actual:   {act_val!r}")

            if len(differing_rows) > 20:
                print(f"\n  ... and {len(differing_rows) - 20} more differing rows")

    @staticmethod
    def apply_types_and_sort(
        df: pd.DataFrame,
        data_types: Dict[str, type | str],
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
    ) -> type | pd.StringDtype:
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
