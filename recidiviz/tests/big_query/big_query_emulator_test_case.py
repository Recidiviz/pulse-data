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
import json
import os
import tempfile
import unittest
from collections import Counter, defaultdict
from concurrent import futures
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence
from unittest.mock import Mock, patch

import db_dtypes
import grpc
import numpy
import pandas as pd
import pytest
import requests
from google.api_core.exceptions import GoogleAPICallError, from_http_response
from google.cloud import bigquery
from google.cloud.bigquery import _helpers as bq_client_helpers
from google.cloud.bigquery import _pandas_helpers as bq_pandas_helpers
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
from recidiviz.big_query.big_query_schema_utils import strip_pseudocolumns_from_schema
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_input_schema_json import (
    SeededTable,
    write_emulator_source_tables_json,
)
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID
from recidiviz.tests.utils.big_query_emulator_control import BigQueryEmulatorControl
from recidiviz.tests.utils.big_query_emulator_log_parser import (
    BigQueryEmulatorLogParser,
)
from recidiviz.tests.utils.big_query_emulator_stats_formatter import (
    format_emulator_stats,
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

    # This can be set to False in the case of a feature the Go-based emulator supports
    # that zigquery does not
    use_zigquery = True

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

    # Addresses of the tables the emulator was seeded with at startup. These outlive
    # each test, so the per-test teardown leaves them in place. Populated by
    # setUpClass from get_seeded_tables().
    seeded_table_addresses: frozenset[BigQueryAddress] = frozenset()

    # Subclasses that boot the emulator themselves, e.g. with
    # restart_emulator_with_source_tables, can set this to False to skip the
    # class-level boot
    start_emulator_automatically = True

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        return []

    @classmethod
    def get_seeded_tables(cls) -> list[SeededTable]:
        """Returns tables the emulator should create and fill with data as it boots,
        instead of every test creating and loading them over HTTP.

        Seeded tables are shared by every test in the class and are not wiped between
        tests, so only seed data that no test writes to.
        """
        return []

    @classmethod
    def _write_source_tables_json(
        cls,
        source_tables: list[SourceTableCollection],
        seeded_tables: Sequence[SeededTable],
    ) -> str:
        """Returns the path of a new JSON file in the fixtures directory describing
        these collections' tables, for the emulator to load at boot.
        """
        with tempfile.NamedTemporaryFile(
            dir=os.path.join(os.path.dirname(__file__), "fixtures"), delete=False
        ) as file:
            return write_emulator_source_tables_json(
                source_table_collections=source_tables,
                file_name=file.name,
                seeded_tables=seeded_tables,
            )

    @classmethod
    def setUpClass(cls) -> None:
        cls.control = BigQueryEmulatorControl.build(use_zigquery=cls.use_zigquery)
        cls.control.pull_image()

        if not cls.start_emulator_automatically:
            return

        seeded_tables = cls.get_seeded_tables()
        cls.seeded_table_addresses = frozenset(
            seeded_table.address for seeded_table in seeded_tables
        )

        input_json_schema_path = None
        if cls.input_json_schema_path is not None:
            if seeded_tables:
                raise ValueError(
                    f"Class [{cls.__name__}] returns seeded tables but also sets "
                    f"input_json_schema_path [{cls.input_json_schema_path}]. Seeded "
                    f"tables are written into the input schema file, so a prebuilt "
                    f"one cannot be reused."
                )
            input_json_schema_path = cls.input_json_schema_path
        elif (source_tables := cls.get_source_tables()) or seeded_tables:
            cls.input_json_schema_path = (
                input_json_schema_path
            ) = cls._write_source_tables_json(
                source_tables,
                seeded_tables,
            )

        cls.control.start_emulator(input_schema_json_path=input_json_schema_path)
        try:
            cls._check_seeded_tables_were_populated(seeded_tables)
        except Exception:
            # Print the emulator logs, which list the rows that failed to load
            print(cls.control.get_logs())

            # A setUpClass that raises means tearDownClass never runs, which would
            # leave the emulator we just started holding its port for the rest of the
            # session - and every later test talking to whichever stale emulator the
            # kernel hands them.
            cls.control.stop_emulator()
            raise

    @classmethod
    def _check_seeded_tables_were_populated(
        cls, seeded_tables: Sequence[SeededTable]
    ) -> None:
        """Raises if the emulator came up without the rows it was seeded with.

        Row seeding is optional in the source table file format, and an emulator that
        does not implement it creates the table from the schema and drops the rows
        without complaint. Tests would then run against fixture tables that are
        silently empty, and fail somewhere far from the cause.
        """
        for seeded_table in seeded_tables:
            if not seeded_table.rows:
                continue
            address = seeded_table.address.to_project_specific_address(
                BQ_EMULATOR_PROJECT_ID
            )
            response = requests.post(
                f"http://0.0.0.0:{cls.control.port}/projects/"
                f"{BQ_EMULATOR_PROJECT_ID}/queries",
                json={
                    "query": address.select_query("SELECT COUNT(*)"),
                    "useLegacySql": False,
                },
                timeout=60,
            ).json()
            if not (rows := response.get("rows")):
                raise RuntimeError(
                    f"Could not count rows in seeded table "
                    f"[{seeded_table.address.to_str()}]: {response}"
                )
            if (seeded_row_count := int(one(one(rows)["f"])["v"])) != len(
                seeded_table.rows
            ):
                raise RuntimeError(
                    f"Emulator seeded [{seeded_row_count}] rows into [{seeded_table.address.to_str()}], "
                    f"but it was given [{len(seeded_table.rows)}]. "
                    f"A required field was likely omitted from a fixture. Check the emulator logs."
                )

    def restart_emulator_with_source_tables(
        self, source_tables: list[SourceTableCollection]
    ) -> None:
        """Stops the emulator and boots a fresh one seeded with these collections'
        tables. The emulator only accepts source tables at boot, so this is the
        only way to replace its loaded source tables mid-test.
        """
        self.control.stop_emulator()
        input_schema_json_path = self._write_source_tables_json(source_tables, [])
        try:
            self.control.start_emulator(input_schema_json_path=input_schema_json_path)
        finally:
            if self.delete_json_input_schema_on_teardown:
                os.remove(input_schema_json_path)

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

        # _download_table_bqstorage polls its single download-worker thread every 200ms.
        # With one stream — all the emulator ever offers — that first poll almost always
        # loses the race against the worker's OS-thread startup and eats the full 200ms
        # before the second pass finds the frame already there, dwarfing the
        # ~10ms it actually takes the emulator to answer.
        # This is a private constant in a third-party library, so a google-cloud-bigquery
        # upgrade could move or remove it without notice.
        self.progress_interval_patcher = patch.object(
            bq_pandas_helpers, "_PROGRESS_INTERVAL", 0.005
        )
        self.progress_interval_patcher.start()

        # The BQ python client always requests REST results with
        # formatOptions.useInt64Timestamp=true and parses TIMESTAMP cells
        # strictly as int64 micros. The emulator honors that format option for
        # top-level columns only — a TIMESTAMP nested inside a STRUCT or
        # repeated RECORD is still serialized as float seconds (e.g.
        # '1768003200.0'), which the client's int() parsing rejects. Widen the
        # parser to accept the emulator's float-seconds form; correctly
        # formatted int64 values parse exactly as before. Guarded by
        # TestBigQueryEmulator.test_timestamp_nested_in_struct*.
        # TODO(OBT-40415): Remove this patch once the emulator applies
        #  formatOptions.useInt64Timestamp to nested TIMESTAMPs.
        original_timestamp_to_py = bq_client_helpers.CellDataParser.timestamp_to_py

        def _timestamp_to_py_tolerating_float_seconds(
            parser_self: bq_client_helpers.CellDataParser,
            value: Any,
            field: bigquery.SchemaField,
        ) -> Any:
            if isinstance(value, str) and "." in value:
                value = str(round(float(value) * 1_000_000))
            return original_timestamp_to_py(parser_self, value, field)

        self.timestamp_parsing_patcher = patch.object(
            bq_client_helpers.CellDataParser,
            "timestamp_to_py",
            _timestamp_to_py_tolerating_float_seconds,
        )
        self.timestamp_parsing_patcher.start()

        # Patch get_table to remove pseudocolumns added by big_query_emulator_input_schema_json.py.
        original_get_table = BigQueryClientImpl.get_table

        def _get_table_without_pseudocolumns(
            client_self: BigQueryClientImpl, address: BigQueryAddress
        ) -> bigquery.Table:
            table = original_get_table(client_self, address)
            table.schema = strip_pseudocolumns_from_schema(table.schema)
            return table

        self.get_table_patcher = patch.object(
            BigQueryClientImpl,
            "get_table",
            _get_table_without_pseudocolumns,
        )
        self.get_table_patcher.start()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        if self.wipe_emulator_data_on_teardown:
            self._wipe_emulator_data()
        self.bq_error_handling_patcher.stop()
        self.read_gbq_patcher.stop()
        self.to_gbq_patcher.stop()
        self.bqstorage_patcher.stop()
        self.progress_interval_patcher.stop()
        self.timestamp_parsing_patcher.stop()
        self.get_table_patcher.stop()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if cls.control.container is not None:
                try:
                    cls._print_emulator_stats_and_logs()
                finally:
                    # Reporting is diagnostics only. If it raises, the container must
                    # still be removed, or it keeps holding this worker's port and
                    # every later test class on the worker fails to start an emulator.
                    cls.control.stop_emulator()
        finally:
            if cls.input_json_schema_path and cls.delete_json_input_schema_on_teardown:
                os.remove(cls.input_json_schema_path)

    @classmethod
    def _print_emulator_stats_and_logs(cls) -> None:
        """Prints the running emulator's query stats, and its logs when
        show_emulator_logs_on_failure is set."""
        logs = cls.control.get_logs()

        print(f"\n\nStats for {cls.__name__}")
        print("=" * 80)
        if cls.use_zigquery and not cls.control.is_running():
            print("Emulator is not running, so no stats are available.")
        elif cls.use_zigquery:
            stats = requests.get(
                f"http://localhost:{cls.control.port}/debug/stats", timeout=10
            ).json()
            print(format_emulator_stats(stats))
        else:
            parser = BigQueryEmulatorLogParser()
            parser.parse_logs(logs)
            parser.print_stats(n=10)
        print("=" * 80)

        if cls.show_emulator_logs_on_failure:
            print(logs)

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
                for table_list_item in self.bq_client.list_tables_excluding_views(
                    dataset_list_item.dataset_id
                )
            ]

        for future in futures.as_completed(to_delete):
            future.result()

    def _wipe_emulator_data(self) -> None:
        """Deletes all tables and datasets loaded into the emulator, other than the
        tables it was seeded with at startup.

        Seeded tables are read-only fixture data shared by every test in the class,
        so dropping them here would mean reloading them for the next test - which is
        the cost seeding exists to avoid.
        """
        seeded_table_ids_by_dataset: dict[str, set[str]] = defaultdict(set)
        for address in self.seeded_table_addresses:
            seeded_table_ids_by_dataset[address.dataset_id].add(address.table_id)

        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            to_delete = []
            for dataset_list_item in self.bq_client.list_datasets():
                dataset_id = dataset_list_item.dataset_id
                if dataset_id not in seeded_table_ids_by_dataset:
                    to_delete.append(
                        executor.submit(
                            self.bq_client.delete_dataset,
                            dataset_id,
                            delete_contents=True,
                            not_found_ok=True,
                        )
                    )
                    continue
                # The dataset holds seeded tables, so it has to stay - drop only the
                # tables this test added to it.
                seeded_table_ids = seeded_table_ids_by_dataset[dataset_id]
                to_delete.extend(
                    executor.submit(
                        self.bq_client.delete_table,
                        BigQueryAddress(
                            dataset_id=dataset_id,
                            table_id=table_list_item.table_id,
                        ),
                        not_found_ok=True,
                    )
                    for table_list_item in self.bq_client.list_tables(dataset_id)
                    if table_list_item.table_id not in seeded_table_ids
                )

        for future in futures.as_completed(to_delete):
            future.result()

    def _assert_rows_equal(
        self,
        expected: Iterable[Dict[str, Any]],
        actual: Iterable[Dict[str, Any]],
        *,
        enforce_order: bool,
    ) -> None:
        if enforce_order:
            self.assertEqual(list(expected), list(actual))
            return
        # Multiset, not set: identical rows must not collapse (e.g. at-least-once
        # writes produce intentional duplicates).
        self.assertEqual(
            Counter(frozenset(row.items()) for row in expected),
            Counter(frozenset(row.items()) for row in actual),
        )

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
        self._assert_rows_equal(
            expected_result, contents_iterator, enforce_order=enforce_order
        )

    def compare_table_to_results(
        self,
        address: BigQueryAddress,
        expected_result: Iterable[Dict[str, Any]],
        *,
        enforce_order: bool = False,
        columns_to_ignore: list[str] | None = None,
    ) -> None:
        """Asserts the full contents of |address| equal |expected_result|."""
        project_specific_address = address.to_project_specific_address(
            metadata.project_id()
        )
        select_statement = (
            f"SELECT * EXCEPT({', '.join(columns_to_ignore)})"
            if columns_to_ignore
            else "SELECT *"
        )
        self.run_query_test(
            project_specific_address.select_query(select_statement=select_statement),
            expected_result,
            enforce_order=enforce_order,
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

    def load_fixture_into_table(
        self,
        address: BigQueryAddress,
        schema: List[bigquery.SchemaField],
        fixture_path: Path | str,
        fixture_columns: list[str] | None,
        allow_comments: bool = True,
    ) -> None:
        """Creates a table and populates it with data from a CSV fixture file."""
        self.create_mock_table(address, schema=schema)
        self.load_fixture_into_existing_table(
            address,
            fixture_path,
            fixture_columns=fixture_columns,
            allow_comments=allow_comments,
        )

    def load_fixture_into_existing_table(
        self,
        address: BigQueryAddress,
        fixture_path: Path | str,
        *,
        fixture_columns: list[str] | None = None,
        allow_comments: bool = True,
    ) -> None:
        """Streams a CSV fixture's rows into an already-created table."""
        df = load_dataframe_from_path(
            fixture_path,
            fixture_columns=fixture_columns,
            allow_comments=allow_comments,
        )
        if not df.empty:
            self.load_rows_into_table(address, df.to_dict("records"))

    def compare_table_to_fixture(
        self,
        address: BigQueryAddress,
        columns_to_ignore: list[str],
        expected_output_fixture_path: str | Path,
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

    @staticmethod
    def _canonicalize_nested_value(value: Any) -> str:
        """Serializes a nested BigQuery value (a REPEATED or STRUCT column, which the
        emulator returns as a numpy array or Python dict, sometimes wrapping further
        numpy arrays and scalars) into a stable JSON string.

        Nested values are neither hashable (breaking the uniqueness check) nor
        round-trippable through CSV in their default `str()` form (numpy array reprs
        embed newlines and `dtype=` markers). A sorted-key JSON encoding gives one
        canonical string that both compares and round-trips cleanly.
        """

        def _default(obj: Any) -> Any:
            if isinstance(obj, numpy.ndarray):
                return obj.tolist()
            if isinstance(obj, numpy.generic):
                return obj.item()
            raise TypeError(f"Cannot canonicalize value of type [{type(obj)}]")

        return json.dumps(value, sort_keys=True, default=_default)

    @classmethod
    def _canonicalize_nested_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Returns a copy of |df| with every column holding nested values (lists,
        dicts, or the numpy arrays the emulator returns REPEATED columns as)
        rewritten to the canonical JSON string produced by
        _canonicalize_nested_value, leaving nulls and scalar columns untouched."""
        # A REPEATED column comes back from the emulator as a numpy.ndarray, not a
        # list; a scalar column's cells are numpy scalars (numpy.generic), which are
        # not ndarrays, so they are correctly left alone.
        nested_types = (list, dict, numpy.ndarray)
        df = df.copy()
        for column in df.columns:
            if any(isinstance(val, nested_types) for val in df[column]):
                df[column] = df[column].map(
                    lambda val: (
                        val
                        if not isinstance(val, nested_types)
                        else cls._canonicalize_nested_value(val)
                    )
                )
        return df

    @classmethod
    def compare_results_to_fixture(
        cls,
        results: pd.DataFrame,
        expected_output_fixture_path: str | Path,
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

        results = cls._canonicalize_nested_columns(results)

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
        results = cls._canonicalize_nested_columns(results)

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

        if python_type is float:
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
